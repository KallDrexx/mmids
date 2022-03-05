use std::sync::Arc;
use anyhow::{anyhow, Result, Context};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info, instrument};
use webrtc::ice_transport::ice_connection_state::RTCIceConnectionState;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::rtp_transceiver::rtp_sender::RTCRtpSender;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;
use webrtc_media::Sample;
use mmids_core::codecs::{AudioCodec, VideoCodec};
use mmids_core::net::ConnectionId;
use mmids_core::workflows::MediaNotificationContent;
use crate::endpoints::webrtc_server::{WebrtcStreamWatcherNotification};
use crate::utils::{create_webrtc_connection, get_audio_mime_type, get_video_mime_type, get_webrtc_connection_answer};

pub struct WatcherConnectionHandlerParams {
    pub connection_id: ConnectionId,
    pub stream_name: String,
    pub audio_codec: Option<AudioCodec>,
    pub video_codec: Option<VideoCodec>,
    pub offer_sdp: String,
    pub watcher_channel: UnboundedSender<WebrtcStreamWatcherNotification>,
}

pub enum WatcherConnectionHandlerRequest {
    CloseConnection,
    SendMedia(MediaNotificationContent),
}

pub fn start_watcher_connection(
    parameters: WatcherConnectionHandlerParams,
) -> UnboundedSender<WatcherConnectionHandlerRequest> {
    let (sender, receiver) = unbounded_channel();
    let actor = WatcherConnectionHandler::new(receiver, &parameters);
    tokio::spawn(actor.run(parameters.offer_sdp));

    sender
}

enum WebRtcNotification {
    IceConnectionStateChanged(RTCIceConnectionState),
    PeerConnectionStateChanged(RTCPeerConnectionState),
}

enum FutureResult {
    AllConsumersGone,
    WebRtcNotificationSendersGone,

    RequestReceived {
        request: WatcherConnectionHandlerRequest,
        receiver: UnboundedReceiver<WatcherConnectionHandlerRequest>,
    },

    WebRtcNotificationReceived {
        notification: WebRtcNotification,
        receiver: UnboundedReceiver<WebRtcNotification>,
    },
}

struct WatcherConnectionHandler {
    connection_id: ConnectionId,
    audio_codec: Option<AudioCodec>,
    video_codec: Option<VideoCodec>,
    futures: FuturesUnordered<BoxFuture<'static, FutureResult>>,
    watcher_channel: UnboundedSender<WebrtcStreamWatcherNotification>,
    terminate: bool,
}

struct Connection {
    _peer_connection: RTCPeerConnection,
    audio_track: Option<Arc<TrackLocalStaticSample>>,
    video_track: Option<Arc<TrackLocalStaticSample>>,
}

impl WatcherConnectionHandler {
    fn new(
        receiver: UnboundedReceiver<WatcherConnectionHandlerRequest>,
        parameters: &WatcherConnectionHandlerParams,
    ) -> WatcherConnectionHandler {
        let futures = FuturesUnordered::new();
        futures.push(notify_on_request_received(receiver).boxed());

        WatcherConnectionHandler {
            connection_id: parameters.connection_id.clone(),
            video_codec: parameters.video_codec,
            audio_codec: parameters.audio_codec,
            futures,
            watcher_channel: parameters.watcher_channel.clone(),
            terminate: false,
        }
    }

    #[instrument(name = "WebRTC Watcher Connection Handler Execution",
        skip(self, offer_sdp),
        fields(connection_id = ?self.connection_id))]
    async fn run(
        mut self,
        offer_sdp: String,
    ) {
        info!("Starting connection handler");

        let connection = match self.create_connection(offer_sdp).await {
            Ok(connection) => connection,
            Err(error) => {
                error!("Failed to create webrtc connection: {:?}", error);
                return;
            }
        };

        while let Some(future_result) = self.futures.next().await {
            match future_result {
                FutureResult::AllConsumersGone => {
                    info!("All consumers gone");
                    self.terminate = true;
                }

                FutureResult::WebRtcNotificationSendersGone => {
                    info!("All webrtc notification senders are gone");
                    self.terminate = true;
                }

                FutureResult::RequestReceived {request, receiver} => {
                    self.futures
                        .push(notify_on_request_received(receiver).boxed());

                    self.handle_request(request, &connection).await;
                }

                FutureResult::WebRtcNotificationReceived {notification, receiver} => {
                    self.futures
                        .push(notify_on_webrtc_notification(receiver).boxed());

                    self.handle_webrtc_notification(notification);
                }
            }

            if self.terminate {
                break;
            }
        }

        info!("Stopping connection handler");
    }

    async fn handle_request(
        &mut self,
        request: WatcherConnectionHandlerRequest,
        connection: &Connection,
    ) {
        match request {
            WatcherConnectionHandlerRequest::CloseConnection => {
                info!("Close connection requested");
                self.terminate = true;
            }

            WatcherConnectionHandlerRequest::SendMedia(media) => {
                match media {
                    // It's not clear how to set timestamps, or if any data transformation
                    // has to be done, especially for sequence headers
                    MediaNotificationContent::Video {data, ..} => {
                        if let Some(track) = &connection.video_track {
                            let _ = track.write_sample(&Sample {
                                data,
                                ..Default::default()
                            }).await;
                        }
                    }

                    MediaNotificationContent::Audio {data, ..} => {
                        if let Some(track) = &connection.audio_track {
                            let _ = track.write_sample(&Sample {
                                data,
                                ..Default::default()
                            }).await;
                        }
                    }

                    _ => (),
                }
            }
        }
    }

    fn handle_webrtc_notification(&mut self, notification: WebRtcNotification) {
        match notification {
            WebRtcNotification::IceConnectionStateChanged(state) => {
                info!("ICE state changed to {:?}", state);

                if state == RTCIceConnectionState::Failed {
                    self.terminate = true;
                }
            }

            WebRtcNotification::PeerConnectionStateChanged(state) => {
                info!("WebRTC Peer state changed to {:?}", state);

                if state == RTCPeerConnectionState::Failed {
                    // Don't terminate on disconnected, as a reconnection may occur.  WebRTC-rs will
                    // enter fail if it goes 30 seconds without network activity
                    self.terminate = true;
                }
            }
        }
    }

    async fn create_connection(&mut self, offer_sdp: String) -> Result<Connection> {
        let webrtc_connection = create_webrtc_connection(self.audio_codec, self.video_codec).await
            .with_context(|| "Creation of RTCPeerConnection failed")?;

        // Create audio and video tracks
        let video_track = if let Some(video_codec) = self.video_codec {
            if let Some(mime_type) = get_video_mime_type(video_codec) {
                let video_track = Arc::new(TrackLocalStaticSample::new(
                    RTCRtpCodecCapability { mime_type, ..Default::default() },
                    "video".to_string(),
                    format!("{}-video", self.connection_id.0),
                ));

                let rtp_sender = webrtc_connection.add_track(video_track.clone()).await
                    .with_context(|| "Failed to add video track")?;

                read_track_rtcp_packets(rtp_sender);

                Some(video_track)
            } else {
                return Err(anyhow!("No mime type for video codec {:?}", video_codec));
            }
        } else { None };

        let audio_track = if let Some(audio_codec) = self.audio_codec {
            if let Some(mime_type) = get_audio_mime_type(audio_codec) {
                let audio_track = Arc::new(TrackLocalStaticSample::new(
                    RTCRtpCodecCapability { mime_type, ..Default::default() },
                    "audio".to_string(),
                    format!("{}-audio", self.connection_id.0),
                ));

                let rtp_sender = webrtc_connection.add_track(audio_track.clone()).await
                    .with_context(|| "Failed to add audio track")?;

                read_track_rtcp_packets(rtp_sender);

                Some(audio_track)
            } else {
                return Err(anyhow!("No mime type for audio codec {:?}", audio_codec));
            }
        } else { None };

        let (notification_sender, notification_receiver) = unbounded_channel();
        self.futures
            .push(notify_on_webrtc_notification(notification_receiver).boxed());

        {
            let notification_sender = notification_sender.clone();
            webrtc_connection.on_ice_connection_state_change(
                Box::new(move |state: RTCIceConnectionState| {
                    let _ = notification_sender
                        .send(WebRtcNotification::IceConnectionStateChanged(state));

                    Box::pin(async {})
                })
            ).await;
        }

        webrtc_connection.on_peer_connection_state_change(
            Box::new(move |state: RTCPeerConnectionState| {
                let _ = notification_sender
                    .send(WebRtcNotification::PeerConnectionStateChanged(state));

                Box::pin(async {})
            })
        ).await;

        let local_description = get_webrtc_connection_answer(&webrtc_connection, offer_sdp).await?;

        // We are now fully ready to accept the watcher
        let _ = self.watcher_channel
            .send(WebrtcStreamWatcherNotification::WatchRequestAccepted {
                answer_sdp: local_description.sdp
            });

        Ok(Connection {
            _peer_connection: webrtc_connection,
            video_track,
            audio_track,
        })
    }
}

fn read_track_rtcp_packets(rtp_sender: Arc<RTCRtpSender>) {
    // Read RTCP packets.  I'm not 100% why this is necessary but I assume based on examples if
    // we never call read then the underlying buffer will fill up?  From the examples it sounds like
    // this is needed to pull the packets through the interceptors for things like NACK.
    tokio::spawn(async move {
        let mut rtcp_buf = vec![0u8; 1500];
        while let Ok((_, _)) = rtp_sender.read(&mut rtcp_buf).await {}
        Result::<()>::Ok(())
    });
}

async fn notify_on_request_received(
    mut receiver: UnboundedReceiver<WatcherConnectionHandlerRequest>,
) -> FutureResult {
    match receiver.recv().await {
        Some(request) => FutureResult::RequestReceived {request, receiver},
        None => FutureResult::AllConsumersGone,
    }
}

async fn notify_on_webrtc_notification(
    mut receiver: UnboundedReceiver<WebRtcNotification>,
) -> FutureResult {
    match receiver.recv().await {
        Some(notification) => FutureResult::WebRtcNotificationReceived {notification, receiver},
        None => FutureResult::WebRtcNotificationSendersGone,
    }
}
