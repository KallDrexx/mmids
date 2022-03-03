use std::sync::Arc;
use std::time::Duration;
use anyhow::{anyhow, Result, Context};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;
use webrtc::ice_transport::ice_connection_state::RTCIceConnectionState;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::sdp::sdp_type::RTCSdpType;
use webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use webrtc::rtp_transceiver::rtp_codec::{RTCRtpCodecParameters, RTPCodecType};
use webrtc::rtp_transceiver::rtp_receiver::RTCRtpReceiver;
use webrtc::rtp_transceiver::SSRC;
use webrtc::track::track_remote::TrackRemote;
use mmids_core::codecs::{AudioCodec, VideoCodec};
use mmids_core::net::ConnectionId;
use mmids_core::reactors::ReactorWorkflowUpdate;
use mmids_core::StreamId;
use mmids_core::workflows::MediaNotificationContent;
use crate::media_senders::{get_media_sender_for_audio_codec, get_media_sender_for_video_codec};
use crate::rtp_track_receiver::receive_rtp_track_media;
use crate::utils::{create_webrtc_connection, get_audio_mime_type, get_video_mime_type, offer_to_sdp_struct};
use crate::endpoints::webrtc_server::{WebrtcServerPublisherRegistrantNotification, WebrtcStreamPublisherNotification};

pub struct PublisherConnectionHandlerParams {
    pub connection_id: ConnectionId,
    pub stream_name: String,
    pub audio_codec: Option<AudioCodec>,
    pub video_codec: Option<VideoCodec>,
    pub offer_sdp: String,
    pub registrant_notification_channel: UnboundedSender<WebrtcServerPublisherRegistrantNotification>,
    pub publisher_notification_channel: UnboundedSender<WebrtcStreamPublisherNotification>,
    pub reactor_update_channel: Option<UnboundedReceiver<ReactorWorkflowUpdate>>,
}

pub enum PublisherConnectionHandlerRequest {
    CloseConnection,
}

pub fn start_publisher_connection(
    parameters: PublisherConnectionHandlerParams
) -> UnboundedSender<PublisherConnectionHandlerRequest> {
    let (sender, receiver) = unbounded_channel();
    let actor = PublisherConnectionHandler::new(receiver, &parameters);
    tokio::spawn(actor.run(parameters.stream_name, parameters.offer_sdp, parameters.reactor_update_channel));

    sender
}

enum WebRtcNotification {
    ConnectionStateChanged(RTCIceConnectionState),
    NewTrackStarted {
        track: Arc<TrackRemote>,
        track_codec: RTCRtpCodecParameters,
        media_channel: UnboundedSender<MediaNotificationContent>,
        cancellation_token: watch::Receiver<bool>,
    }
}

enum FutureResult {
    AllConsumersGone,
    RegistrantGone,
    WebRtcNotificationSendersGone,

    RequestReceived {
        request: PublisherConnectionHandlerRequest,
        receiver: UnboundedReceiver<PublisherConnectionHandlerRequest>,
    },

    WebRtcNotificationReceived {
        notification: WebRtcNotification,
        receiver: UnboundedReceiver<WebRtcNotification>,
    },

    PictureLossIndicatorRequested(SSRC),
}

struct PublisherConnectionHandler {
    connection_id: ConnectionId,
    audio_codec: Option<AudioCodec>,
    video_codec: Option<VideoCodec>,
    futures: FuturesUnordered<BoxFuture<'static, FutureResult>>,
    registrant_notification_channel: UnboundedSender<WebrtcServerPublisherRegistrantNotification>,
    publisher_notification_channel: UnboundedSender<WebrtcStreamPublisherNotification>,
    cancellation_token_sender: Option<watch::Sender<bool>>,
    terminate: bool,
}

impl PublisherConnectionHandler {
    fn new(
        receiver: UnboundedReceiver<PublisherConnectionHandlerRequest>,
        parameters: &PublisherConnectionHandlerParams,
    ) -> PublisherConnectionHandler {
        let futures = FuturesUnordered::new();
        futures.push(notify_on_request_received(receiver).boxed());
        futures.push(
            notify_on_registrant_gone(parameters.registrant_notification_channel.clone())
                .boxed()
        );

        PublisherConnectionHandler {
            connection_id: parameters.connection_id.clone(),
            video_codec: parameters.video_codec,
            audio_codec: parameters.audio_codec,
            futures,
            registrant_notification_channel: parameters.registrant_notification_channel.clone(),
            publisher_notification_channel: parameters.publisher_notification_channel.clone(),
            cancellation_token_sender: None,
            terminate: false,
        }
    }

    #[instrument(name = "WebRTC Publisher Connection Handler Execution",
        skip(self, offer_sdp, reactor_update_channel),
        fields(connection_id = ?self.connection_id))]
    async fn run(
        mut self,
        stream_name: String,
        offer_sdp: String,
        reactor_update_channel: Option<UnboundedReceiver<ReactorWorkflowUpdate>>,
    ) {
        info!("Starting publisher connection handler");

        let peer_connection = match self.create_connection(
            stream_name,
            offer_sdp,
            reactor_update_channel,
        ).await {
            Ok(connection) => connection,
            Err(error) => {
                error!("Failed to create peer connection: {:?}", error);
                return;
            }
        };

        let peer_connection = Arc::new(peer_connection);

        while let Some(future_result) = self.futures.next().await {
            match future_result {
                FutureResult::RegistrantGone => {
                    info!("Registrant gone");
                    self.terminate = true;
                }

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

                    self.handle_request(request);
                }

                FutureResult::WebRtcNotificationReceived {notification, receiver} => {
                    self.futures
                        .push(notify_on_webrtc_notification(receiver).boxed());

                    self.handle_webrtc_notification(notification);
                }

                FutureResult::PictureLossIndicatorRequested(ssrc) => {
                    self.send_pls(ssrc, peer_connection.clone());
                }
            }

            if self.terminate {
                break;
            }
        }

        let _ = peer_connection.close().await;
        info!("Stopping publisher connection handler");
    }

    fn handle_request(&mut self, request: PublisherConnectionHandlerRequest) {
        match request {
            PublisherConnectionHandlerRequest::CloseConnection => {
                self.terminate = true;
            }
        }
    }

    fn handle_webrtc_notification(&mut self, notification: WebRtcNotification) {
        match notification {
            WebRtcNotification::ConnectionStateChanged(state) => {
                info!("Connection entered state {}", state);

                if let RTCIceConnectionState::Failed = state {
                    self.terminate = true;
                }
            }

            WebRtcNotification::NewTrackStarted {
                track,
                media_channel,
                cancellation_token,
                track_codec,
            } => {
                self.handle_new_track(track, track_codec, media_channel, cancellation_token);
            }
        }
    }

    async fn create_connection(
        &mut self,
        stream_name: String,
        offer_sdp: String,
        reactor_update_channel: Option<UnboundedReceiver<ReactorWorkflowUpdate>>,
    ) -> Result<RTCPeerConnection> {
        let (cancel_sender, cancel_receiver) = watch::channel(false);
        self.cancellation_token_sender = Some(cancel_sender);

        let (notification_sender, notification_receiver) = unbounded_channel();
        self.futures
            .push(notify_on_webrtc_notification(notification_receiver).boxed());

        let webrtc_connection = create_webrtc_connection(self.audio_codec, self.video_codec).await
            .with_context(|| "Creation of RTCPeerConnection failed")?;

        if let Some(_) = self.video_codec {
            webrtc_connection.add_transceiver_from_kind(RTPCodecType::Video, &[]).await
                .with_context(|| "Adding video transceiver failed")?;
        }

        if let Some(_) = self.audio_codec {
            webrtc_connection.add_transceiver_from_kind(RTPCodecType::Audio, &[]).await
                .with_context(|| "Adding audio webrtc transceiver failed")?;
        }

        let (media_sender, media_receiver) = unbounded_channel();
        {
            let notification_sender = notification_sender.clone();
            webrtc_connection.on_track(
                Box::new(move |track: Option<Arc<TrackRemote>>, _receiver: Option<Arc<RTCRtpReceiver>>| {
                    let notification_sender = notification_sender.clone();
                    let cancel_receiver = cancel_receiver.clone();
                    let media_sender = media_sender.clone();
                    tokio::spawn(async move {
                        if let Some(track) = track {
                            let track_codec = track.codec().await;
                            let _ = notification_sender.send(WebRtcNotification::NewTrackStarted {
                                track,
                                track_codec,
                                cancellation_token: cancel_receiver.clone(),
                                media_channel: media_sender.clone(),
                            });
                        }
                    });

                    Box::pin(async {})
                })
            ).await;
        }

        webrtc_connection.on_ice_connection_state_change(
            Box::new(move |state: RTCIceConnectionState| {
                let _ = notification_sender
                    .send(WebRtcNotification::ConnectionStateChanged(state));

                Box::pin(async {})
            })
        ).await;

        let offer = offer_to_sdp_struct(offer_sdp)?;
        webrtc_connection.set_remote_description(offer).await
            .with_context(|| "Could not set connection from offer")?;

        let answer = webrtc_connection.create_answer(None).await
            .with_context(|| "Failed to create answer")?;

        let mut ice_channel = webrtc_connection.gathering_complete_promise().await;
        webrtc_connection.set_local_description(answer).await
            .with_context(|| "Failed to set local description")?;

        // Wait until we've gotten the ice candidate.
        let _ = ice_channel.recv().await;

        if let Some(local_description) = webrtc_connection.local_description().await {
            if local_description.sdp_type != RTCSdpType::Answer {
                return Err(anyhow!(
                    "WebRTC local description was {} instead of an answer", local_description.sdp_type
                ));
            }

            // If we got here then we should be ready to accept the publisher
            let _ = self.registrant_notification_channel
                .send(WebrtcServerPublisherRegistrantNotification::NewPublisherConnected {
                    connection_id: self.connection_id.clone(),
                    stream_name,
                    stream_id: StreamId(Uuid::new_v4().to_string()),
                    media_channel: media_receiver,
                    reactor_update_channel,
                });

            let _ = self.publisher_notification_channel
                .send(WebrtcStreamPublisherNotification::PublishRequestAccepted {
                    answer_sdp: local_description.sdp,
                });

            Ok(webrtc_connection)
        } else {
            Err(anyhow!("WebRTC connection did not have a local description"))
        }
    }

    fn handle_new_track(
        &mut self,
        track: Arc<TrackRemote>,
        track_codec: RTCRtpCodecParameters,
        media_channel: UnboundedSender<MediaNotificationContent>,
        cancellation_token: watch::Receiver<bool>,
    ) {
        let mime_type = track_codec.capability.mime_type.to_lowercase();

        info!("New RTP track started with mime type '{}'", mime_type);

        let mut media_sender = None;
        if let Some(video_codec) = self.video_codec {
            if get_video_mime_type(video_codec) == Some(mime_type.clone()) {
                media_sender = get_media_sender_for_video_codec(video_codec, media_channel.clone());
            }
        }

        if media_sender.is_none() {
            if let Some(audio_codec) = self.audio_codec {
                if get_audio_mime_type(audio_codec)  == Some(mime_type.clone()) {
                    media_sender = get_media_sender_for_audio_codec(audio_codec, media_channel.clone())
                }
            }
        }

        if let Some(media_sender) = media_sender {
            self.futures.push(send_pli_after_delay(track.ssrc()).boxed());

            tokio::spawn(receive_rtp_track_media(
                track,
                self.connection_id.clone(),
                cancellation_token,
                media_sender,
            ));
        } else {
            warn!(
                "Either the mime type of '{}' didn't match the audio or video codecs specified ({:?}, \
                {:?}), or the video codecs do not have a defined media sender implementation.  Track is \
                being abandoned.",
                mime_type, self.audio_codec, self.video_codec,
            );

            return;
        }
    }

    fn send_pls(&self, ssrc: SSRC, connection: Arc<RTCPeerConnection>) {
        // send a packet loss indicator to encourage a new keyframe to be sent.  We want to try and
        // get a keyframe relatively often for live streams, so late joiners can see video without
        // too much of a delay.
        tokio::spawn(async move {
            let _ = connection.write_rtcp(&[
                Box::new(PictureLossIndication {
                    sender_ssrc: 0,
                    media_ssrc: ssrc,
                })
            ]).await;
        });

        self.futures
            .push(send_pli_after_delay(ssrc).boxed());
    }
}

async fn notify_on_request_received(
    mut receiver: UnboundedReceiver<PublisherConnectionHandlerRequest>,
) -> FutureResult {
    match receiver.recv().await {
        Some(request) => FutureResult::RequestReceived {
            request,
            receiver,
        },

        None => FutureResult::AllConsumersGone,
    }
}

async fn notify_on_registrant_gone(
    sender: UnboundedSender<WebrtcServerPublisherRegistrantNotification>,
) -> FutureResult {
    sender.closed().await;

    FutureResult::RegistrantGone
}

async fn notify_on_webrtc_notification(
    mut receiver: UnboundedReceiver<WebRtcNotification>,
) -> FutureResult {
    match receiver.recv().await {
        Some(notification) => FutureResult::WebRtcNotificationReceived {
            notification,
            receiver,
        },

        None => FutureResult::WebRtcNotificationSendersGone,
    }
}

async fn send_pli_after_delay(ssrc: SSRC) -> FutureResult {
    tokio::time::sleep(Duration::from_secs(3)).await;

    FutureResult::PictureLossIndicatorRequested(ssrc)
}
