use std::sync::Arc;
use anyhow::{anyhow, Result, Context};
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{info, instrument};
use webrtc::ice_transport::ice_connection_state::RTCIceConnectionState;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;
use mmids_core::codecs::{AudioCodec, VideoCodec};
use mmids_core::net::ConnectionId;
use mmids_core::reactors::ReactorWorkflowUpdate;
use mmids_core::StreamId;
use crate::endpoints::webrtc_server::{WebrtcServerWatcherRegistrantNotification, WebrtcStreamWatcherNotification};
use crate::utils::{create_webrtc_connection, get_audio_mime_type, get_video_mime_type};

pub struct WatcherConnectionHandlerParams {
    pub connection_id: ConnectionId,
    pub stream_name: String,
    pub audio_codec: Option<AudioCodec>,
    pub video_codec: Option<VideoCodec>,
    pub offer_sdp: String,
    pub registrant_channel: UnboundedSender<WebrtcServerWatcherRegistrantNotification>,
    pub watcher_channel: UnboundedSender<WebrtcStreamWatcherNotification>,
    pub reactor_update_channel: Option<UnboundedReceiver<ReactorWorkflowUpdate>>,
    pub stream_id: StreamId,
}

pub enum WatcherConnectionHandlerRequest {
    CloseConnection,
}

enum WebRtcNotification {
    ConnectionStateChanged(RTCIceConnectionState),
}

enum FutureResult {
    AllConsumersGone,
    RegistrantGone,
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
    registrant_channel: UnboundedSender<WebrtcServerWatcherRegistrantNotification>,
    watcher_channel: UnboundedSender<WebrtcStreamWatcherNotification>,
    terminate: bool,
    stream_id: StreamId,
}

impl WatcherConnectionHandler {
    fn new(
        receiver: UnboundedReceiver<WatcherConnectionHandlerRequest>,
        parameters: &WatcherConnectionHandlerParams,
    ) -> WatcherConnectionHandler {
        let futures = FuturesUnordered::new();
        futures.push(notify_on_request_received(receiver).boxed());
        futures.push(
            notify_on_registrant_gone(parameters.registrant_channel.clone()).boxed()
        );

        WatcherConnectionHandler {
            connection_id: parameters.connection_id.clone(),
            video_codec: parameters.video_codec,
            audio_codec: parameters.audio_codec,
            futures,
            registrant_channel: parameters.registrant_channel.clone(),
            watcher_channel: parameters.watcher_channel.clone(),
            terminate: false,
            stream_id: parameters.stream_id.clone(),
        }
    }

    #[instrument(name = "WebRTC Watcher Connection Handler Execution",
        skip(self, offer_sdp, reactor_update_channel),
        fields(connection_id = ?self.connection_id))]
    async fn run(
        mut self,
        stream_name: String,
        offer_sdp: String,
        reactor_update_channel: Option<UnboundedReceiver<ReactorWorkflowUpdate>>,
    ) {
        info!("Starting connection handler");


        info!("Stopping connection handler");
    }

    async fn create_connection(
        &mut self,
        stream_name: String,
        offer_sdp: String,
        reactor_update_channel: Option<UnboundedReceiver<ReactorWorkflowUpdate>>,
    ) -> Result<RTCPeerConnection> {
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

                webrtc_connection.add_track(video_track.clone()).await?;
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

                webrtc_connection.add_track(audio_track.clone()).await?;
                Some(audio_track)
            } else {
                return Err(anyhow!("No mime type for audio codec {:?}", audio_codec));
            }
        } else { None };

        Ok(webrtc_connection)
    }
}

async fn notify_on_request_received(
    mut receiver: UnboundedReceiver<WatcherConnectionHandlerRequest>,
) -> FutureResult {
    match receiver.recv().await {
        Some(request) => FutureResult::RequestReceived {request, receiver},
        None => FutureResult::AllConsumersGone,
    }
}

async fn notify_on_registrant_gone(
    sender: UnboundedSender<WebrtcServerWatcherRegistrantNotification>,
) -> FutureResult {
    sender.closed().await;

    FutureResult::RegistrantGone
}