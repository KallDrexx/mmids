use std::sync::Arc;
use anyhow::{Result, Context};
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;
use tracing::{error, info, instrument, warn};
use webrtc::ice_transport::ice_connection_state::RTCIceConnectionState;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp_transceiver::rtp_codec::RTPCodecType;
use webrtc::rtp_transceiver::rtp_receiver::RTCRtpReceiver;
use webrtc::track::track_remote::TrackRemote;
use crate::codecs::{AudioCodec, VideoCodec};
use crate::endpoints::webrtc_server::{WebrtcServerPublisherRegistrantNotification, WebrtcStreamPublisherNotification};
use crate::net::ConnectionId;
use crate::webrtc::{get_media_sender_for_audio_codec, get_media_sender_for_video_codec};
use crate::webrtc::rtp_track_receiver::receive_rtp_track_media;
use crate::webrtc::utils::{create_webrtc_connection, offer_to_sdp_struct};
use crate::workflows::MediaNotificationContent;

pub struct PublisherConnectionHandlerParams {
    pub connection_id: ConnectionId,
    pub audio_codec: Option<AudioCodec>,
    pub video_codec: Option<VideoCodec>,
    pub offer_sdp: String,
    pub registrant_notification_channel: UnboundedSender<WebrtcServerPublisherRegistrantNotification>,
    pub publisher_notification_channel: UnboundedSender<WebrtcStreamPublisherNotification>,
}

pub enum PublisherConnectionHandlerRequest {
    CloseConnection,
}

pub fn start_publisher_connection(
    parameters: PublisherConnectionHandlerParams
) -> UnboundedSender<PublisherConnectionHandlerRequest> {
    let (sender, receiver) = unbounded_channel();
    let actor = PublisherConnectionHandler::new(receiver, &parameters);
    tokio::spawn(actor.run(parameters.offer_sdp));

    sender
}

enum WebRtcNotification {
    ConnectionStateChanged(RTCIceConnectionState),
}

enum FutureResult {
    AllConsumersGone,
    PublisherGone,
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
}

struct PublisherConnectionHandler {
    connection_id: ConnectionId,
    audio_codec: Option<AudioCodec>,
    video_codec: Option<VideoCodec>,
    futures: FuturesUnordered<BoxFuture<'static, FutureResult>>,
    registrant_notification_channel: UnboundedSender<WebrtcServerPublisherRegistrantNotification>,
    publisher_notification_channel: UnboundedSender<WebrtcStreamPublisherNotification>,
    cancellation_token_sender: Option<watch::Sender<bool>>,
}

impl PublisherConnectionHandler {
    fn new(
        receiver: UnboundedReceiver<PublisherConnectionHandlerRequest>,
        parameters: &PublisherConnectionHandlerParams,
    ) -> PublisherConnectionHandler {
        let futures = FuturesUnordered::new();
        futures.push(notify_on_request_received(receiver).boxed());
        futures.push(
            notify_on_publisher_gone(parameters.publisher_notification_channel.clone())
                .boxed()
        );

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
        }
    }

    #[instrument(name = "WebRTC Publisher Connection Handler Execution",
        skip(self, offer_sdp),
        fields(connection_id = ?self.connection_id))]
    async fn run(mut self, offer_sdp: String) {
        info!("Starting publisher connection handler");

        let peer_connection = match self.create_connection(offer_sdp).await {
            Ok(connection) => connection,
            Err(error) => {
                error!("Failed to create peer connection: {:?}", error);
                return;
            }
        };





        info!("Stopping publisher connection handler");
    }

    async fn create_connection(&mut self, offer_sdp: String) -> Result<RTCPeerConnection> {
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
        let video_codec = self.video_codec.clone();
        let audio_codec = self.audio_codec.clone();
        let connection_id = self.connection_id.clone();
        webrtc_connection.on_track(
            Box::new(move |track: Option<Arc<TrackRemote>>, _receiver: Option<Arc<RTCRtpReceiver>>| {
                if let Some(track) = track {
                    Box::pin(handle_new_track(
                        track,
                        video_codec,
                        audio_codec,
                        media_sender.clone(),
                        connection_id.clone(),
                        cancel_receiver.clone(),
                    ))
                } else {
                    Box::pin(async {})
                }
            })
        ).await;

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

        webrtc_connection.set_local_description(answer).await
            .with_context(|| "Failed to set local description")?;


        Ok(webrtc_connection)
    }
}

async fn handle_new_track(
    track: Arc<TrackRemote>,
    video_codec: Option<VideoCodec>,
    audio_codec: Option<AudioCodec>,
    media_channel: UnboundedSender<MediaNotificationContent>,
    connection_id: ConnectionId,
    cancellation_token: watch::Receiver<bool>,
) {
    let track_codec = track.codec().await;
    let mime_type = track_codec.capability.mime_type.to_lowercase();

    info!(
        connection_id = ?connection_id,
        "New RTP track started with mime type '{}'", mime_type
    );

    let mut media_sender = None;
    if let Some(video_codec) = video_codec {
        if video_codec.to_mime_type() == Some(mime_type.clone()) {
            media_sender = get_media_sender_for_video_codec(video_codec, media_channel.clone());
        }
    }

    if media_sender.is_none() {
        if let Some(audio_codec) = audio_codec {
            if audio_codec.to_mime_type() == Some(mime_type.clone()) {
                media_sender = get_media_sender_for_audio_codec(audio_codec, media_channel.clone())
            }
        }
    }

    if let Some(media_sender) = media_sender {
        tokio::spawn(receive_rtp_track_media(
            track,
            connection_id,
            cancellation_token,
            media_sender,
        ));
    } else {
        warn!(
            connection_id = ?connection_id,
            "Either the mime type of '{}' didn't match the audio or video codecs specified ({:?}, \
            {:?}), or the video codecs do not have a defined media sender implementation.  Track is \
            being abandoned.",
            mime_type, audio_codec, video_codec,
        );

        return;
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

async fn notify_on_publisher_gone(
    sender: UnboundedSender<WebrtcStreamPublisherNotification>,
) -> FutureResult {
    sender.closed().await;

    FutureResult::PublisherGone
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

