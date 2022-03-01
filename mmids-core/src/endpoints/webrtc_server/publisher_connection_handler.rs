use std::sync::Arc;
use anyhow::{anyhow, Result, Context};
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;
use tracing::{error, info, instrument};
use webrtc::media::track::setting::audio::Audio;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp_transceiver::rtp_codec::RTPCodecType;
use webrtc::track::track_remote::TrackRemote;
use crate::codecs::{AudioCodec, VideoCodec};
use crate::endpoints::webrtc_server::{WebrtcServerPublisherRegistrantNotification, WebrtcStreamPublisherNotification};
use crate::net::ConnectionId;
use crate::webrtc_utils::create_webrtc_connection;

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

enum FutureResult {
    AllConsumersGone,
    PublisherGone,
    RegistrantGone,

    RequestReceived {
        request: PublisherConnectionHandlerRequest,
        receiver: UnboundedReceiver<PublisherConnectionHandlerRequest>,
    },
}

struct PublisherConnectionHandler {
    connection_id: ConnectionId,
    audio_codec: Option<AudioCodec>,
    video_codec: Option<VideoCodec>,
    futures: FuturesUnordered<BoxFuture<'static, FutureResult>>,
    registrant_notification_channel: UnboundedSender<WebrtcServerPublisherRegistrantNotification>,
    publisher_notification_channel: UnboundedSender<WebrtcStreamPublisherNotification>,
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
        }
    }

    #[instrument(name = "WebRTC Publisher Connection Handler Execution",
        skip(self, offer_sdp),
        fields(connection_id = ?self.connection_id))]
    async fn run(mut self, offer_sdp: String) {
        info!("Starting publisher connection handler");

        let peer_connection = self.create_connection(offer_sdp).await;

        info!("Stopping publisher connection handler");
    }

    async fn create_connection(&mut self, offer_sdp: String) -> Result<RTCPeerConnection> {
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




        Ok(webrtc_connection)
    }
}

async fn handle__track(
    track: Arc<TrackRemote>,
    video_codec: Option<VideoCodec>,
    audio_codec: Option<AudioCodec>,
    registrant_channel: UnboundedSender<WebrtcServerPublisherRegistrantNotification>,
    connection_id: ConnectionId,
) {
    let track_codec = track.codec().await;
    let mime_type = track_codec.capability.mime_type.to_lowercase();

    enum CodecType { Video, Audio }
    let codec_type = if let Some(video_codec) = video_codec {
        if video_codec.to_mime_type() == Some(mime_type) {
            Some(CodecType::Video)
        } else {
            None
        }
    } else if let Some(audio_codec) = audio_codec {
        if audio_codec.to_mime_type() == Some(mime_type) {
            Some(CodecType::Audio)
        } else {
            None
        }
    } else {
        None
    };

    match codec_type {
        Some(CodecType::Video) => {

        }

        Some(CodecType::Audio) => {

        }

        None => {
            error!(
                connection_id = ?connection_id
                "Received track with mime type of '{}' which could not be matched to the \
                designated audio or video codec", mime_type
            );
        }
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

#[instrument(skip(track, cancellation_token, registrant_channel))]
async fn read_video_track(
    track: Arc<TrackRemote>,
    connection_id: ConnectionId,
    cancellation_token: watch::Receiver<bool>,
    registrant_channel: UnboundedSender<WebrtcServerPublisherRegistrantNotification>,
) {
    loop {
        tokio::select! {
            result = track.read_rtp() => {
                match result {
                    Ok((rtp_packet, _)) => {
                        // Need to switch on video codec, and for h264 call
                        // webrtc::rtp::codecs::h264::H264Packet.depacketize(rtp_packet.payload).
                        // Might need to do more too.  See https://github.com/webrtc-rs/media/blob/main/src/io/h264_writer/mod.rs
                        // and https://github.com/webrtc-rs/rtp/blob/main/src/codecs/h264/mod.rs
                        adfadfadf

                    }

                    Err(error) => {
                        error!("Error reading rtp packet: {:?}", error);
                        break;
                    }
                }
            }

            should_cancel = cancellation_token.changed() => {
                if should_cancel {
                    info!("Cancellation requested, no longer reading track");
                    break;
                }
            }
        }
    }
}
