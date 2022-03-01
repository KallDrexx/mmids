use anyhow::{Result, Context, anyhow};
use tracing::error;
use webrtc::api::APIBuilder;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_H264};
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp_transceiver::rtp_codec::{RTCRtpCodecCapability, RTCRtpCodecParameters, RTPCodecType};
use crate::codecs::{AudioCodec, VideoCodec};
use crate::endpoints::webrtc_server::WebrtcStreamPublisherNotification;

pub async fn create_webrtc_connection(
    audio_codec: Option<AudioCodec>,
    video_codec: Option<VideoCodec>,
) -> Result<RTCPeerConnection> {
    let mut media_engine = MediaEngine::default();
    if let Some(video_codec) = video_codec {
        register_video_codec_to_media_engine(&mut media_engine, video_codec)?;
    }

    if let Some(audio_codec) = audio_codec {
        register_audio_codec_to_media_engine(&mut media_engine, audio_codec)?;
    }

    let mut registry = Registry::new();
    let registry = register_default_interceptors(registry, &mut media_engine)
        .with_context(|| "Failed to register default webrtc interceptors")?;

    let api = APIBuilder::new()
        .with_media_engine(media_engine)
        .with_interceptor_registry(registry)
        .build();

    let config = RTCConfiguration {
        ice_servers: vec![RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_owned()],
            ..Default::default()
        }],
        ..Default::default()
    };

    let peer_connection = api.new_peer_connection(config).await?;

    Ok(peer_connection)
}

fn register_video_codec_to_media_engine(
    media_engine: &mut MediaEngine,
    codec: VideoCodec,
) -> Result<()> {
    match codec {
        VideoCodec::H264 => {
            media_engine.register_codec(
                RTCRtpCodecParameters {
                    capability: RTCRtpCodecCapability {
                        mime_type: MIME_TYPE_H264.to_owned(),
                        clock_rate: 90000,
                        channels: 0,
                        sdp_fmtp_line: "".to_owned(),
                        rtcp_feedback: vec![],
                    },
                    payload_type: 102,
                    ..Default::default()
                },
                RTPCodecType::Video,
            )
                .with_context(|| "Failed to add h264 to the WebRTC media engine")
        }

        VideoCodec::Unknown => {

            Err(anyhow!("Publisher registrant registered with unknown video codec, and thus we \
                cannot initialize WebRTC")
            )
        }
    }
}

fn register_audio_codec_to_media_engine(
    _media_engine: &mut MediaEngine,
    codec: AudioCodec,
) -> Result<()> {
    match codec {
        AudioCodec::Aac => {
            Err(anyhow!(
                    "Publisher registrant registered with the AAC audio codec, which isn't \
                    available for WebRTC!"
            ))
        }

        AudioCodec::Unknown => {
            Err(anyhow!(
                "Publisher registrant registered with unknown video codec, and thus we \
                cannot initialize WebRTC"
            ))
        }
    }
}
