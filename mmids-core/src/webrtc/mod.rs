use rtp::packet::Packet;
use tokio::sync::mpsc::UnboundedSender;
use crate::codecs::{AudioCodec, VideoCodec};
use crate::webrtc::h264_media_sender::H264MediaSender;
use crate::workflows::MediaNotificationContent;

pub mod h264_media_sender;
pub mod rtp_track_receiver;
pub mod utils;

/// Represents a type that can take data from rtp packets, depacketize them, and send them over
/// a tokio channel as a `MediaNotificationContent` message.
pub trait RtpToMediaContentSender {
    fn send_rtp_data(&mut self, packet: &Packet) -> Result<(), webrtc::error::Error>;
}

pub fn get_media_sender_for_video_codec(
    codec: VideoCodec,
    media_channel: UnboundedSender<MediaNotificationContent>,
) -> Option<Box<dyn RtpToMediaContentSender + Send + Sync>> {
    match codec {
        VideoCodec::H264 => Some(Box::new(H264MediaSender::new(media_channel))),
        VideoCodec::Unknown => None,
    }
}

pub fn get_media_sender_for_audio_codec(
    codec: AudioCodec,
    _media_channel: UnboundedSender<MediaNotificationContent>,
) -> Option<Box<dyn RtpToMediaContentSender + Send + Sync>> {
    match codec {
        AudioCodec::Aac => None,
        AudioCodec::Unknown => None,
    }
}
