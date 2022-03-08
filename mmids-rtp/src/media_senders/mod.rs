use crate::media_senders::h264_media_sender::H264MediaSender;
use mmids_core::codecs::{AudioCodec, VideoCodec};
use mmids_core::workflows::MediaNotificationContent;
use rtp::packet::Packet;
use tokio::sync::mpsc::UnboundedSender;

pub mod h264_media_sender;

/// Represents a type that can take data from rtp packets, depacketize them, and send them over
/// a tokio channel as a `MediaNotificationContent` message.
pub trait RtpToMediaContentSender {
    fn send_rtp_data(&mut self, packet: &Packet) -> Result<(), webrtc::error::Error>;
    fn get_name(&self) -> &str;
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
