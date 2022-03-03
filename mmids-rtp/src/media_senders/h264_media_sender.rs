use rtp::codecs::h264::H264Packet;
use rtp::packet::Packet;
use rtp::packetizer::Depacketizer;
use tokio::sync::mpsc::UnboundedSender;
use tracing::debug;
use mmids_core::codecs::VideoCodec;
use mmids_core::VideoTimestamp;
use mmids_core::workflows::MediaNotificationContent;
use crate::media_senders::RtpToMediaContentSender;
use crate::utils::video_timestamp_from_rtp_packet;

const NALU_TTYPE_STAP_A: u32 = 24;
const NALU_TTYPE_SPS: u32 = 7;
const NALU_TYPE_BITMASK: u32 = 0x1F;

/// Takes h264 based RTP packets and sends them over to the specified tokio channel.  This is
/// adapted from the `webrtc-media` `H264Writer` struct, which does everything we need, but is
/// meant for destinations that are Write + Seek, which unbounded channels aren't. There's
/// too much ambiguity to try and create a Write + Seek implementation.
pub struct H264MediaSender {
    media_channel: UnboundedSender<MediaNotificationContent>,
    cached_packet: H264Packet,
    has_sent_keyframe: bool, // TODO: probably need has sent sps too??
}

impl H264MediaSender {
    pub fn new(media_channel: UnboundedSender<MediaNotificationContent>) -> H264MediaSender {
        H264MediaSender {
            media_channel,
            cached_packet: H264Packet::default(),
            has_sent_keyframe: false,
        }
    }
}

unsafe impl Send for H264MediaSender{ }

impl RtpToMediaContentSender for H264MediaSender {
    fn send_rtp_data(&mut self, packet: &Packet) -> Result<(), webrtc::error::Error> {
        if packet.payload.is_empty() {
            return Ok(());
        }

        let is_key_frame = is_key_frame(&packet.payload);
        if !self.has_sent_keyframe && !is_key_frame {
            return Ok(());
        }

        if is_key_frame {
            self.has_sent_keyframe = true;
        }

        let payload = self.cached_packet.depacketize(&packet.payload)?;
        if !payload.is_empty() {
            // Payload will be empty if the RTP packet contained a partial h264 packet, and not
            // the end of the h264 packet
            let _ = self.media_channel.send(MediaNotificationContent::Video {
                codec: VideoCodec::H264,
                is_sequence_header: false, // todo: figure this out
                is_keyframe: is_key_frame,
                timestamp: video_timestamp_from_rtp_packet(&packet),
                data: payload,
            });
        }

        Ok(())
    }
}

fn is_key_frame(data: &[u8]) -> bool {
    if data.len() < 4 {
        false
    } else {
        let word = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let nalu_type = (word >> 24) & NALU_TYPE_BITMASK;
        (nalu_type == NALU_TTYPE_STAP_A && (word & NALU_TYPE_BITMASK) == NALU_TTYPE_SPS)
            || (nalu_type == NALU_TTYPE_SPS)
    }
}
