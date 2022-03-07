use rtp::codecs::h264::H264Packet;
use rtp::packet::Packet;
use rtp::packetizer::Depacketizer;
use tokio::sync::mpsc::UnboundedSender;
use mmids_core::codecs::VideoCodec;
use mmids_core::workflows::MediaNotificationContent;
use crate::media_senders::RtpToMediaContentSender;
use crate::utils::video_timestamp_from_rtp_packet;

/// Takes h264 based RTP packets and sends them over to the specified tokio channel.
pub struct H264MediaSender {
    media_channel: UnboundedSender<MediaNotificationContent>,
    cached_packet: H264Packet,
    has_sent_sequence_header: bool,
}

#[derive(PartialEq, Debug)]
struct NalInfo {
    is_sps: bool,
    is_important: bool,
}

impl H264MediaSender {
    pub fn new(media_channel: UnboundedSender<MediaNotificationContent>) -> H264MediaSender {
        H264MediaSender {
            media_channel,
            cached_packet: H264Packet::default(),
            has_sent_sequence_header: false,
        }
    }
}

unsafe impl Send for H264MediaSender{ }

impl RtpToMediaContentSender for H264MediaSender {
    fn send_rtp_data(&mut self, packet: &Packet) -> Result<(), webrtc::error::Error> {
        if packet.payload.is_empty() {
            return Ok(());
        }

        let nalu_info = match get_nal_info(&packet.payload) {
            Some(info) => info,
            None => return Ok(()),
        };

        if !self.has_sent_sequence_header && !nalu_info.is_sps {
            return Ok(());
        }

        self.has_sent_sequence_header = true;

        let payload = self.cached_packet.depacketize(&packet.payload)?;
        if !payload.is_empty() {
            // Payload will be empty if the RTP packet contained a partial h264 packet, and not
            // the end of the h264 packet
            let _ = self.media_channel.send(MediaNotificationContent::Video {
                codec: VideoCodec::H264,
                is_sequence_header: nalu_info.is_sps,
                timestamp: video_timestamp_from_rtp_packet(&packet),
                data: payload,

                // Since important nalu units are either sequence headers or i-frames, consider
                // any important unit as a key frame.
                is_keyframe: nalu_info.is_important,
            });
        }

        Ok(())
    }

    fn get_name(&self) -> &str { "H264MediaSender" }
}

fn get_nal_info(data: &[u8]) -> Option<NalInfo> {
    if data.len() == 0 {
        return None;
    }

    const STAP_A_TYPE: u8 = 24;
    const SPS_TYPE: u8 = 7;
    const IMPORTANT: u8 = 3;

    let nal_header = data[0];
    let nal_ref_idc = (nal_header & 0b01100000) >> 5;
    let nal_unit_type = nal_header & 0x1F;

    if nal_unit_type == STAP_A_TYPE {
        // Since this is an aggregate packet, move forward past the NAL unit size in the next
        // 2 bytes
        if data.len() < 4 {
            None
        } else {
            get_nal_info(&data[3..])
        }
    } else {
        Some(NalInfo {
            is_important: nal_ref_idc == IMPORTANT,
            is_sps: nal_unit_type == SPS_TYPE,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_nal_info_if_empty() {
        let data = Vec::new();
        let result = get_nal_info(&data);

        assert_eq!(result, None, "Expected no nal info");
    }

    #[test]
    fn expected_important_results() {
        let test_data = [
            vec![0b01100000],
            vec![0b01100111],
            vec![0b00011000, 0b01, 0b01, 0b01100001], // stap-a test
        ];

        for data in test_data {
            if let Some(result) = get_nal_info(&data) {
                assert!(result.is_important, "Data {:?} expected to be marked important but wasn't", data);
            } else {
                panic!("Data {:?} returned no Nal info", data);
            }
        }
    }

    #[test]
    fn expected_non_important_frame_results() {
        let test_data = [
            vec![0b00100000],
            vec![0b01000111],
            vec![0b00000111],
            vec![0b00011000, 0b01, 0b01, 0b00100001], // stap-a test
        ];

        for data in test_data {
            if let Some(result) = get_nal_info(&data) {
                assert!(!result.is_important, "Data {:?} expected not to be marked important but was", data);
            } else {
                panic!("Data {:?} returned no Nal info", data);
            }
        }
    }

    #[test]
    fn expected_sps_results() {
        let test_data = [
            vec![0b01100111],
            vec![0b00011000, 0b01, 0b01, 0b00100111], // stap-a test
        ];

        for data in test_data {
            if let Some(result) = get_nal_info(&data) {
                assert!(result.is_sps, "Data {:?} expected to be sps but wasn't", data);
            } else {
                panic!("Data {:?} returned no Nal info", data);
            }
        }
    }

    #[test]
    fn expected_non_sps_results() {
        let test_data = [
            vec![0b01101111],
            vec![0b01100110],
            vec![0b00011000, 0b01, 0b01, 0b00100011], // stap-a test
        ];

        for data in test_data {
            if let Some(result) = get_nal_info(&data) {
                assert!(!result.is_sps, "Data {:?} expected not to be sps but was", data);
            } else {
                panic!("Data {:?} returned no Nal info", data);
            }
        }
    }
}