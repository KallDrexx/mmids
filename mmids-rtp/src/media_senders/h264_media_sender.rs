use std::num::Wrapping;
use std::time::Duration;
use bytes::{BufMut, Bytes, BytesMut};
use crate::media_senders::RtpToMediaContentSender;
use crate::utils::video_timestamp_from_rtp_packet;
use mmids_core::codecs::VideoCodec;
use mmids_core::workflows::MediaNotificationContent;
use rtp::codecs::h264::H264Packet;
use rtp::packet::Packet;
use rtp::packetizer::Depacketizer;
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;
use webrtc::Error::new;
use mmids_core::VideoTimestamp;

/// Takes h264 based RTP packets and sends them over to the specified tokio channel.
pub struct H264MediaSender {
    media_channel: UnboundedSender<MediaNotificationContent>,
    cached_packet: H264Packet,
    has_sent_sequence_header: bool,
    sps: Vec<Bytes>,
    pps: Vec<Bytes>,
    last_rtp_timestamp: Option<Wrapping<u32>>,
    last_calculated_timestamp: Option<Wrapping<u32>>,
}

#[derive(PartialEq, Debug)]
struct NalInfo {
    is_sps: bool,
    is_pps: bool,
    is_important: bool,
}

impl H264MediaSender {
    pub fn new(media_channel: UnboundedSender<MediaNotificationContent>) -> H264MediaSender {
        let mut packet = H264Packet::default();
        packet.is_avc = true;

        H264MediaSender {
            media_channel,
            cached_packet: packet,
            has_sent_sequence_header: false,
            sps: Vec::new(),
            pps: Vec::new(),
            last_calculated_timestamp: None,
            last_rtp_timestamp: None,
        }
    }

    fn create_avc_sequence_header(&self) -> Option<Bytes> {
        if self.sps.is_empty() || self.pps.is_empty() {
            return None;
        }

        // based on algorithm from http://aviadr1.blogspot.com/2010/05/h264-extradata-partially-explained-for.html
        let mut bytes = BytesMut::new();
        bytes.put_u8(1); // version
        bytes.put_u8(self.sps[0][1]); // profile
        bytes.put_u8(self.sps[0][2]); // compatibility
        bytes.put_u8(self.sps[0][3]); // level
        bytes.put_u8(0xFC | 3); // reserved (6 bits), nalu length size - 1 (2 bits) ????
        bytes.put_u8(0xE0 | (self.sps.len() as u8)); // reserved (3 bits), num of SPS (5 bits)
        for sps in &self.sps {
            bytes.put_u16(sps.len() as u16);
            bytes.extend_from_slice(&sps);
        }

        bytes.put_u8(self.pps.len() as u8);
        for pps in &self.pps {
            bytes.put_u16(pps.len() as u16);
            bytes.extend_from_slice(&pps);
        }

        Some(bytes.freeze())
    }
}

unsafe impl Send for H264MediaSender {}

impl RtpToMediaContentSender for H264MediaSender {
    fn send_rtp_data(&mut self, packet: &Packet) -> Result<(), webrtc::error::Error> {
        if packet.payload.is_empty() {
            return Ok(());
        }

        // calculate new timestamp
        let timestamp = match (self.last_rtp_timestamp, self.last_calculated_timestamp) {
            (Some(rtp_timestamp), Some(calculated_timestamp)) => {
                let difference = Wrapping(packet.header.timestamp) - rtp_timestamp;

                // Assume 90Khz sample rate.  I believe this should be verified in sdp
                let millis = difference.0 / 90;

                let new_timestamp = calculated_timestamp + Wrapping(millis);
                self.last_calculated_timestamp = Some(new_timestamp);

                Duration::from_millis(new_timestamp.0 as u64)
            }

            _ => {
                self.last_calculated_timestamp = Some(Wrapping(0));

                Duration::from_millis(0)
            }
        };

        self.last_rtp_timestamp = Some(Wrapping(packet.header.timestamp));

        let mut payload = self.cached_packet.depacketize(&packet.payload)?;
        while !payload.is_empty() {
            // Since we have avc = true for the h264 packetizer, each nalu is preceded by a u32
            // length value.  Each payload may actually contain multiple nalus, so we need to split
            // them out.  Each `MediaNotificationContent` should contain only one nalu (I think?)
            // and thus does not need the length.
            // TODO: SPS and PPS probably need to be decoded into avc extra data but unsure yet

            let length_bytes = &payload[..4];
            let length = ((length_bytes[0] as u32) << 24) |
                ((length_bytes[1] as u32) << 16) |
                ((length_bytes[2] as u32) << 8) |
                (length_bytes[3] as u32);

            let mut nalu = payload.split_to(4 + length as usize);
            let nalu_info = match get_nal_info(&nalu[4..]) {
                Some(info) => info,
                None => continue,
            };

            let parameters_updated = if nalu_info.is_sps {
                self.sps.push(nalu.split_off(4));
                true
            } else if nalu_info.is_pps {
                self.pps.push(nalu.split_off(4));
                true
            } else {
                false
            };

            if parameters_updated {
                let sequence_header_bytes = match self.create_avc_sequence_header() {
                    Some(bytes) => bytes,
                    None => continue, // haven't received all sequence header information
                };

                let _ = self.media_channel.send(MediaNotificationContent::Video {
                    codec: VideoCodec::H264,
                    is_sequence_header: true,
                    timestamp: VideoTimestamp::from_zero(),
                    data: sequence_header_bytes,
                    is_keyframe: true,
                });

                self.has_sent_sequence_header = true;
            } else {
                if !self.has_sent_sequence_header {
                    // Don't send any packets until we've sent the sequence header
                    continue;
                }

                let _ = self.media_channel.send(MediaNotificationContent::Video {
                    codec: VideoCodec::H264,
                    is_sequence_header: false,
                    timestamp: VideoTimestamp::from_durations(timestamp, timestamp),
                    data: nalu,

                    // Since important nalu units are either sequence headers or i-frames, consider
                    // any important unit as a key frame.
                    is_keyframe: nalu_info.is_important,
                });

            }
        }

        Ok(())
    }

    fn get_name(&self) -> &str {
        "H264MediaSender"
    }
}

fn get_nal_info(data: &[u8]) -> Option<NalInfo> {
    if data.len() == 0 {
        return None;
    }

    const STAP_A_TYPE: u8 = 24;
    const SPS_TYPE: u8 = 7;
    const PPS_TYPE: u8 = 8;
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
            is_pps: nal_unit_type == PPS_TYPE,
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
                assert!(
                    result.is_important,
                    "Data {:?} expected to be marked important but wasn't",
                    data
                );
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
                assert!(
                    !result.is_important,
                    "Data {:?} expected not to be marked important but was",
                    data
                );
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
                assert!(
                    result.is_sps,
                    "Data {:?} expected to be sps but wasn't",
                    data
                );
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
                assert!(
                    !result.is_sps,
                    "Data {:?} expected not to be sps but was",
                    data
                );
            } else {
                panic!("Data {:?} returned no Nal info", data);
            }
        }
    }
}
