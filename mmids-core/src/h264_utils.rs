use bytes::{BufMut, Bytes, BytesMut};

pub struct H264SequenceHeader {
    pub sps_records: Vec<Bytes>,
    pub pps_records: Vec<Bytes>,
}

pub fn parse_avc_decoder_config_record(bytes: &Bytes) -> Option<H264SequenceHeader> {
    if bytes.len() <= 6 {
        return None;
    }

    if bytes[0] != 1 {
        return None;
    }

    let sps_count = bytes[5] & 0b00011111;
    let mut sps_records = Vec::with_capacity(sps_count as usize);
    let mut index = 6;

    for _ in 0..sps_count {
        if bytes.len() <= index + 1 {
            return None;
        }

        let sps_len = ((bytes[index] as usize) << 8) | (bytes[index + 1] as usize);
        let mut sps = BytesMut::with_capacity(sps_len as usize);
        index += 2;

        if bytes.len() < index + sps_len {
            return None;
        }

        sps.extend_from_slice(&bytes[index..(index + sps_len)]);
        sps_records.push(sps.freeze());

        index += sps_len;
    }

    if bytes.len() <= index {
        return None;
    }

    let pps_count = bytes[index];
    let mut pps_records = Vec::with_capacity(pps_count as usize);
    index += 1;

    for _ in 0..pps_count {
        if bytes.len() <= index + 1 {
            return None;
        }

        let pps_len = ((bytes[index] as usize) << 8) | (bytes[index + 1] as usize);
        let mut pps = BytesMut::with_capacity(pps_len as usize);
        index += 2;

        if bytes.len() < index + pps_len {
            return None;
        }

        pps.extend_from_slice(&bytes[index..(index + pps_len)]);
        pps_records.push(pps.freeze());

        index += pps_len;
    }

    Some(H264SequenceHeader {
        sps_records,
        pps_records,
    })
}

pub fn convert_to_avc_decoder_config_record(
    sps_records: &Vec<Bytes>,
    pps_records: &Vec<Bytes>,
) -> Option<Bytes> {
    if sps_records.is_empty() || pps_records.is_empty() {
        return None;
    }

    let mut bytes = BytesMut::new();
    bytes.put_u8(1); // version
    bytes.put_u8(sps_records[0][1]); // profile
    bytes.put_u8(sps_records[0][2]); // compatibility
    bytes.put_u8(sps_records[0][3]); // level
    bytes.put_u8(0xFC | 3); // reserved (6 bits), nalu length size - 1 (2 bits) ????
    bytes.put_u8(0xE0 | (sps_records.len() as u8)); // reserved (3 bits), num of SPS (5 bits)
    for sps in sps_records {
        bytes.put_u16(sps.len() as u16);
        bytes.extend_from_slice(&sps);
    }

    bytes.put_u8(pps_records.len() as u8);
    for pps in pps_records {
        bytes.put_u16(pps.len() as u16);
        bytes.extend_from_slice(&pps);
    }

    Some(bytes.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_convert_records_to_avc_config_record_and_back() {
        let sps_records = vec![
            Bytes::from_static(&[1, 2, 3, 4, 5]),
            Bytes::from_static(&[6, 7, 8, 9]),
        ];

        let pps_records = vec![
            Bytes::from_static(&[10, 11, 12, 13]),
            Bytes::from_static(&[14, 15, 16]),
            Bytes::from_static(&[17, 18, 19, 20]),
        ];

        let avc_record = convert_to_avc_decoder_config_record(&sps_records, &pps_records);
        assert!(avc_record.is_some(), "avc_record was None");

        let header = parse_avc_decoder_config_record(&avc_record.unwrap());
        assert!(header.is_some(), "header was None");

        let header = header.unwrap();
        assert_eq!(header.sps_records[0], sps_records[0], "First sps record did not match");
        assert_eq!(header.sps_records[1], sps_records[1], "Second sps record did not match");
        assert_eq!(header.pps_records[0], pps_records[0], "First pps record did not match");
        assert_eq!(header.pps_records[1], pps_records[1], "Second pps record did not match");
        assert_eq!(header.pps_records[2], pps_records[2], "Third pps record did not match");
    }
}