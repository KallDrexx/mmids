/// Video codecs that can be identified
#[derive(Debug, Clone, PartialEq)]
pub enum VideoCodec {
    Unknown,
    H264,
}

/// Audio codecs that can be identified
#[derive(Debug, Clone, PartialEq)]
pub enum AudioCodec {
    Unknown,
    Aac,
}
