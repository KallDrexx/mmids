/// Video codecs that can be identified
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum VideoCodec {
    Unknown,
    H264,
}

/// Audio codecs that can be identified
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum AudioCodec {
    Unknown,
    Aac,
}
