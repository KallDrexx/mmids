/// Video codecs that can be identified
#[derive(Debug, Clone)]
pub enum VideoCodec {
    Unknown,
    H264,
}

/// Audio codecs that can be identified
#[derive(Debug, Clone)]
pub enum AudioCodec {
    Unknown,
    Aac,
}
