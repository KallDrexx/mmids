use webrtc::api::media_engine::MIME_TYPE_H264;

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

impl VideoCodec {
    /// Gets the mime type for a given video codec
    pub fn to_mime_type(&self) -> Option<String> {
        match self {
            VideoCodec::H264 => Some(MIME_TYPE_H264.to_lowercase()),
            VideoCodec::Unknown => None,
        }
    }
}

impl AudioCodec {
    /// Gets the mime type for a given audio codec
    pub fn to_mime_type(&self) -> Option<String> {
        match self {
            AudioCodec::Aac => None,
            AudioCodec::Unknown => None,
        }
    }
}
