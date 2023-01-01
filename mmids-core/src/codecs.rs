//! Standard codec identifiers
use lazy_static::lazy_static;
use std::sync::Arc;

lazy_static! {
    pub static ref VIDEO_CODEC_H264_AVC: Arc<String> = Arc::new("h264-avc".to_string());
    pub static ref AUDIO_CODEC_AAC_RAW: Arc<String> = Arc::new("aac-raw".to_string());
}
