use crate::encoders::{VideoEncoder, VideoEncoderGenerator};
use bytes::Bytes;
use gstreamer::Pipeline;
use mmids_core::workflows::MediaNotificationContent;
use mmids_core::VideoTimestamp;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

/// Creates a video encoder that drops audio.
pub struct VideoDropEncoderGenerator {}

impl VideoEncoderGenerator for VideoDropEncoderGenerator {
    fn create(
        &self,
        _pipeline: &Pipeline,
        _parameters: &HashMap<String, Option<String>>,
        _media_sender: UnboundedSender<MediaNotificationContent>,
    ) -> anyhow::Result<Box<dyn VideoEncoder + Send>> {
        Ok(Box::new(VideoDropEncoder {}))
    }
}

struct VideoDropEncoder {}

impl VideoEncoder for VideoDropEncoder {
    fn push_data(
        &self,
        _payload: Arc<String>,
        _data: Bytes,
        _timestamp: VideoTimestamp,
        _is_sequence_header: bool,
    ) -> anyhow::Result<()> {
        // Do nothing since we want to drop the video stream

        Ok(())
    }
}
