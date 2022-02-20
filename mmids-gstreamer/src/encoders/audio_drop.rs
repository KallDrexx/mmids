use crate::encoders::{AudioEncoder, AudioEncoderGenerator};
use anyhow::Result;
use bytes::Bytes;
use gstreamer::Pipeline;
use mmids_core::codecs::AudioCodec;
use mmids_core::workflows::MediaNotificationContent;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

/// Creates an encoder that drops the audio stream.
pub struct AudioDropEncoderGenerator {}

impl AudioEncoderGenerator for AudioDropEncoderGenerator {
    fn create(
        &self,
        _pipeline: &Pipeline,
        _parameters: &HashMap<String, Option<String>>,
        _media_sender: UnboundedSender<MediaNotificationContent>,
    ) -> Result<Box<dyn AudioEncoder>> {
        Ok(Box::new(AudioDropEncoder {}))
    }
}

struct AudioDropEncoder {}

impl AudioEncoder for AudioDropEncoder {
    fn push_data(
        &self,
        _codec: AudioCodec,
        _data: Bytes,
        _timestamp: Duration,
        _is_sequence_header: bool,
    ) -> Result<()> {
        // Do nothing with the data since we are dropping the audio stream
        Ok(())
    }
}
