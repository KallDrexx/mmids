mod video_x264;

use std::collections::HashMap;
use std::time::Duration;
use bytes::Bytes;
use mmids_core::VideoTimestamp;
use anyhow::{Context, Result};
use gstreamer::{GenericFormattedValue, Pipeline};
use gstreamer_app::AppSink;
use tokio::sync::mpsc::UnboundedSender;
use mmids_core::codecs::{AudioCodec, VideoCodec};
use mmids_core::workflows::MediaNotificationContent;

pub use video_x264::X264EncoderGenerator;

pub trait VideoEncoder {
    fn push_data(
        &mut self,
        codec: VideoCodec,
        data: Bytes,
        timestamp: VideoTimestamp,
        is_sequence_header: bool,
    ) -> Result<()>;
}

pub trait AudioEncoder {
    fn push_data(
        &mut self,
        codec: AudioCodec,
        data: Bytes,
        timestamp: Duration,
        is_sequence_header: bool,
    ) -> Result<()>;
}

/// Errors that can occur when registering an encoder with the encoder factory
#[derive(thiserror::Error, Debug)]
pub enum EncoderFactoryRegistrationError {
    #[error("An encoder already is registered with the name '{0}'")]
    DuplicateName(String),
}

/// Errors that can occur when retrieving an encoder from the encoder factory
#[derive(thiserror::Error, Debug)]
pub enum EncoderFactoryCreationError {
    #[error("No encoder exists with the name '{0}'")]
    NoEncoderWithName(String),

    #[error("Creation of the encoder failed")]
    CreationFailed(#[from] anyhow::Error),
}

pub trait VideoEncoderGenerator {
    fn create(
        &self,
        pipeline: &Pipeline,
        parameters: &HashMap<String, Option<String>>,
        media_sender: UnboundedSender<MediaNotificationContent>,
    ) -> anyhow::Result<Box<dyn VideoEncoder>>;
}

pub trait AudioEncoderGenerator {
    fn create(
        &self,
        pipeline: &Pipeline,
        parameters: HashMap<String, Option<String>>,
        media_sender: UnboundedSender<MediaNotificationContent>,
    ) -> anyhow::Result<Box<dyn AudioEncoder>>;
}

pub struct EncoderFactory {
    video_encoders: HashMap<String, Box<dyn VideoEncoderGenerator>>,
    audio_encoders: HashMap<String, Box<dyn AudioEncoderGenerator>>,
}

pub struct SampleResult {
    content: Bytes,
    timestamp: VideoTimestamp,
}

impl EncoderFactory {
    pub fn new() -> EncoderFactory {
        EncoderFactory {
            video_encoders: HashMap::new(),
            audio_encoders: HashMap::new(),
        }
    }

    pub fn register_video_encoder(
        &mut self,
        name: String,
        encoder_generator: Box<dyn VideoEncoderGenerator>,
    ) -> Result<(), EncoderFactoryRegistrationError> {
        if self.video_encoders.contains_key(name.as_str()) {
            return Err(EncoderFactoryRegistrationError::DuplicateName(name));
        }

        self.video_encoders.insert(name, encoder_generator);
        Ok(())
    }

    pub fn register_audio_encoder(
        &mut self,
        name: String,
        encoder_generator: Box<dyn AudioEncoderGenerator>,
    ) -> Result<(), EncoderFactoryRegistrationError> {
        if self.audio_encoders.contains_key(name.as_str()) {
            return Err(EncoderFactoryRegistrationError::DuplicateName(name));
        }

        self.audio_encoders.insert(name, encoder_generator);
        Ok(())
    }

    pub fn get_video_encoder(
        &self,
        name: String,
        pipeline: &Pipeline,
        parameters: &HashMap<String, Option<String>>,
        media_sender: UnboundedSender<MediaNotificationContent>,
    ) -> Result<Box<dyn VideoEncoder>, EncoderFactoryCreationError> {
        let generator = match self.video_encoders.get(name.as_str()) {
            Some(generator) => generator,
            None => return Err(EncoderFactoryCreationError::NoEncoderWithName(name)),
        };

        let encoder = generator.create(&pipeline, parameters, media_sender)?;

        Ok(encoder)
    }
}

impl SampleResult {
    pub fn from_sink(sink: &AppSink) -> Result<SampleResult> {
        let sample = sink.pull_sample()
            .with_context(|| "Sink had no sample")?;

        let buffer = sample.buffer()
            .with_context(|| "Sample had no buffer")?;

        let map = buffer.map_readable()
            .with_context(|| "Sample's buffer could not be mapped as readable")?;

        let mut dts = buffer.dts();
        let mut pts = buffer.pts();

        if let Some(segment) = sample.segment() {
            // Some encoders will have a dts and pts value that does not necessarily start at
            // 00:00:00.  This is done for various reasons, but for instance the x264 gstreamer
            // encoder will start at 1000:00:00 to better handle negative dts for B frames.  If we
            // use the dts and pts values as is, then players will have weird times showing, and
            // sync errors may occur.  When this happens the sample will have a segment, and that
            // segment can be used to adjust the pts and dts times to be from 00:00:00
            if let Some(original) = dts {
                if let GenericFormattedValue::Time(Some(adjusted)) = segment.to_running_time(original) {
                    dts = Some(adjusted);
                }
            }

            if let Some(original) = pts {
                if let GenericFormattedValue::Time(Some(adjusted)) = segment.to_running_time(original) {
                    pts = Some(adjusted);
                }
            }
        }

        let timestamp = if let Some(dts) = dts {
            if let Some(pts) = pts {
                VideoTimestamp::from_durations(
                    Duration::from_millis(dts.mseconds()),
                    Duration::from_millis(pts.mseconds()),
                )
            } else {
                VideoTimestamp::from_durations(
                    Duration::from_millis(dts.mseconds()),
                    Duration::from_millis(0),
                )
            }
        } else {
            VideoTimestamp::from_zero()
        };

        Ok(SampleResult {
            timestamp,
            content: Bytes::copy_from_slice(map.as_slice()),
        })
    }
}
