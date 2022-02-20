//! An encoder represents a part of a gstreamer pipeline that takes video or audio data, processes
//! it, and then pushes the results out into a tokio channel.

mod audio_copy;
mod audio_drop;
mod audio_faac;
mod video_copy;
mod video_drop;
mod video_x264;

use anyhow::{Context, Result};
use bytes::Bytes;
use gstreamer::{Format, GenericFormattedValue, Pipeline};
use gstreamer_app::AppSink;
use mmids_core::codecs::{AudioCodec, VideoCodec};
use mmids_core::workflows::MediaNotificationContent;
use mmids_core::VideoTimestamp;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

pub use audio_copy::AudioCopyEncoderGenerator;
pub use audio_drop::AudioDropEncoderGenerator;
pub use audio_faac::FaacEncoderGenerator;

pub use video_copy::VideoCopyEncoderGenerator;
pub use video_drop::VideoDropEncoderGenerator;
pub use video_x264::X264EncoderGenerator;

/// An encoder that processes video in its pipeline.  It is expected that each instance of an
/// encoder is used by one stream at a time, even if multiple media streams require the same
/// transcoding parameters.
pub trait VideoEncoder {
    /// Pushes a video frame into the encoder's pipeline
    fn push_data(
        &self,
        codec: VideoCodec,
        data: Bytes,
        timestamp: VideoTimestamp,
        is_sequence_header: bool,
    ) -> Result<()>;
}

/// An encoder that processes audio in its pipeline.  It is expected that each instance of an
/// encoder is used by one stream at a time, even if multiple media streams require the same
/// transcoding parameters.
pub trait AudioEncoder {
    /// Pushes an audio frame into the encoder's pipeline
    fn push_data(
        &self,
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

/// A type that can generate a new instance of a specific video encoder.
pub trait VideoEncoderGenerator {
    fn create(
        &self,
        pipeline: &Pipeline,
        parameters: &HashMap<String, Option<String>>,
        media_sender: UnboundedSender<MediaNotificationContent>,
    ) -> anyhow::Result<Box<dyn VideoEncoder>>;
}

/// A type that can generate a new instance for a specific audio encoder.
pub trait AudioEncoderGenerator {
    fn create(
        &self,
        pipeline: &Pipeline,
        parameters: &HashMap<String, Option<String>>,
        media_sender: UnboundedSender<MediaNotificationContent>,
    ) -> anyhow::Result<Box<dyn AudioEncoder>>;
}

/// Allows encoder generators to be registered and be referred to via a name that given at
/// registration time.  When an encoder instance is required, the encoder generator requested is
/// invoked and the resulting encoder (or error) is returned.
pub struct EncoderFactory {
    video_encoders: HashMap<String, Box<dyn VideoEncoderGenerator>>,
    audio_encoders: HashMap<String, Box<dyn AudioEncoderGenerator>>,
}

impl EncoderFactory {
    /// Creates a new encoder factory
    pub fn new() -> EncoderFactory {
        EncoderFactory {
            video_encoders: HashMap::new(),
            audio_encoders: HashMap::new(),
        }
    }

    /// Registers a video encoder generator that can be invoked with a specific name
    pub fn register_video_encoder(
        &mut self,
        name: &str,
        encoder_generator: Box<dyn VideoEncoderGenerator>,
    ) -> Result<(), EncoderFactoryRegistrationError> {
        if self.video_encoders.contains_key(name) {
            return Err(EncoderFactoryRegistrationError::DuplicateName(
                name.to_string(),
            ));
        }

        self.video_encoders
            .insert(name.to_string(), encoder_generator);
        Ok(())
    }

    /// Registers an audio encoder generator that can be invoked with a specific name
    pub fn register_audio_encoder(
        &mut self,
        name: &str,
        encoder_generator: Box<dyn AudioEncoderGenerator>,
    ) -> Result<(), EncoderFactoryRegistrationError> {
        if self.audio_encoders.contains_key(name) {
            return Err(EncoderFactoryRegistrationError::DuplicateName(
                name.to_string(),
            ));
        }

        self.audio_encoders
            .insert(name.to_string(), encoder_generator);
        Ok(())
    }

    /// Creates a new instance of a video encoder based on the name it was specified with at
    /// registration
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

    /// Creates a new instance of an audio encoder based on the name it was specified with at
    /// registration
    pub fn get_audio_encoder(
        &self,
        name: String,
        pipeline: &Pipeline,
        parameters: &HashMap<String, Option<String>>,
        media_sender: UnboundedSender<MediaNotificationContent>,
    ) -> Result<Box<dyn AudioEncoder>, EncoderFactoryCreationError> {
        let generator = match self.audio_encoders.get(name.as_str()) {
            Some(generator) => generator,
            None => return Err(EncoderFactoryCreationError::NoEncoderWithName(name)),
        };

        let encoder = generator.create(&pipeline, parameters, media_sender)?;

        Ok(encoder)
    }
}

/// Helper struct that contains the result after parsing a sample pulled from an `appsrc` gstreamer
/// element.  Only used within encoder implementations.
pub struct SampleResult {
    content: Bytes,
    dts: Option<Duration>,
    pts: Option<Duration>,
}

impl SampleResult {
    /// Pulls a sample from the `appsink` element and attempts to parse the contents from it.
    pub fn from_sink(sink: &AppSink) -> Result<SampleResult> {
        let sample = sink.pull_sample().with_context(|| "Sink had no sample")?;
        let buffer = sample.buffer().with_context(|| "Sample had no buffer")?;

        let map = buffer
            .map_readable()
            .with_context(|| "Sample's buffer could not be mapped as readable")?;

        let mut dts = buffer.dts();
        let mut pts = buffer.pts();

        if let Some(segment) = sample.segment() {
            // Sometimes the segment has a format of Bytes.  Unsure what that means, but if we
            // try to convert that to running time it will panic.
            if segment.format() == Format::Time {
                // Some encoders will have a dts and pts value that does not necessarily start at
                // 00:00:00.  This is done for various reasons, but for instance the x264 gstreamer
                // encoder will start at 1000:00:00 to better handle negative dts for B frames.  If we
                // use the dts and pts values as is, then players will have weird times showing, and
                // sync errors may occur.  When this happens the sample will have a segment, and that
                // segment can be used to adjust the pts and dts times to be from 00:00:00
                if let Some(original) = dts {
                    if let GenericFormattedValue::Time(Some(adjusted)) =
                        segment.to_running_time(original)
                    {
                        dts = Some(adjusted);
                    }
                }

                if let Some(original) = pts {
                    if let GenericFormattedValue::Time(Some(adjusted)) =
                        segment.to_running_time(original)
                    {
                        pts = Some(adjusted);
                    }
                }
            }
        }

        let dts = dts.map(|x| Duration::from_millis(x.mseconds()));
        let pts = pts.map(|x| Duration::from_millis(x.mseconds()));

        Ok(SampleResult {
            content: Bytes::copy_from_slice(map.as_slice()),
            dts,
            pts,
        })
    }

    /// Converts the dts and pts from a sample into a video timestamp.
    pub fn to_video_timestamp(&self) -> VideoTimestamp {
        match (&self.dts, &self.pts) {
            (None, None) => VideoTimestamp::from_zero(),
            (Some(dts), Some(pts)) => VideoTimestamp::from_durations(*dts, *pts),
            (Some(dts), None) => VideoTimestamp::from_durations(*dts, Duration::from_millis(0)),
            (None, Some(pts)) => VideoTimestamp::from_durations(*pts, Duration::from_millis(0)),
        }
    }
}
