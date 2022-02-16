use std::collections::HashMap;
use std::time::Duration;
use bytes::Bytes;
use mmids_core::VideoTimestamp;
use anyhow::Result;
use gstreamer::Pipeline;
use tokio::sync::mpsc::UnboundedSender;
use mmids_core::workflows::MediaNotificationContent;

pub trait VideoEncoder {
    fn push_data(
        &mut self,
        data: Bytes,
        timestamp: VideoTimestamp,
        is_sequence_header: bool,
    ) -> Result<()>;
}

pub trait AudioEncoder {
    fn push_data(
        &mut self,
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
        parameters: HashMap<String, Option<String>>,
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
        parameters: HashMap<String, Option<String>>,
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
