use std::collections::HashMap;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use uuid::Uuid;
use mmids_core::workflows::MediaNotificationContent;

pub enum GstTranscoderRequest {
    StartTranscoding {
        id: Uuid,
        input_media: UnboundedReceiver<MediaNotificationContent>,
        video_encoder_name: String,
        audio_encoder_name: String,
        audio_parameters: HashMap<String, Option<String>>,
        video_parameters: HashMap<String, Option<String>>,
    },

    StopTranscoding {
        id: Uuid,
    },
}

pub enum GstTranscoderNotification {
    TranscodingStarted {
        output_media: UnboundedSender<MediaNotificationContent>,
    },

    TranscodingStopped(GstTranscoderStoppedCause),
}

pub enum EncoderType { Video, Audio }

pub enum GstTranscoderStoppedCause {
    InvalidEncoderName {
        encoder_type: EncoderType,
        name: String,
    },

    EncoderCreationFailure {
        encoder_type: EncoderType,
        details: String,
    },

    PipelineReachedEndOfStream,
    PipelineError(String),
}