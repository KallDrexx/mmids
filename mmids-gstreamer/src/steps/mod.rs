use std::collections::HashMap;
use futures::channel::mpsc::UnboundedReceiver;
use futures::FutureExt;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;
use mmids_core::StreamId;
use mmids_core::workflows::definitions::WorkflowStepDefinition;
use mmids_core::workflows::MediaNotificationContent;
use mmids_core::workflows::steps::factory::StepGenerator;
use mmids_core::workflows::steps::{StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep};
use crate::endpoints::gst_transcoder::{GstTranscoderNotification, GstTranscoderRequest};

pub const VIDEO_ENCODER: &'static str = "video";
pub const AUDIO_ENCODER: &'static str = "audio";
pub const VIDEO_PARAM_PREFIX: &'static str = "video_";
pub const AUDIO_PARAM_PREFIX: &'static str = "audio_";

pub struct BasicTranscodeStepGenerator {
    transcode_endpoint: UnboundedSender<GstTranscoderRequest>,
}

struct ActiveTranscode {
    media_sender: UnboundedSender<MediaNotificationContent>,
    transcode_process_id: Uuid,
}

struct BasicTranscodeStep {
    definition: WorkflowStepDefinition,
    status: StepStatus,
    transcoder_endpoint: UnboundedSender<GstTranscoderRequest>,
    active_transcodes: HashMap<StreamId, ActiveTranscode>,
    video_encoder_name: String,
    audio_encoder_name: String,
    video_parameters: HashMap<String, Option<String>>,
    audio_parameters: HashMap<String, Option<String>>,
}

enum FutureResult {
    TranscoderEndpointGone,
    TranscoderNotificationReceived {
        stream_id: StreamId,
        notification: GstTranscoderNotification,
        receiver: UnboundedReceiver<GstTranscoderNotification>,
    },

    TranscodedMediaChannelClosed(StreamId),
    TranscodedMediaReceived(StreamId, MediaNotificationContent, UnboundedReceiver<MediaNotificationContent>),
}

impl StepFutureResult for FutureResult {}

#[derive(thiserror::Error, Debug)]
enum StepStartupError {
    #[error("No video encoder specified")]
    NoVideoEncoderSpecified,

    #[error("No audio encoder specified")]
    NoAudioEncoderSpecified,
}

impl BasicTranscodeStepGenerator {
    pub fn new(
        transcode_endpoint: UnboundedSender<GstTranscoderRequest>,
    ) -> BasicTranscodeStepGenerator {
        BasicTranscodeStepGenerator {
            transcode_endpoint,
        }
    }
}

impl StepGenerator for BasicTranscodeStepGenerator {
    fn generate(&self, definition: WorkflowStepDefinition) -> StepCreationResult {
        let video_encoder_name = match definition.parameters.get(VIDEO_ENCODER) {
            Some(Some(encoder)) => encoder.clone(),
            _ => return Err(Box::new(StepStartupError::NoVideoEncoderSpecified)),
        };

        let audio_encoder_name = match definition.parameters.get(AUDIO_ENCODER) {
            Some(Some(encoder)) => encoder.clone(),
            _ => return Err(Box::new(StepStartupError::NoAudioEncoderSpecified)),
        };

        // Split out audio and video specific parameters based on prefixes.
        let mut audio_params = HashMap::new();
        let mut video_params = HashMap::new();
        for (key, value) in definition.parameters {
            if key.starts_with(VIDEO_PARAM_PREFIX) && key.len() > VIDEO_PARAM_PREFIX.len() {
                video_params.insert(
                    key[VIDEO_PARAM_PREFIX.len()..].to_string(),
                    value.clone(),
                );
            }

            if key.starts_with(AUDIO_PARAM_PREFIX) && key.len() > AUDIO_PARAM_PREFIX.len() {
                audio_params.insert(
                    key[AUDIO_PARAM_PREFIX.len()..].to_string(),
                    value.clone(),
                );
            }
        }

        let step = BasicTranscodeStep {
            definition: definition.clone(),
            status: StepStatus::Active,
            transcoder_endpoint: self.transcode_endpoint.clone(),
            active_transcodes: HashMap::new(),
            video_encoder_name,
            audio_encoder_name,
            video_parameters: video_params,
            audio_parameters: audio_params,
        };

        let futures = vec![
            notify_on_transcoder_gone(self.transcode_endpoint.clone()).boxed(),
        ];

        Ok((Box::new(step), futures))
    }
}

impl WorkflowStep for BasicTranscodeStep {
    fn get_status(&self) -> &StepStatus {
        &self.status
    }

    fn get_definition(&self) -> &WorkflowStepDefinition {
        &self.definition
    }

    fn execute(&mut self, inputs: &mut StepInputs, outputs: &mut StepOutputs) {
        todo!()
    }

    fn shutdown(&mut self) {
        self.status = StepStatus::Shutdown;
    }
}

async fn notify_on_transcoder_gone(
    sender: UnboundedSender<GstTranscoderRequest>,
) -> Box<dyn StepFutureResult> {
    sender.closed().await;

    Box::new(FutureResult::TranscoderEndpointGone)
}