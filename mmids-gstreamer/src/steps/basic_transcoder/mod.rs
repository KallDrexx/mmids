//! The basic transcoding workflow step that allows transcoding audio and video based on passed
//! in parameters. This step expects at least an `audio` and `video` parameter to be specified, each
//! with the name of the respective audio and video encoder to use.
//!
//! Each encoder may have encoder specific parameters that can be specified by prefixing each
//! parameter with either `audio_` or `video_`.  These prefixes allow the workflow step to know
//! which encoder to route the each parameter to.   The prefix is removed from the parameter before
//! passing it to the encoder, so `video_bitrate` gets passed to the video encoder as `bitrate`.

use crate::endpoints::gst_transcoder::{
    GstTranscoderNotification, GstTranscoderRequest, GstTranscoderStoppedCause,
};
use futures::FutureExt;
use mmids_core::workflows::definitions::WorkflowStepDefinition;
use mmids_core::workflows::steps::factory::StepGenerator;
use mmids_core::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use mmids_core::workflows::{MediaNotification, MediaNotificationContent};
use mmids_core::StreamId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

pub const VIDEO_ENCODER: &str = "video";
pub const AUDIO_ENCODER: &str = "audio";
pub const VIDEO_PARAM_PREFIX: &str = "video_";
pub const AUDIO_PARAM_PREFIX: &str = "audio_";

/// Creates a new instance of the basic transcode workflow step.
pub struct BasicTranscodeStepGenerator {
    transcode_endpoint: UnboundedSender<GstTranscoderRequest>,
}

struct ActiveTranscode {
    media_sender: UnboundedSender<MediaNotificationContent>,
    transcode_process_id: Uuid,
    stream_name: Arc<String>,
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
    TranscoderNotificationSenderGone(StreamId),
    TranscoderNotificationReceived {
        stream_id: StreamId,
        notification: GstTranscoderNotification,
        receiver: UnboundedReceiver<GstTranscoderNotification>,
    },

    TranscodedMediaChannelClosed(StreamId),
    TranscodedMediaReceived {
        stream_id: StreamId,
        media: MediaNotificationContent,
        receiver: UnboundedReceiver<MediaNotificationContent>,
    },
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
        BasicTranscodeStepGenerator { transcode_endpoint }
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
        for (key, value) in &definition.parameters {
            if key.starts_with(VIDEO_PARAM_PREFIX) && key.len() > VIDEO_PARAM_PREFIX.len() {
                video_params.insert(key[VIDEO_PARAM_PREFIX.len()..].to_string(), value.clone());
            }

            if key.starts_with(AUDIO_PARAM_PREFIX) && key.len() > AUDIO_PARAM_PREFIX.len() {
                audio_params.insert(key[AUDIO_PARAM_PREFIX.len()..].to_string(), value.clone());
            }
        }

        let step = BasicTranscodeStep {
            definition,
            status: StepStatus::Active,
            transcoder_endpoint: self.transcode_endpoint.clone(),
            active_transcodes: HashMap::new(),
            video_encoder_name,
            audio_encoder_name,
            video_parameters: video_params,
            audio_parameters: audio_params,
        };

        let futures = vec![notify_on_transcoder_gone(self.transcode_endpoint.clone()).boxed()];

        Ok((Box::new(step), futures))
    }
}

impl BasicTranscodeStep {
    fn stop_all_transcodes(&mut self) {
        let stream_ids = self.active_transcodes.keys().cloned().collect::<Vec<_>>();

        for stream_id in stream_ids {
            self.stop_transcode(stream_id);
        }
    }

    #[instrument(skip(self))]
    fn stop_transcode(&mut self, stream_id: StreamId) {
        if let Some(transcode) = self.active_transcodes.remove(&stream_id) {
            info!("Stopping transcode");

            let _ = self
                .transcoder_endpoint
                .send(GstTranscoderRequest::StopTranscoding {
                    id: transcode.transcode_process_id,
                });
        }
    }

    #[instrument(skip(self, outputs))]
    fn start_transcode(
        &mut self,
        stream_id: StreamId,
        stream_name: Arc<String>,
        outputs: &mut StepOutputs,
    ) {
        if self.active_transcodes.contains_key(&stream_id) {
            warn!(
                "Attempted to start transcode for stream that already has a transcode in progress"
            );
            return;
        }

        let (media_sender, media_receiver) = unbounded_channel();
        let (notification_sender, notification_receiver) = unbounded_channel();

        let process_id = Uuid::new_v4();
        self.active_transcodes.insert(
            stream_id.clone(),
            ActiveTranscode {
                transcode_process_id: process_id,
                media_sender,
                stream_name: stream_name.clone(),
            },
        );

        info!(
            "Starting transcode process id {} for stream {}",
            process_id, stream_name
        );
        let _ = self
            .transcoder_endpoint
            .send(GstTranscoderRequest::StartTranscoding {
                id: process_id,
                notification_channel: notification_sender,
                input_media: media_receiver,
                video_encoder_name: self.video_encoder_name.clone(),
                video_parameters: self.video_parameters.clone(),
                audio_encoder_name: self.audio_encoder_name.clone(),
                audio_parameters: self.audio_parameters.clone(),
            });

        outputs
            .futures
            .push(notify_on_transcoder_notification(notification_receiver, stream_id).boxed());
    }

    fn handle_media(&mut self, media: MediaNotification, outputs: &mut StepOutputs) {
        match &media.content {
            MediaNotificationContent::NewIncomingStream { stream_name } => {
                self.start_transcode(media.stream_id.clone(), stream_name.clone(), outputs);

                outputs.media.push(media);
            }

            MediaNotificationContent::StreamDisconnected => {
                self.stop_transcode(media.stream_id.clone());
                outputs.media.push(media);
            }

            MediaNotificationContent::Video { .. } => {
                if let Some(transcode) = self.active_transcodes.get(&media.stream_id) {
                    let _ = transcode.media_sender.send(media.content.clone());
                }
            }

            MediaNotificationContent::MediaPayload { .. } => {
                if let Some(transcode) = self.active_transcodes.get(&media.stream_id) {
                    let _ = transcode.media_sender.send(media.content.clone());
                }
            }

            MediaNotificationContent::Metadata { .. } => (),
        }
    }

    fn handle_transcode_notification(
        &mut self,
        stream_id: StreamId,
        notification: GstTranscoderNotification,
        outputs: &mut StepOutputs,
    ) {
        match notification {
            GstTranscoderNotification::TranscodingStopped(cause) => {
                let transcode = match self.active_transcodes.remove(&stream_id) {
                    Some(transcode) => transcode,
                    None => return,
                };

                if cause != GstTranscoderStoppedCause::StopRequested {
                    warn!(
                        stream_id = ?stream_id,
                        cause = ?cause,
                        "Transcoding unexpectedly stopped: {:?}", cause
                    );

                    // Since the stop wasn't requested, try restarting it
                    self.start_transcode(stream_id, transcode.stream_name, outputs);
                }
            }

            GstTranscoderNotification::TranscodingStarted { output_media } => {
                outputs
                    .futures
                    .push(notify_on_transcoder_media(output_media, stream_id).boxed());
            }
        }
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
        for media in inputs.media.drain(..) {
            self.handle_media(media, outputs);
        }

        for future_result in inputs.notifications.drain(..) {
            let future_result = match future_result.downcast::<FutureResult>() {
                Ok(result) => result,
                Err(_) => {
                    error!("Received future result that could not be casted to the internal future result type");
                    continue;
                }
            };

            match *future_result {
                FutureResult::TranscoderEndpointGone => {
                    self.status = StepStatus::Error {
                        message: "Transcoder endpoint went away".to_string(),
                    };

                    self.stop_all_transcodes();
                    return;
                }

                FutureResult::TranscoderNotificationSenderGone(stream_id) => {
                    error!(
                        stream_id = ?stream_id,
                        "Transcode notification sender for stream {:?} disappeared",
                        stream_id,
                    );

                    self.stop_transcode(stream_id);
                }

                FutureResult::TranscodedMediaChannelClosed(stream_id) => {
                    error!(
                        stream_id = ?stream_id,
                        "Sender of transcoded media for stream {:?} disappeared",
                        stream_id,
                    );

                    self.stop_transcode(stream_id);
                }

                FutureResult::TranscoderNotificationReceived {
                    notification,
                    stream_id,
                    receiver,
                } => {
                    outputs.futures.push(
                        notify_on_transcoder_notification(receiver, stream_id.clone()).boxed(),
                    );
                    self.handle_transcode_notification(stream_id, notification, outputs);
                }

                FutureResult::TranscodedMediaReceived {
                    media,
                    stream_id,
                    receiver,
                } => {
                    outputs
                        .futures
                        .push(notify_on_transcoder_media(receiver, stream_id.clone()).boxed());

                    outputs.media.push(MediaNotification {
                        stream_id,
                        content: media,
                    });
                }
            }
        }
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

async fn notify_on_transcoder_notification(
    mut receiver: UnboundedReceiver<GstTranscoderNotification>,
    stream_id: StreamId,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(notification) => FutureResult::TranscoderNotificationReceived {
            stream_id,
            notification,
            receiver,
        },

        None => FutureResult::TranscoderNotificationSenderGone(stream_id),
    };

    Box::new(result)
}

async fn notify_on_transcoder_media(
    mut receiver: UnboundedReceiver<MediaNotificationContent>,
    stream_id: StreamId,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(media) => FutureResult::TranscodedMediaReceived {
            stream_id,
            media,
            receiver,
        },
        None => FutureResult::TranscodedMediaChannelClosed(stream_id),
    };

    Box::new(result)
}
