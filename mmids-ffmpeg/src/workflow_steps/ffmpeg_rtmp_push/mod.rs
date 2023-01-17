//! This step utilizes the ffmpeg endpoint to send media to an external system, such as another
//! RTMP server.
//!
//! Any incoming media packets are passed to the rtmp endpoint for sending to ffmpeg, and then
//! passed along as is for the next workflow step.

use crate::endpoint::{
    AudioTranscodeParams, FfmpegEndpointRequest, FfmpegParams, TargetParams, VideoTranscodeParams,
};
use crate::workflow_steps::ffmpeg_handler::{FfmpegHandlerGenerator, FfmpegParameterGenerator};
use mmids_core::workflows::definitions::WorkflowStepDefinition;
use mmids_core::workflows::metadata::MetadataKey;
use mmids_core::workflows::steps::factory::StepGenerator;
use mmids_core::workflows::steps::futures_channel::WorkflowStepFuturesChannel;
use mmids_core::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use mmids_core::StreamId;
use mmids_rtmp::rtmp_server::RtmpEndpointRequest;
use mmids_rtmp::workflow_steps::external_stream_reader::ExternalStreamReader;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;

const TARGET: &str = "target";

/// Generates new instances of the ffmpeg rtmp push workflow step based on specified step definitions.
pub struct FfmpegRtmpPushStepGenerator {
    rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
    ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    is_keyframe_metadata_key: MetadataKey,
    pts_offset_metadata_key: MetadataKey,
}

struct FfmpegRtmpPushStep {
    definition: WorkflowStepDefinition,
    status: StepStatus,
    stream_reader: ExternalStreamReader,
}

enum FutureResult {
    FfmpegEndpointGone,
}

impl StepFutureResult for FutureResult {}

#[derive(Error, Debug)]
enum StepStartupError {
    #[error("No rtmp target specified.  A 'target' parameter is required")]
    NoTargetProvided,
}

struct ParamGenerator {
    rtmp_app: String,
    target: String,
}

impl FfmpegRtmpPushStepGenerator {
    pub fn new(
        rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
        ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
        is_keyframe_metadata_key: MetadataKey,
        pts_offset_metadata_key: MetadataKey,
    ) -> Self {
        FfmpegRtmpPushStepGenerator {
            rtmp_endpoint,
            ffmpeg_endpoint,
            is_keyframe_metadata_key,
            pts_offset_metadata_key,
        }
    }
}

impl StepGenerator for FfmpegRtmpPushStepGenerator {
    fn generate(
        &self,
        definition: WorkflowStepDefinition,
        futures_channel: WorkflowStepFuturesChannel,
    ) -> StepCreationResult {
        let target = match definition.parameters.get(TARGET) {
            Some(Some(value)) => value,
            _ => return Err(Box::new(StepStartupError::NoTargetProvided)),
        };

        let param_generator = ParamGenerator {
            rtmp_app: get_rtmp_app(definition.get_id().to_string()),
            target: target.to_string(),
        };

        let handler_generator =
            FfmpegHandlerGenerator::new(self.ffmpeg_endpoint.clone(), Box::new(param_generator));

        let reader = ExternalStreamReader::new(
            Arc::new(format!("ffmpeg-rtmp-push-{}", definition.get_id())),
            self.rtmp_endpoint.clone(),
            Box::new(handler_generator),
            self.is_keyframe_metadata_key,
            self.pts_offset_metadata_key,
            &futures_channel,
        );

        let step = FfmpegRtmpPushStep {
            definition,
            status: StepStatus::Active,
            stream_reader: reader,
        };

        let ffmpeg_endpoint = self.ffmpeg_endpoint.clone();
        futures_channel.send_on_generic_future_completion(async move {
            ffmpeg_endpoint.closed().await;
            FutureResult::FfmpegEndpointGone
        });

        Ok(Box::new(step))
    }
}

impl WorkflowStep for FfmpegRtmpPushStep {
    fn get_status(&self) -> &StepStatus {
        &self.status
    }

    fn get_definition(&self) -> &WorkflowStepDefinition {
        &self.definition
    }

    fn execute(
        &mut self,
        inputs: &mut StepInputs,
        outputs: &mut StepOutputs,
        futures_channel: WorkflowStepFuturesChannel,
    ) {
        if let StepStatus::Error { message } = &self.stream_reader.status {
            error!("External stream reader is in error status, so putting the step in in error status as well.");

            self.status = StepStatus::Error {
                message: message.to_string(),
            };

            return;
        }

        for future_result in inputs.notifications.drain(..) {
            match future_result.downcast::<FutureResult>() {
                Err(future_result) => {
                    // Not a future we can handle
                    self.stream_reader
                        .handle_resolved_future(future_result, &futures_channel)
                }

                Ok(future_result) => match *future_result {
                    FutureResult::FfmpegEndpointGone => {
                        error!("Ffmpeg endpoint has disappeared.  Closing all streams");
                        self.stream_reader.stop_all_streams();
                    }
                },
            };
        }

        for media in inputs.media.drain(..) {
            self.stream_reader
                .handle_media(media, outputs, &futures_channel);
        }
    }

    fn shutdown(&mut self) {
        self.stream_reader.stop_all_streams();
        self.status = StepStatus::Shutdown;
    }
}

impl FfmpegParameterGenerator for ParamGenerator {
    fn form_parameters(&self, stream_id: &StreamId, _stream_name: &str) -> FfmpegParams {
        FfmpegParams {
            read_in_real_time: true,
            input: format!("rtmp://localhost/{}/{}", self.rtmp_app, stream_id.0),
            video_transcode: VideoTranscodeParams::Copy,
            audio_transcode: AudioTranscodeParams::Copy,
            scale: None,
            bitrate_in_kbps: None,
            target: TargetParams::Rtmp {
                url: self.target.clone(),
            },
        }
    }
}

fn get_rtmp_app(id: String) -> String {
    format!("ffmpeg-rtmp-push-{}", id)
}
