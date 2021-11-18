//! This step utilizes the ffmpeg endpoint to send media to an external system, such as another
//! RTMP server.
//!
//! Any incoming media packets are passed to the rtmp endpoint for sending to ffmpeg, and then
//! passed along as is for the next workflow step.

use super::external_stream_reader::ExternalStreamReader;
use crate::endpoints::ffmpeg::{
    AudioTranscodeParams, FfmpegEndpointRequest, FfmpegParams, TargetParams, VideoTranscodeParams,
};
use crate::endpoints::rtmp_server::RtmpEndpointRequest;
use crate::workflows::definitions::WorkflowStepDefinition;
use crate::workflows::steps::ffmpeg_handler::{FfmpegHandlerGenerator, FfmpegParameterGenerator};
use crate::workflows::steps::{
    CreateFactoryFnResult, StepCreationResult, StepFutureResult, StepInputs, StepOutputs,
    StepStatus, WorkflowStep,
};
use crate::StreamId;
use futures::FutureExt;
use log::error;
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;

const TARGET: &str = "target";

pub struct FfmpegRtmpPushStep {
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

impl FfmpegRtmpPushStep {
    pub fn create_factory_fn(
        ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
        rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
    ) -> CreateFactoryFnResult {
        Box::new(move |definition| {
            FfmpegRtmpPushStep::new(definition, ffmpeg_endpoint.clone(), rtmp_endpoint.clone())
        })
    }

    pub fn new(
        definition: &WorkflowStepDefinition,
        ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
        rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
    ) -> StepCreationResult {
        let target = match definition.parameters.get(TARGET) {
            Some(value) => value,
            None => return Err(Box::new(StepStartupError::NoTargetProvided)),
        };

        let param_generator = ParamGenerator {
            rtmp_app: get_rtmp_app(definition.get_id().to_string()),
            target: target.to_string(),
        };

        let handler_generator =
            FfmpegHandlerGenerator::new(ffmpeg_endpoint.clone(), Box::new(param_generator));

        let (reader, mut futures) = ExternalStreamReader::new(
            definition.get_id().to_string(),
            format!("ffmpeg-rtmp-push-{}", definition.get_id()),
            rtmp_endpoint,
            Box::new(handler_generator),
        );

        let step = FfmpegRtmpPushStep {
            definition: definition.clone(),
            status: StepStatus::Active,
            stream_reader: reader,
        };

        futures.push(notify_when_ffmpeg_endpoint_is_gone(ffmpeg_endpoint).boxed());

        Ok((Box::new(step), futures))
    }
}

impl WorkflowStep for FfmpegRtmpPushStep {
    fn get_status(&self) -> &StepStatus {
        &self.status
    }

    fn get_definition(&self) -> &WorkflowStepDefinition {
        &self.definition
    }

    fn execute(&mut self, inputs: &mut StepInputs, outputs: &mut StepOutputs) {
        if self.status == StepStatus::Error {
            return;
        }

        if self.stream_reader.status == StepStatus::Error {
            error!(
                "Step {}: external stream reader is in error status, so putting the step in \
            in error status as well.",
                self.definition.get_id()
            );

            self.status = StepStatus::Error;
            return;
        }

        for future_result in inputs.notifications.drain(..) {
            match future_result.downcast::<FutureResult>() {
                Err(future_result) => {
                    // Not a future we can handle
                    self.stream_reader
                        .handle_resolved_future(future_result, outputs)
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
            self.stream_reader.handle_media(media, outputs);
        }
    }
}

impl FfmpegParameterGenerator for ParamGenerator {
    fn form_parameters(&self, stream_id: &StreamId, _stream_name: &String) -> FfmpegParams {
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

async fn notify_when_ffmpeg_endpoint_is_gone(
    endpoint: UnboundedSender<FfmpegEndpointRequest>,
) -> Box<dyn StepFutureResult> {
    endpoint.closed().await;

    Box::new(FutureResult::FfmpegEndpointGone)
}
