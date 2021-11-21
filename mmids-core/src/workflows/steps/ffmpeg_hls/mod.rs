//! This step utilizes ffmpeg to create an HLS playlist.
//!
//! Media packets that are received from previous steps are passed to the RTMP endpoint for ffmpeg
//! consumption, and then passed on to the next step as-is.

use crate::endpoints::ffmpeg::{
    AudioTranscodeParams, FfmpegEndpointRequest, FfmpegParams, TargetParams, VideoTranscodeParams,
};
use crate::endpoints::rtmp_server::RtmpEndpointRequest;
use crate::workflows::definitions::WorkflowStepDefinition;
use crate::workflows::steps::ffmpeg_handler::{FfmpegHandlerGenerator, FfmpegParameterGenerator};
use crate::workflows::steps::{
    CreateFactoryFnResult, ExternalStreamReader, StepCreationResult, StepFutureResult, StepInputs,
    StepOutputs, StepStatus, WorkflowStep,
};
use crate::StreamId;
use futures::FutureExt;
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;

const PATH: &str = "path";
const SEGMENT_DURATION: &str = "duration";
const SEGMENT_COUNT: &str = "count";

pub struct FfmpegHlsStep {
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
    #[error("No path specified.  A 'path' is required")]
    NoPathProvided,

    #[error("Invalid duration of '{0}'.  {} should be a number.", SEGMENT_DURATION)]
    InvalidSegmentLength(String),

    #[error(
        "Invalid segment count of '{0}'.  {} should be a positive number",
        SEGMENT_COUNT
    )]
    InvalidSegmentCount(String),
}

struct ParamGenerator {
    rtmp_app: String,
    path: String,
    segment_duration: u16,
    segment_count: u16,
}

impl FfmpegHlsStep {
    pub fn create_factory_fn(
        ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
        rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
    ) -> CreateFactoryFnResult {
        Box::new(move |definition| {
            FfmpegHlsStep::new(definition, ffmpeg_endpoint.clone(), rtmp_endpoint.clone())
        })
    }

    pub fn new(
        definition: &WorkflowStepDefinition,
        ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
        rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
    ) -> StepCreationResult {
        let path = match definition.parameters.get(PATH) {
            Some(value) => value,
            None => return Err(Box::new(StepStartupError::NoPathProvided)),
        };

        let duration = match definition.parameters.get(SEGMENT_DURATION) {
            Some(value) => match value.parse() {
                Ok(num) => num,
                Err(_) => {
                    return Err(Box::new(StepStartupError::InvalidSegmentLength(
                        value.clone(),
                    )));
                }
            },

            None => 2,
        };

        let count = match definition.parameters.get(SEGMENT_COUNT) {
            Some(value) => match value.parse::<u16>() {
                Ok(num) => num,
                Err(_) => {
                    return Err(Box::new(StepStartupError::InvalidSegmentCount(
                        value.clone(),
                    )));
                }
            },

            None => 0,
        };

        let param_generator = ParamGenerator {
            rtmp_app: get_rtmp_app(definition.get_id().to_string()),
            path: path.clone(),
            segment_duration: duration,
            segment_count: count,
        };

        let handler_generator =
            FfmpegHandlerGenerator::new(ffmpeg_endpoint.clone(), Box::new(param_generator));

        let (reader, mut futures) = ExternalStreamReader::new(
            get_rtmp_app(definition.get_id().to_string()),
            rtmp_endpoint,
            Box::new(handler_generator),
        );

        let step = FfmpegHlsStep {
            definition: definition.clone(),
            status: StepStatus::Active,
            stream_reader: reader,
        };

        futures.push(notify_when_ffmpeg_endpoint_is_gone(ffmpeg_endpoint).boxed());

        Ok((Box::new(step), futures))
    }
}

impl WorkflowStep for FfmpegHlsStep {
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
            error!("external stream reader is in error status, so putting the step in in error status as well.");
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
    fn form_parameters(&self, stream_id: &StreamId, stream_name: &String) -> FfmpegParams {
        FfmpegParams {
            read_in_real_time: true,
            input: format!("rtmp://localhost/{}/{}", self.rtmp_app, stream_id.0),
            video_transcode: VideoTranscodeParams::Copy,
            audio_transcode: AudioTranscodeParams::Copy,
            scale: None,
            bitrate_in_kbps: None,
            target: TargetParams::Hls {
                path: format!("{}/{}.m3u8", self.path, stream_name),
                max_entries: Some(self.segment_count),
                segment_length: self.segment_duration,
            },
        }
    }
}

fn get_rtmp_app(id: String) -> String {
    format!("ffmpeg-hls-{}", id)
}

async fn notify_when_ffmpeg_endpoint_is_gone(
    endpoint: UnboundedSender<FfmpegEndpointRequest>,
) -> Box<dyn StepFutureResult> {
    endpoint.closed().await;

    Box::new(FutureResult::FfmpegEndpointGone)
}
