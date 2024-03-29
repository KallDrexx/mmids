//! This step utilizes ffmpeg to create an HLS playlist.
//!
//! Media packets that are received from previous steps are passed to the RTMP endpoint for ffmpeg
//! consumption, and then passed on to the next step as-is.

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

const PATH: &str = "path";
const SEGMENT_DURATION: &str = "duration";
const SEGMENT_COUNT: &str = "count";
const STREAM_NAME: &str = "stream_name";

/// Generates new instances of the ffmpeg HLS workflow step based on specified step definitions.
pub struct FfmpegHlsStepGenerator {
    rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
    ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    is_keyframe_metadata_key: MetadataKey,
    pts_offset_metadata_key: MetadataKey,
}

struct FfmpegHlsStep {
    status: StepStatus,
    stream_reader: ExternalStreamReader,
    path: String,
}

enum FutureResult {
    FfmpegEndpointGone,
    HlsPathCreated(tokio::io::Result<()>),
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
    rtmp_app: Arc<String>,
    path: String,
    segment_duration: u16,
    segment_count: u16,
    stream_name: Option<String>,
}

impl FfmpegHlsStepGenerator {
    pub fn new(
        rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
        ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
        is_keyframe_metadata_key: MetadataKey,
        pts_offset_metadata_key: MetadataKey,
    ) -> Self {
        FfmpegHlsStepGenerator {
            rtmp_endpoint,
            ffmpeg_endpoint,
            is_keyframe_metadata_key,
            pts_offset_metadata_key,
        }
    }
}

impl StepGenerator for FfmpegHlsStepGenerator {
    fn generate(
        &self,
        definition: WorkflowStepDefinition,
        futures_channel: WorkflowStepFuturesChannel,
    ) -> StepCreationResult {
        let path = match definition.parameters.get(PATH) {
            Some(Some(value)) => value,
            _ => return Err(Box::new(StepStartupError::NoPathProvided)),
        };

        let duration = match definition.parameters.get(SEGMENT_DURATION) {
            Some(Some(value)) => match value.parse() {
                Ok(num) => num,
                Err(_) => {
                    return Err(Box::new(StepStartupError::InvalidSegmentLength(
                        value.clone(),
                    )));
                }
            },

            _ => 2,
        };

        let count = match definition.parameters.get(SEGMENT_COUNT) {
            Some(Some(value)) => match value.parse::<u16>() {
                Ok(num) => num,
                Err(_) => {
                    return Err(Box::new(StepStartupError::InvalidSegmentCount(
                        value.clone(),
                    )));
                }
            },

            _ => 0,
        };

        let stream_name = definition.parameters.get(STREAM_NAME).cloned().flatten();
        let rtmp_app = Arc::new(get_rtmp_app(definition.get_id().to_string()));

        let param_generator = ParamGenerator {
            rtmp_app: rtmp_app.clone(),
            path: path.clone(),
            segment_duration: duration,
            segment_count: count,
            stream_name,
        };

        let handler_generator =
            FfmpegHandlerGenerator::new(self.ffmpeg_endpoint.clone(), Box::new(param_generator));

        let reader = ExternalStreamReader::new(
            rtmp_app,
            self.rtmp_endpoint.clone(),
            Box::new(handler_generator),
            self.is_keyframe_metadata_key,
            self.pts_offset_metadata_key,
            &futures_channel,
        );

        let path = path.clone();
        let step = FfmpegHlsStep {
            status: StepStatus::Created,
            stream_reader: reader,
            path: path.clone(),
        };

        let ffmpeg_endpoint = self.ffmpeg_endpoint.clone();
        futures_channel.send_on_generic_future_completion(async move {
            ffmpeg_endpoint.closed().await;
            FutureResult::FfmpegEndpointGone
        });

        futures_channel.send_on_generic_future_completion(async move {
            let result = tokio::fs::create_dir_all(&path).await;
            FutureResult::HlsPathCreated(result)
        });

        let status = step.status.clone();
        Ok((Box::new(step), status))
    }
}

impl WorkflowStep for FfmpegHlsStep {
    fn execute(
        &mut self,
        inputs: &mut StepInputs,
        outputs: &mut StepOutputs,
        futures_channel: WorkflowStepFuturesChannel,
    ) -> StepStatus {
        if let StepStatus::Error { message } = &self.stream_reader.status {
            error!("external stream reader is in error status, so putting the step in in error status as well.");
            return StepStatus::Error {
                message: message.to_string(),
            };
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

                        return StepStatus::Error {
                            message: "Ffmpeg endpoint gone".to_string(),
                        };
                    }

                    FutureResult::HlsPathCreated(result) => match result {
                        Ok(()) => {
                            self.status = StepStatus::Active;
                        }

                        Err(error) => {
                            error!("Could not create HLS path: '{}': {:?}", self.path, error);
                            return StepStatus::Error {
                                message: format!(
                                    "Could not create HLS path: '{}': {:?}",
                                    self.path, error
                                ),
                            };
                        }
                    },
                },
            };
        }

        for media in inputs.media.drain(..) {
            self.stream_reader
                .handle_media(media, outputs, &futures_channel);
        }

        self.status.clone()
    }
}

impl Drop for FfmpegHlsStep {
    fn drop(&mut self) {
        self.stream_reader.stop_all_streams();
    }
}

impl FfmpegParameterGenerator for ParamGenerator {
    fn form_parameters(&self, stream_id: &StreamId, stream_name: &str) -> FfmpegParams {
        FfmpegParams {
            read_in_real_time: true,
            input: format!("rtmp://localhost/{}/{}", self.rtmp_app, stream_id.0),
            video_transcode: VideoTranscodeParams::Copy,
            audio_transcode: AudioTranscodeParams::Copy,
            scale: None,
            bitrate_in_kbps: None,
            target: TargetParams::Hls {
                path: format!(
                    "{}/{}.m3u8",
                    self.path,
                    self.stream_name.as_deref().unwrap_or(stream_name)
                ),
                max_entries: Some(self.segment_count),
                segment_length: self.segment_duration,
            },
        }
    }
}

fn get_rtmp_app(id: String) -> String {
    format!("ffmpeg-hls-{}", id)
}
