//! This workflow step utilizes ffmpeg to read video from an external source.  The external source
//! can be a remote RTMP server or a file on the file system.  If ffmpeg closes (such as when the
//! video file has been fully streamed) then the ffmpeg will restart until the workflow is
//! removed.
//!
//! Media packets that come in from previous steps are ignored.

use crate::endpoints::ffmpeg::{
    AudioTranscodeParams, FfmpegEndpointNotification, FfmpegEndpointRequest, FfmpegParams,
    TargetParams, VideoTranscodeParams,
};
use crate::endpoints::rtmp_server::{
    IpRestriction, RtmpEndpointPublisherMessage, RtmpEndpointRequest, StreamKeyRegistration,
};
use crate::workflows::definitions::WorkflowStepDefinition;
use crate::workflows::steps::{
    CreateFactoryFnResult, StepCreationResult, StepFutureResult, StepInputs, StepOutputs,
    StepStatus, WorkflowStep,
};
use crate::workflows::{MediaNotification, MediaNotificationContent};
use crate::StreamId;
use futures::FutureExt;
use log::{error, info};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

pub const LOCATION: &'static str = "location";
pub const STREAM_NAME: &'static str = "stream_name";

pub struct FfmpegPullStep {
    definition: WorkflowStepDefinition,
    ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    status: StepStatus,
    rtmp_app: String,
    pull_location: String,
    stream_name: String,
    ffmpeg_id: Option<Uuid>,
    active_stream_id: Option<StreamId>,
}

enum FutureResult {
    RtmpEndpointGone,
    FfmpegEndpointGone,
    RtmpEndpointResponseReceived(
        RtmpEndpointPublisherMessage,
        UnboundedReceiver<RtmpEndpointPublisherMessage>,
    ),
    FfmpegNotificationReceived(
        FfmpegEndpointNotification,
        UnboundedReceiver<FfmpegEndpointNotification>,
    ),
}

impl StepFutureResult for FutureResult {}

#[derive(Error, Debug)]
enum StepStartupError {
    #[error("No {} parameter specified", LOCATION)]
    NoLocationSpecified,

    #[error("No {} parameter specified", STREAM_NAME)]
    NoStreamNameSpecified,
}

impl FfmpegPullStep {
    pub fn create_factory_fn(
        rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
        ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    ) -> CreateFactoryFnResult {
        Box::new(move |definition| {
            FfmpegPullStep::new(definition, rtmp_endpoint.clone(), ffmpeg_endpoint.clone())
        })
    }

    pub fn new(
        definition: &WorkflowStepDefinition,
        rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
        ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    ) -> StepCreationResult {
        let location = match definition.parameters.get(LOCATION) {
            Some(value) => value.clone(),
            None => return Err(Box::new(StepStartupError::NoLocationSpecified)),
        };

        let stream_name = match definition.parameters.get(STREAM_NAME) {
            Some(value) => value.clone(),
            None => return Err(Box::new(StepStartupError::NoStreamNameSpecified)),
        };

        let step = FfmpegPullStep {
            definition: definition.clone(),
            status: StepStatus::Created,
            rtmp_app: format!("ffmpeg-pull-{}", definition.get_id()),
            ffmpeg_endpoint: ffmpeg_endpoint.clone(),
            pull_location: location,
            stream_name: stream_name.clone(),
            ffmpeg_id: None,
            active_stream_id: None,
        };

        let (sender, receiver) = unbounded_channel();
        let _ = rtmp_endpoint.send(RtmpEndpointRequest::ListenForPublishers {
            port: 1935,
            rtmp_app: step.rtmp_app.clone(),
            rtmp_stream_key: StreamKeyRegistration::Exact(stream_name),
            stream_id: None,
            message_channel: sender,
            ip_restrictions: IpRestriction::None,
            use_tls: false,
        });

        let futures = vec![
            notify_rtmp_endpoint_gone(rtmp_endpoint).boxed(),
            notify_ffmpeg_endpoint_gone(ffmpeg_endpoint).boxed(),
            wait_for_rtmp_notification(receiver).boxed(),
        ];

        Ok((Box::new(step), futures))
    }

    fn handle_resolved_future(&mut self, result: FutureResult, outputs: &mut StepOutputs) {
        if self.status == StepStatus::Error {
            return;
        }

        match result {
            FutureResult::FfmpegEndpointGone => {
                error!("Step {}: Ffmpeg endpoint is gone", self.definition.get_id());
                self.status = StepStatus::Error;
                self.stop_ffmpeg();
            }

            FutureResult::RtmpEndpointGone => {
                error!("Step {}: Rtmp endpoint gone", self.definition.get_id());
                self.status = StepStatus::Error;
                self.stop_ffmpeg();
            }

            FutureResult::RtmpEndpointResponseReceived(response, receiver) => {
                outputs
                    .futures
                    .push(wait_for_rtmp_notification(receiver).boxed());

                self.handle_rtmp_notification(outputs, response);
            }

            FutureResult::FfmpegNotificationReceived(notification, receiver) => {
                self.handle_ffmpeg_notification(outputs, notification, receiver);
            }
        }
    }

    fn handle_ffmpeg_notification(
        &mut self,
        outputs: &mut StepOutputs,
        message: FfmpegEndpointNotification,
        receiver: UnboundedReceiver<FfmpegEndpointNotification>,
    ) {
        match message {
            FfmpegEndpointNotification::FfmpegFailedToStart { cause } => {
                error!(
                    "Step {}: Ffmpeg failed to start: {:?}",
                    self.definition.get_id(),
                    cause
                );
                self.status = StepStatus::Error;
            }

            FfmpegEndpointNotification::FfmpegStarted => {
                info!("Step {}: Ffmpeg started", self.definition.get_id());
                outputs
                    .futures
                    .push(wait_for_ffmpeg_notification(receiver).boxed());
            }

            FfmpegEndpointNotification::FfmpegStopped => {
                info!("Step {}: Ffmpeg stopped", self.definition.get_id());
            }
        }
    }

    fn handle_rtmp_notification(
        &mut self,
        outputs: &mut StepOutputs,
        message: RtmpEndpointPublisherMessage,
    ) {
        match message {
            RtmpEndpointPublisherMessage::PublisherRegistrationFailed => {
                error!(
                    "Step {}: publisher registration failed",
                    self.definition.get_id()
                );
                self.status = StepStatus::Error;
            }

            RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => {
                info!(
                    "Step {}: publisher registration successful",
                    self.definition.get_id()
                );
                self.status = StepStatus::Active;
                self.start_ffmpeg(outputs);
            }

            RtmpEndpointPublisherMessage::NewPublisherConnected {
                stream_id,
                stream_key,
                connection_id,
            } => {
                info!(
                    "Step {}: new RTMP publisher seen: {:?}, {:?}, {:?}",
                    self.definition.get_id(),
                    stream_id,
                    connection_id,
                    stream_key
                );

                if stream_key != self.stream_name {
                    error!(
                        "Step {}: Expected publisher to have a stream name of {} but instead it \
                    was {}",
                        self.definition.get_id(),
                        self.stream_name,
                        stream_key
                    );

                    self.status = StepStatus::Error;
                    self.stop_ffmpeg();
                }

                self.active_stream_id = Some(stream_id.clone());
                outputs.media.push(MediaNotification {
                    stream_id,
                    content: MediaNotificationContent::NewIncomingStream {
                        stream_name: self.stream_name.clone(),
                    },
                });
            }

            RtmpEndpointPublisherMessage::PublishingStopped { connection_id: _ } => {
                info!(
                    "Step {}: RTMP publisher has stopped",
                    self.definition.get_id()
                );

                if let Some(stream_id) = &self.active_stream_id {
                    outputs.media.push(MediaNotification {
                        stream_id: stream_id.clone(),
                        content: MediaNotificationContent::StreamDisconnected,
                    });
                }
            }

            RtmpEndpointPublisherMessage::StreamMetadataChanged {
                publisher: _,
                metadata,
            } => {
                if let Some(stream_id) = &self.active_stream_id {
                    outputs.media.push(MediaNotification {
                        stream_id: stream_id.clone(),
                        content: MediaNotificationContent::Metadata {
                            data: crate::utils::stream_metadata_to_hash_map(metadata),
                        },
                    });
                } else {
                    error!(
                        "Step {}: Received stream metadata without an active stream id",
                        self.definition.get_id()
                    );
                    self.stop_ffmpeg();
                    self.status = StepStatus::Error;
                }
            }

            RtmpEndpointPublisherMessage::NewVideoData {
                publisher: _,
                data,
                is_keyframe,
                is_sequence_header,
                timestamp,
                codec,
            } => {
                if let Some(stream_id) = &self.active_stream_id {
                    outputs.media.push(MediaNotification {
                        stream_id: stream_id.clone(),
                        content: MediaNotificationContent::Video {
                            codec,
                            timestamp: Duration::from_millis(timestamp.value as u64),
                            is_keyframe,
                            is_sequence_header,
                            data,
                        },
                    });
                } else {
                    error!(
                        "Step {}: Received video data without an active stream id",
                        self.definition.get_id()
                    );
                    self.stop_ffmpeg();
                    self.status = StepStatus::Error;
                }
            }

            RtmpEndpointPublisherMessage::NewAudioData {
                publisher: _,
                data,
                is_sequence_header,
                timestamp,
                codec,
            } => {
                if let Some(stream_id) = &self.active_stream_id {
                    outputs.media.push(MediaNotification {
                        stream_id: stream_id.clone(),
                        content: MediaNotificationContent::Audio {
                            codec,
                            timestamp: Duration::from_millis(timestamp.value as u64),
                            is_sequence_header,
                            data,
                        },
                    });
                } else {
                    error!(
                        "Step {}: Received audio data without an active stream id",
                        self.definition.get_id()
                    );
                    self.stop_ffmpeg();
                    self.status = StepStatus::Error;
                }
            }
        }
    }

    fn start_ffmpeg(&mut self, outputs: &mut StepOutputs) {
        if self.ffmpeg_id.is_none() {
            info!("Step {}: Starting ffmpeg", self.definition.get_id());
            let id = Uuid::new_v4();
            let (sender, receiver) = unbounded_channel();
            let _ = self
                .ffmpeg_endpoint
                .send(FfmpegEndpointRequest::StartFfmpeg {
                    id: id.clone(),
                    notification_channel: sender,
                    params: FfmpegParams {
                        read_in_real_time: true,
                        input: self.pull_location.clone(),
                        video_transcode: VideoTranscodeParams::Copy,
                        audio_transcode: AudioTranscodeParams::Copy,
                        scale: None,
                        bitrate_in_kbps: None,
                        target: TargetParams::Rtmp {
                            url: format!("rtmp://localhost/{}/{}", self.rtmp_app, self.stream_name),
                        },
                    },
                });

            outputs
                .futures
                .push(wait_for_ffmpeg_notification(receiver).boxed());
        }
    }

    fn stop_ffmpeg(&mut self) {
        if let Some(id) = &self.ffmpeg_id {
            let _ = self
                .ffmpeg_endpoint
                .send(FfmpegEndpointRequest::StopFfmpeg { id: id.clone() });
        }

        self.ffmpeg_id = None;
    }
}

impl WorkflowStep for FfmpegPullStep {
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

        for result in inputs.notifications.drain(..) {
            if let Ok(result) = result.downcast::<FutureResult>() {
                self.handle_resolved_future(*result, outputs);
            }
        }
    }
}

async fn notify_rtmp_endpoint_gone(
    endpoint: UnboundedSender<RtmpEndpointRequest>,
) -> Box<dyn StepFutureResult> {
    endpoint.closed().await;

    Box::new(FutureResult::RtmpEndpointGone)
}

async fn notify_ffmpeg_endpoint_gone(
    endpoint: UnboundedSender<FfmpegEndpointRequest>,
) -> Box<dyn StepFutureResult> {
    endpoint.closed().await;

    Box::new(FutureResult::FfmpegEndpointGone)
}

async fn wait_for_rtmp_notification(
    mut receiver: UnboundedReceiver<RtmpEndpointPublisherMessage>,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(msg) => FutureResult::RtmpEndpointResponseReceived(msg, receiver),
        None => FutureResult::RtmpEndpointGone,
    };

    Box::new(result)
}

async fn wait_for_ffmpeg_notification(
    mut receiver: UnboundedReceiver<FfmpegEndpointNotification>,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(msg) => FutureResult::FfmpegNotificationReceived(msg, receiver),
        None => FutureResult::FfmpegEndpointGone,
    };

    Box::new(result)
}
