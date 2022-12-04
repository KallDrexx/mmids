//! This workflow step utilizes ffmpeg to read video from an external source.  The external source
//! can be a remote RTMP server or a file on the file system.  If ffmpeg closes (such as when the
//! video file has been fully streamed) then the ffmpeg will restart until the workflow is
//! removed.
//!
//! Media packets that come in from previous steps are ignored.

use crate::endpoint::{
    AudioTranscodeParams, FfmpegEndpointNotification, FfmpegEndpointRequest, FfmpegParams,
    TargetParams, VideoTranscodeParams,
};
use bytes::BytesMut;
use futures::FutureExt;
use mmids_core::codecs::AUDIO_CODEC_AAC_RAW;
use mmids_core::workflows::definitions::WorkflowStepDefinition;
use mmids_core::workflows::metadata::MediaPayloadMetadataCollection;
use mmids_core::workflows::steps::factory::StepGenerator;
use mmids_core::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use mmids_core::workflows::{MediaNotification, MediaNotificationContent, MediaType};
use mmids_core::StreamId;
use mmids_rtmp::rtmp_server::{
    IpRestriction, RegistrationType, RtmpEndpointPublisherMessage, RtmpEndpointRequest,
    StreamKeyRegistration,
};
use mmids_rtmp::utils::video_timestamp_from_rtmp_data;
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info};
use uuid::Uuid;

pub const LOCATION: &str = "location";
pub const STREAM_NAME: &str = "stream_name";

/// Generates new instances of the ffmpeg pull workflow step based on specified step definitions.
pub struct FfmpegPullStepGenerator {
    rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
    ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
}

struct FfmpegPullStep {
    definition: WorkflowStepDefinition,
    ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
    status: StepStatus,
    rtmp_app: Arc<String>,
    pull_location: String,
    stream_name: Arc<String>,
    ffmpeg_id: Option<Uuid>,
    active_stream_id: Option<StreamId>,
    metadata_buffer: BytesMut,
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

impl FfmpegPullStepGenerator {
    pub fn new(
        rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
        ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    ) -> Self {
        FfmpegPullStepGenerator {
            rtmp_endpoint,
            ffmpeg_endpoint,
        }
    }
}

impl StepGenerator for FfmpegPullStepGenerator {
    fn generate(&self, definition: WorkflowStepDefinition) -> StepCreationResult {
        let location = match definition.parameters.get(LOCATION) {
            Some(Some(value)) => value.clone(),
            _ => return Err(Box::new(StepStartupError::NoLocationSpecified)),
        };

        let stream_name = match definition.parameters.get(STREAM_NAME) {
            Some(Some(value)) => Arc::new(value.clone()),
            _ => return Err(Box::new(StepStartupError::NoStreamNameSpecified)),
        };

        let step = FfmpegPullStep {
            definition: definition.clone(),
            status: StepStatus::Created,
            rtmp_app: Arc::new(format!("ffmpeg-pull-{}", definition.get_id())),
            ffmpeg_endpoint: self.ffmpeg_endpoint.clone(),
            rtmp_endpoint: self.rtmp_endpoint.clone(),
            pull_location: location,
            stream_name: stream_name.clone(),
            ffmpeg_id: None,
            active_stream_id: None,
            metadata_buffer: BytesMut::new(),
        };

        let (sender, receiver) = unbounded_channel();
        let _ = self
            .rtmp_endpoint
            .send(RtmpEndpointRequest::ListenForPublishers {
                port: 1935,
                rtmp_app: step.rtmp_app.clone(),
                rtmp_stream_key: StreamKeyRegistration::Exact(stream_name),
                stream_id: None,
                message_channel: sender,
                ip_restrictions: IpRestriction::None,
                use_tls: false,
                requires_registrant_approval: false,
            });

        let futures = vec![
            notify_rtmp_endpoint_gone(self.rtmp_endpoint.clone()).boxed(),
            notify_ffmpeg_endpoint_gone(self.ffmpeg_endpoint.clone()).boxed(),
            wait_for_rtmp_notification(receiver).boxed(),
        ];

        Ok((Box::new(step), futures))
    }
}

impl FfmpegPullStep {
    fn handle_resolved_future(&mut self, result: FutureResult, outputs: &mut StepOutputs) {
        match result {
            FutureResult::FfmpegEndpointGone => {
                error!("Ffmpeg endpoint is gone");
                self.status = StepStatus::Error {
                    message: "Ffmpeg endpoint is gone".to_string(),
                };
                self.stop_ffmpeg();
            }

            FutureResult::RtmpEndpointGone => {
                error!("Rtmp endpoint gone");
                self.status = StepStatus::Error {
                    message: "Rtmp endpoint gone".to_string(),
                };
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
                error!("Ffmpeg failed to start: {:?}", cause);
                self.status = StepStatus::Error {
                    message: format!("Ffmpeg failed to start: {:?}", cause),
                };
            }

            FfmpegEndpointNotification::FfmpegStarted => {
                info!("Ffmpeg started");
                outputs
                    .futures
                    .push(wait_for_ffmpeg_notification(receiver).boxed());
            }

            FfmpegEndpointNotification::FfmpegStopped => {
                info!("Ffmpeg stopped");
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
                error!("Publisher registration failed");
                self.status = StepStatus::Error {
                    message: "Publisher registration failed".to_string(),
                };
            }

            RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => {
                info!("Publisher registration successful");
                self.status = StepStatus::Active;
                self.start_ffmpeg(outputs);
            }

            RtmpEndpointPublisherMessage::NewPublisherConnected {
                stream_id,
                stream_key,
                connection_id,
                reactor_update_channel: _,
            } => {
                info!(
                    stream_id = ?stream_id,
                    connection_id = ?connection_id,
                    stream_key = %stream_key,
                    "New RTMP publisher seen: {:?}, {:?}, {:?}", stream_id, connection_id, stream_key
                );

                if stream_key != self.stream_name {
                    error!(
                        stream_name = %self.stream_name,
                        stream_key = %stream_key,
                        "Expected publisher to have a stream name of {} but instead it was {}", self.stream_name, stream_key
                    );

                    self.status = StepStatus::Error {
                        message: format!(
                            "Expected publisher to have a stream name of {} but instead it was {}",
                            self.stream_name, stream_key
                        ),
                    };

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
                info!("RTMP publisher has stopped");
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
                            data: mmids_rtmp::utils::stream_metadata_to_hash_map(metadata),
                        },
                    });
                } else {
                    error!("Received stream metadata without an active stream id");
                    self.stop_ffmpeg();
                    self.status = StepStatus::Error {
                        message: "Received stream metadata without an active stream id".to_string(),
                    };
                }
            }

            RtmpEndpointPublisherMessage::NewVideoData {
                publisher: _,
                data,
                is_keyframe,
                is_sequence_header,
                timestamp,
                codec,
                composition_time_offset,
            } => {
                if let Some(stream_id) = &self.active_stream_id {
                    outputs.media.push(MediaNotification {
                        stream_id: stream_id.clone(),
                        content: MediaNotificationContent::Video {
                            codec,
                            timestamp: video_timestamp_from_rtmp_data(
                                timestamp,
                                composition_time_offset,
                            ),
                            is_keyframe,
                            is_sequence_header,
                            data,
                        },
                    });
                } else {
                    error!("Received video data without an active stream id");
                    self.stop_ffmpeg();
                    self.status = StepStatus::Error {
                        message: "Received video data without an active stream id".to_string(),
                    };
                }
            }

            RtmpEndpointPublisherMessage::NewAudioData {
                publisher: _,
                data,
                is_sequence_header,
                timestamp,
            } => {
                if let Some(stream_id) = &self.active_stream_id {
                    outputs.media.push(MediaNotification {
                        stream_id: stream_id.clone(),
                        content: MediaNotificationContent::MediaPayload {
                            timestamp: Duration::from_millis(timestamp.value as u64),
                            is_required_for_decoding: is_sequence_header,
                            data,
                            media_type: MediaType::Audio,
                            payload_type: AUDIO_CODEC_AAC_RAW.clone(),
                            metadata: MediaPayloadMetadataCollection::new(
                                iter::empty(),
                                &mut self.metadata_buffer,
                            ),
                        },
                    });
                } else {
                    error!("Received audio data without an active stream id");
                    self.stop_ffmpeg();
                    self.status = StepStatus::Error {
                        message: "Received audio data without an active stream id".to_string(),
                    };
                }
            }

            RtmpEndpointPublisherMessage::PublisherRequiringApproval { .. } => {
                error!("Publisher approval requested but publishers should be auto-approved");
                self.status = StepStatus::Error {
                    message: "Publisher approval requested but publishers should be auto-approved"
                        .to_string(),
                };
            }
        }
    }

    fn start_ffmpeg(&mut self, outputs: &mut StepOutputs) {
        if self.ffmpeg_id.is_none() {
            info!("Starting ffmpeg");
            let id = Uuid::new_v4();
            let (sender, receiver) = unbounded_channel();
            let _ = self
                .ffmpeg_endpoint
                .send(FfmpegEndpointRequest::StartFfmpeg {
                    id,
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
                .send(FfmpegEndpointRequest::StopFfmpeg { id: *id });
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
        for result in inputs.notifications.drain(..) {
            if let Ok(result) = result.downcast::<FutureResult>() {
                self.handle_resolved_future(*result, outputs);
            }
        }
    }

    fn shutdown(&mut self) {
        self.status = StepStatus::Shutdown;
        self.stop_ffmpeg();

        let _ = self
            .rtmp_endpoint
            .send(RtmpEndpointRequest::RemoveRegistration {
                registration_type: RegistrationType::Publisher,
                port: 1935,
                rtmp_app: self.rtmp_app.clone(),
                rtmp_stream_key: StreamKeyRegistration::Exact(self.stream_name.clone()),
            });
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
