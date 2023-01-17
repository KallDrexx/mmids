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
use mmids_core::codecs::{AUDIO_CODEC_AAC_RAW, VIDEO_CODEC_H264_AVC};
use mmids_core::workflows::definitions::WorkflowStepDefinition;
use mmids_core::workflows::metadata::{
    MediaPayloadMetadataCollection, MetadataEntry, MetadataKey, MetadataValue,
};
use mmids_core::workflows::steps::factory::StepGenerator;
use mmids_core::workflows::steps::futures_channel::WorkflowStepFuturesChannel;
use mmids_core::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use mmids_core::workflows::{MediaNotification, MediaNotificationContent, MediaType};
use mmids_core::StreamId;
use mmids_rtmp::rtmp_server::{
    IpRestriction, RegistrationType, RtmpEndpointPublisherMessage, RtmpEndpointRequest,
    StreamKeyRegistration,
};
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tracing::{error, info};
use uuid::Uuid;

pub const LOCATION: &str = "location";
pub const STREAM_NAME: &str = "stream_name";

/// Generates new instances of the ffmpeg pull workflow step based on specified step definitions.
pub struct FfmpegPullStepGenerator {
    rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
    ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    is_keyframe_metadata_key: MetadataKey,
    pts_offset_metadata_key: MetadataKey,
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
    is_keyframe_metadata_key: MetadataKey,
    pts_offset_metadata_key: MetadataKey,
}

enum FutureResult {
    RtmpEndpointGone,
    FfmpegEndpointGone,
    RtmpEndpointResponseReceived(RtmpEndpointPublisherMessage),
    FfmpegNotificationReceived(FfmpegEndpointNotification),
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
        is_keyframe_metadata_key: MetadataKey,
        pts_offset_metadata_key: MetadataKey,
    ) -> Self {
        FfmpegPullStepGenerator {
            rtmp_endpoint,
            ffmpeg_endpoint,
            is_keyframe_metadata_key,
            pts_offset_metadata_key,
        }
    }
}

impl StepGenerator for FfmpegPullStepGenerator {
    fn generate(
        &self,
        definition: WorkflowStepDefinition,
        futures_channel: WorkflowStepFuturesChannel,
    ) -> StepCreationResult {
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
            is_keyframe_metadata_key: self.is_keyframe_metadata_key,
            pts_offset_metadata_key: self.pts_offset_metadata_key,
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

        let ffmpeg_endpoint = self.ffmpeg_endpoint.clone();
        futures_channel.send_on_generic_future_completion(async move {
            ffmpeg_endpoint.closed().await;
            FutureResult::FfmpegEndpointGone
        });

        futures_channel.send_on_generic_unbounded_recv(
            receiver,
            FutureResult::RtmpEndpointResponseReceived,
            || FutureResult::RtmpEndpointGone,
        );

        Ok(Box::new(step))
    }
}

impl FfmpegPullStep {
    fn handle_resolved_future(
        &mut self,
        result: FutureResult,
        outputs: &mut StepOutputs,
        futures_channel: &WorkflowStepFuturesChannel,
    ) {
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

            FutureResult::RtmpEndpointResponseReceived(response) => {
                self.handle_rtmp_notification(outputs, response, futures_channel);
            }

            FutureResult::FfmpegNotificationReceived(notification) => {
                self.handle_ffmpeg_notification(notification);
            }
        }
    }

    fn handle_ffmpeg_notification(&mut self, message: FfmpegEndpointNotification) {
        match message {
            FfmpegEndpointNotification::FfmpegFailedToStart { cause } => {
                error!("Ffmpeg failed to start: {:?}", cause);
                self.status = StepStatus::Error {
                    message: format!("Ffmpeg failed to start: {:?}", cause),
                };
            }

            FfmpegEndpointNotification::FfmpegStarted => {
                info!("Ffmpeg started");
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
        futures_channel: &WorkflowStepFuturesChannel,
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
                self.start_ffmpeg(futures_channel);
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
                composition_time_offset,
            } => {
                if let Some(stream_id) = &self.active_stream_id {
                    let is_keyframe_metadata = MetadataEntry::new(
                        self.is_keyframe_metadata_key,
                        MetadataValue::Bool(is_keyframe),
                        &mut self.metadata_buffer,
                    )
                    .unwrap(); // Should only happen if type mismatch occurs

                    let pts_offset_metadata = MetadataEntry::new(
                        self.pts_offset_metadata_key,
                        MetadataValue::I32(composition_time_offset),
                        &mut self.metadata_buffer,
                    )
                    .unwrap(); // Should only happen if type mismatch occurs

                    let metadata = MediaPayloadMetadataCollection::new(
                        [is_keyframe_metadata, pts_offset_metadata].into_iter(),
                        &mut self.metadata_buffer,
                    );

                    outputs.media.push(MediaNotification {
                        stream_id: stream_id.clone(),
                        content: MediaNotificationContent::MediaPayload {
                            media_type: MediaType::Video,
                            payload_type: VIDEO_CODEC_H264_AVC.clone(),
                            is_required_for_decoding: is_sequence_header,
                            timestamp: Duration::from_millis(timestamp.value.into()),
                            metadata,
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

    fn start_ffmpeg(&mut self, futures_channel: &WorkflowStepFuturesChannel) {
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

            futures_channel.send_on_generic_unbounded_recv(
                receiver,
                FutureResult::FfmpegNotificationReceived,
                || FutureResult::FfmpegEndpointGone,
            );
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

    fn execute(
        &mut self,
        inputs: &mut StepInputs,
        outputs: &mut StepOutputs,
        futures_channel: WorkflowStepFuturesChannel,
    ) {
        for result in inputs.notifications.drain(..) {
            if let Ok(result) = result.downcast::<FutureResult>() {
                self.handle_resolved_future(*result, outputs, &futures_channel);
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
