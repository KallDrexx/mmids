use crate::endpoints::ffmpeg::{
    AudioTranscodeParams, FfmpegEndpointNotification, FfmpegEndpointRequest, FfmpegParams,
    TargetParams, VideoTranscodeParams,
};
use crate::endpoints::rtmp_server::{
    RtmpEndpointMediaMessage, RtmpEndpointRequest, RtmpEndpointWatcherNotification,
    StreamKeyRegistration,
};
use crate::workflows::definitions::WorkflowStepDefinition;
use crate::workflows::steps::{
    CreateFactoryFnResult, StepCreationResult, StepFutureResult, StepInputs, StepOutputs,
    StepStatus, WorkflowStep,
};
use crate::workflows::{MediaNotification, MediaNotificationContent};
use crate::StreamId;
use futures::FutureExt;
use log::{error, info, warn};
use std::collections::{HashMap, VecDeque};
use thiserror::Error;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

const PATH: &str = "path";
const SEGMENT_DURATION: &str = "duration";
const SEGMENT_COUNT: &str = "count";

pub struct FfmpegHlsStep {
    definition: WorkflowStepDefinition,
    ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    rtmp_server_endpoint: UnboundedSender<RtmpEndpointRequest>,
    active_streams: HashMap<StreamId, ActiveStream>,
    status: StepStatus,
    path: String,
    duration: u16,
    segment_count: Option<u16>,
}

#[derive(Debug)]
enum WatchRegistrationStatus {
    Inactive,
    Pending {
        media_channel: UnboundedSender<RtmpEndpointMediaMessage>,
    },
    Active {
        media_channel: UnboundedSender<RtmpEndpointMediaMessage>,
    },
}

#[derive(Debug)]
enum FfmpegStatus {
    Inactive,
    Pending { id: Uuid },
    Active { id: Uuid },
}

struct ActiveStream {
    id: StreamId,
    stream_name: String,
    pending_media: VecDeque<MediaNotificationContent>,
    rtmp_output_status: WatchRegistrationStatus,
    ffmpeg_status: FfmpegStatus,
}

enum FutureResult {
    RtmpEndpointGone,
    FfmpegEndpointGone,
    RtmpWatchNotificationReceived(
        StreamId,
        RtmpEndpointWatcherNotification,
        UnboundedReceiver<RtmpEndpointWatcherNotification>,
    ),
    RtmpWatchChannelGone(StreamId),
    FfmpegNotificationReceived(
        StreamId,
        FfmpegEndpointNotification,
        UnboundedReceiver<FfmpegEndpointNotification>,
    ),
    FfmpegChannelGone(StreamId),
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
                    )))
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
                    )))
                }
            },

            None => 0,
        };

        let step = FfmpegHlsStep {
            definition: definition.clone(),
            rtmp_server_endpoint: rtmp_endpoint.clone(),
            ffmpeg_endpoint: ffmpeg_endpoint.clone(),
            active_streams: HashMap::new(),
            status: StepStatus::Active,
            path: path.clone(),
            segment_count: Some(count),
            duration,
        };

        let futures = vec![
            notify_when_ffmpeg_endpoint_is_gone(ffmpeg_endpoint).boxed(),
            notify_when_rtmp_endpoint_is_gone(rtmp_endpoint).boxed(),
        ];

        Ok((Box::new(step), futures))
    }

    fn get_rtmp_app(&self) -> String {
        format!("ffmpeg-hls-{}", self.definition.get_id())
    }

    fn handle_resolved_future(
        &mut self,
        notification: Box<dyn StepFutureResult>,
        outputs: &mut StepOutputs,
    ) {
        if self.status == StepStatus::Error {
            return;
        }

        let notification = match notification.downcast::<FutureResult>() {
            Ok(x) => *x,
            Err(_) => return,
        };

        match notification {
            FutureResult::FfmpegEndpointGone => {
                error!(
                    "Step {}: Ffmpeg endpoint is gone!",
                    self.definition.get_id()
                );
                self.status = StepStatus::Error;

                let ids: Vec<StreamId> = self.active_streams.keys().map(|x| x.clone()).collect();
                for id in ids {
                    self.stop_stream(&id);
                }
            }

            FutureResult::RtmpEndpointGone => {
                error!("Step {}: RTMP endpoint is gone!", self.definition.get_id());
                self.status = StepStatus::Error;

                let ids: Vec<StreamId> = self.active_streams.keys().map(|x| x.clone()).collect();
                for id in ids {
                    self.stop_stream(&id);
                }
            }

            FutureResult::RtmpWatchChannelGone(stream_id) => {
                if self.stop_stream(&stream_id) {
                    error!(
                        "Step {}: Rtmp watch channel disappeared for stream id {:?}",
                        self.definition.get_id(),
                        stream_id
                    );
                }
            }

            FutureResult::FfmpegChannelGone(stream_id) => {
                if self.stop_stream(&stream_id) {
                    error!(
                        "Step {}: Ffmpeg channel disappeared for stream id {:?}",
                        self.definition.get_id(),
                        stream_id
                    );
                }
            }

            FutureResult::RtmpWatchNotificationReceived(stream_id, notification, receiver) => {
                if !self.active_streams.contains_key(&stream_id) {
                    // late notification after stopping a stream
                    return;
                }

                outputs
                    .futures
                    .push(wait_for_watch_notification(stream_id.clone(), receiver).boxed());
                self.handle_rtmp_watch_notification(stream_id, notification, outputs);
            }

            FutureResult::FfmpegNotificationReceived(stream_id, notification, receiver) => {
                if !self.active_streams.contains_key(&stream_id) {
                    // late notification after stopping a stream
                    return;
                }

                outputs
                    .futures
                    .push(wait_for_ffmpeg_notification(stream_id.clone(), receiver).boxed());
                self.handle_ffmpeg_notification(stream_id, notification, outputs);
            }
        }
    }

    fn handle_media(&mut self, media: MediaNotification, outputs: &mut StepOutputs) {
        if self.status == StepStatus::Error {
            return;
        }

        match &media.content {
            MediaNotificationContent::NewIncomingStream { stream_name } => {
                if let Some(stream) = self.active_streams.get(&media.stream_id) {
                    if &stream.stream_name != stream_name {
                        warn!("Step {}: Unexpected new incoming stream notification received on \
                        stream id {:?} and stream name '{}', but we already have this stream id active \
                        for stream name '{}'.  Ignoring this notification",
                            self.definition.get_id(), media.stream_id, stream_name, stream.stream_name);
                    } else {
                        // Since the stream id / name combination is already set, this is a duplicate
                        // notification.  This is probably a bug somewhere but it's not harmful
                        // to ignore
                    }

                    return;
                }

                let stream = ActiveStream {
                    id: media.stream_id.clone(),
                    stream_name: stream_name.clone(),
                    pending_media: VecDeque::new(),
                    rtmp_output_status: WatchRegistrationStatus::Inactive,
                    ffmpeg_status: FfmpegStatus::Inactive,
                };

                self.active_streams.insert(media.stream_id.clone(), stream);
                self.prepare_stream(media.stream_id.clone(), outputs);
            }

            MediaNotificationContent::StreamDisconnected => {
                if self.stop_stream(&media.stream_id) {
                    info!(
                        "Step {}: Stopping stream id {:?} due to stream disconnection notification",
                        self.definition.get_id(),
                        media.stream_id
                    );
                }
            }

            _ => {
                if let Some(stream) = self.active_streams.get_mut(&media.stream_id) {
                    if let WatchRegistrationStatus::Active { media_channel } =
                        &stream.rtmp_output_status
                    {
                        if let Some(media_data) = media.content.to_rtmp_media_data() {
                            let _ = media_channel.send(RtmpEndpointMediaMessage {
                                stream_key: stream.id.0.clone(),
                                data: media_data,
                            });
                        }
                    } else {
                        stream.pending_media.push_back(media.content.clone());
                    }
                }
            }
        }

        outputs.media.push(media);
    }

    fn prepare_stream(&mut self, stream_id: StreamId, outputs: &mut StepOutputs) {
        let rtmp_app = self.get_rtmp_app();

        if let Some(stream) = self.active_streams.get_mut(&stream_id) {
            let output_is_active = match &stream.rtmp_output_status {
                WatchRegistrationStatus::Inactive => {
                    let (media_sender, media_receiver) = unbounded_channel();
                    let (watch_sender, watch_receiver) = unbounded_channel();
                    let _ =
                        self.rtmp_server_endpoint
                            .send(RtmpEndpointRequest::ListenForWatchers {
                                notification_channel: watch_sender,
                                rtmp_app: rtmp_app.clone(),
                                rtmp_stream_key: StreamKeyRegistration::Exact(stream.id.0.clone()),
                                port: 1935,
                                media_channel: media_receiver,
                            });

                    outputs.futures.push(
                        wait_for_watch_notification(stream.id.clone(), watch_receiver).boxed(),
                    );
                    stream.rtmp_output_status = WatchRegistrationStatus::Pending {
                        media_channel: media_sender,
                    };

                    false
                }

                WatchRegistrationStatus::Pending { media_channel: _ } => false,
                WatchRegistrationStatus::Active { media_channel: _ } => true,
            };

            match &stream.ffmpeg_status {
                FfmpegStatus::Inactive => {
                    // Not worth starting ffmpeg until both input and outputs registrations are complete
                    if output_is_active {
                        let parameters = FfmpegParams {
                            read_in_real_time: true,
                            input: format!("rtmp://localhost/{}/{}", rtmp_app, stream.id.0),
                            video_transcode: VideoTranscodeParams::Copy,
                            audio_transcode: AudioTranscodeParams::Copy,
                            scale: None,
                            bitrate_in_kbps: None,
                            target: TargetParams::Hls {
                                path: format!("{}/{}.m3u8", self.path, stream.stream_name),
                                max_entries: self.segment_count,
                                segment_length: self.duration,
                            },
                        };

                        let id = Uuid::new_v4();
                        let (sender, receiver) = unbounded_channel();
                        let _ = self
                            .ffmpeg_endpoint
                            .send(FfmpegEndpointRequest::StartFfmpeg {
                                id: id.clone(),
                                params: parameters,
                                notification_channel: sender,
                            });

                        outputs.futures.push(
                            wait_for_ffmpeg_notification(stream.id.clone(), receiver).boxed(),
                        );
                        stream.ffmpeg_status = FfmpegStatus::Pending { id };
                    }
                }

                _ => (),
            }
        }
    }

    fn stop_stream(&mut self, stream_id: &StreamId) -> bool {
        if let Some(stream) = self.active_streams.remove(stream_id) {
            match &stream.ffmpeg_status {
                FfmpegStatus::Pending { id } => {
                    let _ = self
                        .ffmpeg_endpoint
                        .send(FfmpegEndpointRequest::StopFfmpeg { id: id.clone() });
                }

                FfmpegStatus::Active { id } => {
                    let _ = self
                        .ffmpeg_endpoint
                        .send(FfmpegEndpointRequest::StopFfmpeg { id: id.clone() });
                }

                FfmpegStatus::Inactive => (),
            }

            // TODO: Add support for manually unregistering from rtmp endpoints

            return true;
        }

        return false;
    }

    fn handle_rtmp_watch_notification(
        &mut self,
        stream_id: StreamId,
        notification: RtmpEndpointWatcherNotification,
        outputs: &mut StepOutputs,
    ) {
        if let Some(stream) = self.active_streams.get_mut(&stream_id) {
            match notification {
                RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful => {
                    let new_status = match &stream.rtmp_output_status {
                        WatchRegistrationStatus::Pending { media_channel } => {
                            info!(
                                "Step {}: Watch registration successful for stream id {:?}",
                                self.definition.get_id(),
                                stream.id
                            );
                            Some(WatchRegistrationStatus::Active {
                                media_channel: media_channel.clone(),
                            })
                        }

                        status => {
                            error!("Step {}: Received watch registration successful notification for stream id \
                            {:?}, but this stream's watch status is {:?}", self.definition.get_id(), stream.id, status);

                            None
                        }
                    };

                    if let Some(new_status) = new_status {
                        stream.rtmp_output_status = new_status;
                    }
                }

                RtmpEndpointWatcherNotification::WatcherRegistrationFailed => {
                    warn!(
                        "Step {}: Received watch registration failed for stream id {:?}",
                        self.definition.get_id(),
                        stream.id
                    );
                    stream.rtmp_output_status = WatchRegistrationStatus::Inactive;
                }

                RtmpEndpointWatcherNotification::StreamKeyBecameActive { stream_key: _ } => (),
                RtmpEndpointWatcherNotification::StreamKeyBecameInactive { stream_key: _ } => (),
            }
        }

        self.prepare_stream(stream_id, outputs);
    }

    fn handle_ffmpeg_notification(
        &mut self,
        stream_id: StreamId,
        notification: FfmpegEndpointNotification,
        outputs: &mut StepOutputs,
    ) {
        if let Some(stream) = self.active_streams.get_mut(&stream_id) {
            match notification {
                FfmpegEndpointNotification::FfmpegStarted => {
                    let new_status = match &stream.ffmpeg_status {
                        FfmpegStatus::Pending { id } => {
                            info!("Step {}: Received notification that ffmpeg became active for stream id \
                        {:?} with ffmpeg id {}", self.definition.get_id(), stream.id, id);

                            Some(FfmpegStatus::Active { id: id.clone() })
                        }

                        status => {
                            error!("Step {}: Received notification that ffmpeg became active for stream id \
                        {:?}, but this stream was in the {:?} status instead of pending", self.definition.get_id(), stream.id, status);

                            None
                        }
                    };

                    if let Some(new_status) = new_status {
                        stream.ffmpeg_status = new_status;
                    }
                }

                FfmpegEndpointNotification::FfmpegStopped => {
                    info!(
                        "Step {}: Got ffmpeg stopped notification for stream {:?}",
                        self.definition.get_id(),
                        stream.id
                    );
                    stream.ffmpeg_status = FfmpegStatus::Inactive;
                }

                FfmpegEndpointNotification::FfmpegFailedToStart { cause } => {
                    warn!(
                        "Step {}: Ffmpeg failed to start for stream {:?}: {:?}",
                        self.definition.get_id(),
                        stream.id,
                        cause
                    );
                    stream.ffmpeg_status = FfmpegStatus::Inactive;
                }
            }
        }

        self.prepare_stream(stream_id, outputs);
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

        for notification in inputs.notifications.drain(..) {
            self.handle_resolved_future(notification, outputs);
        }

        for media in inputs.media.drain(..) {
            self.handle_media(media, outputs);
        }
    }
}

async fn notify_when_ffmpeg_endpoint_is_gone(
    endpoint: UnboundedSender<FfmpegEndpointRequest>,
) -> Box<dyn StepFutureResult> {
    endpoint.closed().await;

    Box::new(FutureResult::FfmpegEndpointGone)
}

async fn notify_when_rtmp_endpoint_is_gone(
    endpoint: UnboundedSender<RtmpEndpointRequest>,
) -> Box<dyn StepFutureResult> {
    endpoint.closed().await;

    Box::new(FutureResult::RtmpEndpointGone)
}

async fn wait_for_watch_notification(
    stream_id: StreamId,
    mut receiver: UnboundedReceiver<RtmpEndpointWatcherNotification>,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(msg) => FutureResult::RtmpWatchNotificationReceived(stream_id, msg, receiver),
        None => FutureResult::RtmpWatchChannelGone(stream_id),
    };

    Box::new(result)
}

async fn wait_for_ffmpeg_notification(
    stream_id: StreamId,
    mut receiver: UnboundedReceiver<FfmpegEndpointNotification>,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(msg) => FutureResult::FfmpegNotificationReceived(stream_id, msg, receiver),
        None => FutureResult::FfmpegChannelGone(stream_id),
    };

    Box::new(result)
}
