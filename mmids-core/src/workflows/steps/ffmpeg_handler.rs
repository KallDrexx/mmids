use crate::endpoints::ffmpeg::{FfmpegEndpointNotification, FfmpegEndpointRequest, FfmpegParams};
use crate::workflows::steps::external_stream_handler::{
    ExternalStreamHandler, ExternalStreamHandlerGenerator, ResolvedFutureStatus,
    StreamHandlerFutureResult, StreamHandlerFutureWrapper,
};
use crate::workflows::steps::{StepFutureResult, StepOutputs};
use crate::StreamId;
use futures::FutureExt;
use log::{error, info, warn};
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

pub struct FfmpegHandler {
    ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    status: FfmpegHandlerStatus,
    param_generator: Arc<Box<dyn FfmpegParameterGenerator + Sync + Send>>,
    stream_id: StreamId,
}

pub struct FfmpegHandlerGenerator {
    ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    param_generator: Arc<Box<dyn FfmpegParameterGenerator + Sync + Send>>,
}

pub trait FfmpegParameterGenerator {
    fn form_parameters(&self, stream_id: &StreamId, stream_name: &String) -> FfmpegParams;
}

#[derive(Debug)]
enum FfmpegHandlerStatus {
    Inactive,
    Pending { id: Uuid },
    Active { id: Uuid },
}

enum FutureResult {
    FfmpegChannelGone,
    NotificationReceived(
        FfmpegEndpointNotification,
        UnboundedReceiver<FfmpegEndpointNotification>,
    ),
}

impl StreamHandlerFutureResult for FutureResult {}

impl FfmpegHandlerGenerator {
    pub fn new(
        ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
        param_generator: Box<dyn FfmpegParameterGenerator + Sync + Send>,
    ) -> Self {
        FfmpegHandlerGenerator {
            ffmpeg_endpoint,
            param_generator: Arc::new(param_generator),
        }
    }
}

impl ExternalStreamHandlerGenerator for FfmpegHandlerGenerator {
    fn generate(&mut self, stream_id: StreamId) -> Box<dyn ExternalStreamHandler + Sync + Send> {
        Box::new(FfmpegHandler {
            ffmpeg_endpoint: self.ffmpeg_endpoint.clone(),
            param_generator: self.param_generator.clone(),
            stream_id,
            status: FfmpegHandlerStatus::Inactive,
        })
    }
}

impl FfmpegHandler {
    fn handle_ffmpeg_notification(&mut self, notification: FfmpegEndpointNotification) {
        match notification {
            FfmpegEndpointNotification::FfmpegStarted => {
                match &self.status {
                    FfmpegHandlerStatus::Pending { id } => {
                        info!("Received notification that ffmpeg became active for stream id {:?} and \
                       ffmpeg id {}", self.stream_id, id);

                        self.status = FfmpegHandlerStatus::Active { id: id.clone() };
                    }

                    status => {
                        error!(
                            "Received notification that ffmpeg became active for stream id {:?}, \
                        but the handler's status was {:?} instead of pending",
                            self.stream_id, status
                        );
                    }
                }
            }

            FfmpegEndpointNotification::FfmpegStopped => {
                info!(
                    "Received ffmpeg stopped notification for stream {:?}",
                    self.stream_id
                );
                self.status = FfmpegHandlerStatus::Inactive;
            }

            FfmpegEndpointNotification::FfmpegFailedToStart { cause } => {
                warn!(
                    "Ffmpeg failed to start for stream {:?}: {:?}",
                    self.stream_id, cause
                );
                self.status = FfmpegHandlerStatus::Inactive;
            }
        }
    }
}

impl ExternalStreamHandler for FfmpegHandler {
    fn prepare_stream(
        &mut self,
        stream_id: &StreamId,
        stream_name: &String,
        outputs: &mut StepOutputs,
    ) {
        match &self.status {
            FfmpegHandlerStatus::Inactive => {
                let parameters = self.param_generator.form_parameters(stream_id, stream_name);
                let id = Uuid::new_v4();
                let (sender, receiver) = unbounded_channel();
                let _ = self
                    .ffmpeg_endpoint
                    .send(FfmpegEndpointRequest::StartFfmpeg {
                        id: id.clone(),
                        params: parameters,
                        notification_channel: sender,
                    });

                outputs
                    .futures
                    .push(wait_for_ffmpeg_notification(stream_id.clone(), receiver).boxed());

                self.status = FfmpegHandlerStatus::Pending { id };
            }

            _ => (),
        }
    }

    fn stop_stream(&mut self) {
        match &self.status {
            FfmpegHandlerStatus::Pending { id } => {
                let _ = self
                    .ffmpeg_endpoint
                    .send(FfmpegEndpointRequest::StopFfmpeg { id: id.clone() });
            }

            FfmpegHandlerStatus::Active { id } => {
                let _ = self
                    .ffmpeg_endpoint
                    .send(FfmpegEndpointRequest::StopFfmpeg { id: id.clone() });
            }

            FfmpegHandlerStatus::Inactive => (),
        }
    }

    fn handle_resolved_future(
        &mut self,
        future: Box<dyn StreamHandlerFutureResult>,
        outputs: &mut StepOutputs,
    ) -> ResolvedFutureStatus {
        let future = match future.downcast::<FutureResult>() {
            Ok(x) => *x,
            Err(_) => return ResolvedFutureStatus::Success,
        };

        match future {
            FutureResult::FfmpegChannelGone => ResolvedFutureStatus::StreamShouldBeStopped,
            FutureResult::NotificationReceived(notification, receiver) => {
                outputs
                    .futures
                    .push(wait_for_ffmpeg_notification(self.stream_id.clone(), receiver).boxed());

                self.handle_ffmpeg_notification(notification);

                ResolvedFutureStatus::Success
            }
        }
    }
}

async fn wait_for_ffmpeg_notification(
    stream_id: StreamId,
    mut receiver: UnboundedReceiver<FfmpegEndpointNotification>,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(msg) => FutureResult::NotificationReceived(msg, receiver),

        None => FutureResult::FfmpegChannelGone,
    };

    Box::new(StreamHandlerFutureWrapper {
        stream_id,
        future: Box::new(result),
    })
}
