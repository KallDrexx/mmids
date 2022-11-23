use crate::endpoint::{FfmpegEndpointNotification, FfmpegEndpointRequest, FfmpegParams};
use futures::FutureExt;
use mmids_core::workflows::steps::{StepFutureResult, StepOutputs};
use mmids_core::StreamId;
use mmids_rtmp::workflow_steps::external_stream_handler::{
    ExternalStreamHandler, ExternalStreamHandlerGenerator, ResolvedFutureStatus,
    StreamHandlerFutureResult, StreamHandlerFutureWrapper,
};
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

pub struct FfmpegHandler {
    ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    status: FfmpegHandlerStatus,
    param_generator: Arc<Box<dyn FfmpegParameterGenerator + Sync + Send>>,
    stream_id: StreamId,
    ffmpeg_id: Uuid,
}

pub struct FfmpegHandlerGenerator {
    ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    param_generator: Arc<Box<dyn FfmpegParameterGenerator + Sync + Send>>,
}

pub trait FfmpegParameterGenerator {
    fn form_parameters(&self, stream_id: &StreamId, stream_name: &str) -> FfmpegParams;
}

#[derive(Debug)]
enum FfmpegHandlerStatus {
    Inactive,
    Pending,
    Active,
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
    fn generate(&self, stream_id: StreamId) -> Box<dyn ExternalStreamHandler + Sync + Send> {
        Box::new(FfmpegHandler {
            ffmpeg_endpoint: self.ffmpeg_endpoint.clone(),
            param_generator: self.param_generator.clone(),
            stream_id,
            status: FfmpegHandlerStatus::Inactive,
            ffmpeg_id: Uuid::new_v4(),
        })
    }
}

impl FfmpegHandler {
    #[instrument(skip(self, notification), fields(stream_id = ?self.stream_id, ffmpeg_id = ?self.ffmpeg_id))]
    fn handle_ffmpeg_notification(&mut self, notification: FfmpegEndpointNotification) {
        match notification {
            FfmpegEndpointNotification::FfmpegStarted => match &self.status {
                FfmpegHandlerStatus::Pending => {
                    info!(
                        "Received notification that ffmpeg became active for stream id {:?} and ffmpeg id {}",
                        self.stream_id, self.ffmpeg_id
                    );

                    self.status = FfmpegHandlerStatus::Active;
                }

                status => {
                    error!(
                        "Received notification that ffmpeg became active for stream id {:?}, \
                        but the handler's status was {:?} instead of pending",
                        self.stream_id, status
                    );
                }
            },

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
    fn prepare_stream(&mut self, stream_name: &str, outputs: &mut StepOutputs) {
        if let FfmpegHandlerStatus::Inactive = &self.status {
            let parameters = self
                .param_generator
                .form_parameters(&self.stream_id, stream_name);
            let (sender, receiver) = unbounded_channel();
            let _ = self
                .ffmpeg_endpoint
                .send(FfmpegEndpointRequest::StartFfmpeg {
                    id: self.ffmpeg_id,
                    params: parameters,
                    notification_channel: sender,
                });

            outputs
                .futures
                .push(wait_for_ffmpeg_notification(self.stream_id.clone(), receiver).boxed());

            self.status = FfmpegHandlerStatus::Pending;
        }
    }

    fn stop_stream(&mut self) {
        match &self.status {
            FfmpegHandlerStatus::Pending => {
                let _ = self
                    .ffmpeg_endpoint
                    .send(FfmpegEndpointRequest::StopFfmpeg { id: self.ffmpeg_id });
            }

            FfmpegHandlerStatus::Active => {
                let _ = self
                    .ffmpeg_endpoint
                    .send(FfmpegEndpointRequest::StopFfmpeg { id: self.ffmpeg_id });
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::{AudioTranscodeParams, TargetParams, VideoTranscodeParams};

    struct TestParamGenerator;
    impl FfmpegParameterGenerator for TestParamGenerator {
        fn form_parameters(&self, stream_id: &StreamId, stream_name: &str) -> FfmpegParams {
            FfmpegParams {
                audio_transcode: AudioTranscodeParams::Copy,
                video_transcode: VideoTranscodeParams::Copy,
                bitrate_in_kbps: None,
                scale: None,
                read_in_real_time: true,
                input: stream_name.to_string(),
                target: TargetParams::Rtmp {
                    url: stream_id.0.to_string(),
                },
            }
        }
    }

    struct TestContext {
        ffmpeg: UnboundedReceiver<FfmpegEndpointRequest>,
        handler: Box<dyn ExternalStreamHandler>,
    }

    impl TestContext {
        fn new() -> Self {
            let (sender, receiver) = unbounded_channel();
            let generator = FfmpegHandlerGenerator {
                ffmpeg_endpoint: sender,
                param_generator: Arc::new(Box::new(TestParamGenerator)),
            };

            let handler = generator.generate(StreamId(Arc::new("test".to_string())));
            TestContext {
                handler,
                ffmpeg: receiver,
            }
        }
    }

    #[test]
    fn prepare_stream_sends_start_ffmpeg_request() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        context.handler.prepare_stream("name", &mut outputs);

        match context.ffmpeg.try_recv() {
            Ok(FfmpegEndpointRequest::StartFfmpeg {
                id: _,
                params,
                notification_channel: _,
            }) => {
                assert_eq!(&params.input, "name", "Unexpected parameter name");
            }

            other => panic!("Expected Ok(StartFfmpeg), instead got {:?}", other),
        }
    }

    #[test]
    fn stop_ffmpeg_sent_when_stop_stream_called() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        context.handler.prepare_stream("name", &mut outputs);
        let _ = context.ffmpeg.try_recv();
        context.handler.stop_stream();

        match context.ffmpeg.try_recv() {
            Ok(FfmpegEndpointRequest::StopFfmpeg { id: _ }) => (),
            other => panic!("Expected Ok(StopFfmpeg) instead got {:?}", other),
        }
    }
}
