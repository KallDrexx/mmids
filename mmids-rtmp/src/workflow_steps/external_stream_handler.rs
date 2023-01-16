use downcast_rs::{impl_downcast, Downcast};
use mmids_core::workflows::steps::futures_channel::WorkflowStepFuturesChannel;
use mmids_core::workflows::steps::StepFutureResult;
use mmids_core::StreamId;

/// Trait used to handle different external resources for a single stream
pub trait ExternalStreamHandler {
    fn prepare_stream(&mut self, stream_name: &str, futures_channel: &WorkflowStepFuturesChannel);

    fn stop_stream(&mut self);

    fn handle_resolved_future(
        &mut self,
        future: Box<dyn StreamHandlerFutureResult>,
    ) -> ResolvedFutureStatus;
}

/// Allows creating a new external stream handler for any stream
pub trait ExternalStreamHandlerGenerator {
    fn generate(&self, stream_id: StreamId) -> Box<dyn ExternalStreamHandler + Sync + Send>;
}

pub struct StreamHandlerFutureWrapper {
    pub stream_id: StreamId,
    pub future: Box<dyn StreamHandlerFutureResult + Sync + Send>,
}

impl StepFutureResult for StreamHandlerFutureWrapper {}

impl_downcast!(StreamHandlerFutureResult);
pub trait StreamHandlerFutureResult: Downcast {}

pub enum ResolvedFutureStatus {
    Success,
    StreamShouldBeStopped,
}
