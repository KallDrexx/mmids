//! Workflow steps are individual actions that can be taken on media as part of a media pipeline.

mod external_stream_handler;
mod external_stream_reader;
pub mod factory;
mod ffmpeg_handler;
pub mod ffmpeg_hls;
pub mod ffmpeg_pull;
pub mod ffmpeg_rtmp_push;
pub mod ffmpeg_transcode;
pub mod rtmp_receive;
pub mod rtmp_watch;

pub use external_stream_handler::*;
pub use external_stream_reader::*;

use super::MediaNotification;
use crate::workflows::definitions::WorkflowStepDefinition;
use downcast_rs::{impl_downcast, Downcast};
use futures::future::BoxFuture;

/// Represents the result of a future for a workflow step.  It is expected that the workflow step
/// will downcast this result into a struct that it owns.
pub trait StepFutureResult: Downcast {}
impl_downcast!(StepFutureResult);

pub type FutureList<'a> = Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>;
pub type StepCreationResult = Result<
    (Box<dyn WorkflowStep + Sync + Send>, FutureList<'static>),
    Box<dyn std::error::Error + Sync + Send>,
>;
pub type CreateFactoryFnResult =
    Box<dyn Fn(&WorkflowStepDefinition) -> StepCreationResult + Send + Sync>;

/// Various statuses of an individual step
#[derive(Clone, Debug, PartialEq)]
pub enum StepStatus {
    /// The step has been created but it is not yet ready to handle media
    Created,

    /// The step is fully active and ready for handling media
    Active,

    /// The step has encountered an unrecoverable error and can no longer handle media or
    /// notifications.  It will likely have to be recreated.
    Error,
}

/// Inputs to be passed in for execution of a workflow step.
pub struct StepInputs {
    /// Media notifications that the step may be interested in
    pub media: Vec<MediaNotification>,

    /// Any resolved futures that are specific to this step
    pub notifications: Vec<Box<dyn StepFutureResult>>,
}

impl StepInputs {
    pub fn new() -> Self {
        StepInputs {
            media: Vec::new(),
            notifications: Vec::new(),
        }
    }

    pub fn clear(&mut self) {
        self.media.clear();
        self.notifications.clear();
    }
}

/// Resulting outputs that come from executing a workflow step.
pub struct StepOutputs<'a> {
    /// Media notifications that the workflow step intends to pass to the next workflow step
    pub media: Vec<MediaNotification>,

    /// Any futures the workflow should track for this step
    pub futures: Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>,
}

impl<'a> StepOutputs<'a> {
    pub fn new() -> Self {
        StepOutputs {
            media: Vec::new(),
            futures: Vec::new(),
        }
    }

    pub fn clear(&mut self) {
        self.futures.clear();
        self.media.clear();
    }
}

/// Represents a workflow step that can be executed
pub trait WorkflowStep {
    fn get_status(&self) -> &StepStatus;
    fn get_definition(&self) -> &WorkflowStepDefinition;
    fn execute(&mut self, inputs: &mut StepInputs, outputs: &mut StepOutputs);
}

#[cfg(test)]
mod test_utils {
    use super::*;
    use futures::stream::FuturesUnordered;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::time::timeout;

    pub async fn get_pending_future_result<'a>(
        futures: Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>,
    ) -> Box<dyn StepFutureResult> {
        let mut awaitable_futures = FuturesUnordered::new();
        for future in futures {
            awaitable_futures.push(future);
        }

        match timeout(Duration::from_millis(10), awaitable_futures.next()).await {
            Ok(Some(result)) => result,
            _ => panic!("Message channel future didn't resolve as expected"),
        }
    }

    pub fn create_step_parameters<'a>() -> (StepInputs, StepOutputs<'a>) {
        (
            StepInputs {
                media: Vec::new(),
                notifications: Vec::new(),
            },
            StepOutputs {
                media: Vec::new(),
                futures: Vec::new(),
            },
        )
    }
}
