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
pub mod workflow_forwarder;

use super::MediaNotification;
use crate::workflows::definitions::WorkflowStepDefinition;
use downcast_rs::{impl_downcast, Downcast};
use futures::future::BoxFuture;

pub use external_stream_handler::*;
pub use external_stream_reader::*;

/// Represents the result of a future for a workflow step.  It is expected that the workflow step
/// will downcast this result into a struct that it owns.
pub trait StepFutureResult: Downcast {}
impl_downcast!(StepFutureResult);

pub type FutureList = Vec<BoxFuture<'static, Box<dyn StepFutureResult>>>;
pub type StepCreationResult = Result<
    (Box<dyn WorkflowStep + Sync + Send>, FutureList),
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
    Error { message: String },

    /// The step has been shut down and is not expected to be invoked anymore. If it's wanted to be
    /// used it will have to be recreated
    Shutdown,
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
pub struct StepOutputs {
    /// Media notifications that the workflow step intends to pass to the next workflow step
    pub media: Vec<MediaNotification>,

    /// Any futures the workflow should track for this step
    pub futures: Vec<BoxFuture<'static, Box<dyn StepFutureResult>>>,
}

impl StepOutputs {
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
    /// Returns a reference to the status of the current workflow step
    fn get_status(&self) -> &StepStatus;

    /// Returns a reference to the definition this workflow step was created with
    fn get_definition(&self) -> &WorkflowStepDefinition;

    /// Executes the workflow step with the specified media and future resolution inputs.  Any outputs
    /// that are generated as a result of this execution will be placed in the `outputs` parameter,
    /// to allow vectors to be re-used.
    ///
    /// It is expected that `execute()` will not be called if the step is in an Error or Torn Down
    /// state.
    fn execute(&mut self, inputs: &mut StepInputs, outputs: &mut StepOutputs);

    /// Notifies the step that it is no longer needed and that all streams its managing should be
    /// closed.  All endpoints the step has interacted with should be proactively notified that it
    /// is being removed, as it can not be guaranteed that all channels will be automatically
    /// closed.
    ///
    /// After this is called it is expected that the workflow step is in a `TornDown` state.
    fn shutdown(&mut self);
}

#[cfg(test)]
use crate::workflows::steps::factory::StepGenerator;
#[cfg(test)]
use anyhow::{anyhow, Result};
#[cfg(test)]
use futures::stream::FuturesUnordered;
#[cfg(test)]
use futures::StreamExt;
#[cfg(test)]
use std::iter::FromIterator;
#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
struct StepTestContext {
    step: Box<dyn WorkflowStep>,
    futures: FuturesUnordered<BoxFuture<'static, Box<dyn StepFutureResult>>>,
    media_outputs: Vec<MediaNotification>,
}

#[cfg(test)]
impl StepTestContext {
    fn new(generator: Box<dyn StepGenerator>, definition: WorkflowStepDefinition) -> Result<Self> {
        let (step, futures) = generator
            .generate(definition)
            .or_else(|error| Err(anyhow!("Failed to generate workflow step: {:?}", error)))?;

        Ok(StepTestContext {
            step,
            futures: FuturesUnordered::from_iter(futures),
            media_outputs: Vec::new(),
        })
    }

    fn execute_with_media(&mut self, media: MediaNotification) {
        let mut outputs = StepOutputs::new();
        let mut inputs = StepInputs::new();
        inputs.media.push(media);

        self.step.execute(&mut inputs, &mut outputs);

        self.futures.extend(outputs.futures.drain(..));
        self.media_outputs = outputs.media;
    }

    async fn execute_notification(&mut self, notification: Box<dyn StepFutureResult>) {
        let mut outputs = StepOutputs::new();
        let mut inputs = StepInputs::new();
        inputs.notifications.push(notification);

        self.step.execute(&mut inputs, &mut outputs);

        self.futures.extend(outputs.futures.drain(..));
        self.media_outputs = outputs.media;

        self.execute_pending_notifications().await;
    }

    async fn execute_pending_notifications(&mut self) {
        loop {
            let notification =
                match tokio::time::timeout(Duration::from_millis(10), self.futures.next()).await {
                    Ok(Some(notification)) => notification,
                    _ => break,
                };

            let mut outputs = StepOutputs::new();
            let mut inputs = StepInputs::new();
            inputs.notifications.push(notification);

            self.step.execute(&mut inputs, &mut outputs);

            self.futures.extend(outputs.futures.drain(..));
            self.media_outputs = outputs.media;
        }
    }

    fn assert_media_passed_through(&mut self, media: MediaNotification) {
        self.execute_with_media(media.clone());

        assert_eq!(
            self.media_outputs.len(),
            1,
            "Unexpected number of media outputs"
        );
        assert_eq!(self.media_outputs[0], media, "Unexpected media message");
    }

    fn assert_media_not_passed_through(&mut self, media: MediaNotification) {
        self.execute_with_media(media.clone());

        assert!(self.media_outputs.is_empty(), "Expected no media outputs");
    }
}
