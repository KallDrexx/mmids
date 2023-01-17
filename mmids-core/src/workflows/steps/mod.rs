//! Workflow steps are individual actions that can be taken on media as part of a media pipeline.

pub mod factory;
pub mod futures_channel;
pub mod workflow_forwarder;

use super::MediaNotification;
use crate::workflows::definitions::WorkflowStepDefinition;
use downcast_rs::{impl_downcast, Downcast};

/// Represents the result of a future for a workflow step.  It is expected that the workflow step
/// will downcast this result into a struct that it owns.
pub trait StepFutureResult: Downcast + Send {}
impl_downcast!(StepFutureResult);

pub type StepCreationResult =
    Result<Box<dyn WorkflowStep + Sync + Send>, Box<dyn std::error::Error + Sync + Send>>;

pub type CreateFactoryFnResult =
    Box<dyn Fn(&WorkflowStepDefinition) -> StepCreationResult + Send + Sync>;

/// Various statuses of an individual step
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Default)]
pub struct StepInputs {
    /// Media notifications that the step may be interested in
    pub media: Vec<MediaNotification>,

    /// Any resolved futures that are specific to this step
    pub notifications: Vec<Box<dyn StepFutureResult>>,
}

impl StepInputs {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn clear(&mut self) {
        self.media.clear();
        self.notifications.clear();
    }
}

/// Resulting outputs that come from executing a workflow step.
#[derive(Default)]
pub struct StepOutputs {
    /// Media notifications that the workflow step intends to pass to the next workflow step
    pub media: Vec<MediaNotification>,
}

impl StepOutputs {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn clear(&mut self) {
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
    fn execute(
        &mut self,
        inputs: &mut StepInputs,
        outputs: &mut StepOutputs,
        futures_channel: WorkflowStepFuturesChannel,
    );

    /// Notifies the step that it is no longer needed and that all streams its managing should be
    /// closed.  All endpoints the step has interacted with should be proactively notified that it
    /// is being removed, as it can not be guaranteed that all channels will be automatically
    /// closed.
    ///
    /// After this is called it is expected that the workflow step is in a `TornDown` state.
    fn shutdown(&mut self);
}

#[cfg(feature = "test-utils")]
use crate::workflows::steps::factory::StepGenerator;
#[cfg(feature = "test-utils")]
use crate::workflows::steps::futures_channel::FuturesChannelResult;
use crate::workflows::steps::futures_channel::{
    FuturesChannelInnerResult, WorkflowStepFuturesChannel,
};
#[cfg(feature = "test-utils")]
use anyhow::{anyhow, Result};
#[cfg(feature = "test-utils")]
use std::time::Duration;
#[cfg(feature = "test-utils")]
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
#[cfg(feature = "test-utils")]
use tokio::time::timeout;

#[cfg(feature = "test-utils")]
pub struct StepTestContext {
    pub step: Box<dyn WorkflowStep>,
    pub media_outputs: Vec<MediaNotification>,
    pub futures_channel_sender: WorkflowStepFuturesChannel,
    futures_channel_receiver: UnboundedReceiver<FuturesChannelResult>,
}

#[cfg(feature = "test-utils")]
impl StepTestContext {
    pub fn new(
        generator: Box<dyn StepGenerator>,
        definition: WorkflowStepDefinition,
    ) -> Result<Self> {
        let (sender, receiver) = unbounded_channel();
        let channel = WorkflowStepFuturesChannel::new(definition.get_id(), sender);

        let step = generator
            .generate(definition, channel.clone())
            .map_err(|error| anyhow!("Failed to generate workflow step: {:?}", error))?;

        Ok(StepTestContext {
            step,
            media_outputs: Vec::new(),
            futures_channel_sender: channel,
            futures_channel_receiver: receiver,
        })
    }

    pub fn execute_with_media(&mut self, media: MediaNotification) {
        let mut outputs = StepOutputs::new();
        let mut inputs = StepInputs::new();
        inputs.media.push(media);

        self.step.execute(
            &mut inputs,
            &mut outputs,
            self.futures_channel_sender.clone(),
        );

        self.media_outputs = outputs.media;
    }

    pub async fn execute_notification(&mut self, notification: Box<dyn StepFutureResult>) {
        let mut outputs = StepOutputs::new();
        let mut inputs = StepInputs::new();
        inputs.notifications.push(notification);

        self.step.execute(
            &mut inputs,
            &mut outputs,
            self.futures_channel_sender.clone(),
        );
        self.media_outputs = outputs.media;

        self.execute_pending_notifications().await;
    }

    pub async fn execute_pending_notifications(&mut self) {
        loop {
            let duration = Duration::from_millis(10);
            let future = self.futures_channel_receiver.recv();

            // We explicitly want to do a timeout instead of `try_recv` to ensure that any futures
            // that are triggered get a chance to execute and complete. This is important since it's
            // single threaded. A `yield()` may work but this is probably more reliable to give us
            // enough time.
            let notification = match timeout(duration, future).await {
                Ok(Some(notification)) => notification,
                _ => break,
            };

            let notification = match notification.result {
                FuturesChannelInnerResult::Generic(notification) => notification,
            };

            let mut outputs = StepOutputs::new();
            let mut inputs = StepInputs::new();
            inputs.notifications.push(notification);

            self.step.execute(
                &mut inputs,
                &mut outputs,
                self.futures_channel_sender.clone(),
            );
            self.media_outputs = outputs.media;
        }
    }

    pub fn assert_media_passed_through(&mut self, media: MediaNotification) {
        self.execute_with_media(media.clone());

        assert_eq!(
            self.media_outputs.len(),
            1,
            "Unexpected number of media outputs"
        );
        assert_eq!(self.media_outputs[0], media, "Unexpected media message");
    }

    pub fn assert_media_not_passed_through(&mut self, media: MediaNotification) {
        self.execute_with_media(media);

        assert!(self.media_outputs.is_empty(), "Expected no media outputs");
    }

    /// Gets the first future that was resolved on the workflow step futures channel. If no future
    /// is resolved, then a panic will ensue.
    pub async fn expect_future_resolved(&mut self) -> FuturesChannelInnerResult {
        let future = self.futures_channel_receiver.recv();
        match timeout(Duration::from_millis(10), future).await {
            Ok(Some(response)) => response.result,
            _ => panic!("No future resolved within timeout period"),
        }
    }
}
