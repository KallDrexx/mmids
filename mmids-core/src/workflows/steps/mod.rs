//! Workflow steps are individual actions that can be taken on media as part of a media pipeline.

pub mod factory;
pub mod futures_channel;
pub mod workflow_forwarder;

#[cfg(feature = "test-utils")]
pub mod test_utils;

use super::MediaNotification;
use crate::workflows::definitions::WorkflowStepDefinition;
use crate::workflows::steps::futures_channel::WorkflowStepFuturesChannel;
use downcast_rs::{impl_downcast, Downcast};

/// Represents the result of a future for a workflow step.  It is expected that the workflow step
/// will downcast this result into a struct that it owns.
pub trait StepFutureResult: Downcast + Send {}
impl_downcast!(StepFutureResult);

/// The type that is returned by workflow step generators. On the successful generation of
/// workflow steps, the usable instance of the step is returned as well as its initial status.
pub type StepCreationResult = Result<
    (Box<dyn WorkflowStep + Sync + Send>, StepStatus),
    Box<dyn std::error::Error + Sync + Send>,
>;

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
    /// notifications.
    Error { message: String },

    /// The step has been shut down and is not expected to be invoked anymore. Workflow steps
    /// that have been shut down must be regenerated to be used.
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
    /// Executes the workflow step with the specified media and future resolution inputs.  Any outputs
    /// that are generated as a result of this execution will be placed in the `outputs` parameter,
    /// to allow vectors to be re-used.
    ///
    /// This function returns the status the step should be considered in after its execution.
    /// An error state being returned will cause the workflow step to be dropped.
    fn execute(
        &mut self,
        inputs: &mut StepInputs,
        outputs: &mut StepOutputs,
        futures_channel: WorkflowStepFuturesChannel,
    ) -> StepStatus;
}
