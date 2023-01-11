//! This module provides abstractions over MPSC channels, which make it easy for workflow steps
//! to execute a future and send the results of those futures back to the correct workflow runner
//! with minimal allocations.

use crate::workflows::definitions::WorkflowStepId;
use crate::workflows::steps::StepFutureResult;
use tokio::sync::mpsc::UnboundedSender;

/// An channel which can be used by workflow steps to send future completion results to the
/// workflow runner.
#[derive(Clone)]
pub struct WorkflowStepFuturesChannel {
    step_id: WorkflowStepId,
    sender: UnboundedSender<FuturesChannelResult>,
}

/// The type of information that's returned to the workflow upon a future's completion
pub(crate) struct FuturesChannelResult {
    pub step_id: WorkflowStepId,
    pub result: Box<dyn StepFutureResult>,
}

impl WorkflowStepFuturesChannel {
    pub(crate) fn new(
        step_id: WorkflowStepId,
        sender: &UnboundedSender<FuturesChannelResult>,
    ) -> Self {
        WorkflowStepFuturesChannel {
            step_id,
            sender: sender.clone()
        }
    }

    /// Sends the workflow step's future result over the channel. Returns an error if the channel
    /// is closed.
    pub fn send(&self, message: impl StepFutureResult) -> Result<(), Box<dyn StepFutureResult>> {
        let message = FuturesChannelResult {
            step_id: self.step_id,
            result: Box::new(message),
        };

        self.sender
            .send(message)
            .map_err(|e| e.0.result)
    }

    /// Completes when the channel is closed due to there being no receiver
    pub async fn closed(&self) {
        self.sender.closed().await
    }
}
