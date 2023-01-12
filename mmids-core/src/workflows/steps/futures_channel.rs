//! This module provides abstractions over MPSC channels, which make it easy for workflow steps
//! to execute a future and send the results of those futures back to the correct workflow runner
//! with minimal allocations.

use std::future::Future;
use crate::workflows::definitions::WorkflowStepId;
use crate::workflows::steps::StepFutureResult;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_util::sync::CancellationToken;

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

    /// Helper function for workflow steps to watch a receiver for messages, and send them back
    /// to the workflow step for processing.
    pub fn send_on_unbounded_recv<ReceiverMessage, FutureResult>(
        &self,
        mut receiver: UnboundedReceiver<ReceiverMessage>,
        on_recv: impl Fn(ReceiverMessage) -> FutureResult + Send,
        on_closed: impl FnOnce() -> FutureResult + Send,
    ) where
        ReceiverMessage: Send,
        FutureResult: StepFutureResult + Send,
    {
        let channel = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    message = receiver.recv() => {
                        match message {
                            Some(message) => {
                                let future_result = on_recv(message);
                                let _ = channel.send(future_result);
                            }

                            None => {
                                let future_result = on_closed();
                                let _ = channel.send(future_result);
                                break;
                            }
                        }
                    }

                    _ = channel.closed() => {
                        break;
                    }
                }
            }
        });
    }

    /// Helper function for workflow steps to watch a receiver for messages, and send them back
    /// to the workflow step for processing. Cancellable via a token.
    pub fn send_on_unbounded_recv_cancellable<ReceiverMessage, FutureResult>(
        &self,
        mut receiver: UnboundedReceiver<ReceiverMessage>,
        cancellation_token: CancellationToken,
        on_recv: impl Fn(ReceiverMessage) -> FutureResult + Send,
        on_closed: impl FnOnce() -> FutureResult + Send,
        on_cancelled: impl FnOnce() -> FutureResult + Send,
    ) where
        ReceiverMessage: Send,
        FutureResult: StepFutureResult + Send,
    {
        let channel = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    message = receiver.recv() => {
                        match message {
                            Some(message) => {
                                let future_result = on_recv(message);
                                let _ = channel.send(future_result);
                            }

                            None => {
                                let future_result = on_closed();
                                let _ = channel.send(future_result);
                                break;
                            }
                        }
                    }

                    _ = cancellation_token.cancelled() => {
                        let future_result = on_cancelled();
                        let _ = channel.send(future_result);
                        break;
                    }

                    _ = channel.closed() => {
                        // Nothing ot send since the channel is closed
                        break;
                    }
                }
            }
        });
    }

    /// Helper function for workflow steps to easily send a message upon future completion
    pub fn send_on_future_completion(
        &self,
        future: impl Future<Output = impl StepFutureResult + Send> + Send,
    ) {
        let channel = self.clone();
        tokio::spawn(async move {
            tokio::select! {
                result = future => {
                    let _ = channel.send(result);
                }

                _ = channel.closed() => {
                    // No where to send the result, so cancel the future by exiting
                }
            }
        });
    }
}
