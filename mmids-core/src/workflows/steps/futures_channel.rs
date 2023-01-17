//! This module provides abstractions over MPSC channels, which make it easy for workflow steps
//! to execute a future and send the results of those futures back to the correct workflow runner
//! with minimal allocations.

use crate::workflows::definitions::WorkflowStepId;
use crate::workflows::steps::StepFutureResult;
use std::future::Future;
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
pub struct FuturesChannelResult {
    pub step_id: WorkflowStepId,
    pub result: FuturesChannelInnerResult,
}

/// The type of result being sent over the future channel
pub enum FuturesChannelInnerResult {
    /// Declares the result is a type that implements the `StepFutureResult` trait, and therefore
    /// is only readable by the raising step itself.
    Generic(Box<dyn StepFutureResult>),
}

impl WorkflowStepFuturesChannel {
    pub fn new(step_id: WorkflowStepId, sender: UnboundedSender<FuturesChannelResult>) -> Self {
        WorkflowStepFuturesChannel { step_id, sender }
    }

    /// Sends the workflow step's future result over the channel. Returns an error if the channel
    /// is closed.
    pub fn send(
        &self,
        message: FuturesChannelInnerResult,
    ) -> Result<(), FuturesChannelInnerResult> {
        let message = FuturesChannelResult {
            step_id: self.step_id,
            result: message,
        };

        self.sender.send(message).map_err(|e| e.0.result)
    }

    /// Completes when the channel is closed due to there being no receiver
    pub async fn closed(&self) {
        self.sender.closed().await
    }

    /// Helper function for workflow steps to watch a receiver for messages, and send them back
    /// to the workflow step for processing.
    ///
    /// This only sends a generic `StepFutureResult` value.
    pub fn send_on_generic_unbounded_recv<ReceiverMessage, FutureResult>(
        &self,
        mut receiver: UnboundedReceiver<ReceiverMessage>,
        on_recv: impl Fn(ReceiverMessage) -> FutureResult + Send + 'static,
        on_closed: impl FnOnce() -> FutureResult + Send + 'static,
    ) where
        ReceiverMessage: Send + 'static,
        FutureResult: StepFutureResult + Send + 'static,
    {
        let channel = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    message = receiver.recv() => {
                        match message {
                            Some(message) => {
                                let future_result = FuturesChannelInnerResult::Generic(
                                    Box::new(on_recv(message))
                                );

                                let _ = channel.send(future_result);
                            }

                            None => {
                                let future_result = FuturesChannelInnerResult::Generic(
                                    Box::new(on_closed())
                                );

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
    ///
    /// This only sends a generic `StepFutureResult` value.
    pub fn send_on_generic_unbounded_recv_cancellable<ReceiverMessage, FutureResult>(
        &self,
        mut receiver: UnboundedReceiver<ReceiverMessage>,
        cancellation_token: CancellationToken,
        on_recv: impl Fn(ReceiverMessage) -> FutureResult + Send + 'static,
        on_closed: impl FnOnce() -> FutureResult + Send + 'static,
        on_cancelled: impl FnOnce() -> FutureResult + Send + 'static,
    ) where
        ReceiverMessage: Send + 'static,
        FutureResult: StepFutureResult + Send + 'static,
    {
        let channel = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    message = receiver.recv() => {
                        match message {
                            Some(message) => {
                                let future_result = FuturesChannelInnerResult::Generic(
                                    Box::new(on_recv(message))
                                );

                                let _ = channel.send(future_result);
                            }

                            None => {
                                let future_result = FuturesChannelInnerResult::Generic(
                                    Box::new(on_closed())
                                );

                                let _ = channel.send(future_result);
                                break;
                            }
                        }
                    }

                    _ = cancellation_token.cancelled() => {
                        let future_result = FuturesChannelInnerResult::Generic(
                            Box::new(on_cancelled())
                        );

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

    /// Helper function for workflow steps to track a tokio watch receiver for messages, and send
    /// them back to the workflow step for processing.
    ///
    /// This only sends a generic `StepFutureResult` value.
    pub fn send_on_generic_watch_recv<ReceiverMessage, FutureResult>(
        &self,
        mut receiver: tokio::sync::watch::Receiver<ReceiverMessage>,
        on_recv: impl Fn(&ReceiverMessage) -> FutureResult + Send + 'static,
        on_closed: impl FnOnce() -> FutureResult + Send + 'static,
    ) where
        ReceiverMessage: Send + Sync + 'static,
        FutureResult: StepFutureResult + Send + 'static,
    {
        let channel = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    message = receiver.changed() => {
                        match message {
                            Ok(_) => {
                                let value = receiver.borrow();
                                let future_result = FuturesChannelInnerResult::Generic(
                                   Box::new(on_recv(&value))
                                );

                                let _ = channel.send(future_result);
                            }

                            Err(_) => {
                                let future_result = FuturesChannelInnerResult::Generic(
                                    Box::new(on_closed())
                                );

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

    /// Helper function for workflow steps to easily send a message upon future completion.
    ///
    /// This only sends a generic `StepFutureResult` value.
    pub fn send_on_generic_future_completion(
        &self,
        future: impl Future<Output = impl StepFutureResult + Send> + Send + 'static,
    ) {
        let channel = self.clone();
        tokio::spawn(async move {
            tokio::select! {
                result = future => {
                    let _ = channel.send(FuturesChannelInnerResult::Generic(Box::new(result)));
                }

                _ = channel.closed() => {
                    // No where to send the result, so cancel the future by exiting
                }
            }
        });
    }
}
