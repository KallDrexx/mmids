//! Utilities useful for actor implementations.

use std::future::Future;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

/// Watches a tokio `UnboundedReceiver` for a message, and when a message is received sends that
/// message to the actor via the `received_message` transformation function.
pub fn notify_on_unbounded_recv<RecvMessage, ActorMessage>(
    mut receiver: UnboundedReceiver<RecvMessage>,
    actor_channel: UnboundedSender<ActorMessage>,
    received_message: impl Fn(RecvMessage) -> ActorMessage + Send + 'static,
    closed_message: impl FnOnce() -> ActorMessage + Send + 'static,
) where
    RecvMessage: Send + 'static,
    ActorMessage: Send + 'static,
{
    tokio::spawn(async move {
        loop {
            tokio::select! {
                received = receiver.recv() => {
                    match received {
                        Some(msg) => {
                            let actor_msg = received_message(msg);
                            let _ = actor_channel.send(actor_msg);
                        }

                        None => {
                            let actor_msg = closed_message();
                            let _ = actor_channel.send(actor_msg);
                            break;
                        }
                    }
                }

                _ = actor_channel.closed() => {
                    break;
                }
            }
        }
    });
}

/// Watches a tokio `UnboundedSender` to be notified when the channel closes. Once the channel
/// is closed it will send the specified message to the actor.
pub fn notify_on_unbounded_closed<SenderMessage, ActorMessage>(
    sender: UnboundedSender<SenderMessage>,
    actor_channel: UnboundedSender<ActorMessage>,
    closed_message: impl FnOnce() -> ActorMessage + Send + 'static,
) where
    SenderMessage: Send + 'static,
    ActorMessage: Send + 'static,
{
    tokio::spawn(async move {
        tokio::select! {
            _ = sender.closed() => {
                let actor_msg = closed_message();
                let _ = actor_channel.send(actor_msg);
            }

            _ = actor_channel.closed() => {
                // Can't send a message anywhere so just stop.
            }
        }
    });
}

/// Allows notifying an actor when any arbitrary future is resolved.
pub fn notify_on_future_completion<FutureResult, ActorMessage>(
    future: impl Future<Output = FutureResult> + Send + 'static,
    actor_channel: UnboundedSender<ActorMessage>,
    completion_message: impl FnOnce(FutureResult) -> ActorMessage + Send + 'static,
) where
    FutureResult: Send + 'static,
    ActorMessage: Send + 'static,
{
    tokio::spawn(async move {
        tokio::select! {
            result = future => {
                let actor_msg = completion_message(result);
                let _ = actor_channel.send(actor_msg);
            }

            _ = actor_channel.closed() => {
                // Can't send a message so just end
            }
        }
    });
}
