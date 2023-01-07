//! Utilities useful for actor implementations.

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
