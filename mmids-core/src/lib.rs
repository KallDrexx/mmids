pub mod net;

use log::error;
use std::future::Future;
use tokio::sync::mpsc;

/// Sends a message over an `mpsc::UnboundedSender` and returns a boolean if it was successful.
/// Sending is not successful if the channel is closed.  Makes it easier to not `match` every
/// send request.
fn send<T>(sender: &mut mpsc::UnboundedSender<T>, message: T) -> bool {
    match sender.send(message) {
        Ok(_) => true,
        Err(_) => false,
    }
}

/// Executes the future, and will log if an error returns
fn spawn_and_log<F, E>(future: F)
where
    F: Future<Output = Result<(), E>> + Sync + Send + 'static,
    E: std::fmt::Display,
{
    tokio::spawn(async {
        if let Err(error) = future.await {
            error!("Error occurred: {}", error);
        }
    });
}
