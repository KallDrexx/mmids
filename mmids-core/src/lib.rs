extern crate pest;
#[macro_use]
extern crate pest_derive;

pub mod codecs;
pub mod config;
pub mod endpoints;
pub mod net;
mod utils;
pub mod workflows;

use log::error;
use std::future::Future;
use tokio::sync::mpsc;

/// Unique identifier that identifies the flow of video end-to-end.  Normally when media data enters
/// the beginning of a workflow it will be given a unique stream identifier, and it will keep that
/// identifier until it leaves the last stage of the workflow.  This allows for logging to give
/// visibility of how media is processed throughout it's all lifetime.
///
/// If a workflow has a step that requires media to leave the system and then come back in for
/// further steps, than it should keep the same stream identifier.  For example, if
/// a workflow has an ffmpeg transcoding step in the workflow (e.g. to add a watermark), when
/// ffmpeg pushes the video back in it will keep the same identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamId(pub String);

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
