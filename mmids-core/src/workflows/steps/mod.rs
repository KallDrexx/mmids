pub mod factory;
pub mod ffmpeg_transcode;
pub mod rtmp_receive;
pub mod rtmp_watch;

use super::MediaNotification;
use crate::workflows::definitions::WorkflowStepDefinition;
use downcast_rs::{impl_downcast, Downcast};
use futures::future::BoxFuture;

pub trait StepFutureResult: Downcast {}
impl_downcast!(StepFutureResult);

pub type FutureList<'a> = Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>;
pub type StepCreationResult = Result<
    (Box<dyn WorkflowStep + Sync + Send>, FutureList<'static>),
    Box<dyn std::error::Error + Sync + Send>,
>;
pub type CreateFactoryFnResult =
    Box<dyn Fn(&WorkflowStepDefinition) -> StepCreationResult + Send + Sync>;

#[derive(Clone, Debug, PartialEq)]
pub enum StepStatus {
    Created,
    Active,
    Error,
}

pub struct StepInputs {
    pub media: Vec<MediaNotification>,
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

pub struct StepOutputs<'a> {
    pub media: Vec<MediaNotification>,
    pub futures: Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>,
}

impl<'a> StepOutputs<'a> {
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

pub trait WorkflowStep {
    fn get_status(&self) -> &StepStatus;
    fn get_definition(&self) -> &WorkflowStepDefinition;
    fn execute(&mut self, inputs: &mut StepInputs, outputs: &mut StepOutputs);
}

#[cfg(test)]
mod test_utils {
    use super::*;
    use futures::stream::FuturesUnordered;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::time::timeout;

    pub async fn get_pending_future_result<'a>(
        futures: Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>,
    ) -> Box<dyn StepFutureResult> {
        let mut awaitable_futures = FuturesUnordered::new();
        for future in futures {
            awaitable_futures.push(future);
        }

        match timeout(Duration::from_millis(10), awaitable_futures.next()).await {
            Ok(Some(result)) => result,
            _ => panic!("Message channel future didn't resolve as expected"),
        }
    }

    pub fn create_step_parameters<'a>() -> (StepInputs, StepOutputs<'a>) {
        (
            StepInputs {
                media: Vec::new(),
                notifications: Vec::new(),
            },
            StepOutputs {
                media: Vec::new(),
                futures: Vec::new(),
            },
        )
    }
}
