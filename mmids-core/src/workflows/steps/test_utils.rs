use crate::workflows::steps::factory::StepGenerator;
use crate::workflows::steps::futures_channel::FuturesChannelResult;
use crate::workflows::steps::futures_channel::{
    FuturesChannelInnerResult, WorkflowStepFuturesChannel,
};

use crate::workflows::definitions::WorkflowStepDefinition;
use crate::workflows::steps::{StepFutureResult, StepInputs, StepOutputs, WorkflowStep};
use crate::workflows::MediaNotification;
use anyhow::{anyhow, Result};
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::time::timeout;

pub struct StepTestContext {
    pub step: Box<dyn WorkflowStep>,
    pub media_outputs: Vec<MediaNotification>,
    pub futures_channel_sender: WorkflowStepFuturesChannel,
    futures_channel_receiver: UnboundedReceiver<FuturesChannelResult>,
}

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

        self.execute_pending_futures().await;
    }

    pub async fn execute_pending_futures(&mut self) {
        self.media_outputs.clear();

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

            let mut outputs = StepOutputs::new();
            let mut inputs = StepInputs::new();

            match notification.result {
                FuturesChannelInnerResult::Generic(notification) => {
                    inputs.notifications.push(notification)
                }

                FuturesChannelInnerResult::Media(media) => {
                    // Media raised as a result goes to the next step, not the current step, so
                    // it just gets added directly as a step output.
                    self.media_outputs.push(media);
                    continue;
                }
            };

            self.step.execute(
                &mut inputs,
                &mut outputs,
                self.futures_channel_sender.clone(),
            );

            self.media_outputs.extend(outputs.media);
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
