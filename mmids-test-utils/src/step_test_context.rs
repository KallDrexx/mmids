use anyhow::{anyhow, Result};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::iter::FromIterator;
use std::time::Duration;
use futures::future::BoxFuture;
use mmids_core::workflows::definitions::WorkflowStepDefinition;
use mmids_core::workflows::MediaNotification;
use mmids_core::workflows::steps::{StepFutureResult, StepInputs, StepOutputs, WorkflowStep};
use mmids_core::workflows::steps::factory::StepGenerator;

pub struct StepTestContext {
    step: Box<dyn WorkflowStep>,
    futures: FuturesUnordered<BoxFuture<'static, Box<dyn StepFutureResult>>>,
    media_outputs: Vec<MediaNotification>,
}

impl StepTestContext {
    pub fn new(generator: Box<dyn StepGenerator>, definition: WorkflowStepDefinition) -> Result<Self> {
        let (step, futures) = generator
            .generate(definition)
            .or_else(|error| Err(anyhow!("Failed to generate workflow step: {:?}", error)))?;

        Ok(StepTestContext {
            step,
            futures: FuturesUnordered::from_iter(futures),
            media_outputs: Vec::new(),
        })
    }

    pub fn execute_with_media(&mut self, media: MediaNotification) {
        let mut outputs = StepOutputs::new();
        let mut inputs = StepInputs::new();
        inputs.media.push(media);

        self.step.execute(&mut inputs, &mut outputs);

        self.futures.extend(outputs.futures.drain(..));
        self.media_outputs = outputs.media;
    }

    pub async fn execute_notification(&mut self, notification: Box<dyn StepFutureResult>) {
        let mut outputs = StepOutputs::new();
        let mut inputs = StepInputs::new();
        inputs.notifications.push(notification);

        self.step.execute(&mut inputs, &mut outputs);

        self.futures.extend(outputs.futures.drain(..));
        self.media_outputs = outputs.media;

        self.execute_pending_notifications().await;
    }

    pub async fn execute_pending_notifications(&mut self) {
        loop {
            let notification =
                match tokio::time::timeout(Duration::from_millis(10), self.futures.next()).await {
                    Ok(Some(notification)) => notification,
                    _ => break,
                };

            let mut outputs = StepOutputs::new();
            let mut inputs = StepInputs::new();
            inputs.notifications.push(notification);

            self.step.execute(&mut inputs, &mut outputs);

            self.futures.extend(outputs.futures.drain(..));
            self.media_outputs = outputs.media;
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
        self.execute_with_media(media.clone());

        assert!(self.media_outputs.is_empty(), "Expected no media outputs");
    }
}
