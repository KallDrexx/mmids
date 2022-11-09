use crate::workflows::definitions::WorkflowStepDefinition;
use crate::workflows::steps::factory::StepGenerator;
use crate::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use crate::workflows::MediaNotification;
use futures::FutureExt;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::watch::Receiver;

pub struct TestInputStepGenerator {
    pub media_receiver: Receiver<MediaNotification>,
    pub status_change: Receiver<StepStatus>,
}

pub struct TestOutputStepGenerator {
    pub media_sender: UnboundedSender<MediaNotification>,
    pub status_change: Receiver<StepStatus>,
}

struct TestInputStep {
    status: StepStatus,
    definition: WorkflowStepDefinition,
}

struct TestOutputStep {
    status: StepStatus,
    definition: WorkflowStepDefinition,
    media: UnboundedSender<MediaNotification>,
}

impl StepFutureResult for InputFutureResult {}
enum InputFutureResult {
    StatusChannelClosed,
    MediaChannelClosed,
    StatusReceived(Receiver<StepStatus>),
    MediaReceived(Receiver<MediaNotification>),
}

impl StepFutureResult for OutputFutureResult {}
enum OutputFutureResult {
    StatusChannelClosed,
    StatusReceived(Receiver<StepStatus>),
}

impl StepGenerator for TestInputStepGenerator {
    fn generate(&self, definition: WorkflowStepDefinition) -> StepCreationResult {
        let step = TestInputStep {
            status: StepStatus::Created,
            definition,
        };

        let futures = vec![
            input_media_received(self.media_receiver.clone()).boxed(),
            input_status_received(self.status_change.clone()).boxed(),
        ];

        Ok((Box::new(step), futures))
    }
}

impl StepGenerator for TestOutputStepGenerator {
    fn generate(&self, definition: WorkflowStepDefinition) -> StepCreationResult {
        let step = TestOutputStep {
            status: StepStatus::Created,
            definition,
            media: self.media_sender.clone(),
        };

        let futures = vec![output_status_received(self.status_change.clone()).boxed()];

        Ok((Box::new(step), futures))
    }
}

impl WorkflowStep for TestInputStep {
    fn get_status(&self) -> &StepStatus {
        &self.status
    }

    fn get_definition(&self) -> &WorkflowStepDefinition {
        &self.definition
    }

    fn execute(&mut self, inputs: &mut StepInputs, outputs: &mut StepOutputs) {
        for notification in inputs.notifications.drain(..) {
            let future_result = match notification.downcast::<InputFutureResult>() {
                Ok(result) => result,
                Err(_) => panic!("Received future that wasn't an InputFutureResult"),
            };

            match *future_result {
                InputFutureResult::MediaChannelClosed => {
                    self.status = StepStatus::Error {
                        message: "media channel closed".to_string(),
                    };
                }

                InputFutureResult::StatusChannelClosed => {
                    self.status = StepStatus::Error {
                        message: "status channel closed".to_string(),
                    };
                }

                InputFutureResult::MediaReceived(receiver) => {
                    let media = (*receiver.borrow()).clone();
                    outputs.futures.push(input_media_received(receiver).boxed());
                    outputs.media.push(media);
                }

                InputFutureResult::StatusReceived(receiver) => {
                    let status = (*receiver.borrow()).clone();
                    outputs
                        .futures
                        .push(input_status_received(receiver).boxed());
                    self.status = status;
                }
            }
        }

        for media in inputs.media.drain(..) {
            outputs.media.push(media); // for workflow forwarding tests
        }
    }

    fn shutdown(&mut self) {
        self.status = StepStatus::Shutdown;
    }
}

impl WorkflowStep for TestOutputStep {
    fn get_status(&self) -> &StepStatus {
        &self.status
    }

    fn get_definition(&self) -> &WorkflowStepDefinition {
        &self.definition
    }

    fn execute(&mut self, inputs: &mut StepInputs, outputs: &mut StepOutputs) {
        for notification in inputs.notifications.drain(..) {
            let future_result = match notification.downcast::<OutputFutureResult>() {
                Ok(result) => result,
                Err(_) => panic!("Received future that wasn't an OutputFutureResult"),
            };

            match *future_result {
                OutputFutureResult::StatusChannelClosed => {
                    self.status = StepStatus::Error {
                        message: "status channel closed".to_string(),
                    };
                }

                OutputFutureResult::StatusReceived(receiver) => {
                    self.status = (*receiver.borrow()).clone();
                    outputs
                        .futures
                        .push(output_status_received(receiver).boxed());
                }
            }
        }

        for media in inputs.media.drain(..) {
            let _ = self.media.send(media);
        }
    }

    fn shutdown(&mut self) {
        self.status = StepStatus::Shutdown;
    }
}

async fn input_media_received(
    mut receiver: Receiver<MediaNotification>,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.changed().await {
        Ok(()) => InputFutureResult::MediaReceived(receiver),
        Err(_) => InputFutureResult::MediaChannelClosed,
    };

    Box::new(result)
}

async fn input_status_received(mut receiver: Receiver<StepStatus>) -> Box<dyn StepFutureResult> {
    let result = match receiver.changed().await {
        Ok(()) => InputFutureResult::StatusReceived(receiver),
        Err(_) => InputFutureResult::StatusChannelClosed,
    };

    Box::new(result)
}

async fn output_status_received(mut receiver: Receiver<StepStatus>) -> Box<dyn StepFutureResult> {
    let result = match receiver.changed().await {
        Ok(()) => OutputFutureResult::StatusReceived(receiver),
        Err(_) => OutputFutureResult::StatusChannelClosed,
    };

    Box::new(result)
}
