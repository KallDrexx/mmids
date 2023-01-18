use crate::workflows::definitions::WorkflowStepDefinition;
use crate::workflows::steps::factory::StepGenerator;
use crate::workflows::steps::futures_channel::{
    FuturesChannelInnerResult, WorkflowStepFuturesChannel,
};
use crate::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use crate::workflows::MediaNotification;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::watch::Receiver;

pub struct TestInputStepGenerator {
    pub media_receiver: Receiver<MediaNotification>,
    pub status_change: Receiver<StepStatus>,
    pub future_result_media_receiver: Receiver<MediaNotification>,
    pub media_received_count: Arc<AtomicU16>,
}

pub struct TestOutputStepGenerator {
    pub media_sender: UnboundedSender<MediaNotification>,
    pub status_change: Receiver<StepStatus>,
}

struct TestInputStep {
    status: StepStatus,
    definition: WorkflowStepDefinition,
    media_receiver: Receiver<MediaNotification>,
    status_receiver: Receiver<StepStatus>,
    media_received_count: Arc<AtomicU16>,
}

struct TestOutputStep {
    status: StepStatus,
    definition: WorkflowStepDefinition,
    media: UnboundedSender<MediaNotification>,
    status_receiver: Receiver<StepStatus>,
}

impl StepFutureResult for InputFutureResult {}

enum InputFutureResult {
    StatusChannelClosed,
    MediaChannelClosed,
    StatusReceived,
    MediaReceived,
    FutureResultMediaReceived(MediaNotification),
    FutureResultMediaChannelClosed,
}

impl StepFutureResult for OutputFutureResult {}

enum OutputFutureResult {
    StatusChannelClosed,
    StatusReceived,
}

impl StepGenerator for TestInputStepGenerator {
    fn generate(
        &self,
        definition: WorkflowStepDefinition,
        futures_channel: WorkflowStepFuturesChannel,
    ) -> StepCreationResult {
        let step = TestInputStep {
            status: StepStatus::Created,
            definition,
            media_receiver: self.media_receiver.clone(),
            status_receiver: self.status_change.clone(),
            media_received_count: self.media_received_count.clone(),
        };

        input_media_received(self.media_receiver.clone(), &futures_channel);
        input_status_received(self.status_change.clone(), &futures_channel);

        futures_channel.send_on_generic_watch_recv(
            self.future_result_media_receiver.clone(),
            |media| InputFutureResult::FutureResultMediaReceived(media.clone()),
            || InputFutureResult::FutureResultMediaChannelClosed,
        );

        Ok(Box::new(step))
    }
}

impl StepGenerator for TestOutputStepGenerator {
    fn generate(
        &self,
        definition: WorkflowStepDefinition,
        futures_channel: WorkflowStepFuturesChannel,
    ) -> StepCreationResult {
        let step = TestOutputStep {
            status: StepStatus::Created,
            definition,
            media: self.media_sender.clone(),
            status_receiver: self.status_change.clone(),
        };

        output_status_received(self.status_change.clone(), &futures_channel);

        Ok(Box::new(step))
    }
}

impl WorkflowStep for TestInputStep {
    fn get_status(&self) -> &StepStatus {
        &self.status
    }

    fn get_definition(&self) -> &WorkflowStepDefinition {
        &self.definition
    }

    fn execute(
        &mut self,
        inputs: &mut StepInputs,
        outputs: &mut StepOutputs,
        futures_channel: WorkflowStepFuturesChannel,
    ) {
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

                InputFutureResult::MediaReceived => {
                    let media = (*self.media_receiver.borrow()).clone();
                    outputs.media.push(media);
                    self.media_received_count.fetch_add(1, Ordering::SeqCst);
                }

                InputFutureResult::StatusReceived => {
                    let status = (*self.status_receiver.borrow()).clone();
                    self.status = status;
                }

                InputFutureResult::FutureResultMediaReceived(media) => {
                    let _ = futures_channel.send(FuturesChannelInnerResult::Media(media));
                }

                InputFutureResult::FutureResultMediaChannelClosed => {
                    self.status = StepStatus::Error {
                        message: "futures media channel closed".to_string(),
                    };
                }
            }
        }

        for media in inputs.media.drain(..) {
            outputs.media.push(media); // for workflow forwarding tests
            self.media_received_count.fetch_add(1, Ordering::SeqCst);
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

    fn execute(
        &mut self,
        inputs: &mut StepInputs,
        _outputs: &mut StepOutputs,
        _futures_channel: WorkflowStepFuturesChannel,
    ) {
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

                OutputFutureResult::StatusReceived => {
                    self.status = (*self.status_receiver.borrow()).clone();
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

fn input_media_received(
    receiver: Receiver<MediaNotification>,
    futures_channel: &WorkflowStepFuturesChannel,
) {
    futures_channel.send_on_generic_watch_recv(
        receiver,
        |_| InputFutureResult::MediaReceived,
        || InputFutureResult::MediaChannelClosed,
    );
}

fn input_status_received(
    receiver: Receiver<StepStatus>,
    futures_channel: &WorkflowStepFuturesChannel,
) {
    futures_channel.send_on_generic_watch_recv(
        receiver,
        |_| InputFutureResult::StatusReceived,
        || InputFutureResult::StatusChannelClosed,
    );
}

fn output_status_received(
    receiver: Receiver<StepStatus>,
    futures_channel: &WorkflowStepFuturesChannel,
) {
    futures_channel.send_on_generic_watch_recv(
        receiver,
        |_| OutputFutureResult::StatusReceived,
        || OutputFutureResult::StatusChannelClosed,
    );
}
