//! The workflow forwarder step takes all media notifications it receives and sends them to the
//! specified workflow, using the workflow media relay. All media notifications are also passed
//! to subsequent steps.

use crate::event_hub::{SubscriptionRequest, WorkflowStartedOrStoppedEvent};
use crate::workflows::definitions::WorkflowStepDefinition;
use crate::workflows::steps::factory::StepGenerator;
use crate::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use crate::workflows::{
    MediaNotification, MediaNotificationContent, WorkflowRequest, WorkflowRequestOperation,
};
use crate::StreamId;
use futures::FutureExt;
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info};

pub const TARGET_WORKFLOW: &'static str = "target_workflow";

/// Generates a new workflow forwarder step
pub struct WorkflowForwarderStepGenerator {
    event_hub_subscriber: UnboundedSender<SubscriptionRequest>,
}

struct WorkflowForwarderStep {
    target_workflow_name: String,
    target_workflow_channel: Option<UnboundedSender<WorkflowRequest>>,
    definition: WorkflowStepDefinition,
    status: StepStatus,
    required_media: HashMap<StreamId, Vec<MediaNotification>>,
}

enum FutureResult {
    EventHubGone,
    WorkflowStartedOrStopped(
        WorkflowStartedOrStoppedEvent,
        UnboundedReceiver<WorkflowStartedOrStoppedEvent>,
    ),
    TargetWorkflowGone,
}

impl StepFutureResult for FutureResult {}

#[derive(Error, Debug)]
enum StepStartupError {
    #[error("A {} value must be specified", TARGET_WORKFLOW)]
    NoTargetWorkflowSpecified,
}

impl WorkflowForwarderStepGenerator {
    pub fn new(event_hub_subscriber: UnboundedSender<SubscriptionRequest>) -> Self {
        WorkflowForwarderStepGenerator {
            event_hub_subscriber,
        }
    }
}

impl StepGenerator for WorkflowForwarderStepGenerator {
    fn generate(&self, definition: WorkflowStepDefinition) -> StepCreationResult {
        let target_workflow_name = match definition.parameters.get(&TARGET_WORKFLOW.to_string()) {
            Some(Some(name)) => name.clone(),
            _ => return Err(Box::new(StepStartupError::NoTargetWorkflowSpecified)),
        };

        let (event_sender, event_receiver) = unbounded_channel();
        let _ = self
            .event_hub_subscriber
            .send(SubscriptionRequest::WorkflowStartedOrStopped {
                channel: event_sender,
            });

        let step = WorkflowForwarderStep {
            target_workflow_name,
            target_workflow_channel: None,
            definition: definition.clone(),
            status: StepStatus::Active,
            required_media: HashMap::new(),
        };

        let futures = vec![wait_for_workflow_event(event_receiver).boxed()];

        Ok((Box::new(step), futures))
    }
}

impl WorkflowForwarderStep {
    fn handle_workflow_event(
        &mut self,
        event: WorkflowStartedOrStoppedEvent,
        outputs: &mut StepOutputs,
    ) {
        match event {
            WorkflowStartedOrStoppedEvent::WorkflowStarted { name, channel } => {
                if name == self.target_workflow_name {
                    info!("Received notification that workflow '{}' has started", name);
                    self.target_workflow_channel = Some(channel.clone());
                    outputs
                        .futures
                        .push(notify_target_workflow_gone(channel.clone()).boxed());

                    // Send this workflow any required media notifications it needs to be able to
                    // parse subsequent media notifications (particularly sequence headers).
                    for (_, vec) in &self.required_media {
                        for media in vec {
                            let _ = channel.send(WorkflowRequest {
                                request_id: "sourced-from-workflow-forwarder".to_string(),
                                operation: WorkflowRequestOperation::MediaNotification {
                                    media: media.clone(),
                                },
                            });
                        }
                    }
                }
            }

            WorkflowStartedOrStoppedEvent::WorkflowEnded { name } => {
                if name == self.target_workflow_name {
                    if self.target_workflow_channel.is_some() {
                        info!("Received notification that workflow '{}' has stopped", name);
                        self.target_workflow_channel = None;
                    }
                }
            }
        }
    }

    fn handle_media(&mut self, media: MediaNotification, outputs: &mut StepOutputs) {
        match media.content {
            MediaNotificationContent::NewIncomingStream { .. } => {
                let collection = vec![media.clone()];
                self.required_media
                    .insert(media.stream_id.clone(), collection);
            }

            MediaNotificationContent::StreamDisconnected => {
                self.required_media.remove(&media.stream_id);
            }

            MediaNotificationContent::Metadata { .. } => {
                // I don't think this can be considered required, as I think closed captions and
                // other data will come down as metadata that we don't want to permanently store.
            }

            MediaNotificationContent::Video {
                is_sequence_header: true,
                ..
            } => {
                if let Some(collection) = self.required_media.get_mut(&media.stream_id) {
                    collection.push(media.clone());
                }
            }

            MediaNotificationContent::Audio {
                is_sequence_header: true,
                ..
            } => {
                if let Some(collection) = self.required_media.get_mut(&media.stream_id) {
                    collection.push(media.clone());
                }
            }

            _ => (),
        }

        if let Some(channel) = &self.target_workflow_channel {
            let _ = channel.send(WorkflowRequest {
                request_id: "sourced-from-workflow_forwarder".to_string(),
                operation: WorkflowRequestOperation::MediaNotification {
                    media: media.clone(),
                },
            });
        }

        outputs.media.push(media);
    }
}

impl WorkflowStep for WorkflowForwarderStep {
    fn get_status(&self) -> &StepStatus {
        &self.status
    }

    fn get_definition(&self) -> &WorkflowStepDefinition {
        &self.definition
    }

    fn execute(&mut self, inputs: &mut StepInputs, outputs: &mut StepOutputs) {
        for notification in inputs.notifications.drain(..) {
            let future_result = match notification.downcast::<FutureResult>() {
                Ok(x) => *x,
                Err(_) => {
                    error!(
                        "Workflow forwarder step received a notification that is not a known type"
                    );
                    self.status = StepStatus::Error;

                    return;
                }
            };

            match future_result {
                FutureResult::EventHubGone => {
                    error!("Received a notification that the event hub is gone");
                    self.status = StepStatus::Error;

                    return;
                }

                FutureResult::TargetWorkflowGone => {
                    if self.target_workflow_channel.is_some() {
                        info!("Target workflow is gone");
                        self.target_workflow_channel = None;
                    }
                }

                FutureResult::WorkflowStartedOrStopped(event, receiver) => {
                    outputs
                        .futures
                        .push(wait_for_workflow_event(receiver).boxed());

                    self.handle_workflow_event(event, outputs);
                }
            }
        }

        for media in inputs.media.drain(..) {
            self.handle_media(media, outputs);
        }
    }

    fn shutdown(&mut self) {
        self.status = StepStatus::Shutdown;

        // Send a disconnect signal for any active streams we are tracking, so the target workflow
        // knows not ot expect more media from them.
        if let Some(channel) = &self.target_workflow_channel {
            for (stream_id, _) in &self.required_media {
                let _ = channel.send(WorkflowRequest {
                    request_id: "workflow-forwarder-shutdown".to_string(),
                    operation: WorkflowRequestOperation::MediaNotification {
                        media: MediaNotification {
                            stream_id: stream_id.clone(),
                            content: MediaNotificationContent::StreamDisconnected,
                        },
                    },
                });
            }
        }
    }
}

async fn notify_target_workflow_gone(
    sender: UnboundedSender<WorkflowRequest>,
) -> Box<dyn StepFutureResult> {
    sender.closed().await;
    Box::new(FutureResult::TargetWorkflowGone)
}

async fn wait_for_workflow_event(
    mut receiver: UnboundedReceiver<WorkflowStartedOrStoppedEvent>,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(event) => FutureResult::WorkflowStartedOrStopped(event, receiver),
        None => FutureResult::EventHubGone,
    };

    Box::new(result)
}
