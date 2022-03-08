//! The workflow forwarder step takes all media notifications it receives and sends them to the
//! specified workflow, using the workflow media relay. All media notifications are also passed
//! to subsequent steps.

#[cfg(test)]
mod tests;

use crate::event_hub::{SubscriptionRequest, WorkflowStartedOrStoppedEvent};
use crate::reactors::manager::ReactorManagerRequest;
use crate::reactors::ReactorWorkflowUpdate;
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
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info, span, Level};

pub const TARGET_WORKFLOW: &'static str = "target_workflow";
pub const REACTOR_NAME: &'static str = "reactor";

/// Generates a new workflow forwarder step
pub struct WorkflowForwarderStepGenerator {
    event_hub_subscriber: UnboundedSender<SubscriptionRequest>,
    reactor_manager: UnboundedSender<ReactorManagerRequest>,
}

struct StreamDetails {
    target_workflow_names: HashSet<String>,
    required_media: Vec<MediaNotification>,

    // Used to cancel the reactor update future. When a stream disconnects, this cancellation
    // channel will be dropped causing the future waiting for reactor updates to be closed. This
    // will inform the reactor that this step is no longer interested in whatever workflow it was
    // managing for it. Not using a one shot, as the channel needs to live across multiple futures
    // if updates come in.
    _cancellation_channel: Option<UnboundedSender<()>>,
}

struct WorkflowForwarderStep {
    global_workflow_name: Option<String>,
    reactor_name: Option<String>,
    reactor_manager: UnboundedSender<ReactorManagerRequest>,
    definition: WorkflowStepDefinition,
    status: StepStatus,
    active_streams: HashMap<StreamId, StreamDetails>,
    stream_for_workflow_name: HashMap<String, HashSet<StreamId>>,
    known_workflows: HashMap<String, UnboundedSender<WorkflowRequest>>,
}

enum FutureResult {
    EventHubGone,
    ReactorManagerGone,
    ReactorGone,

    WorkflowGone {
        workflow_name: String,
    },

    WorkflowStartedOrStopped(
        WorkflowStartedOrStoppedEvent,
        UnboundedReceiver<WorkflowStartedOrStoppedEvent>,
    ),

    ReactorResponseReceived {
        stream_id: StreamId,
        stream_name: String,
        update: ReactorWorkflowUpdate,
        reactor_update_channel: UnboundedReceiver<ReactorWorkflowUpdate>,
    },

    ReactorUpdateReceived {
        stream_id: StreamId,
        update: ReactorWorkflowUpdate,
        reactor_update_channel: UnboundedReceiver<ReactorWorkflowUpdate>,
        cancellation_channel: UnboundedReceiver<()>,
    },

    ReactorCancellationReceived {
        stream_id: StreamId,
    },
}

impl StepFutureResult for FutureResult {}

#[derive(Error, Debug)]
enum StepStartupError {
    #[error("A {} or {} value must be specified", TARGET_WORKFLOW, REACTOR_NAME)]
    NoTargetWorkflowSpecified,

    #[error("A target workflow and reactor were specified. Only one can be used at a time")]
    ReactorAndTargetWorkflowBothSpecified,
}

impl WorkflowForwarderStepGenerator {
    pub fn new(
        event_hub_subscriber: UnboundedSender<SubscriptionRequest>,
        reactor_manager: UnboundedSender<ReactorManagerRequest>,
    ) -> Self {
        WorkflowForwarderStepGenerator {
            event_hub_subscriber,
            reactor_manager,
        }
    }
}

impl StepGenerator for WorkflowForwarderStepGenerator {
    fn generate(&self, definition: WorkflowStepDefinition) -> StepCreationResult {
        let target_workflow_name = match definition.parameters.get(TARGET_WORKFLOW) {
            Some(Some(name)) => Some(name.clone()),
            _ => None,
        };

        let reactor_name = match definition.parameters.get(REACTOR_NAME) {
            Some(Some(reactor)) => Some(reactor.clone()),
            _ => None,
        };

        if reactor_name.is_none() && target_workflow_name.is_none() {
            return Err(Box::new(StepStartupError::NoTargetWorkflowSpecified));
        }

        if reactor_name.is_some() && target_workflow_name.is_some() {
            return Err(Box::new(
                StepStartupError::ReactorAndTargetWorkflowBothSpecified,
            ));
        }

        let (event_sender, event_receiver) = unbounded_channel();
        let _ = self
            .event_hub_subscriber
            .send(SubscriptionRequest::WorkflowStartedOrStopped {
                channel: event_sender,
            });

        let step = WorkflowForwarderStep {
            global_workflow_name: target_workflow_name,
            reactor_name,
            stream_for_workflow_name: HashMap::new(),
            definition: definition.clone(),
            status: StepStatus::Active,
            active_streams: HashMap::new(),
            reactor_manager: self.reactor_manager.clone(),
            known_workflows: HashMap::new(),
        };

        let futures = vec![
            wait_for_workflow_event(event_receiver).boxed(),
            notify_reactor_manager_gone(self.reactor_manager.clone()).boxed(),
        ];

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
                // We need to track all workflows started, in case we need the channel of a workflow
                // that starts after the reactor lets us know its relevant to a stream
                self.known_workflows.insert(name.clone(), channel.clone());
                outputs
                    .futures
                    .push(notify_target_workflow_gone(name.clone(), channel.clone()).boxed());

                if let Some(stream_ids) = self.stream_for_workflow_name.get(&name) {
                    info!(
                        workflow_name = %name,
                        "Received notification that workflow {} has started", name
                    );

                    for stream_id in stream_ids {
                        if let Some(stream) = self.active_streams.get_mut(stream_id) {
                            for media in &stream.required_media {
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
            }

            WorkflowStartedOrStoppedEvent::WorkflowEnded { name } => {
                self.known_workflows.remove(&name);

                if self.stream_for_workflow_name.contains_key(&name) {
                    info!(
                        workflow_name = %name,
                        "Received notification that workflow {} has stopped", name
                    );
                }
            }
        }
    }

    fn handle_media(&mut self, media: MediaNotification, outputs: &mut StepOutputs) {
        match &media.content {
            MediaNotificationContent::NewIncomingStream { stream_name } => {
                if !self.active_streams.contains_key(&media.stream_id) {
                    let mut stream_details = StreamDetails {
                        target_workflow_names: HashSet::new(),
                        required_media: vec![media.clone()],
                        _cancellation_channel: None,
                    };

                    if let Some(workflow) = &self.global_workflow_name {
                        stream_details
                            .target_workflow_names
                            .insert(workflow.clone());
                        let entry = self
                            .stream_for_workflow_name
                            .entry(workflow.clone())
                            .or_insert(HashSet::new());

                        entry.insert(media.stream_id.clone());
                    }

                    if let Some(reactor) = &self.reactor_name {
                        let (sender, receiver) = unbounded_channel();
                        let _ = self.reactor_manager.send(
                            ReactorManagerRequest::CreateWorkflowForStreamName {
                                reactor_name: reactor.clone(),
                                stream_name: stream_name.clone(),
                                response_channel: sender,
                            },
                        );

                        outputs.futures.push(
                            wait_for_reactor_response(
                                media.stream_id.clone(),
                                stream_name.clone(),
                                receiver,
                            )
                            .boxed(),
                        );
                    }

                    self.active_streams
                        .insert(media.stream_id.clone(), stream_details);
                }
            }

            MediaNotificationContent::StreamDisconnected => {
                if let Some(stream) = self.active_streams.remove(&media.stream_id) {
                    for workflow in stream.target_workflow_names {
                        if let Some(channel) = self.known_workflows.get(&workflow) {
                            let _ = channel.send(WorkflowRequest {
                                request_id: "from-workflow-forwarder_disconnection".to_string(),
                                operation: WorkflowRequestOperation::MediaNotification {
                                    media: media.clone(),
                                },
                            });
                        }

                        if let Some(stream_ids) = self.stream_for_workflow_name.get_mut(&workflow) {
                            stream_ids.remove(&media.stream_id);
                            if stream_ids.is_empty() {
                                self.stream_for_workflow_name.remove(&workflow);
                            }
                        }
                    }
                }
            }

            MediaNotificationContent::Metadata { .. } => {
                // I don't think this can be considered required, as I think closed captions and
                // other data will come down as metadata that we don't want to permanently store.
            }

            MediaNotificationContent::Video {
                is_sequence_header: true,
                ..
            } => {
                if let Some(stream) = self.active_streams.get_mut(&media.stream_id) {
                    stream.required_media.push(media.clone());
                }
            }

            MediaNotificationContent::Audio {
                is_sequence_header: true,
                ..
            } => {
                if let Some(stream) = self.active_streams.get_mut(&media.stream_id) {
                    stream.required_media.push(media.clone());
                }
            }

            _ => (),
        }

        if let Some(stream) = self.active_streams.get(&media.stream_id) {
            for workflow_name in &stream.target_workflow_names {
                if let Some(channel) = self.known_workflows.get(workflow_name) {
                    let _ = channel.send(WorkflowRequest {
                        request_id: "sourced-from-workflow_forwarder".to_string(),
                        operation: WorkflowRequestOperation::MediaNotification {
                            media: media.clone(),
                        },
                    });
                }
            }
        }

        outputs.media.push(media);
    }

    fn handle_reactor_update(
        &mut self,
        stream_id: StreamId,
        update: ReactorWorkflowUpdate,
        reactor_channel: UnboundedReceiver<ReactorWorkflowUpdate>,
        cancellation_channel: UnboundedReceiver<()>,
        outputs: &mut StepOutputs,
    ) {
        if let Some(stream) = self.active_streams.get_mut(&stream_id) {
            if update.is_valid {
                let new_workflows = update
                    .routable_workflow_names
                    .iter()
                    .filter(|x| !stream.target_workflow_names.contains(*x))
                    .map(|x| x.clone())
                    .collect::<Vec<_>>();

                let removed_workflows = stream
                    .target_workflow_names
                    .iter()
                    .filter(|x| !update.routable_workflow_names.contains(*x))
                    .map(|x| x.clone())
                    .collect::<Vec<_>>();

                if !new_workflows.is_empty() || !removed_workflows.is_empty() {
                    info!(
                        stream_id = ?stream_id,
                        new_workflows = %new_workflows.len(),
                        removed_workflows = %removed_workflows.len(),
                        "Reactor sent update for stream {:?} with {} new workflows and {} removed workflows",
                        stream_id, new_workflows.len(), removed_workflows.len(),
                    );
                }

                // Remove target workflows and send disconnection message to removed workflows
                for workflow in removed_workflows {
                    stream.target_workflow_names.remove(&workflow);

                    if let Some(channel) = self.known_workflows.get(&workflow) {
                        let _ = channel.send(WorkflowRequest {
                            request_id: "workflow_forwarder_reactor_update".to_string(),
                            operation: WorkflowRequestOperation::MediaNotification {
                                media: MediaNotification {
                                    stream_id: stream_id.clone(),
                                    content: MediaNotificationContent::StreamDisconnected,
                                },
                            },
                        });
                    }

                    if let Some(stream_ids) = self.stream_for_workflow_name.get_mut(&workflow) {
                        stream_ids.remove(&stream_id);
                        if stream_ids.is_empty() {
                            self.stream_for_workflow_name.remove(&workflow);
                        }
                    }
                }

                // Add new target workflows and send required media to new workflows
                for workflow in new_workflows {
                    stream.target_workflow_names.insert(workflow.clone());

                    if let Some(channel) = self.known_workflows.get(&workflow) {
                        for media in &stream.required_media {
                            let _ = channel.send(WorkflowRequest {
                                request_id: "workflow_forwarder_reactor_update".to_string(),
                                operation: WorkflowRequestOperation::MediaNotification {
                                    media: media.clone(),
                                },
                            });
                        }
                    }
                }

                for workflow in update.routable_workflow_names {
                    let entry = self
                        .stream_for_workflow_name
                        .entry(workflow.clone())
                        .or_insert(HashSet::new());

                    entry.insert(stream_id.clone());
                }
            } else {
                // This stream is no longer valid according to the reactor
                for workflow in stream.target_workflow_names.drain() {
                    // Send disconnection message to workflow
                    if let Some(channel) = self.known_workflows.get(&workflow) {
                        let _ = channel.send(WorkflowRequest {
                            request_id: "workflow_forwarder_reactor_update".to_string(),
                            operation: WorkflowRequestOperation::MediaNotification {
                                media: MediaNotification {
                                    stream_id: stream_id.clone(),
                                    content: MediaNotificationContent::StreamDisconnected,
                                },
                            },
                        });
                    }

                    if let Some(stream_ids) = self.stream_for_workflow_name.get_mut(&workflow) {
                        stream_ids.remove(&stream_id);
                        if stream_ids.is_empty() {
                            self.stream_for_workflow_name.remove(&workflow);
                        }
                    }
                }
            }

            // Keep polling for updates even if no workflows were returned, as one may come in after
            // the fact.
            let future = wait_for_reactor_update(stream_id, reactor_channel, cancellation_channel);
            outputs.futures.push(future.boxed());
        }
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

                    self.status = StepStatus::Error {
                        message: "Received future result of unknown type".to_string(),
                    };

                    return;
                }
            };

            match future_result {
                FutureResult::EventHubGone => {
                    error!("Received a notification that the event hub is gone");
                    self.status = StepStatus::Error {
                        message: "Event hub gone".to_string(),
                    };

                    return;
                }

                FutureResult::ReactorManagerGone => {
                    error!("Reactor manager is gone");
                    self.status = StepStatus::Error {
                        message: "Reactor manager gone".to_string(),
                    };

                    return;
                }

                FutureResult::ReactorGone => {
                    if let Some(name) = &self.reactor_name {
                        error!("Reactor {} is gone", name);
                    } else {
                        error!("Received notice that a reactor is gone but we aren't using one");
                    }

                    self.status = StepStatus::Error {
                        message: "Reactor gone".to_string(),
                    };

                    return;
                }

                FutureResult::ReactorResponseReceived {
                    stream_id,
                    stream_name,
                    update,
                    reactor_update_channel,
                } => {
                    if let Some(stream) = self.active_streams.get_mut(&stream_id) {
                        let span = span!(Level::INFO, "Reactor response received", stream_name = %stream_name);
                        let _enter = span.enter();

                        let (cancellation_sender, cancellation_receiver) = unbounded_channel();
                        stream._cancellation_channel = Some(cancellation_sender);

                        self.handle_reactor_update(
                            stream_id,
                            update,
                            reactor_update_channel,
                            cancellation_receiver,
                            outputs,
                        );
                    }
                }

                FutureResult::ReactorUpdateReceived {
                    stream_id,
                    update,
                    reactor_update_channel,
                    cancellation_channel,
                } => {
                    self.handle_reactor_update(
                        stream_id,
                        update,
                        reactor_update_channel,
                        cancellation_channel,
                        outputs,
                    );
                }

                FutureResult::WorkflowGone { workflow_name } => {
                    self.known_workflows.remove(&workflow_name);

                    if self.stream_for_workflow_name.contains_key(&workflow_name) {
                        info!(
                            workflow_name = %workflow_name,
                            "Workflow {} is gone", workflow_name,
                        );
                    }
                }

                FutureResult::ReactorCancellationReceived { stream_id } => {
                    if let Some(stream) = self.active_streams.get_mut(&stream_id) {
                        for workflow_name in stream.target_workflow_names.drain() {
                            // Send disconnection message to old workflow
                            if let Some(channel) = self.known_workflows.get(&workflow_name) {
                                let _ = channel.send(WorkflowRequest {
                                    request_id: "workflow_forwarder_reactor_update".to_string(),
                                    operation: WorkflowRequestOperation::MediaNotification {
                                        media: MediaNotification {
                                            stream_id: stream_id.clone(),
                                            content: MediaNotificationContent::StreamDisconnected,
                                        },
                                    },
                                });
                            }

                            if let Some(stream_ids) =
                                self.stream_for_workflow_name.get_mut(&workflow_name)
                            {
                                stream_ids.remove(&stream_id);
                                if stream_ids.is_empty() {
                                    self.stream_for_workflow_name.remove(&workflow_name);
                                }
                            }
                        }

                        stream._cancellation_channel = None;
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
        // knows not to expect more media from them.
        for (stream_id, mut stream) in self.active_streams.drain() {
            for workflow_name in stream.target_workflow_names.drain() {
                if let Some(channel) = self.known_workflows.get(&workflow_name) {
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
}

async fn notify_target_workflow_gone(
    workflow_name: String,
    sender: UnboundedSender<WorkflowRequest>,
) -> Box<dyn StepFutureResult> {
    sender.closed().await;
    Box::new(FutureResult::WorkflowGone { workflow_name })
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

async fn wait_for_reactor_response(
    stream_id: StreamId,
    stream_name: String,
    mut receiver: UnboundedReceiver<ReactorWorkflowUpdate>,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(response) => response,
        None => ReactorWorkflowUpdate {
            is_valid: false,
            routable_workflow_names: HashSet::new(),
        },
    };

    Box::new(FutureResult::ReactorResponseReceived {
        stream_id,
        stream_name,
        update: result,
        reactor_update_channel: receiver,
    })
}

async fn wait_for_reactor_update(
    stream_id: StreamId,
    mut update_receiver: UnboundedReceiver<ReactorWorkflowUpdate>,
    mut cancellation_receiver: UnboundedReceiver<()>,
) -> Box<dyn StepFutureResult> {
    let result = tokio::select! {
        update = update_receiver.recv() => {
            match update {
                Some(update) => FutureResult::ReactorUpdateReceived{
                    stream_id,
                    update,
                    reactor_update_channel: update_receiver,
                    cancellation_channel: cancellation_receiver,
                },

                None => FutureResult::ReactorGone,
            }
        }

        _ = cancellation_receiver.recv() => FutureResult::ReactorCancellationReceived {
            stream_id,
        }
    };

    Box::new(result)
}

async fn notify_reactor_manager_gone(
    sender: UnboundedSender<ReactorManagerRequest>,
) -> Box<dyn StepFutureResult> {
    sender.closed().await;
    Box::new(FutureResult::ReactorManagerGone)
}
