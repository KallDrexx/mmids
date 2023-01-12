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
use crate::workflows::steps::futures_channel::WorkflowStepFuturesChannel;
use crate::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use crate::workflows::{
    MediaNotification, MediaNotificationContent, WorkflowRequest, WorkflowRequestOperation,
};
use crate::StreamId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, span, Level};

pub const TARGET_WORKFLOW: &str = "target_workflow";
pub const REACTOR_NAME: &str = "reactor";

/// Generates a new workflow forwarder step
pub struct WorkflowForwarderStepGenerator {
    event_hub_subscriber: UnboundedSender<SubscriptionRequest>,
    reactor_manager: UnboundedSender<ReactorManagerRequest>,
}

struct StreamDetails {
    target_workflow_names: HashSet<Arc<String>>,
    required_media: Vec<MediaNotification>,

    // Used to cancel the reactor update future. When a stream disconnects, this cancellation
    // channel will be dropped causing the future waiting for reactor updates to be closed. This
    // will inform the reactor that this step is no longer interested in whatever workflow it was
    // managing for it. Not using a one shot, as the channel needs to live across multiple futures
    // if updates come in.
    _cancellation_channel: Option<UnboundedSender<()>>,
}

struct WorkflowForwarderStep {
    global_workflow_name: Option<Arc<String>>,
    reactor_name: Option<Arc<String>>,
    reactor_manager: UnboundedSender<ReactorManagerRequest>,
    definition: WorkflowStepDefinition,
    status: StepStatus,
    active_streams: HashMap<StreamId, StreamDetails>,
    stream_for_workflow_name: HashMap<Arc<String>, HashSet<StreamId>>,
    known_workflows: HashMap<Arc<String>, UnboundedSender<WorkflowRequest>>,
}

enum FutureResult {
    EventHubGone,
    ReactorManagerGone,
    ReactorGone,

    WorkflowGone {
        workflow_name: Arc<String>,
    },

    WorkflowStartedOrStopped(
        WorkflowStartedOrStoppedEvent,
        UnboundedReceiver<WorkflowStartedOrStoppedEvent>,
    ),

    ReactorResponseReceived {
        stream_id: StreamId,
        stream_name: Arc<String>,
        update: ReactorWorkflowUpdate,
        reactor_update_channel: UnboundedReceiver<ReactorWorkflowUpdate>,
    },

    ReactorUpdateReceived {
        stream_id: StreamId,
        update: ReactorWorkflowUpdate,
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
    fn generate(
        &self,
        definition: WorkflowStepDefinition,
        futures_channel: WorkflowStepFuturesChannel,
    ) -> StepCreationResult {
        let target_workflow_name = match definition.parameters.get(TARGET_WORKFLOW) {
            Some(Some(name)) => Some(Arc::new(name.clone())),
            _ => None,
        };

        let reactor_name = match definition.parameters.get(REACTOR_NAME) {
            Some(Some(reactor)) => Some(Arc::new(reactor.clone())),
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
            definition,
            status: StepStatus::Active,
            active_streams: HashMap::new(),
            reactor_manager: self.reactor_manager.clone(),
            known_workflows: HashMap::new(),
        };

        notify_on_workflow_event(event_receiver, &futures_channel);
        notify_reactor_manager_gone(self.reactor_manager.clone(), &futures_channel);

        Ok((Box::new(step), Vec::new()))
    }
}

impl WorkflowForwarderStep {
    fn handle_workflow_event(
        &mut self,
        event: WorkflowStartedOrStoppedEvent,
        futures_channel: &WorkflowStepFuturesChannel,
    ) {
        match event {
            WorkflowStartedOrStoppedEvent::WorkflowStarted { name, channel } => {
                // We need to track all workflows started, in case we need the channel of a workflow
                // that starts after the reactor lets us know its relevant to a stream
                self.known_workflows.insert(name.clone(), channel.clone());

                {
                    let channel = channel.clone();
                    let name = name.clone();
                    futures_channel.send_on_future_completion(async move {
                        channel.closed().await;
                        FutureResult::WorkflowGone {
                            workflow_name: name,
                        }
                    })
                }

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

    fn handle_media(
        &mut self,
        media: MediaNotification,
        outputs: &mut StepOutputs,
        futures_channel: &WorkflowStepFuturesChannel,
    ) {
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
                            .or_default();

                        entry.insert(media.stream_id.clone());
                    }

                    if let Some(reactor) = &self.reactor_name {
                        let (sender, mut receiver) = unbounded_channel();
                        let _ = self.reactor_manager.send(
                            ReactorManagerRequest::CreateWorkflowForStreamName {
                                reactor_name: reactor.clone(),
                                stream_name: stream_name.clone(),
                                response_channel: sender,
                            },
                        );

                        let stream_id = media.stream_id.clone();
                        let stream_name = stream_name.clone();
                        futures_channel.send_on_future_completion(async move {
                            let result = match receiver.recv().await {
                                Some(response) => response,
                                None => ReactorWorkflowUpdate {
                                    is_valid: false,
                                    routable_workflow_names: HashSet::new(),
                                },
                            };

                            FutureResult::ReactorResponseReceived {
                                stream_id,
                                stream_name,
                                update: result,
                                reactor_update_channel: receiver,
                            }
                        });
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

            MediaNotificationContent::MediaPayload {
                is_required_for_decoding: true,
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

    fn handle_reactor_update(&mut self, stream_id: StreamId, update: ReactorWorkflowUpdate) {
        if let Some(stream) = self.active_streams.get_mut(&stream_id) {
            if update.is_valid {
                let new_workflows = update
                    .routable_workflow_names
                    .iter()
                    .filter(|x| !stream.target_workflow_names.contains(*x))
                    .cloned()
                    .collect::<Vec<_>>();

                let removed_workflows = stream
                    .target_workflow_names
                    .iter()
                    .filter(|x| !update.routable_workflow_names.contains(*x))
                    .cloned()
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
                        .or_default();

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

    fn execute(
        &mut self,
        inputs: &mut StepInputs,
        outputs: &mut StepOutputs,
        futures_channel: WorkflowStepFuturesChannel,
    ) {
        for notification in inputs.notifications.drain(..) {
            let notification: Box<dyn StepFutureResult> = notification;
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

                        self.handle_reactor_update(stream_id, update);
                    }
                }

                FutureResult::ReactorUpdateReceived { stream_id, update } => {
                    self.handle_reactor_update(stream_id, update);
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
                    self.handle_workflow_event(event, &futures_channel);
                }
            }
        }

        for media in inputs.media.drain(..) {
            self.handle_media(media, outputs, &futures_channel);
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

fn notify_on_workflow_event(
    mut receiver: UnboundedReceiver<WorkflowStartedOrStoppedEvent>,
    futures_channel: &WorkflowStepFuturesChannel,
) {
    futures_channel.send_on_future_completion(async move {
        match receiver.recv().await {
            Some(event) => FutureResult::WorkflowStartedOrStopped(event, receiver),

            None => FutureResult::EventHubGone,
        }
    });
}

fn notify_on_reactor_update(
    stream_id: StreamId,
    update_receiver: UnboundedReceiver<ReactorWorkflowUpdate>,
    cancellation_token: CancellationToken,
    futures_channel: WorkflowStepFuturesChannel,
) {
    let recv_stream_id = stream_id.clone();
    let cancelled_stream_id = stream_id.clone();
    futures_channel.send_on_unbounded_recv_cancellable(
        update_receiver,
        cancellation_token,
        move |update| FutureResult::ReactorUpdateReceived {
            stream_id: recv_stream_id.clone(),
            update,
        },
        || FutureResult::ReactorGone,
        move || FutureResult::ReactorCancellationReceived {
            stream_id: cancelled_stream_id,
        },
    );
}

fn notify_reactor_manager_gone(
    sender: UnboundedSender<ReactorManagerRequest>,
    futures_channel: &WorkflowStepFuturesChannel,
) {
    futures_channel.send_on_future_completion(async move {
        sender.closed().await;
        FutureResult::ReactorManagerGone
    });
}
