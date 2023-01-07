//! A workflow manager is a centralized actor that orchestrates multiple workflows.  It can be
//! used to start new workflows, change the steps of a managed workflow, get status the of managed
//! workflows, and stop a managed workflow.

use crate::actor_utils::notify_on_unbounded_recv;
use crate::event_hub::{PublishEventRequest, WorkflowManagerEvent, WorkflowStartedOrStoppedEvent};
use crate::workflows::definitions::WorkflowDefinition;
use crate::workflows::runner::{WorkflowRequestOperation, WorkflowState};
use crate::workflows::steps::factory::WorkflowStepFactory;
use crate::workflows::{start_workflow, WorkflowRequest};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tracing::{info, instrument, warn};

/// Requests an action be taken by the workflow manager
#[derive(Debug)]
pub struct WorkflowManagerRequest {
    /// An identifier that can identify this request. Mostly used for correlations
    pub request_id: String,

    /// The specific operation being requested of the workflow manager
    pub operation: WorkflowManagerRequestOperation,
}

/// Operations consumers can request the workflow manager to perform
#[derive(Debug)]
pub enum WorkflowManagerRequestOperation {
    /// Starts or updates a specified workflow based on the passed in definition
    UpsertWorkflow { definition: WorkflowDefinition },

    /// Stops the specified workflow, if it is running
    StopWorkflow { name: Arc<String> },

    /// Requests information about all workflows currently running
    GetRunningWorkflows {
        response_channel: Sender<Vec<GetWorkflowResponse>>,
    },

    /// Requests details about a specific workflow
    GetWorkflowDetails {
        name: Arc<String>,
        response_channel: Sender<Option<WorkflowState>>,
    },
}

#[derive(Debug)]
pub struct GetWorkflowResponse {
    pub name: Arc<String>,
}

pub fn start_workflow_manager(
    step_factory: Arc<WorkflowStepFactory>,
    event_hub_publisher: UnboundedSender<PublishEventRequest>,
) -> UnboundedSender<WorkflowManagerRequest> {
    let (sender, receiver) = unbounded_channel();
    let (actor_sender, actor_receiver) = unbounded_channel();
    let actor = Actor::new(step_factory, event_hub_publisher, receiver, actor_sender);
    tokio::spawn(actor.run(sender.clone(), actor_receiver));

    sender
}

enum FutureResult {
    AllConsumersGone,
    EventHubGone,
    WorkflowManagerRequestReceived(WorkflowManagerRequest),
    WorkflowGone(Arc<String>),
}

struct Actor {
    internal_sender: UnboundedSender<FutureResult>,
    workflows: HashMap<Arc<String>, UnboundedSender<WorkflowRequest>>,
    step_factory: Arc<WorkflowStepFactory>,
    event_hub_publisher: UnboundedSender<PublishEventRequest>,
}

impl Actor {
    fn new(
        step_factory: Arc<WorkflowStepFactory>,
        event_hub_publisher: UnboundedSender<PublishEventRequest>,
        request_receiver: UnboundedReceiver<WorkflowManagerRequest>,
        actor_sender: UnboundedSender<FutureResult>,
    ) -> Self {
        notify_on_unbounded_recv(
            request_receiver,
            actor_sender.clone(),
            FutureResult::WorkflowManagerRequestReceived,
            || FutureResult::AllConsumersGone,
        );

        Actor {
            internal_sender: actor_sender,
            workflows: HashMap::new(),
            step_factory,
            event_hub_publisher,
        }
    }

    #[instrument(name = "Workflow Manager Execution", skip_all)]
    async fn run(
        mut self,
        request_sender: UnboundedSender<WorkflowManagerRequest>,
        mut actor_receiver: UnboundedReceiver<FutureResult>,
    ) {
        notify_when_event_hub_is_gone(
            self.event_hub_publisher.clone(),
            self.internal_sender.clone(),
        );

        info!("Starting workflow manager");
        let _ = self
            .event_hub_publisher
            .send(PublishEventRequest::WorkflowManagerEvent(
                WorkflowManagerEvent::WorkflowManagerRegistered {
                    channel: request_sender,
                },
            ));

        while let Some(result) = actor_receiver.recv().await {
            match result {
                FutureResult::AllConsumersGone => {
                    info!("All consumers gone");
                    break;
                }

                FutureResult::EventHubGone => {
                    warn!("Event hub is gone");
                    break;
                }

                FutureResult::WorkflowManagerRequestReceived(request) => {
                    self.handle_request(request);
                }

                FutureResult::WorkflowGone(name) => {
                    if self.workflows.remove(&name).is_some() {
                        let event =
                            WorkflowStartedOrStoppedEvent::WorkflowEnded { name: name.clone() };
                        let _ = self
                            .event_hub_publisher
                            .send(PublishEventRequest::WorkflowStartedOrStopped(event));

                        warn!(
                            workflow_name = %name,
                            "Workflow '{}' had its request channel disappear", name
                        );
                    }
                }
            }
        }

        info!("Workflow manager closing")
    }

    #[instrument(skip(self, request), fields(request_id = %request.request_id))]
    fn handle_request(&mut self, request: WorkflowManagerRequest) {
        match request.operation {
            WorkflowManagerRequestOperation::UpsertWorkflow { definition } => {
                if let Some(sender) = self.workflows.get_mut(&definition.name) {
                    info!(
                        workflow_name = %definition.name,
                        "Updating existing workflow '{}' with new definition", definition.name,
                    );

                    let _ = sender.send(WorkflowRequest {
                        request_id: request.request_id,
                        operation: WorkflowRequestOperation::UpdateDefinition {
                            new_definition: definition,
                        },
                    });
                } else {
                    info!(
                        workflow_name = %definition.name,
                        "Starting workflow '{}'", definition.name,
                    );

                    let name = definition.name.clone();
                    let sender = start_workflow(definition, self.step_factory.clone());
                    notify_on_workflow_gone(
                        sender.clone(),
                        name.clone(),
                        self.internal_sender.clone(),
                    );

                    self.workflows.insert(name.clone(), sender.clone());

                    let event = WorkflowStartedOrStoppedEvent::WorkflowStarted {
                        name,
                        channel: sender,
                    };

                    let _ = self
                        .event_hub_publisher
                        .send(PublishEventRequest::WorkflowStartedOrStopped(event));
                }
            }

            WorkflowManagerRequestOperation::StopWorkflow { name } => {
                info!(
                    workflow_name = %name,
                    "Stopping workflow '{}'", name,
                );

                if let Some(sender) = self.workflows.remove(&name) {
                    let _ = sender.send(WorkflowRequest {
                        request_id: request.request_id,
                        operation: WorkflowRequestOperation::StopWorkflow,
                    });

                    let event = WorkflowStartedOrStoppedEvent::WorkflowEnded { name: name.clone() };

                    let _ = self
                        .event_hub_publisher
                        .send(PublishEventRequest::WorkflowStartedOrStopped(event));
                }
            }

            WorkflowManagerRequestOperation::GetRunningWorkflows { response_channel } => {
                let mut response = self
                    .workflows
                    .keys()
                    .map(|x| GetWorkflowResponse { name: x.clone() })
                    .collect::<Vec<_>>();

                response.sort_by(|a, b| b.name.cmp(&a.name));

                let _ = response_channel.send(response);
            }

            WorkflowManagerRequestOperation::GetWorkflowDetails {
                name,
                response_channel,
            } => match self.workflows.get(&name) {
                None => {
                    let _ = response_channel.send(None);
                }

                Some(sender) => {
                    let _ = sender.send(WorkflowRequest {
                        request_id: request.request_id,
                        operation: WorkflowRequestOperation::GetState { response_channel },
                    });
                }
            },
        }
    }
}

fn notify_when_event_hub_is_gone(
    sender: UnboundedSender<PublishEventRequest>,
    actor_sender: UnboundedSender<FutureResult>,
) {
    tokio::spawn(async move {
        tokio::select! {
            _ = sender.closed() => {
                let _ = actor_sender.send(FutureResult::EventHubGone);
            }

            _ = actor_sender.closed() => { }
        }
    });
}

fn notify_on_workflow_gone(
    sender: UnboundedSender<WorkflowRequest>,
    name: Arc<String>,
    actor_sender: UnboundedSender<FutureResult>,
) {
    tokio::spawn(async move {
        tokio::select! {
            _ = sender.closed() => {
                let _ = actor_sender.send(FutureResult::WorkflowGone(name));
            }

            _ = actor_sender.closed() => { }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils;
    use tokio::sync::oneshot::channel;

    struct TestContext {
        event_hub: UnboundedReceiver<PublishEventRequest>,
        manager: UnboundedSender<WorkflowManagerRequest>,
    }

    impl TestContext {
        fn new() -> Self {
            let (sender, receiver) = unbounded_channel();
            let factory = Arc::new(WorkflowStepFactory::new());
            let manager = start_workflow_manager(factory, sender);

            TestContext {
                event_hub: receiver,
                manager,
            }
        }
    }

    #[tokio::test]
    async fn new_workflow_manager_registers_with_event_hub() {
        let mut context = TestContext::new();

        let event = test_utils::expect_mpsc_response(&mut context.event_hub).await;
        match event {
            PublishEventRequest::WorkflowManagerEvent(event) => match event {
                WorkflowManagerEvent::WorkflowManagerRegistered { channel: _ } => (),
            },

            event => panic!("Expected workflow manager event, instead got {:?}", event),
        }
    }

    #[tokio::test]
    async fn created_workflow_has_event_published() {
        let mut context = TestContext::new();
        test_utils::expect_mpsc_response(&mut context.event_hub).await; // manager registered event

        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::UpsertWorkflow {
                    definition: WorkflowDefinition {
                        name: Arc::new("workflow".to_string()),
                        routed_by_reactor: false,
                        steps: Vec::new(),
                    },
                },
            })
            .expect("Failed to send upsert request");

        let event = test_utils::expect_mpsc_response(&mut context.event_hub).await;
        match event {
            PublishEventRequest::WorkflowStartedOrStopped(event) => match event {
                WorkflowStartedOrStoppedEvent::WorkflowStarted { name, channel: _ } => {
                    assert_eq!(name.as_str(), "workflow", "Unexpected workflow name");
                }

                event => panic!("Unexpected workflow event received: {:?}", event),
            },

            event => panic!("Unexpected publish event received; {:?}", event),
        }

        test_utils::expect_mpsc_timeout(&mut context.event_hub).await;
    }

    #[tokio::test]
    async fn created_workflow_shows_in_workflow_list() {
        let context = TestContext::new();
        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::UpsertWorkflow {
                    definition: WorkflowDefinition {
                        name: Arc::new("workflow".to_string()),
                        routed_by_reactor: false,
                        steps: Vec::new(),
                    },
                },
            })
            .expect("Failed to send upsert request");

        let (sender, receiver) = channel();
        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::GetRunningWorkflows {
                    response_channel: sender,
                },
            })
            .expect("failed to send list workflow request");

        let response = test_utils::expect_oneshot_response(receiver).await;
        assert_eq!(response.len(), 1, "Unexpected number of workflows");
        assert_eq!(
            response[0].name.as_str(),
            "workflow",
            "Unexpected workflow name"
        );
    }

    #[tokio::test]
    async fn can_get_details_of_created_workflow() {
        let context = TestContext::new();
        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::UpsertWorkflow {
                    definition: WorkflowDefinition {
                        name: Arc::new("workflow".to_string()),
                        routed_by_reactor: false,
                        steps: Vec::new(),
                    },
                },
            })
            .expect("Failed to send upsert request");

        let (sender, receiver) = channel();
        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::GetWorkflowDetails {
                    name: Arc::new("workflow".to_string()),
                    response_channel: sender,
                },
            })
            .expect("failed to send list workflow request");

        let response = test_utils::expect_oneshot_response(receiver).await;
        assert!(
            response.is_some(),
            "Expected workflow details to be returned"
        );
    }

    #[tokio::test]
    async fn second_upsert_request_does_not_send_second_stated_event() {
        let mut context = TestContext::new();
        test_utils::expect_mpsc_response(&mut context.event_hub).await; // manager registered event

        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::UpsertWorkflow {
                    definition: WorkflowDefinition {
                        name: Arc::new("workflow".to_string()),
                        routed_by_reactor: false,
                        steps: Vec::new(),
                    },
                },
            })
            .expect("Failed to send upsert request");

        let _ = test_utils::expect_mpsc_response(&mut context.event_hub).await;

        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::UpsertWorkflow {
                    definition: WorkflowDefinition {
                        name: Arc::new("workflow".to_string()),
                        routed_by_reactor: false,
                        steps: Vec::new(),
                    },
                },
            })
            .expect("Failed to send upsert request");

        test_utils::expect_mpsc_timeout(&mut context.event_hub).await;
    }

    #[tokio::test]
    async fn second_created_workflow_does_not_duplicate_in_workflow_list() {
        let context = TestContext::new();
        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::UpsertWorkflow {
                    definition: WorkflowDefinition {
                        name: Arc::new("workflow".to_string()),
                        routed_by_reactor: false,
                        steps: Vec::new(),
                    },
                },
            })
            .expect("Failed to send upsert request");

        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::UpsertWorkflow {
                    definition: WorkflowDefinition {
                        name: Arc::new("workflow".to_string()),
                        routed_by_reactor: false,
                        steps: Vec::new(),
                    },
                },
            })
            .expect("Failed to send upsert request");

        let (sender, receiver) = channel();
        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::GetRunningWorkflows {
                    response_channel: sender,
                },
            })
            .expect("failed to send list workflow request");

        let response = test_utils::expect_oneshot_response(receiver).await;
        assert_eq!(response.len(), 1, "Unexpected number of workflows");
        assert_eq!(
            response[0].name.as_str(),
            "workflow",
            "Unexpected workflow name"
        );
    }

    #[tokio::test]
    async fn stopping_workflow_sends_stopped_event() {
        let mut context = TestContext::new();
        test_utils::expect_mpsc_response(&mut context.event_hub).await; // manager registered event

        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::UpsertWorkflow {
                    definition: WorkflowDefinition {
                        name: Arc::new("workflow".to_string()),
                        routed_by_reactor: false,
                        steps: Vec::new(),
                    },
                },
            })
            .expect("Failed to send upsert request");

        let _ = test_utils::expect_mpsc_response(&mut context.event_hub).await;
        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::StopWorkflow {
                    name: Arc::new("workflow".to_string()),
                },
            })
            .expect("Failed to send stop command");

        let event = test_utils::expect_mpsc_response(&mut context.event_hub).await;
        match event {
            PublishEventRequest::WorkflowStartedOrStopped(event) => match event {
                WorkflowStartedOrStoppedEvent::WorkflowEnded { name } => {
                    assert_eq!(name.as_str(), "workflow", "Unexpected workflow name");
                }

                event => panic!("Unexpected workflow event received: {:?}", event),
            },

            event => panic!("Unexpected publish event received; {:?}", event),
        }

        test_utils::expect_mpsc_timeout(&mut context.event_hub).await;
    }

    #[tokio::test]
    async fn stopped_workflow_does_not_show_in_workflow_list() {
        let mut context = TestContext::new();
        test_utils::expect_mpsc_response(&mut context.event_hub).await; // manager registered event

        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::UpsertWorkflow {
                    definition: WorkflowDefinition {
                        name: Arc::new("workflow".to_string()),
                        routed_by_reactor: false,
                        steps: Vec::new(),
                    },
                },
            })
            .expect("Failed to send upsert request");

        let _ = test_utils::expect_mpsc_response(&mut context.event_hub).await;
        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::StopWorkflow {
                    name: Arc::new("workflow".to_string()),
                },
            })
            .expect("Failed to send stop command");

        let _ = test_utils::expect_mpsc_response(&mut context.event_hub).await;

        let (sender, receiver) = channel();
        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::GetRunningWorkflows {
                    response_channel: sender,
                },
            })
            .expect("Failed to send get running workflow request");

        let response = test_utils::expect_oneshot_response(receiver).await;
        assert!(response.is_empty(), "Expected empty workflow list");
    }

    #[tokio::test]
    async fn no_details_returned_for_stopped_workflow() {
        let mut context = TestContext::new();
        test_utils::expect_mpsc_response(&mut context.event_hub).await; // manager registered event

        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::UpsertWorkflow {
                    definition: WorkflowDefinition {
                        name: Arc::new("workflow".to_string()),
                        routed_by_reactor: false,
                        steps: Vec::new(),
                    },
                },
            })
            .expect("Failed to send upsert request");

        let _ = test_utils::expect_mpsc_response(&mut context.event_hub).await;
        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::StopWorkflow {
                    name: Arc::new("workflow".to_string()),
                },
            })
            .expect("Failed to send stop command");

        let _ = test_utils::expect_mpsc_response(&mut context.event_hub).await;

        let (sender, receiver) = channel();
        context
            .manager
            .send(WorkflowManagerRequest {
                request_id: "".to_string(),
                operation: WorkflowManagerRequestOperation::GetWorkflowDetails {
                    name: Arc::new("workflow".to_string()),
                    response_channel: sender,
                },
            })
            .expect("Failed to send get running workflow request");

        let response = test_utils::expect_oneshot_response(receiver).await;
        assert!(response.is_none(), "Expected no workflow details returned");
    }
}
