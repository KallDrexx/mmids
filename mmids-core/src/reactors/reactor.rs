use crate::event_hub::{SubscriptionRequest, WorkflowManagerEvent};
use crate::reactors::executors::{ReactorExecutionResult, ReactorExecutor};
use crate::workflows::definitions::WorkflowDefinition;
use crate::workflows::manager::{WorkflowManagerRequest, WorkflowManagerRequestOperation};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{info, instrument, warn};

/// Requests that can be made to a reactor
#[derive(Debug)]
pub enum ReactorRequest {
    /// Requests that the reactor creates and manages a workflow for the specified stream name
    CreateWorkflowNameForStream {
        /// Name of the stream to get a workflow for
        stream_name: String,

        /// The channel to send a response for. This channel will not only be used for the
        /// initial response, but updates will be sent any time the reactor detects changes.
        response_channel: UnboundedSender<ReactorWorkflowUpdate>,
    },
}

/// Contains information about a workflow from a reactor
#[derive(Debug)]
pub struct ReactorWorkflowUpdate {
    /// If the reactor considers the stream name valid and workflows have been created for it.
    pub is_valid: bool,

    /// The names of workflows that the reactor expects streams to be routed to.
    pub routable_workflow_names: HashSet<String>,
}

pub fn start_reactor(
    name: String,
    executor: Box<dyn ReactorExecutor>,
    event_hub_subscriber: UnboundedSender<SubscriptionRequest>,
    update_interval: Duration,
) -> UnboundedSender<ReactorRequest> {
    let (sender, receiver) = unbounded_channel();
    let actor = Actor::new(
        name,
        receiver,
        executor,
        event_hub_subscriber,
        update_interval,
    );
    tokio::spawn(actor.run());

    sender
}

enum FutureResult {
    AllRequestConsumersGone,
    EventHubGone,
    WorkflowManagerGone,
    RequestReceived(ReactorRequest, UnboundedReceiver<ReactorRequest>),
    ExecutorResponseReceived {
        stream_name: String,
        result: ReactorExecutionResult,
    },

    WorkflowManagerEventReceived(
        WorkflowManagerEvent,
        UnboundedReceiver<WorkflowManagerEvent>,
    ),

    ClientResponseChannelClosed {
        stream_name: String,
    },

    UpdateStreamNameRequested {
        stream_name: String,
    },
}

struct CachedWorkflows {
    definitions: Vec<WorkflowDefinition>,
}

struct Actor {
    name: String,
    executor: Box<dyn ReactorExecutor>,
    futures: FuturesUnordered<BoxFuture<'static, FutureResult>>,
    workflow_manager: Option<UnboundedSender<WorkflowManagerRequest>>,
    cached_workflows_for_stream_name: HashMap<String, CachedWorkflows>,
    update_interval: Duration,
    stream_response_channels: HashMap<String, Vec<UnboundedSender<ReactorWorkflowUpdate>>>,
}

unsafe impl Send for Actor {}

impl Actor {
    fn new(
        name: String,
        receiver: UnboundedReceiver<ReactorRequest>,
        executor: Box<dyn ReactorExecutor>,
        event_hub_subscriber: UnboundedSender<SubscriptionRequest>,
        update_interval: Duration,
    ) -> Self {
        let futures = FuturesUnordered::new();
        futures.push(wait_for_request(receiver).boxed());

        let (manager_sender, manager_receiver) = unbounded_channel();
        let _ = event_hub_subscriber.send(SubscriptionRequest::WorkflowManagerEvents {
            channel: manager_sender,
        });

        futures.push(wait_for_workflow_manager_event(manager_receiver).boxed());

        Actor {
            name,
            executor,
            futures,
            workflow_manager: None,
            cached_workflows_for_stream_name: HashMap::new(),
            update_interval,
            stream_response_channels: HashMap::new(),
        }
    }

    #[instrument(name = "Reactor Execution", skip(self), fields(name = %self.name))]
    async fn run(mut self) {
        info!("Starting reactor");

        while let Some(result) = self.futures.next().await {
            match result {
                FutureResult::AllRequestConsumersGone => {
                    info!("All consumers gone");
                    break;
                }

                FutureResult::EventHubGone => {
                    info!("Event manager gone");
                    break;
                }

                FutureResult::WorkflowManagerGone => {
                    info!("Workflow manager gone");
                    break;
                }

                FutureResult::ClientResponseChannelClosed { stream_name } => {
                    self.handle_response_channel_closed(stream_name);
                }

                FutureResult::RequestReceived(request, receiver) => {
                    self.futures.push(wait_for_request(receiver).boxed());
                    self.handle_request(request);
                }

                FutureResult::ExecutorResponseReceived {
                    stream_name,
                    result: workflow,
                } => {
                    self.handle_executor_response(stream_name, workflow);
                }

                FutureResult::UpdateStreamNameRequested { stream_name } => {
                    if self
                        .cached_workflows_for_stream_name
                        .contains_key(&stream_name)
                    {
                        let future = self.executor.get_workflow(stream_name.clone());
                        self.futures
                            .push(wait_for_executor_response(stream_name, future).boxed());
                    }
                }

                FutureResult::WorkflowManagerEventReceived(event, receiver) => {
                    self.futures
                        .push(wait_for_workflow_manager_event(receiver).boxed());

                    self.handle_workflow_manager_event(event);
                }
            }
        }

        info!("Reactor closing");
    }

    fn handle_request(&mut self, request: ReactorRequest) {
        match request {
            ReactorRequest::CreateWorkflowNameForStream {
                stream_name,
                response_channel,
            } => {
                info!(
                    stream_name = %stream_name,
                    "Received request to get workflow for stream '{}'", stream_name
                );

                let channels = self
                    .stream_response_channels
                    .entry(stream_name.clone())
                    .or_default();

                channels.push(response_channel.clone());

                if let Some(cache) = self.cached_workflows_for_stream_name.get_mut(&stream_name) {
                    let _ = response_channel.send(ReactorWorkflowUpdate {
                        is_valid: true,
                        routable_workflow_names: cache
                            .definitions
                            .iter()
                            .filter(|w| w.routed_by_reactor)
                            .map(|w| w.name.clone())
                            .collect::<HashSet<_>>(),
                    });
                } else {
                    let future = self.executor.get_workflow(stream_name.clone());
                    self.futures
                        .push(wait_for_executor_response(stream_name.clone(), future).boxed());
                }

                self.futures.push(
                    notify_when_response_channel_closed(response_channel, stream_name).boxed(),
                );
            }
        }
    }

    fn handle_executor_response(&mut self, stream_name: String, result: ReactorExecutionResult) {
        if let Some(channels) = self.stream_response_channels.get(&stream_name) {
            let routed_workflow_names = result
                .workflows_returned
                .iter()
                .filter(|w| w.routed_by_reactor)
                .map(|w| w.name.clone())
                .collect::<HashSet<_>>();

            info!(
                stream_name = %stream_name,
                workflow_count = %result.workflows_returned.len(),
                routed_count = %routed_workflow_names.len(),
                "Executor returned {} workflows ({} routed) for the stream '{}'",
                result.workflows_returned.len(), routed_workflow_names.len(), stream_name,
            );

            if !result.stream_is_valid {
                if let Some(cache) = self.cached_workflows_for_stream_name.remove(&stream_name) {
                    // Since we had some workflows cached, and now the external service isn't giving us
                    // any workflows, that means this stream name is no longer valid.
                    if let Some(manager) = &self.workflow_manager {
                        for workflow in cache.definitions {
                            let _ = manager.send(WorkflowManagerRequest {
                                request_id: format!(
                                    "reactor_{}_stream_{}_ended",
                                    self.name, stream_name
                                ),
                                operation: WorkflowManagerRequestOperation::StopWorkflow {
                                    name: workflow.name,
                                },
                            });
                        }
                    }
                }
            } else {
                if routed_workflow_names.is_empty() {
                    warn!(
                        stream_name = %stream_name,
                        "Zero routed workflows returned for stream '{}'. Any workflow router steps \
                            will not forward media to these workflows", stream_name
                    );
                }

                // Upsert all returned workflows
                if let Some(manager) = &self.workflow_manager {
                    for workflow in &result.workflows_returned {
                        let _ = manager.send(WorkflowManagerRequest {
                            request_id: format!(
                                "reactor_{}_stream_{}_update",
                                self.name, stream_name
                            ),
                            operation: WorkflowManagerRequestOperation::UpsertWorkflow {
                                definition: workflow.clone(),
                            },
                        });
                    }
                }

                let current_workflow_names = result
                    .workflows_returned
                    .iter()
                    .map(|w| w.name.clone())
                    .collect::<HashSet<_>>();

                let new_cache = CachedWorkflows {
                    definitions: result.workflows_returned,
                };

                if let Some(old_cache) = self
                    .cached_workflows_for_stream_name
                    .insert(stream_name.clone(), new_cache)
                {
                    // Stop any workflows that are were not returned by the executor
                    if let Some(manager) = &self.workflow_manager {
                        for workflow in old_cache.definitions {
                            if !current_workflow_names.contains(&workflow.name) {
                                let _ = manager.send(WorkflowManagerRequest {
                                    request_id: format!(
                                        "reactor_{}_stream_{}_partially_ended",
                                        self.name, stream_name
                                    ),
                                    operation: WorkflowManagerRequestOperation::StopWorkflow {
                                        name: workflow.name,
                                    },
                                });
                            }
                        }
                    }
                }
            }

            for channel in channels {
                let _ = channel.send(ReactorWorkflowUpdate {
                    is_valid: result.stream_is_valid,
                    routable_workflow_names: routed_workflow_names.clone(),
                });
            }

            if !self.update_interval.is_zero() {
                self.futures
                    .push(wait_for_update_interval(stream_name, self.update_interval).boxed());
            }
        }
    }

    fn handle_workflow_manager_event(&mut self, event: WorkflowManagerEvent) {
        match event {
            WorkflowManagerEvent::WorkflowManagerRegistered { channel } => {
                info!("Reactor received a workflow manager channel");
                self.futures
                    .push(notify_workflow_manager_gone(channel.clone()).boxed());

                // Upsert all cached workflows
                for cached_workflow in self.cached_workflows_for_stream_name.values() {
                    for workflow in &cached_workflow.definitions {
                        let _ = channel.send(WorkflowManagerRequest {
                            request_id: format!("reactor_{}_cache_catchup", self.name),
                            operation: WorkflowManagerRequestOperation::UpsertWorkflow {
                                definition: workflow.clone(),
                            },
                        });
                    }
                }

                self.workflow_manager = Some(channel);
            }
        }
    }

    fn handle_response_channel_closed(&mut self, stream_name: String) {
        if let Some(channels) = self.stream_response_channels.get_mut(&stream_name) {
            for x in (0..channels.len()).rev() {
                if channels[x].is_closed() {
                    channels.remove(x);
                }
            }

            if channels.is_empty() {
                info!(
                    stream_name = %stream_name,
                    "All response channels for stream {} closed", stream_name
                );

                self.stream_response_channels.remove(&stream_name);

                if let Some(channel) = &self.workflow_manager {
                    if let Some(cache) = self.cached_workflows_for_stream_name.remove(&stream_name)
                    {
                        for workflow in cache.definitions {
                            let _ = channel.send(WorkflowManagerRequest {
                                request_id: format!(
                                    "reactor_{}_stream_{}_closed",
                                    self.name, stream_name
                                ),
                                operation: WorkflowManagerRequestOperation::StopWorkflow {
                                    name: workflow.name,
                                },
                            });
                        }
                    }
                }
            } else {
                info!(
                    stream_name = %stream_name,
                    "Response channel for stream {} closed but {} still remain",
                    stream_name, channels.len(),
                );
            }
        }
    }
}

async fn wait_for_request(mut receiver: UnboundedReceiver<ReactorRequest>) -> FutureResult {
    match receiver.recv().await {
        Some(request) => FutureResult::RequestReceived(request, receiver),
        None => FutureResult::AllRequestConsumersGone,
    }
}

async fn wait_for_executor_response(
    stream_name: String,
    future: BoxFuture<'static, ReactorExecutionResult>,
) -> FutureResult {
    let result = future.await;
    FutureResult::ExecutorResponseReceived {
        stream_name,
        result,
    }
}

async fn wait_for_workflow_manager_event(
    mut receiver: UnboundedReceiver<WorkflowManagerEvent>,
) -> FutureResult {
    match receiver.recv().await {
        Some(event) => FutureResult::WorkflowManagerEventReceived(event, receiver),
        None => FutureResult::EventHubGone,
    }
}

async fn notify_workflow_manager_gone(
    sender: UnboundedSender<WorkflowManagerRequest>,
) -> FutureResult {
    sender.closed().await;
    FutureResult::WorkflowManagerGone
}

async fn notify_when_response_channel_closed(
    channel: UnboundedSender<ReactorWorkflowUpdate>,
    stream_name: String,
) -> FutureResult {
    channel.closed().await;
    FutureResult::ClientResponseChannelClosed { stream_name }
}

async fn wait_for_update_interval(stream_name: String, wait_time: Duration) -> FutureResult {
    tokio::time::sleep(wait_time).await;
    FutureResult::UpdateStreamNameRequested { stream_name }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils;
    use crate::workflows::definitions::{WorkflowStepDefinition, WorkflowStepType};
    use tokio::time::timeout;

    struct TestContext {
        _event_hub: UnboundedReceiver<SubscriptionRequest>,
        _workflow_manager_events: UnboundedSender<WorkflowManagerEvent>,
        workflow_manager: UnboundedReceiver<WorkflowManagerRequest>,
        reactor: UnboundedSender<ReactorRequest>,
    }

    struct TestExecutor {
        expected_name: String,
        workflows: Vec<WorkflowDefinition>,
    }

    impl TestContext {
        async fn new(name: String, duration: Duration, executor: TestExecutor) -> Self {
            let (sender, mut sub_receiver) = unbounded_channel();
            let reactor = start_reactor(name, Box::new(executor), sender, duration);

            let response = test_utils::expect_mpsc_response(&mut sub_receiver).await;
            let response_channel = match response {
                SubscriptionRequest::WorkflowManagerEvents { channel } => channel,
                event => panic!("Unexpected event: {:?}", event),
            };

            let (wm_sender, wm_receiver) = unbounded_channel();
            response_channel
                .send(WorkflowManagerEvent::WorkflowManagerRegistered { channel: wm_sender })
                .expect("Channel closed");

            TestContext {
                reactor,
                _event_hub: sub_receiver,
                _workflow_manager_events: response_channel,
                workflow_manager: wm_receiver,
            }
        }
    }

    impl ReactorExecutor for TestExecutor {
        fn get_workflow(&self, stream_name: String) -> BoxFuture<'static, ReactorExecutionResult> {
            let future = if self.expected_name == stream_name {
                let workflows = self.workflows.clone();
                async { ReactorExecutionResult::valid(workflows) }.boxed()
            } else {
                async { ReactorExecutionResult::invalid() }.boxed()
            };

            future
        }
    }

    #[tokio::test]
    async fn can_get_routable_workflows_from_executor() {
        let executor = TestExecutor {
            expected_name: "stream".to_string(),
            workflows: get_test_workflows(),
        };

        let context =
            TestContext::new("reactor".to_string(), Duration::from_millis(0), executor).await;
        let (sender, mut receiver) = unbounded_channel();
        context
            .reactor
            .send(ReactorRequest::CreateWorkflowNameForStream {
                stream_name: "stream".to_string(),
                response_channel: sender,
            })
            .expect("Channel closed");

        let update = test_utils::expect_mpsc_response(&mut receiver).await;
        assert!(update.is_valid, "Expected is valid to be true");
        assert_eq!(
            update.routable_workflow_names.len(),
            2,
            "Expected 2 routable workflows"
        );
        assert!(
            update.routable_workflow_names.contains("first"),
            "Did not find 'first' workflow in routable results"
        );

        assert!(
            update.routable_workflow_names.contains("third"),
            "Did not find 'third' workflow in routable results"
        );
    }

    #[tokio::test]
    async fn not_valid_if_stream_name_invalid() {
        let executor = TestExecutor {
            expected_name: "stream".to_string(),
            workflows: get_test_workflows(),
        };

        let context =
            TestContext::new("reactor".to_string(), Duration::from_millis(0), executor).await;
        let (sender, mut receiver) = unbounded_channel();
        context
            .reactor
            .send(ReactorRequest::CreateWorkflowNameForStream {
                stream_name: "invalid".to_string(),
                response_channel: sender,
            })
            .expect("Channel closed");

        let update = test_utils::expect_mpsc_response(&mut receiver).await;

        assert!(!update.is_valid, "Expected is valid to be false");
        assert_eq!(
            update.routable_workflow_names.len(),
            0,
            "Expected no routable workflow names"
        );
    }

    #[tokio::test]
    async fn all_workflows_upserted_to_workflow_manager() {
        let executor = TestExecutor {
            expected_name: "stream".to_string(),
            workflows: get_test_workflows(),
        };

        let mut context =
            TestContext::new("reactor".to_string(), Duration::from_millis(0), executor).await;
        let (sender, _receiver) = unbounded_channel();
        context
            .reactor
            .send(ReactorRequest::CreateWorkflowNameForStream {
                stream_name: "stream".to_string(),
                response_channel: sender,
            })
            .expect("Channel closed");

        let mut workflows_found = [false, false, false];
        loop {
            let request = test_utils::expect_mpsc_response(&mut context.workflow_manager).await;
            match request.operation {
                WorkflowManagerRequestOperation::UpsertWorkflow { definition } => {
                    if &definition.name == "first" {
                        if workflows_found[0] {
                            panic!("Received duplicate upsert request for workflow 'first'");
                        }

                        assert_eq!(definition.steps.len(), 1, "Expected 1 workflows");
                        workflows_found[0] = true;
                    } else if &definition.name == "second" {
                        if workflows_found[1] {
                            panic!("Received duplicate upsert request for workflow 'second'");
                        }

                        assert_eq!(definition.steps.len(), 2, "Expected 2 workflow steps");
                        workflows_found[1] = true;
                    } else if &definition.name == "third" {
                        if workflows_found[2] {
                            panic!("Received duplicate upsert request for workflow 'third'");
                        }

                        assert_eq!(definition.steps.len(), 3, "Expected 3 workflow steps");
                        workflows_found[2] = true;
                    } else {
                        panic!("Unexpected workflow: {}", definition.name);
                    }
                }

                operation => panic!("Expected upsert request, instead got {:?}", operation),
            }

            if workflows_found[0] && workflows_found[1] && workflows_found[2] {
                break;
            }
        }

        test_utils::expect_mpsc_timeout(&mut context.workflow_manager).await;
    }

    #[tokio::test]
    async fn workflows_not_updated_when_duration_is_zero() {
        let executor = TestExecutor {
            expected_name: "stream".to_string(),
            workflows: get_test_workflows(),
        };

        let context =
            TestContext::new("reactor".to_string(), Duration::from_secs(10), executor).await;
        let (sender, mut receiver) = unbounded_channel();
        context
            .reactor
            .send(ReactorRequest::CreateWorkflowNameForStream {
                stream_name: "stream".to_string(),
                response_channel: sender,
            })
            .expect("Channel closed");

        let _ = test_utils::expect_mpsc_response(&mut receiver).await;
        test_utils::expect_mpsc_timeout(&mut receiver).await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        test_utils::expect_mpsc_timeout(&mut receiver).await;
    }

    #[tokio::test]
    async fn routable_workflows_updated_when_duration_set() {
        let executor = TestExecutor {
            expected_name: "stream".to_string(),
            workflows: get_test_workflows(),
        };

        let context =
            TestContext::new("reactor".to_string(), Duration::from_millis(500), executor).await;
        let (sender, mut receiver) = unbounded_channel();
        context
            .reactor
            .send(ReactorRequest::CreateWorkflowNameForStream {
                stream_name: "stream".to_string(),
                response_channel: sender,
            })
            .expect("Channel closed");

        let _ = test_utils::expect_mpsc_response(&mut receiver).await;
        test_utils::expect_mpsc_timeout(&mut receiver).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let update = test_utils::expect_mpsc_response(&mut receiver).await;
        assert!(update.is_valid, "Expected is valid to be true");
        assert_eq!(
            update.routable_workflow_names.len(),
            2,
            "Expected 2 routable workflows"
        );
        assert!(
            update.routable_workflow_names.contains("first"),
            "Did not find 'first' workflow in routable results"
        );

        assert!(
            update.routable_workflow_names.contains("third"),
            "Did not find 'third' workflow in routable results"
        );
    }

    #[tokio::test]
    async fn all_workflows_upserted_to_workflow_manager_again_after_duration() {
        let executor = TestExecutor {
            expected_name: "stream".to_string(),
            workflows: get_test_workflows(),
        };

        let mut context =
            TestContext::new("reactor".to_string(), Duration::from_millis(500), executor).await;

        let (sender, _receiver) = unbounded_channel();
        context
            .reactor
            .send(ReactorRequest::CreateWorkflowNameForStream {
                stream_name: "stream".to_string(),
                response_channel: sender,
            })
            .expect("Channel closed");

        let wait_time = Duration::from_millis(10);
        while timeout(wait_time, context.workflow_manager.recv())
            .await
            .is_ok()
        {
            // Keep looping until we time out, thus the workflow manager channel becomes empty
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        let mut workflows_found = [false, false, false];
        loop {
            let request = test_utils::expect_mpsc_response(&mut context.workflow_manager).await;
            match request.operation {
                WorkflowManagerRequestOperation::UpsertWorkflow { definition } => {
                    if &definition.name == "first" {
                        if workflows_found[0] {
                            panic!("Received duplicate upsert request for workflow 'first'");
                        }

                        assert_eq!(definition.steps.len(), 1, "Expected 1 workflows");
                        workflows_found[0] = true;
                    } else if &definition.name == "second" {
                        if workflows_found[1] {
                            panic!("Received duplicate upsert request for workflow 'second'");
                        }

                        assert_eq!(definition.steps.len(), 2, "Expected 2 workflow steps");
                        workflows_found[1] = true;
                    } else if &definition.name == "third" {
                        if workflows_found[2] {
                            panic!("Received duplicate upsert request for workflow 'third'");
                        }

                        assert_eq!(definition.steps.len(), 3, "Expected 3 workflow steps");
                        workflows_found[2] = true;
                    } else {
                        panic!("Unexpected workflow: {}", definition.name);
                    }
                }

                operation => panic!("Expected upsert request, instead got {:?}", operation),
            }

            if workflows_found[0] && workflows_found[1] && workflows_found[2] {
                break;
            }
        }

        test_utils::expect_mpsc_timeout(&mut context.workflow_manager).await;
    }

    #[tokio::test]
    async fn workflow_manager_not_given_new_workflows_when_duration_is_zero() {
        let executor = TestExecutor {
            expected_name: "stream".to_string(),
            workflows: get_test_workflows(),
        };

        let mut context =
            TestContext::new("reactor".to_string(), Duration::from_millis(0), executor).await;
        let (sender, _receiver) = unbounded_channel();
        context
            .reactor
            .send(ReactorRequest::CreateWorkflowNameForStream {
                stream_name: "stream".to_string(),
                response_channel: sender,
            })
            .expect("Channel closed");

        let wait_time = Duration::from_millis(10);
        while timeout(wait_time, context.workflow_manager.recv())
            .await
            .is_ok()
        {
            // Keep looping until we time out, thus the workflow manager channel becomes empty
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
        test_utils::expect_mpsc_timeout(&mut context.workflow_manager).await;
    }

    fn get_test_workflows() -> Vec<WorkflowDefinition> {
        vec![
            WorkflowDefinition {
                name: "first".to_string(),
                routed_by_reactor: true,
                steps: vec![WorkflowStepDefinition {
                    step_type: WorkflowStepType("a".to_string()),
                    parameters: HashMap::new(),
                }],
            },
            WorkflowDefinition {
                name: "second".to_string(),
                routed_by_reactor: false,
                steps: vec![
                    WorkflowStepDefinition {
                        step_type: WorkflowStepType("b".to_string()),
                        parameters: HashMap::new(),
                    },
                    WorkflowStepDefinition {
                        step_type: WorkflowStepType("c".to_string()),
                        parameters: HashMap::new(),
                    },
                ],
            },
            WorkflowDefinition {
                name: "third".to_string(),
                routed_by_reactor: true,
                steps: vec![
                    WorkflowStepDefinition {
                        step_type: WorkflowStepType("d".to_string()),
                        parameters: HashMap::new(),
                    },
                    WorkflowStepDefinition {
                        step_type: WorkflowStepType("e".to_string()),
                        parameters: HashMap::new(),
                    },
                    WorkflowStepDefinition {
                        step_type: WorkflowStepType("f".to_string()),
                        parameters: HashMap::new(),
                    },
                ],
            },
        ]
    }
}
