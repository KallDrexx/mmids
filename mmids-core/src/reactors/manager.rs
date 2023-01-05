//! The reactor manager creates new reactors and allows relaying requests to the correct reactor
//! based on names.

use crate::actor_utils::notify_on_unbounded_recv;
use crate::event_hub::SubscriptionRequest;
use crate::reactors::executors::{GenerationError, ReactorExecutorFactory};
use crate::reactors::reactor::ReactorWorkflowUpdate;
use crate::reactors::{start_reactor, ReactorDefinition, ReactorRequest};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tracing::{error, info, instrument, warn};

/// Requests that can be made to the reactor manager
#[derive(Debug)]
pub enum ReactorManagerRequest {
    /// Requests a reactor to be created based on the specified definition
    CreateReactor {
        definition: ReactorDefinition,
        response_channel: Sender<CreateReactorResult>,
    },

    /// Requests that the specified reactor start a workflow based on the specified stream name
    CreateWorkflowForStreamName {
        /// The name of the reactor to send this request to
        reactor_name: Arc<String>,

        /// The name of the stream to look up a workflow for
        stream_name: Arc<String>,

        /// Channel that will be used to keep the created workflow alive. When the sender end of
        /// the channel is closed, that will be a signal to the reactor to remove the created
        /// workflow.
        response_channel: UnboundedSender<ReactorWorkflowUpdate>,
    },
}

#[derive(Debug)]
pub enum CreateReactorResult {
    Success,
    DuplicateReactorName,
    ExecutorGeneratorError(GenerationError),
    ExecutorReturnedError(Box<dyn std::error::Error + Sync + Send>),
}

pub fn start_reactor_manager(
    executor_factory: ReactorExecutorFactory,
    event_hub_subscriber: UnboundedSender<SubscriptionRequest>,
) -> UnboundedSender<ReactorManagerRequest> {
    let (sender, receiver) = unbounded_channel();
    let (actor_sender, actor_receiver) = unbounded_channel();
    let actor = Actor::new(
        executor_factory,
        receiver,
        event_hub_subscriber,
        actor_sender,
    );
    tokio::spawn(actor.run(actor_receiver));

    sender
}

enum FutureResult {
    AllConsumersGone,
    RequestReceived(ReactorManagerRequest),
}

struct Actor {
    executor_factory: ReactorExecutorFactory,
    event_hub_subscriber: UnboundedSender<SubscriptionRequest>,
    reactors: HashMap<Arc<String>, UnboundedSender<ReactorRequest>>,
}

unsafe impl Send for Actor {}

impl Actor {
    fn new(
        executor_factory: ReactorExecutorFactory,
        receiver: UnboundedReceiver<ReactorManagerRequest>,
        event_hub_subscriber: UnboundedSender<SubscriptionRequest>,
        actor_sender: UnboundedSender<FutureResult>,
    ) -> Self {
        notify_on_unbounded_recv(
            receiver,
            actor_sender,
            FutureResult::RequestReceived,
            || FutureResult::AllConsumersGone,
        );

        Actor {
            executor_factory,
            event_hub_subscriber,
            reactors: HashMap::new(),
        }
    }

    #[instrument(name = "Reactor Manager Execution", skip(self))]
    async fn run(mut self, mut receiver: UnboundedReceiver<FutureResult>) {
        info!("Starting reactor manager");

        while let Some(result) = receiver.recv().await {
            match result {
                FutureResult::AllConsumersGone => {
                    info!("All consumers gone");
                    break;
                }

                FutureResult::RequestReceived(request) => {
                    self.handle_request(request);
                }
            }
        }

        info!("Reactor manager closing");
    }

    fn handle_request(&mut self, request: ReactorManagerRequest) {
        match request {
            ReactorManagerRequest::CreateReactor {
                definition,
                response_channel,
            } => {
                if self.reactors.contains_key(&definition.name) {
                    let _ = response_channel.send(CreateReactorResult::DuplicateReactorName);
                    return;
                }

                let generator = match self.executor_factory.get_generator(&definition.executor) {
                    Ok(generator) => generator,
                    Err(error) => {
                        warn!(
                            reactor_name = %definition.name,
                            executor_name = %definition.executor,
                            "Reactor {} is configured to use executor {}, but the factory \
                            returned an error when trying to get it: {:?}",
                            definition.name, definition.executor, error
                        );

                        let _ = response_channel
                            .send(CreateReactorResult::ExecutorGeneratorError(error));
                        return;
                    }
                };

                let executor = match generator.generate(&definition.parameters) {
                    Ok(executor) => executor,
                    Err(error) => {
                        warn!(
                            reactor_name = %definition.name,
                            "Executor failed to be generated for reactor {}: {:?}",
                            definition.name, error
                        );

                        let _ = response_channel
                            .send(CreateReactorResult::ExecutorReturnedError(error));
                        return;
                    }
                };

                let reactor = start_reactor(
                    definition.name.clone(),
                    executor,
                    self.event_hub_subscriber.clone(),
                    definition.update_interval,
                );

                self.reactors.insert(definition.name, reactor);

                let _ = response_channel.send(CreateReactorResult::Success);
            }

            ReactorManagerRequest::CreateWorkflowForStreamName {
                reactor_name,
                stream_name,
                response_channel,
            } => {
                let reactor = match self.reactors.get(&reactor_name) {
                    Some(reactor) => reactor,
                    None => {
                        error!(
                            reactor_name = %reactor_name,
                            "Request received for reactor {}, but no reactor exists with that name",
                            reactor_name,
                        );

                        let _ = response_channel.send(ReactorWorkflowUpdate {
                            is_valid: false,
                            routable_workflow_names: HashSet::new(),
                        });

                        return;
                    }
                };

                let _ = reactor.send(ReactorRequest::CreateWorkflowNameForStream {
                    stream_name,
                    response_channel,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reactors::executors::{
        ReactorExecutionResult, ReactorExecutor, ReactorExecutorGenerator,
    };
    use crate::test_utils;
    use crate::workflows::definitions::WorkflowDefinition;
    use futures::future::BoxFuture;
    use futures::FutureExt;
    use std::error::Error;
    use std::time::Duration;
    use tokio::sync::oneshot::channel;

    #[tokio::test]
    async fn successful_result_for_new_reactor() {
        let context = TestContext::new();

        let mut parameters = HashMap::new();
        parameters.insert("abc".to_string(), None);

        let (sender, receiver) = channel();
        context
            .manager
            .send(ReactorManagerRequest::CreateReactor {
                definition: ReactorDefinition {
                    name: Arc::new("reactor".to_string()),
                    update_interval: Duration::new(0, 0),
                    parameters,
                    executor: "exe".to_string(),
                },
                response_channel: sender,
            })
            .expect("Failed to send create request");

        let response = test_utils::expect_oneshot_response(receiver).await;
        match response {
            CreateReactorResult::Success => (),
            response => panic!("Expected a success response, instead got {:?}", response),
        }
    }

    #[tokio::test]
    async fn duplicate_name_error_when_multiple_reactors_have_same_name() {
        let context = TestContext::new();

        let mut parameters = HashMap::new();
        parameters.insert("abc".to_string(), None);

        let (sender, receiver) = channel();
        context
            .manager
            .send(ReactorManagerRequest::CreateReactor {
                definition: ReactorDefinition {
                    name: Arc::new("reactor".to_string()),
                    update_interval: Duration::new(0, 0),
                    parameters: parameters.clone(),
                    executor: "exe".to_string(),
                },
                response_channel: sender,
            })
            .expect("Failed to send create request");

        let _ = test_utils::expect_oneshot_response(receiver).await;

        let (sender, receiver) = channel();
        context
            .manager
            .send(ReactorManagerRequest::CreateReactor {
                definition: ReactorDefinition {
                    name: Arc::new("reactor".to_string()),
                    update_interval: Duration::new(0, 0),
                    parameters: parameters.clone(),
                    executor: "exe".to_string(),
                },
                response_channel: sender,
            })
            .expect("Failed to send create request");

        let response = test_utils::expect_oneshot_response(receiver).await;
        match response {
            CreateReactorResult::DuplicateReactorName => (),
            response => panic!("Expected a success response, instead got {:?}", response),
        }
    }

    #[tokio::test]
    async fn error_when_generator_fails() {
        let context = TestContext::new();

        let mut parameters = HashMap::new();
        parameters.insert("abcd".to_string(), None);

        let (sender, receiver) = channel();
        context
            .manager
            .send(ReactorManagerRequest::CreateReactor {
                definition: ReactorDefinition {
                    name: Arc::new("reactor".to_string()),
                    update_interval: Duration::new(0, 0),
                    parameters,
                    executor: "exe".to_string(),
                },
                response_channel: sender,
            })
            .expect("Failed to send create request");

        let response = test_utils::expect_oneshot_response(receiver).await;
        match response {
            CreateReactorResult::ExecutorReturnedError(_) => (),
            response => panic!("Expected a success response, instead got {:?}", response),
        }
    }

    #[tokio::test]
    async fn error_when_generator_not_found() {
        let context = TestContext::new();

        let mut parameters = HashMap::new();
        parameters.insert("abc".to_string(), None);

        let (sender, receiver) = channel();
        context
            .manager
            .send(ReactorManagerRequest::CreateReactor {
                definition: ReactorDefinition {
                    name: Arc::new("reactor".to_string()),
                    update_interval: Duration::new(0, 0),
                    parameters,
                    executor: "exe2".to_string(),
                },
                response_channel: sender,
            })
            .expect("Failed to send create request");

        let response = test_utils::expect_oneshot_response(receiver).await;
        match response {
            CreateReactorResult::ExecutorGeneratorError(
                GenerationError::NoRegisteredGenerator(name),
            ) => {
                assert_eq!(&name, "exe2", "Error contained an unexpected name");
            }
            response => panic!("Expected a success response, instead got {:?}", response),
        }
    }

    #[tokio::test]
    async fn create_workflow_request_sends_to_correct_reactor() {
        let context = TestContext::new();

        let mut parameters = HashMap::new();
        parameters.insert("abc".to_string(), None);

        let (sender, receiver) = channel();
        context
            .manager
            .send(ReactorManagerRequest::CreateReactor {
                definition: ReactorDefinition {
                    name: Arc::new("reactor".to_string()),
                    update_interval: Duration::new(0, 0),
                    parameters,
                    executor: "exe".to_string(),
                },
                response_channel: sender,
            })
            .expect("Failed to send create request");

        let response = test_utils::expect_oneshot_response(receiver).await;
        match response {
            CreateReactorResult::Success => (),
            response => panic!("Expected a success response, instead got {:?}", response),
        }

        let (sender, mut receiver) = unbounded_channel();
        context
            .manager
            .send(ReactorManagerRequest::CreateWorkflowForStreamName {
                reactor_name: Arc::new("reactor".to_string()),
                stream_name: Arc::new("def".to_string()),
                response_channel: sender,
            })
            .expect("Failed to send create workflow request");

        let response = test_utils::expect_mpsc_response(&mut receiver).await;
        assert!(
            response.is_valid,
            "Expected response to have an is_valid flag of true"
        );
    }

    #[tokio::test]
    async fn create_workflow_request_returns_not_valid_when_no_reactor_has_specified_name() {
        let context = TestContext::new();

        let mut parameters = HashMap::new();
        parameters.insert("abc".to_string(), None);

        let (sender, receiver) = channel();
        context
            .manager
            .send(ReactorManagerRequest::CreateReactor {
                definition: ReactorDefinition {
                    name: Arc::new("reactor".to_string()),
                    update_interval: Duration::new(0, 0),
                    parameters,
                    executor: "exe".to_string(),
                },
                response_channel: sender,
            })
            .expect("Failed to send create request");

        let response = test_utils::expect_oneshot_response(receiver).await;
        match response {
            CreateReactorResult::Success => (),
            response => panic!("Expected a success response, instead got {:?}", response),
        }

        let (sender, mut receiver) = unbounded_channel();
        context
            .manager
            .send(ReactorManagerRequest::CreateWorkflowForStreamName {
                reactor_name: Arc::new("reactor2".to_string()),
                stream_name: Arc::new("def".to_string()),
                response_channel: sender,
            })
            .expect("Failed to send create workflow request");

        let response = test_utils::expect_mpsc_response(&mut receiver).await;
        assert!(
            !response.is_valid,
            "Expected response to have an is_valid flag of false"
        );
    }

    struct TestContext {
        manager: UnboundedSender<ReactorManagerRequest>,
        _event_receiver: UnboundedReceiver<SubscriptionRequest>,
    }

    struct TestExecutorGenerator;
    struct TestExecutor;

    impl TestContext {
        fn new() -> Self {
            let mut factory = ReactorExecutorFactory::new();
            factory
                .register("exe".to_string(), Box::new(TestExecutorGenerator))
                .expect("Registration failed");

            let (event_sender, event_receiver) = unbounded_channel();
            let manager = start_reactor_manager(factory, event_sender);

            TestContext {
                manager,
                _event_receiver: event_receiver,
            }
        }
    }

    impl ReactorExecutor for TestExecutor {
        fn get_workflow(
            &self,
            _stream_name: Arc<String>,
        ) -> BoxFuture<'static, ReactorExecutionResult> {
            async {
                ReactorExecutionResult::valid(vec![WorkflowDefinition {
                    name: Arc::new("test".to_string()),
                    routed_by_reactor: false,
                    steps: Vec::new(),
                }])
            }
            .boxed()
        }
    }

    impl ReactorExecutorGenerator for TestExecutorGenerator {
        fn generate(
            &self,
            parameters: &HashMap<String, Option<String>>,
        ) -> Result<Box<dyn ReactorExecutor + Send>, Box<dyn Error + Sync + Send>> {
            if parameters.contains_key("abc") {
                Ok(Box::new(TestExecutor))
            } else {
                Err("Test".into())
            }
        }
    }
}
