//! The reactor manager creates new reactors and allows relaying requests to the correct reactor
//! based on names.

use crate::event_hub::SubscriptionRequest;
use crate::reactors::executors::simple_http_executor::SimpleHttpExecutorGenerator;
use crate::reactors::executors::{
    GenerationError, ReactorExecutorFactory, ReactorExecutorGenerator,
};
use crate::reactors::{start_reactor, ReactorDefinition, ReactorRequest};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tracing::{error, info, instrument, warn};

/// Requests that can be made to the reactor manager
pub enum ReactorManagerRequest {
    /// Requests a reactor to be created based on the specified definition
    CreateReactor {
        definition: ReactorDefinition,
        response_channel: Sender<CreateReactorResult>,
    },

    /// Requests that the specified reactor start a workflow based on the specified stream name
    CreateWorkflowForStreamName {
        /// The name of the reactor to send this request to
        reactor_name: String,

        /// The name of the stream to look up a workflow for
        stream_name: String,

        /// The channel in which to send the response to. The response will contain the name of
        /// the workflow the stream is associated with, if one was found.
        response_channel: Sender<Option<String>>,
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
    let actor = Actor::new(executor_factory, receiver, event_hub_subscriber);
    tokio::spawn(actor.run());

    sender
}

enum FutureResult {
    AllConsumersGone,
    RequestReceived(
        ReactorManagerRequest,
        UnboundedReceiver<ReactorManagerRequest>,
    ),
}

struct Actor {
    executor_factory: ReactorExecutorFactory,
    event_hub_subscriber: UnboundedSender<SubscriptionRequest>,
    futures: FuturesUnordered<BoxFuture<'static, FutureResult>>,
    reactors: HashMap<String, UnboundedSender<ReactorRequest>>,
}

unsafe impl Send for Actor {}

impl Actor {
    fn new(
        executor_factory: ReactorExecutorFactory,
        receiver: UnboundedReceiver<ReactorManagerRequest>,
        event_hub_subscriber: UnboundedSender<SubscriptionRequest>,
    ) -> Self {
        let futures = FuturesUnordered::new();
        futures.push(wait_for_request(receiver).boxed());

        Actor {
            executor_factory,
            event_hub_subscriber,
            futures,
            reactors: HashMap::new(),
        }
    }

    #[instrument(name = "Reactor Manager Execution", skip(self))]
    async fn run(mut self) {
        info!("Starting reactor manager");

        while let Some(result) = self.futures.next().await {
            match result {
                FutureResult::AllConsumersGone => {
                    info!("All consumers gone");
                    break;
                }

                FutureResult::RequestReceived(request, receiver) => {
                    self.futures.push(wait_for_request(receiver).boxed());
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

                let default_generator = get_default_generator();
                let generator = match definition.executor {
                    None => &default_generator,
                    Some(executor) => match self.executor_factory.get_generator(&executor) {
                        Ok(generator) => generator,
                        Err(error) => {
                            warn!(
                                reactor_name = %definition.name,
                                executor_name = %executor,
                                "Reactor {} is configured to use executor {}, but the factory \
                                returned an error when trying to get it: {:?}",
                                definition.name, executor, error
                            );

                            let _ = response_channel
                                .send(CreateReactorResult::ExecutorGeneratorError(error));
                            return;
                        }
                    },
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

                        let _ = response_channel.send(None);
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

fn get_default_generator() -> Box<dyn ReactorExecutorGenerator> {
    Box::new(SimpleHttpExecutorGenerator {})
}

async fn wait_for_request(mut receiver: UnboundedReceiver<ReactorManagerRequest>) -> FutureResult {
    match receiver.recv().await {
        Some(request) => FutureResult::RequestReceived(request, receiver),
        None => FutureResult::AllConsumersGone,
    }
}
