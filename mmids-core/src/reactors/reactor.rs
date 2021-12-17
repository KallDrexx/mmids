use crate::event_hub::{SubscriptionRequest, WorkflowManagerEvent};
use crate::reactors::executors::ReactorExecutor;
use crate::workflows::definitions::WorkflowDefinition;
use crate::workflows::manager::WorkflowManagerRequest;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tracing::{error, info, instrument};

/// Requests that can be made to a reactor
pub enum ReactorRequest {
    /// Requests that the reactor gets the name of the workflow for a stream name
    GetWorkflowNameForStream {
        /// Name of the stream to get a workflow for
        stream_name: String,

        /// The channel to send a response for. It will either send the name of the workflow
        /// associated with the stream name, or `None`, representing that no workflow is associated
        /// with the stream.
        response_channel: Sender<Option<String>>,
    },
}

pub fn start_reactor(
    name: String,
    executor: Box<dyn ReactorExecutor>,
    event_hub_subscriber: UnboundedSender<SubscriptionRequest>,
) -> UnboundedSender<ReactorRequest> {
    let (sender, receiver) = unbounded_channel();
    let actor = Actor::new(name, receiver, executor, event_hub_subscriber);
    tokio::spawn(actor.run());

    sender
}

enum FutureResult {
    AllRequestConsumersGone,
    EventHubGone,
    WorkflowManagerGone,
    RequestReceived(ReactorRequest, UnboundedReceiver<ReactorRequest>),
    ExecutorResponseReceived(String, Option<WorkflowDefinition>),
    WorkflowManagerEventReceived(
        WorkflowManagerEvent,
        UnboundedReceiver<WorkflowManagerEvent>,
    ),
}

struct Actor {
    name: String,
    executor: Box<dyn ReactorExecutor>,
    futures: FuturesUnordered<BoxFuture<'static, FutureResult>>,
    active_requests: HashMap<String, Sender<Option<String>>>,
    workflow_manager: Option<UnboundedSender<WorkflowManagerRequest>>,
}

unsafe impl Send for Actor {}

impl Actor {
    fn new(
        name: String,
        receiver: UnboundedReceiver<ReactorRequest>,
        executor: Box<dyn ReactorExecutor>,
        event_hub_subscriber: UnboundedSender<SubscriptionRequest>,
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
            active_requests: HashMap::new(),
            workflow_manager: None,
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

                FutureResult::RequestReceived(request, receiver) => {
                    self.futures.push(wait_for_request(receiver).boxed());
                    self.handle_request(request);
                }

                FutureResult::ExecutorResponseReceived(stream_name, workflow) => {
                    self.handle_executor_response(stream_name, workflow);
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
            ReactorRequest::GetWorkflowNameForStream {
                stream_name,
                response_channel,
            } => {
                info!(
                    stream_name = %stream_name,
                    "Received request to get workflow for stream '{}'", stream_name
                );

                self.active_requests
                    .insert(stream_name.clone(), response_channel);
                let future = self.executor.get_workflow(stream_name.clone());
                self.futures
                    .push(wait_for_executor_response(stream_name, future).boxed());
            }
        }
    }

    fn handle_executor_response(
        &mut self,
        stream_name: String,
        workflow: Option<WorkflowDefinition>,
    ) {
        let channel = match self.active_requests.remove(&stream_name) {
            Some(channel) => channel,
            None => {
                error!(
                    stream_name = %stream_name,
                    "Received executor response for stream '{}' but we do not have that logged as an \
                    active request.", stream_name,
                );

                return;
            }
        };

        if let Some(workflow) = workflow {
            info!(
                stream_name = %stream_name,
                workflow_name = %workflow.name,
                "Executor returned a workflow with the name {} for the stream {}",
                workflow.name, stream_name,
            );

            let _ = channel.send(Some(workflow.name));
        } else {
            info!(
                stream_name = %stream_name,
                "Executor returned no workflow for the stream {}", stream_name,
            );

            let _ = channel.send(None);
        }
    }

    fn handle_workflow_manager_event(&mut self, event: WorkflowManagerEvent) {
        match event {
            WorkflowManagerEvent::WorkflowManagerRegistered { channel } => {
                info!("Reactor received a workflow manager channel");
                self.futures
                    .push(notify_workflow_manager_gone(channel.clone()).boxed());
                self.workflow_manager = Some(channel);
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
    future: BoxFuture<'static, Option<WorkflowDefinition>>,
) -> FutureResult {
    let result = future.await;
    FutureResult::ExecutorResponseReceived(stream_name, result)
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
