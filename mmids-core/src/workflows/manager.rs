//! A workflow manager is a centralized actor that orchestrates multiple workflows.  It can be
//! used to start new workflows, change the steps of a managed workflow, get status the of managed
//! workflows, and stop a managed workflow.

use crate::workflows::definitions::WorkflowDefinition;
use crate::workflows::steps::factory::WorkflowStepFactory;
use crate::workflows::{start_workflow, WorkflowRequest};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{info, instrument, warn};

/// Operations consumers can request the workflow manager to perform
pub enum WorkflowManagerRequest {
    /// Starts or updates a specified workflow based on the passed in definition
    UpsertWorkflow { definition: WorkflowDefinition },

    /// Stops the specified workflow, if it is running
    StopWorkflow { name: String },
}

pub fn start_workflow_manager(
    step_factory: Arc<WorkflowStepFactory>,
) -> UnboundedSender<WorkflowManagerRequest> {
    let (sender, receiver) = unbounded_channel();
    let actor = Actor::new(step_factory);
    tokio::spawn(actor.run(receiver));

    sender
}

enum FutureResult {
    AllConsumersGone,
    WorkflowManagerRequestReceived(
        WorkflowManagerRequest,
        UnboundedReceiver<WorkflowManagerRequest>,
    ),
    WorkflowGone(String),
}

struct Actor<'a> {
    futures: FuturesUnordered<BoxFuture<'a, FutureResult>>,
    workflows: HashMap<String, UnboundedSender<WorkflowRequest>>,
    step_factory: Arc<WorkflowStepFactory>,
}

impl<'a> Actor<'a> {
    fn new(step_factory: Arc<WorkflowStepFactory>) -> Self {
        Actor {
            futures: FuturesUnordered::new(),
            workflows: HashMap::new(),
            step_factory,
        }
    }

    #[instrument(name = "Workflow Manager Execution", skip(self))]
    async fn run(mut self, request_receiver: UnboundedReceiver<WorkflowManagerRequest>) {
        self.futures
            .push(wait_for_request(request_receiver).boxed());

        info!("Starting workflow manager");
        while let Some(result) = self.futures.next().await {
            match result {
                FutureResult::AllConsumersGone => {
                    info!("All consumers gone");
                    break;
                }

                FutureResult::WorkflowManagerRequestReceived(request, receiver) => {
                    self.futures.push(wait_for_request(receiver).boxed());
                    self.handle_request(request);
                }

                FutureResult::WorkflowGone(name) => {
                    if let Some(_) = self.workflows.remove(&name) {
                        warn!(
                            workflow_name = %name,
                            "Workflow '{}' unexpectedly had its request channel disappear", name
                        );
                    }
                }
            }
        }

        info!("Workflow manager closing")
    }

    fn handle_request(&mut self, request: WorkflowManagerRequest) {
        match request {
            WorkflowManagerRequest::UpsertWorkflow { definition } => {
                if let Some(sender) = self.workflows.get_mut(&definition.name) {
                    info!(
                        workflow_name = %definition.name,
                        "Updating existing workflow '{}' with new definition", definition.name,
                    );

                    let _ = sender.send(WorkflowRequest::UpdateDefinition {
                        new_definition: definition,
                    });
                } else {
                    info!(
                        workflow_name = %definition.name,
                        "Starting workflow '{}'", definition.name,
                    );

                    let name = definition.name.clone();
                    let sender = start_workflow(definition, self.step_factory.clone());
                    self.futures
                        .push(wait_for_workflow_gone(sender.clone(), name.clone()).boxed());
                    self.workflows.insert(name, sender);
                }
            }

            WorkflowManagerRequest::StopWorkflow { name } => {
                info!(
                    workflow_name = %name,
                    "Stopping workflow '{}'", name,
                );

                // Removing the workflow from the hashmap should be enough, as all consumers of
                // the workflow request channel will be gone and it'll shut itself down.
                self.workflows.remove(&name);
            }
        }
    }
}

async fn wait_for_request(mut receiver: UnboundedReceiver<WorkflowManagerRequest>) -> FutureResult {
    match receiver.recv().await {
        Some(request) => FutureResult::WorkflowManagerRequestReceived(request, receiver),
        None => FutureResult::AllConsumersGone,
    }
}

async fn wait_for_workflow_gone(
    sender: UnboundedSender<WorkflowRequest>,
    name: String,
) -> FutureResult {
    sender.closed().await;
    FutureResult::WorkflowGone(name)
}
