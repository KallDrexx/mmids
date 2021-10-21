use crate::workflows::definitions::WorkflowDefinition;
use crate::workflows::steps::{WorkflowStep, StepFutureResult, StepInputs, StepOutputs, StepStatus};
use crate::workflows::steps::factory::{FactoryRequest, FactoryCreateResponse, FactoryCreateError};
use std::collections::HashMap;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use futures::stream::FuturesUnordered;
use log::{info, warn, error};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

pub enum WorkflowRequest {}

pub fn start(
    definition: WorkflowDefinition,
    step_factory: UnboundedSender<FactoryRequest>,
) -> UnboundedSender<WorkflowRequest> {
    let (sender, receiver) = unbounded_channel();
    let actor = Actor::new(definition, step_factory, receiver);
    tokio::spawn(actor.run());

    sender
}

enum FutureResult {
    AllConsumersGone,
    WorkflowRequestReceived(WorkflowRequest, UnboundedReceiver<WorkflowRequest>),
    StepFactoryGone,
    StepCreationResultReceived {
        step_id: u64,
        result: FactoryCreateResponse,
    },

    StepFutureResolved {
        step_id: u64,
        result: Box<dyn StepFutureResult>,
    }
}

struct Actor<'a> {
    name: String,
    steps_by_definition_id: HashMap<u64, Box<dyn WorkflowStep>>,
    active_steps: Vec<u64>,
    pending_steps: Vec<u64>,
    futures: FuturesUnordered<BoxFuture<'a, FutureResult>>,
    step_inputs: StepInputs,
    step_outputs: StepOutputs<'a>,
}

impl<'a> Actor<'a> {
    fn new(
        definition: WorkflowDefinition,
        step_factory: UnboundedSender<FactoryRequest>,
        receiver: UnboundedReceiver<WorkflowRequest>,
    ) -> Self {
        let futures = FuturesUnordered::new();
        let mut pending_steps = Vec::new();
        for step in &definition.steps {
            pending_steps.push(step.get_id());

            let (factory_sender, factory_receiver) = unbounded_channel();
            let _ = step_factory.send(FactoryRequest::CreateInstance {
                definition: step.clone(),
                response_channel: factory_sender,
            });

            futures.push(wait_for_factory_response(factory_receiver, step.get_id()).boxed())
        }

        futures.push(wait_for_workflow_request(receiver).boxed());

        Actor {
            name: definition.name.clone(),
            futures,
            steps_by_definition_id: HashMap::new(),
            active_steps: Vec::new(),
            pending_steps: Vec::new(),
            step_inputs: StepInputs::new(),
            step_outputs: StepOutputs::new(),
        }
    }

    async fn run(mut self) {
        info!("Starting workflow '{}'", self.name);

        while let Some(future) = self.futures.next().await {
            match future {
                FutureResult::AllConsumersGone => {
                    warn!("Workflow {}: All channel owners gone", self.name);
                    break;
                }

                FutureResult::StepFactoryGone => {
                    warn!("Workflow {}: step factory is gone", self.name);
                    break;
                }

                FutureResult::WorkflowRequestReceived(_request, receiver) => {
                   self.futures.push(wait_for_workflow_request(receiver).boxed());
                }

                FutureResult::StepCreationResultReceived {step_id, result: factory_response} => {
                    self.handle_step_creation_result(step_id, factory_response);
                }

                FutureResult::StepFutureResolved {step_id, result} => {
                    self.execute_steps(step_id, Some(result));
                }
            }
        }

        info!("Workflow '{}' closing", self.name);
    }

    fn handle_step_creation_result(&mut self, step_id: u64, factory_response: FactoryCreateResponse) {
        let step_result = match factory_response {
            Ok(x) => x,
            Err(error) => match error {
                FactoryCreateError::NoRegisteredStep(step) => {
                    error!("Workflow {}: Requested creation of step '{}' but no step has \
                                been registered with that name.", self.name, step);

                    return;
                }
            }
        };

        let (step, mut future_list) = match step_result {
            Ok(x) => x,
            Err(error) => {
                error!("Workflow {}: Creation of step id {} failed: {}", self.name, step_id, error);
                return;
            }
        };

        info!("Workflow {}: Successfully got created instance of step id {}", self.name, step_id);
        self.steps_by_definition_id.insert(step.get_definition().get_id(), step);

        for future in future_list.drain(..) {
            self.futures.push(wait_for_step_future(step_id, future).boxed());
        }

        // TODO: Check if all steps have been instantiated.  If so swap pending and active steps so
        // the pipeline becomes active.
    }

    fn execute_steps(&mut self, initial_step_id: u64, future_result: Option<Box<dyn StepFutureResult>>) {
        self.step_inputs.clear();
        self.step_outputs.clear();

        if let Some(future_result) = future_result {
            self.step_inputs.notifications.push(future_result);
        }

        let mut start_index = None;
        for x in 0..self.active_steps.len() {
            if self.active_steps[x] == initial_step_id {
                start_index = Some(x);
                break;
            }
        }

        if let Some(start_index) = start_index {
            for x in start_index..self.active_steps.len() {
                let step = match self.steps_by_definition_id.get_mut(&self.active_steps[x]) {
                    Some(x) => x,
                    None => {
                        error!("Workflow {}: step id {} is marked as active but no definition exists", self.name, self.active_steps[x]);
                        return;
                    }
                };

                step.execute(&mut self.step_inputs, &mut self.step_outputs);

                self.step_inputs.clear();
                self.step_inputs.media.extend( self.step_outputs.media.drain(..));

                for future in self.step_outputs.futures.drain(..) {
                    self.futures.push(wait_for_step_future(step.get_definition().get_id(), future).boxed());
                }

                self.step_outputs.clear();
            }
        } else {
            // We are trying to execute a workflow step that is not yet active, most likely due to
            // a resolved future specifically for it.
            let step = match self.steps_by_definition_id.get_mut(&initial_step_id) {
                Some(x) => x,
                None => {
                    error!("Workflow {}: step id {} got a resolved future but we do not have \
                    a definition for it", self.name, initial_step_id);

                    return;
                }
            };

            step.execute(&mut self.step_inputs, &mut self.step_outputs);

            for future in self.step_outputs.futures.drain(..) {
                self.futures.push(wait_for_step_future(step.get_definition().get_id(), future).boxed());
            }

            // TODO: Figure out what to do with media outputs in this case.  We probably need to
            // store it and play it down the active stat once this becomes active.
        }

        self.check_if_all_pending_steps_are_active();
    }

    fn check_if_all_pending_steps_are_active(&mut self) {
        let mut any_pending = false;
        for id in &self.pending_steps {
            let step = match self.steps_by_definition_id.get(id) {
                Some(x) => x,
                None => {
                    error!("Workflow {}: workflow had step id {} pending but this step was not defined", self.name, id);
                    return; // TODO: set workflow in error state
                },
            };

            match step.get_status() {
                StepStatus::Created => any_pending = true,
                StepStatus::Active => (),
                StepStatus::Error => return, // TODO: Set workflow in error state
            }
        }

        if self.pending_steps.len() > 0 && !any_pending {
            // Since all created steps are fully active, we can now make them all active
            std::mem::swap(&mut self.pending_steps, &mut self.active_steps);

            // remove any steps that were in the active list that were removed
            for id in &self.pending_steps {
                if !self.active_steps.contains(id) {
                    self.steps_by_definition_id.remove(id);
                    info!("Workflow {}: Removing now unused step id {}", self.name, id);
                }
            }
        }
    }
}

unsafe impl Send for Actor<'_> {}

async fn wait_for_workflow_request<'a>(mut receiver: UnboundedReceiver<WorkflowRequest>) -> FutureResult {
    match receiver.recv().await {
        Some(x) => FutureResult::WorkflowRequestReceived(x, receiver),
       None => FutureResult::AllConsumersGone,
   }
}

async fn wait_for_factory_response<'a>(
    mut receiver: UnboundedReceiver<FactoryCreateResponse>,
    step_id: u64,
) -> FutureResult {
    match receiver.recv().await {
        Some(result) => FutureResult::StepCreationResultReceived {step_id, result},
        None => FutureResult::StepFactoryGone,
    }
}

async fn wait_for_step_future<'a>(step_id: u64, future: BoxFuture<'a, Box<dyn StepFutureResult>>) -> FutureResult {
    let result = future.await;
    FutureResult::StepFutureResolved {
        step_id,
        result,
    }
}