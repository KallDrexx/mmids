use crate::workflows::definitions::{WorkflowStepDefinition, WorkflowStepType};
use crate::workflows::steps::StepCreationResult;
use log::info;
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

pub type FactoryCreateResponse = Result<StepCreationResult, FactoryCreateError>;

pub enum FactoryRequest {
    RegisterFunction {
        step_type: WorkflowStepType,
        creation_fn: Box<dyn Fn(&WorkflowStepDefinition) -> StepCreationResult + Send + Sync>,
        response_channel: UnboundedSender<Result<(), FactoryRegistrationError>>,
    },

    CreateInstance {
        definition: WorkflowStepDefinition,
        response_channel: UnboundedSender<FactoryCreateResponse>,
    },
}

#[derive(Error, Debug)]
pub enum FactoryRegistrationError {
    #[error("The workflow step factory already has a step registered with the name '{0}'")]
    DuplicateName(String),
}

#[derive(Error, Debug)]
pub enum FactoryCreateError {
    #[error("No workflow step is registered for the name '{0}'")]
    NoRegisteredStep(String),
}

pub fn start_step_factory() -> UnboundedSender<FactoryRequest> {
    let (sender, receiver) = unbounded_channel();
    tokio::spawn(run(receiver));

    sender
}

async fn run(mut receiver: UnboundedReceiver<FactoryRequest>) {
    let mut registered_functions = HashMap::new();

    info!("Starting workflow step factory");
    while let Some(request) = receiver.recv().await {
        match request {
            FactoryRequest::RegisterFunction { step_type, creation_fn, response_channel } => {
                if registered_functions.contains_key(&step_type.0) {
                    let _ = response_channel.send(Err(FactoryRegistrationError::DuplicateName(step_type.0)));
                    continue;
                }

                registered_functions.insert(step_type.0, creation_fn);
                let _ = response_channel.send(Ok(()));
            }

            FactoryRequest::CreateInstance { definition, response_channel } => {
                let creation_fn = match registered_functions.get(&definition.step_type.0) {
                    Some(x) => x,
                    None => {
                        let _ = response_channel.send(Err(FactoryCreateError::NoRegisteredStep(definition.step_type.0.clone())));
                        continue;
                    }
                };

                let result = creation_fn(&definition);
                let _ = response_channel.send(Ok(result));
            }
        }
    }

    info!("Workflow step factory closing");
}
