use crate::workflows::definitions::WorkflowStepDefinition;
use crate::workflows::steps::{StepFutureResult, WorkflowStep};
use futures::future::BoxFuture;
use log::info;
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

pub type FutureList<'a> = Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>;
pub type StepCreationResult<'a> = Result<(Box<dyn WorkflowStep + Sync + Send>, FutureList<'a>), Box<dyn std::error::Error + Sync + Send>>;

pub enum FactoryRequest<'a> {
    RegisterFunction {
        name: String,
        creation_fn: Box<dyn Fn(WorkflowStepDefinition) -> StepCreationResult<'a> + Send + Sync>,
        response_channel: UnboundedSender<Result<(), FactoryRegistrationError>>,
    },

    CreateInstance {
        name: String,
        definition: WorkflowStepDefinition,
        response_channel: UnboundedSender<Result<StepCreationResult<'a>, FactoryCreateError>>,
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

pub fn start() -> UnboundedSender<FactoryRequest<'static>> {
    let (sender, receiver) = unbounded_channel();
    tokio::spawn(run(receiver));

    sender
}

async fn run(mut receiver: UnboundedReceiver<FactoryRequest<'_>>) {
    let mut registered_functions = HashMap::new();

    info!("Starting workflow step factory");
    while let Some(request) = receiver.recv().await {
        match request {
            FactoryRequest::RegisterFunction { name, creation_fn, response_channel } => {
                if registered_functions.contains_key(&name) {
                    let _ = response_channel.send(Err(FactoryRegistrationError::DuplicateName(name)));
                    continue;
                }

                registered_functions.insert(name, creation_fn);
                let _ = response_channel.send(Ok(()));
            }

            FactoryRequest::CreateInstance {name, definition, response_channel} => {
                let creation_fn = match registered_functions.get(&name) {
                    Some(x) => x,
                    None => {
                        let _ = response_channel.send(Err(FactoryCreateError::NoRegisteredStep(name.clone())));
                        continue;
                    }
                };

                let result = creation_fn(definition);
                let _ = response_channel.send(Ok(result));
            }
        }
    }

    info!("Workflow step factory closing");
}
