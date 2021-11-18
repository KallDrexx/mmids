//! The workflow step factory is an actor used to create new instances of workflow steps as needed.
//! Any workflow steps that can be created are registered with the factory by passing in a closure.
//! When the factory is requested to create an instance of that step, it will be created based on
//! the step definition given.
use crate::workflows::definitions::{WorkflowStepDefinition, WorkflowStepType};
use crate::workflows::steps::StepCreationResult;
use log::info;
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub type FactoryCreateResponse = Result<StepCreationResult, FactoryCreateError>;

/// Requests being made to a workflow factory
pub enum FactoryRequest {
    /// Requests to register a function to be used to create a specific type of workflow step
    RegisterFunction {
        /// The type of workflow step to register for
        step_type: WorkflowStepType,

        /// The closure to use for creating new instances of the workflow step
        creation_fn: Box<dyn Fn(&WorkflowStepDefinition) -> StepCreationResult + Send + Sync>,

        /// The channel to be used to send the result of the registration
        response_channel: UnboundedSender<Result<(), FactoryRegistrationError>>,
    },

    /// Requests the workflow factory to create the specified step definition
    CreateInstance {
        /// The definition for the workflow step to create
        definition: WorkflowStepDefinition,

        /// The channel to send the creation result with
        response_channel: UnboundedSender<FactoryCreateResponse>,
    },
}

/// Errors that can occur when a factory registration request fails
#[derive(Error, Debug)]
pub enum FactoryRegistrationError {
    #[error("The workflow step factory already has a step registered with the name '{0}'")]
    DuplicateName(String),
}

/// Errors that can occur when a creation request fails
#[derive(Error, Debug)]
pub enum FactoryCreateError {
    #[error("No workflow step is registered for the name '{0}'")]
    NoRegisteredStep(String),
}

/// Starts a new workflow step factory instance, and returns the channel that can be used to send
/// requests to it.
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
            FactoryRequest::RegisterFunction {
                step_type,
                creation_fn,
                response_channel,
            } => {
                if registered_functions.contains_key(&step_type.0) {
                    let _ = response_channel
                        .send(Err(FactoryRegistrationError::DuplicateName(step_type.0)));
                    continue;
                }

                registered_functions.insert(step_type.0, creation_fn);
                let _ = response_channel.send(Ok(()));
            }

            FactoryRequest::CreateInstance {
                definition,
                response_channel,
            } => {
                let creation_fn = match registered_functions.get(&definition.step_type.0) {
                    Some(x) => x,
                    None => {
                        let _ = response_channel.send(Err(FactoryCreateError::NoRegisteredStep(
                            definition.step_type.0.clone(),
                        )));
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
