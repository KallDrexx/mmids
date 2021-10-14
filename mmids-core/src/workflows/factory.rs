use crate::workflows::steps::{StepFutureResult, WorkflowStep};
use futures::future::BoxFuture;
use std::collections::HashMap;
use thiserror::Error;

pub type FutureList<'a> = Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>;
pub type StepCreationResult<'a> =
    Result<(Box<dyn WorkflowStep>, FutureList<'a>), Box<dyn std::error::Error>>;

pub struct WorkflowStepFactory<'a> {
    registered_functions: HashMap<String, Box<dyn Fn() -> StepCreationResult<'a>>>,
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

impl<'a> WorkflowStepFactory<'a> {
    pub fn new() -> Self {
        WorkflowStepFactory {
            registered_functions: HashMap::new(),
        }
    }

    pub fn register(
        &mut self,
        name: String,
        creation_fn: Box<dyn Fn() -> StepCreationResult<'a>>,
    ) -> Result<(), FactoryRegistrationError> {
        if self.registered_functions.contains_key(&name) {
            return Err(FactoryRegistrationError::DuplicateName(name));
        }

        self.registered_functions.insert(name, creation_fn);

        Ok(())
    }

    pub fn create(&mut self, name: String) -> Result<StepCreationResult, FactoryCreateError> {
        let function = match self.registered_functions.get(&name) {
            Some(x) => x,
            None => return Err(FactoryCreateError::NoRegisteredStep(name)),
        };

        let result = function();
        Ok(result)
    }
}
