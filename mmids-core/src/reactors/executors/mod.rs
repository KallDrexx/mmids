pub mod simple_http_executor;

use crate::workflows::definitions::WorkflowDefinition;
use futures::future::BoxFuture;
use std::collections::HashMap;
use thiserror::Error;

/// Performs a request for workflow information on behalf of a reactor
pub trait ReactorExecutor {
    /// Requests the definition of a workflow based on a stream name
    fn get_workflow(&self, stream_name: String) -> BoxFuture<'static, Vec<WorkflowDefinition>>;
}

/// Allows generating a reactor executor using parameters from a reactor definition
pub trait ReactorExecutorGenerator {
    fn generate(
        &self,
        parameters: &HashMap<String, Option<String>>,
    ) -> Result<Box<dyn ReactorExecutor>, Box<dyn std::error::Error + Sync + Send>>;
}

pub struct ReactorExecutorFactory {
    generators: HashMap<String, Box<dyn ReactorExecutorGenerator>>,
}

#[derive(Error, Debug)]
pub enum RegistrationError {
    #[error("A reactor executor generator is already registered with the name '{0}'")]
    DuplicateName(String),
}

#[derive(Error, Debug)]
pub enum GenerationError {
    #[error("No generators have been registered for the executor name '{0}'")]
    NoRegisteredGenerator(String),

    #[error("The reactor to create did not have an executor specified")]
    NoExecutorDefined,
}

impl ReactorExecutorFactory {
    pub fn new() -> Self {
        ReactorExecutorFactory {
            generators: HashMap::new(),
        }
    }

    pub fn register(
        &mut self,
        name: String,
        generator: Box<dyn ReactorExecutorGenerator>,
    ) -> Result<(), RegistrationError> {
        if self.generators.contains_key(&name) {
            return Err(RegistrationError::DuplicateName(name));
        }

        self.generators.insert(name, generator);
        Ok(())
    }

    pub fn get_generator(
        &self,
        name: &str,
    ) -> Result<&Box<dyn ReactorExecutorGenerator>, GenerationError> {
        match self.generators.get(name) {
            Some(generator) => Ok(generator),
            None => return Err(GenerationError::NoRegisteredGenerator(name.to_string())),
        }
    }
}
