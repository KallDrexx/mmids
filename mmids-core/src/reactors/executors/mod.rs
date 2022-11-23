pub mod simple_http_executor;

use crate::workflows::definitions::WorkflowDefinition;
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

/// Contains the result from a reactor execution request about a stream
pub struct ReactorExecutionResult {
    /// Was the stream the reactor queried about valid
    pub stream_is_valid: bool,

    /// If the stream was valid, what workflows were defined. it's valid for a stream to be valid
    /// without any workflows.
    pub workflows_returned: Vec<WorkflowDefinition>,
}

/// Performs a request for workflow information on behalf of a reactor
pub trait ReactorExecutor {
    /// Requests the definition of a workflow based on a stream name
    fn get_workflow(&self, stream_name: Arc<String>) -> BoxFuture<'static, ReactorExecutionResult>;
}

/// Allows generating a reactor executor using parameters from a reactor definition
pub trait ReactorExecutorGenerator {
    fn generate(
        &self,
        parameters: &HashMap<String, Option<String>>,
    ) -> Result<Box<dyn ReactorExecutor>, Box<dyn std::error::Error + Sync + Send>>;
}

#[derive(Default)]
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
}

impl ReactorExecutionResult {
    pub fn invalid() -> Self {
        ReactorExecutionResult {
            stream_is_valid: false,
            workflows_returned: Vec::new(),
        }
    }

    pub fn valid(workflows: Vec<WorkflowDefinition>) -> Self {
        ReactorExecutionResult {
            stream_is_valid: true,
            workflows_returned: workflows,
        }
    }
}

impl ReactorExecutorFactory {
    pub fn new() -> Self {
        Default::default()
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
    ) -> Result<&dyn ReactorExecutorGenerator, GenerationError> {
        match self.generators.get(name) {
            Some(generator) => Ok(generator.as_ref()),
            None => Err(GenerationError::NoRegisteredGenerator(name.to_string())),
        }
    }
}
