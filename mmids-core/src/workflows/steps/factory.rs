use crate::workflows::definitions::{WorkflowStepDefinition, WorkflowStepType};
use crate::workflows::steps::StepCreationResult;
use std::collections::HashMap;
use thiserror::Error;

/// Represents a type that can generate an instance of a workflow step
pub trait StepGenerator {
    /// Creates a brand new instance of a workflow step based on the supplied definition
    fn generate(&self, definition: WorkflowStepDefinition) -> StepCreationResult;
}

/// The workflow step factory allows consumers to register different workflow step generation
/// instances to use for specific workflow step types.  Consumers can then request the factory
/// to generate workflow steps based on the passed in step definition.
pub struct WorkflowStepFactory {
    generators: HashMap<WorkflowStepType, Box<dyn StepGenerator>>,
}

/// Errors that can occur when an attempting to register a generator fails
#[derive(Error, Debug)]
pub enum FactoryRegistrationError {
    #[error(
        "The workflow step factory already has a step generator registered with the type '{0}'"
    )]
    DuplicateName(WorkflowStepType),
}

/// Errors that can occur when an attempt to generate a workflow step fails
#[derive(Error, Debug)]
pub enum FactoryCreateError {
    #[error("No workflow step generator is registered for the type '{0}'")]
    NoRegisteredStep(WorkflowStepType),
}

impl WorkflowStepFactory {
    /// Creates a new workflow step factory, with an empty registration
    pub fn new() -> Self {
        WorkflowStepFactory {
            generators: HashMap::new(),
        }
    }

    /// Attempts to register a specific generator instance with the specified
    pub fn register(
        &mut self,
        step_type: WorkflowStepType,
        generator: Box<dyn StepGenerator>,
    ) -> Result<(), FactoryRegistrationError> {
        if self.generators.contains_key(&step_type) {
            return Err(FactoryRegistrationError::DuplicateName(step_type));
        }

        self.generators.insert(step_type, generator);
        return Ok(());
    }

    /// Attempts to create a new instance of a workflow step based on a specified definition
    pub fn create_step(
        &self,
        definition: WorkflowStepDefinition,
    ) -> Result<StepCreationResult, FactoryCreateError> {
        let generator = match self.generators.get(&definition.step_type) {
            Some(generator) => generator,
            None => return Err(FactoryCreateError::NoRegisteredStep(definition.step_type)),
        };

        Ok(generator.generate(definition))
    }
}
