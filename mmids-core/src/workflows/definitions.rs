use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};

/// Identifier representing the type of the workflow step being defined
#[derive(Clone, Hash, Debug, Eq, PartialEq)]
pub struct WorkflowStepType(pub String);

/// The definition of a workflow step and any parameters it may be using
#[derive(Clone, Debug)]
pub struct WorkflowStepDefinition {
    pub step_type: WorkflowStepType,
    pub parameters: HashMap<String, Option<String>>,
}

/// The definition of a workflow and the steps (in order) it contains
#[derive(Clone, Debug)]
pub struct WorkflowDefinition {
    pub name: String,
    pub routed_by_reactor: bool,
    pub steps: Vec<WorkflowStepDefinition>,
}

impl std::fmt::Display for WorkflowStepType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl WorkflowStepDefinition {
    /// Gets an identifier for the workflow step that's based on the step's parameters.  Two
    /// steps with the same set of parameters and values will always produce the same id within
    /// a single run of the the application, but the identifiers are not guaranteed to be consistent
    /// across application runs.
    pub fn get_id(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Hash for WorkflowStepDefinition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut sorted_keys: Vec<&String> = self.parameters.keys().collect();
        sorted_keys.sort();

        self.step_type.hash(state);
        for key in sorted_keys {
            key.hash(state);
            self.parameters.get(key).hash(state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_steps_with_identical_setups_have_same_id() {
        let mut step1 = WorkflowStepDefinition {
            step_type: WorkflowStepType("test".to_string()),
            parameters: HashMap::new(),
        };

        step1
            .parameters
            .insert("a".to_string(), Some("b".to_string()));
        step1
            .parameters
            .insert("c".to_string(), Some("d".to_string()));

        let mut step2 = WorkflowStepDefinition {
            step_type: WorkflowStepType("test".to_string()),
            parameters: HashMap::new(),
        };

        step2
            .parameters
            .insert("c".to_string(), Some("d".to_string()));
        step2
            .parameters
            .insert("a".to_string(), Some("b".to_string()));

        assert_eq!(step1.get_id(), step2.get_id());
    }

    #[test]
    fn two_steps_with_different_types_do_not_have_same_id() {
        let mut step1 = WorkflowStepDefinition {
            step_type: WorkflowStepType("test".to_string()),
            parameters: HashMap::new(),
        };

        step1
            .parameters
            .insert("a".to_string(), Some("b".to_string()));
        step1
            .parameters
            .insert("c".to_string(), Some("d".to_string()));

        let mut step2 = WorkflowStepDefinition {
            step_type: WorkflowStepType("test2".to_string()),
            parameters: HashMap::new(),
        };

        step2
            .parameters
            .insert("c".to_string(), Some("d".to_string()));
        step2
            .parameters
            .insert("a".to_string(), Some("b".to_string()));

        assert_ne!(step1.get_id(), step2.get_id());
    }

    #[test]
    fn two_steps_with_different_parameters_do_not_have_same_id() {
        let mut step1 = WorkflowStepDefinition {
            step_type: WorkflowStepType("test".to_string()),
            parameters: HashMap::new(),
        };

        step1
            .parameters
            .insert("a".to_string(), Some("b".to_string()));
        step1
            .parameters
            .insert("c".to_string(), Some("d".to_string()));

        let mut step2 = WorkflowStepDefinition {
            step_type: WorkflowStepType("test2".to_string()),
            parameters: HashMap::new(),
        };

        step2
            .parameters
            .insert("c".to_string(), Some("d".to_string()));
        step2
            .parameters
            .insert("a".to_string(), Some("f".to_string()));

        assert_ne!(step1.get_id(), step2.get_id());
    }
}
