use std::collections::HashMap;

#[derive(Clone)]
pub struct WorkflowStepType(pub String);

#[derive(Clone)]
pub struct WorkflowStepDefinition {
    pub step_type: WorkflowStepType,
    pub parameters: HashMap<String, String>,
}

pub struct Workflow {
    pub name: String,
    pub steps: Vec<String>,
}
