pub mod executors;

use std::collections::HashMap;

/// How reactors are defined
pub struct ReactorDefinition {
    /// The name of the reactor. Used by endpoints and workflow steps to identify which workflow
    /// they want to interact with.
    pub name: String,

    /// The name of the query executor this reactor should use to perform queries. If one is
    /// not specified then the default executor will be used.
    pub executor: Option<String>,

    /// Key value pairs used to instruct the reactor's executor. Valid values here are specific
    /// to the executor that was picked.
    pub parameters: HashMap<String, Option<String>>,
}
