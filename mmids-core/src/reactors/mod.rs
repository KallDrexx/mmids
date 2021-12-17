//! Reactors are actors that are used to manage workflows for specific stream names. This is a
//! pull mechanism for dynamic workflow capabilities in mmids. When a reactor is asked for a
//! workflow for a stream name, the reactor will reach out to an external service (configured
//! by a reactor executor) to obtain a workflow definition for the requested stream name. If none
//! is returned then that normally means the stream name is not allowed. If a valid workflow
//! definition is returned, the reactor will ensure that the workflow is created so media can be
//! routed to it.

pub mod executors;
pub mod manager;
mod reactor;

use std::collections::HashMap;

pub use reactor::{start_reactor, ReactorRequest};

/// How reactors are defined
#[derive(Clone)]
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
