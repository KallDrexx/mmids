//! Handler that allows a workflow to be stopped

use crate::routing::RouteHandler;
use async_trait::async_trait;
use hyper::{Body, Error, Request, Response, StatusCode};
use mmids_core::workflows::manager::{WorkflowManagerRequest, WorkflowManagerRequestOperation};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;

/// Handles HTTP requests to stop a running workflow.  It requires a single path parameter
/// named `workflow` that contains the name of the workflow to be stopped.  It will always return
/// a 200 OK, even if the workflow isn't running.
pub struct StopWorkflowHandler {
    manager: UnboundedSender<WorkflowManagerRequest>,
}

impl StopWorkflowHandler {
    pub fn new(manager: UnboundedSender<WorkflowManagerRequest>) -> Self {
        StopWorkflowHandler { manager }
    }
}

#[async_trait]
impl RouteHandler for StopWorkflowHandler {
    async fn execute(
        &self,
        _request: &mut Request<Body>,
        path_parameters: HashMap<String, String>,
        request_id: String,
    ) -> Result<Response<Body>, Error> {
        let workflow_name = match path_parameters.get("workflow") {
            Some(value) => Arc::new(value.to_string()),
            None => {
                error!("Get workflow endpoint called without a 'workflow' path parameter");
                let mut response = Response::default();
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;

                return Ok(response);
            }
        };

        match self.manager.send(WorkflowManagerRequest {
            request_id,
            operation: WorkflowManagerRequestOperation::StopWorkflow {
                name: workflow_name,
            },
        }) {
            Ok(_) => (),
            Err(_) => {
                error!("Workflow manager endpoint gone");
                let mut response = Response::default();
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;

                return Ok(response);
            }
        };

        Ok(Response::default())
    }
}
