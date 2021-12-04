//! Handler that allows a workflow to be stopped

use crate::http_api::routing::RouteHandler;
use crate::workflows::manager::WorkflowManagerRequest;
use async_trait::async_trait;
use hyper::{Body, Error, Request, Response, StatusCode};
use std::collections::HashMap;
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
    ) -> Result<Response<Body>, Error> {
        let workflow_name = match path_parameters.get("workflow") {
            Some(value) => value.to_string(),
            None => {
                error!("Get workflow endpoint called without a 'workflow' path parameter");
                let mut response = Response::default();
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;

                return Ok(response);
            }
        };

        match self.manager.send(WorkflowManagerRequest::StopWorkflow {
            name: workflow_name,
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
