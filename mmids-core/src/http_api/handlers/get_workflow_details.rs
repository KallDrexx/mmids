//! Contains the handler for getting details about a running workflow

use crate::http_api::routing::RouteHandler;
use crate::workflows::manager::{WorkflowManagerRequest, WorkflowManagerRequestOperation};
use crate::workflows::steps::StepStatus;
use crate::workflows::{WorkflowState, WorkflowStatus, WorkflowStepState};
use async_trait::async_trait;
use hyper::http::HeaderValue;
use hyper::{Body, Error, Request, Response, StatusCode};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::channel;
use tokio::time::timeout;
use tracing::error;

/// Handles HTTP requests to get details for a specific workflow.  It requires a single path
/// parameter with the name `workflow` containing the name of the workflow to query for.  Response
/// will always be returned in json format.
pub struct GetWorkflowDetailsHandler {
    manager: UnboundedSender<WorkflowManagerRequest>,
}

/// The API's response for the state of the requested workflow
#[derive(Serialize)]
pub struct WorkflowStateResponse {
    status: String,
    active_steps: Vec<WorkflowStepStateResponse>,
    pending_steps: Vec<WorkflowStepStateResponse>,
}

/// API's response for the details of an individual workflow step
#[derive(Serialize)]
pub struct WorkflowStepStateResponse {
    step_id: String,
    step_type: String,
    parameters: HashMap<String, Option<String>>,
    status: String,
}

impl GetWorkflowDetailsHandler {
    pub fn new(manager: UnboundedSender<WorkflowManagerRequest>) -> Self {
        GetWorkflowDetailsHandler { manager }
    }
}

#[async_trait]
impl RouteHandler for GetWorkflowDetailsHandler {
    async fn execute(
        &self,
        _request: &mut Request<Body>,
        path_parameters: HashMap<String, String>,
        request_id: String,
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

        let (sender, receiver) = channel();
        let _ = self.manager.send(WorkflowManagerRequest {
            request_id,
            operation: WorkflowManagerRequestOperation::GetWorkflowDetails {
                name: Arc::new(workflow_name),
                response_channel: sender,
            },
        });

        let details = match timeout(Duration::from_secs(1), receiver).await {
            Ok(Ok(details)) => details,
            Ok(Err(_)) => {
                error!("Receiver was dropped prior to sending a response");
                let mut response = Response::default();
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;

                return Ok(response);
            }

            Err(_) => {
                error!("Request timed out");
                let mut response = Response::default();
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;

                return Ok(response);
            }
        };

        let response = if let Some(details) = details {
            let details = WorkflowStateResponse::from(details);
            let json = match serde_json::to_string_pretty(&details) {
                Ok(json) => json,
                Err(e) => {
                    error!("Could not serialize workflow details response: {:?}", e);
                    let mut response = Response::default();
                    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;

                    return Ok(response);
                }
            };

            let mut response = Response::new(Body::from(json));
            let headers = response.headers_mut();
            headers.insert(
                hyper::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );

            response
        } else {
            let mut response = Response::new(Body::from("Workflow not found"));
            *response.status_mut() = StatusCode::NOT_FOUND;

            response
        };

        Ok(response)
    }
}

impl From<WorkflowState> for WorkflowStateResponse {
    fn from(workflow: WorkflowState) -> Self {
        WorkflowStateResponse {
            status: match workflow.status {
                WorkflowStatus::Running => "Running".to_string(),
                WorkflowStatus::Error {
                    failed_step_id,
                    message,
                } => format!("Step id {} failed: {}", failed_step_id, message),
            },

            active_steps: workflow
                .active_steps
                .into_iter()
                .map(WorkflowStepStateResponse::from)
                .collect(),

            pending_steps: workflow
                .pending_steps
                .into_iter()
                .map(WorkflowStepStateResponse::from)
                .collect(),
        }
    }
}

impl From<WorkflowStepState> for WorkflowStepStateResponse {
    fn from(step_state: WorkflowStepState) -> Self {
        WorkflowStepStateResponse {
            step_id: step_state.definition.get_id().0.to_string(),
            step_type: step_state.definition.step_type.0,
            parameters: step_state.definition.parameters,
            status: match step_state.status {
                StepStatus::Created => "Created".to_string(),
                StepStatus::Active => "Active".to_string(),
                StepStatus::Error { message } => format!("Error: {}", message),
                StepStatus::Shutdown => "Shut Down".to_string(),
            },
        }
    }
}
