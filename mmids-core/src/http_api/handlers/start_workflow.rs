//! Contains the handler that creates and updates workflows

use crate::http_api::routing::RouteHandler;
use crate::workflows::definitions::WorkflowDefinition;
use crate::workflows::manager::{WorkflowManagerRequest, WorkflowManagerRequestOperation};
use async_trait::async_trait;
use bytes::Bytes;
use hyper::http::HeaderValue;
use hyper::{Body, Error, Request, Response, StatusCode};
use serde::Serialize;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, warn};

const MMIDS_MIME_TYPE: &str = "application/vnd.mmids.workflow";

/// Handles requests to start a workflow. Every workflow must have a name, and if a workflow is
/// specified with a name that matches an already running workflow then the existing workflow
/// will be updated to match the passed in workflow instead of a new workflow starting.
///
/// A successful result does not mean that the workflow has fully started, only that the workflow
/// has been submitted to the workflow manager. Status updates of the workflow will need to be
/// queried to know if it successfully became active.
///
/// The details of the workflow are expected in the passed in via the request body.  The format
/// that the details come in are based on the `Content-Type` header of the request:
///
/// * `application/vnd.mmids.workflow` - Worklow definition that matches how workflows are defined in
/// the mmids configuration files.
///
/// If no `Content-Type` is specified than `application/vnd.mmids.workflow` is assumed.
pub struct StartWorkflowHandler {
    manager: UnboundedSender<WorkflowManagerRequest>,
}

/// Response provided when an error is returned, such as an invalid workflow specified
#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

impl StartWorkflowHandler {
    pub fn new(manager: UnboundedSender<WorkflowManagerRequest>) -> Self {
        StartWorkflowHandler { manager }
    }
}

#[async_trait]
impl RouteHandler for StartWorkflowHandler {
    async fn execute(
        &self,
        request: &mut Request<Body>,
        _path_parameters: HashMap<String, String>,
        request_id: String,
    ) -> Result<Response<Body>, Error> {
        let body = hyper::body::to_bytes(request.body_mut()).await?;
        let content_type = match request.headers().get(hyper::http::header::CONTENT_TYPE) {
            Some(content_type) => content_type.to_str().unwrap_or(MMIDS_MIME_TYPE),
            None => {
                warn!("No content type specified, assuming '{}'", MMIDS_MIME_TYPE);
                MMIDS_MIME_TYPE
            }
        };

        let workflow = match content_type.to_lowercase().trim() {
            MMIDS_MIME_TYPE => parse_mmids_mime_type(body)?,

            x => {
                warn!("Invalid content type specified: '{}'", x);
                let error = ErrorResponse {
                    error: format!("Invalid content type specified: {}", x),
                };
                return Ok(error.into_json_bad_request());
            }
        };

        let workflow = match workflow {
            Ok(workflow) => workflow,
            Err(error) => {
                return Ok(error.into_json_bad_request());
            }
        };

        let result = self.manager.send(WorkflowManagerRequest {
            request_id,
            operation: WorkflowManagerRequestOperation::UpsertWorkflow {
                definition: workflow,
            },
        });

        match result {
            Ok(_) => Ok(Response::default()),

            Err(_) => {
                error!("Workflow manager no longer exists");
                let mut response = Response::default();
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;

                Ok(response)
            }
        }
    }
}

impl ErrorResponse {
    fn into_json_bad_request(self) -> Response<Body> {
        let json = match serde_json::to_string_pretty(&self) {
            Ok(json) => json,
            Err(error) => {
                error!("Failed to serialize error response to json: {:?}", error);
                let mut response = Response::default();
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;

                return response;
            }
        };

        let mut response = Response::new(Body::from(json));
        *response.status_mut() = StatusCode::BAD_REQUEST;

        let headers = response.headers_mut();
        headers.insert(
            hyper::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );

        response
    }
}

fn parse_mmids_mime_type(body: Bytes) -> Result<Result<WorkflowDefinition, ErrorResponse>, Error> {
    let content = match String::from_utf8(body.to_vec()) {
        Ok(content) => content,
        Err(utf8_error) => {
            return Ok(Err(ErrorResponse {
                error: format!("Body was not valid utf8 content: {}", utf8_error),
            }));
        }
    };

    let mut config = match crate::config::parse(content.as_str()) {
        Ok(config) => config,
        Err(parse_error) => {
            return Ok(Err(ErrorResponse {
                error: format!("Failed to parse input: {:?}", parse_error),
            }));
        }
    };

    if config.workflows.len() > 1 {
        return Ok(Err(ErrorResponse {
            error: format!(
                "Each request can only contain 1 workflow, {} were specified",
                config.workflows.len()
            ),
        }));
    }

    if config.workflows.is_empty() {
        return Ok(Err(ErrorResponse {
            error: "No workflows specified".to_string(),
        }));
    }

    let workflow = config.workflows.drain().next().unwrap().1;
    Ok(Ok(workflow))
}
