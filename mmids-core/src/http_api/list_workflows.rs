use crate::http_api::routing::RouteHandler;
use crate::workflows::manager::WorkflowManagerRequest;
use async_trait::async_trait;
use hyper::{Body, Error, Request, Response, StatusCode};
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::channel;
use tokio::time::timeout;
use tracing::error;

/// HTTP handler which provides a list of workflows that are actively running
pub struct ListWorkflowsHandler;

/// Defines what data the API will return for each running workflow
#[derive(Serialize)]
pub struct WorkflowListItemResponse {
    name: String,
}

#[async_trait]
impl RouteHandler for ListWorkflowsHandler {
    async fn execute(
        &self,
        _request: &mut Request<Body>,
        _path_parameters: HashMap<String, String>,
        manager: UnboundedSender<WorkflowManagerRequest>,
    ) -> Result<Response<Body>, Error> {
        let (response_sender, response_receiver) = channel();
        let message = WorkflowManagerRequest::GetRunningWorkflows {
            response_channel: response_sender,
        };

        match manager.send(message) {
            Ok(_) => (),
            Err(_) => {
                error!("Workflow manager is no longer operational");
                let mut response = Response::default();
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;

                return Ok(response);
            }
        };

        let response = match timeout(Duration::from_secs(10), response_receiver).await {
            Ok(Ok(response)) => response,

            Ok(Err(_)) => {
                error!("Workflow manager is no longer operational");
                let mut response = Response::default();
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;

                return Ok(response);
            }

            Err(_) => {
                error!("Get workflow request timed out");
                let mut response = Response::default();
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;

                return Ok(response);
            }
        };

        let response = response
            .into_iter()
            .map(|x| WorkflowListItemResponse { name: x.name })
            .collect::<Vec<_>>();
        let json = match serde_json::to_string_pretty(&response) {
            Ok(json) => json,
            Err(error) => {
                error!("Failed to serialize workflows to json: {:?}", error);
                let mut response = Response::default();
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;

                return Ok(response);
            }
        };

        Ok(Response::new(Body::from(json)))
    }
}
