use crate::http_api::routing::RouteHandler;
use crate::workflows::manager::WorkflowManagerRequest;
use async_trait::async_trait;
use hyper::{Body, Error, Request, Response, StatusCode};
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::time::timeout;
use tracing::error;

pub struct ListWorkflowsHandler;

#[derive(Serialize)]
struct Workflow {
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
        let (response_sender, mut response_receiver) = unbounded_channel();
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

        let response = match timeout(Duration::from_secs(10), response_receiver.recv()).await {
            Ok(Some(response)) => response,

            Ok(None) => {
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
            .map(|x| Workflow { name: x.name })
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
