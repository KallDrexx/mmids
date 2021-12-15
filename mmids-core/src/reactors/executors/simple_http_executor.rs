use crate::reactors::executors::{ReactorExecutor, ReactorExecutorGenerator};
use crate::workflows::definitions::WorkflowDefinition;
use futures::future::BoxFuture;
use futures::FutureExt;
use hyper::{Body, Client, Method, Request, StatusCode};
use std::collections::HashMap;
use std::error::Error;
use thiserror::Error;
use tracing::{error, info, instrument};

/// Attempts to query for a workflow definition by performing a simple HTTP POST request to the
/// configured URL. The request will contain a body with just a string of the stream name to look
/// up the workflow for. It's expecting a response of either 404 (denoting that no workflow exists
/// for the stream name) or a 200. When a 200 is returned we are expecting a workflow definition
/// in the standard mmids configuration format.
pub struct SimpleHttpExecutor {
    url: String,
}

impl ReactorExecutor for SimpleHttpExecutor {
    fn get_workflow(&self, stream_name: String) -> BoxFuture<'static, Option<WorkflowDefinition>> {
        execute_simple_http_executor(self.url.clone(), stream_name).boxed()
    }
}

pub struct SimpleHttpExecutorGenerator {}

#[derive(Error, Debug)]
pub enum SimpleHttpExecutorError {
    #[error("The required parameter 'url' was not provided")]
    UrlParameterNotProvided,
}

impl ReactorExecutorGenerator for SimpleHttpExecutorGenerator {
    fn generate(
        &self,
        parameters: &HashMap<String, Option<String>>,
    ) -> Result<Box<dyn ReactorExecutor>, Box<dyn Error + Sync + Send>> {
        let url = match parameters.get("url") {
            Some(Some(url)) => url.trim().to_string(),
            _ => return Err(Box::new(SimpleHttpExecutorError::UrlParameterNotProvided)),
        };

        Ok(Box::new(SimpleHttpExecutor { url }))
    }
}

#[instrument]
async fn execute_simple_http_executor(
    url: String,
    stream_name: String,
) -> Option<WorkflowDefinition> {
    info!("Querying {} for workflow for stream '{}'", url, stream_name);
    let request = Request::builder()
        .method(Method::POST)
        .uri(url.to_string())
        .body(Body::from(stream_name));

    let request = match request {
        Ok(request) => request,
        Err(error) => {
            error!("Failed to build request: {}", error);
            return None;
        }
    };

    let client = Client::new();
    let response = match client.request(request).await {
        Ok(response) => response,
        Err(error) => {
            error!("Error performing request: {}", error);
            return None;
        }
    };

    match response.status() {
        StatusCode::OK => (),
        StatusCode::NOT_FOUND => {
            info!("Not found returned for request");
            return None;
        }

        status => {
            error!("Unexpected status code returned: {}", status);
            return None;
        }
    };

    let bytes = match hyper::body::to_bytes(response.into_body()).await {
        Ok(bytes) => bytes,
        Err(error) => {
            error!("Failed to convert response to bytes: {}", error);
            return None;
        }
    };

    let content = match String::from_utf8(bytes.to_vec()) {
        Ok(content) => content,
        Err(error) => {
            error!("Failed to convert response to a UTF8 string: {}", error);
            return None;
        }
    };

    let mut config = match crate::config::parse(content.as_str()) {
        Ok(config) => config,
        Err(parse_error) => {
            error!(
                "The response was not a valid mmids config format: {:?}",
                parse_error
            );
            return None;
        }
    };

    if config.workflows.len() > 1 {
        error!(
            "The response contained {} workflows, but only 1 is allowed",
            config.workflows.len()
        );
        return None;
    }

    if config.workflows.len() == 0 {
        error!("The response did not contain any workflows");
        return None;
    }

    let workflow = config.workflows.drain().nth(0).unwrap().1;
    Some(workflow)
}
