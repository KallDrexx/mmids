use crate::config::MmidsConfig;
use crate::reactors::executors::{
    ReactorExecutionResult, ReactorExecutor, ReactorExecutorGenerator,
};
use async_recursion::async_recursion;
use futures::future::BoxFuture;
use futures::FutureExt;
use hyper::{Body, Client, Method, Request, StatusCode};
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use thiserror::Error;
use tracing::{error, info, instrument};

const MAX_RETRIES: u64 = 3;
const RETRY_DELAY: u64 = 5;

/// Attempts to query for a workflow definition by performing a simple HTTP POST request to the
/// configured URL. The request will contain a body with just a string of the stream name to look
/// up the workflow for. It's expecting a response of either 404 (denoting that no workflow exists
/// for the stream name) or a 200. When a 200 is returned we are expecting a workflow definition
/// in the standard mmids configuration format.
pub struct SimpleHttpExecutor {
    url: String,
}

impl ReactorExecutor for SimpleHttpExecutor {
    fn get_workflow(&self, stream_name: String) -> BoxFuture<'static, ReactorExecutionResult> {
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
async fn execute_simple_http_executor(url: String, stream_name: String) -> ReactorExecutionResult {
    info!("Querying {} for workflow for stream '{}'", url, stream_name);
    let mut config = match execute_with_retry(&url, &stream_name, 0).await {
        Ok(config) => config,
        Err(_) => return ReactorExecutionResult::invalid(),
    };

    let workflows = config.workflows.drain().map(|kvp| kvp.1).collect();
    ReactorExecutionResult::valid(workflows)
}

fn build_request(url: &String, stream_name: &String) -> Result<Request<Body>, ()> {
    let request = Request::builder()
        .method(Method::POST)
        .uri(url.to_string())
        .body(Body::from(stream_name.clone()));

    match request {
        Ok(request) => Ok(request),
        Err(error) => {
            error!("Failed to build request: {}", error);
            return Err(());
        }
    }
}

#[async_recursion]
async fn execute_with_retry(
    url: &String,
    stream_name: &String,
    times_retried: u64,
) -> Result<MmidsConfig, ()> {
    if times_retried >= MAX_RETRIES {
        info!("Too many retries, giving up");
        return Err(());
    }

    let delay = times_retried * RETRY_DELAY;
    tokio::time::sleep(Duration::from_secs(delay)).await;
    if times_retried > 0 {
        info!("Attempting retry #{}", times_retried);
    }

    let request = match build_request(&url, &stream_name) {
        Ok(request) => request,
        Err(_) => return Err(()), // retry wont' help building the request
    };

    if let Ok(config) = execute_http_call(request).await {
        if let Some(config) = config {
            Ok(config)
        } else {
            Err(()) // Since we got a valid not found result, don't bother retrying
        }
    } else {
        execute_with_retry(url, stream_name, times_retried + 1).await
    }
}

async fn execute_http_call(request: Request<Body>) -> Result<Option<MmidsConfig>, ()> {
    let client = Client::new();
    let response = match client.request(request).await {
        Ok(response) => response,
        Err(error) => {
            error!("Error performing request: {}", error);
            return Err(());
        }
    };

    match response.status() {
        StatusCode::OK => (),
        StatusCode::NOT_FOUND => {
            info!("Not found returned for request");
            return Ok(None);
        }

        status => {
            error!("Unexpected status code returned: {}", status);
            return Err(());
        }
    };

    let bytes = match hyper::body::to_bytes(response.into_body()).await {
        Ok(bytes) => bytes,
        Err(error) => {
            error!("Failed to convert response to bytes: {}", error);
            return Err(());
        }
    };

    let content = match String::from_utf8(bytes.to_vec()) {
        Ok(content) => content,
        Err(error) => {
            error!("Failed to convert response to a UTF8 string: {}", error);
            return Err(());
        }
    };

    let config = match crate::config::parse(content.as_str()) {
        Ok(config) => config,
        Err(parse_error) => {
            error!(
                "The response was not a valid mmids config format: {:?}",
                parse_error
            );
            return Err(());
        }
    };

    return Ok(Some(config));
}
