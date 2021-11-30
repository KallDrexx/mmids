use async_trait::async_trait;
use hyper::{Body, Error, Request, Response};
use mmids_core::http_api::routing::RouteHandler;
use mmids_core::workflows::manager::WorkflowManagerRequest;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

pub struct VersionHandler;

#[async_trait]
impl RouteHandler for VersionHandler {
    async fn execute(
        &self,
        _request: &mut Request<Body>,
        _path_parameters: HashMap<String, String>,
        _manager: UnboundedSender<WorkflowManagerRequest>,
    ) -> Result<Response<Body>, Error> {
        let output = format!("Mmids version {}", env!("CARGO_PKG_VERSION"));
        return Ok(Response::new(Body::from(output)));
    }
}
