use std::collections::HashMap;
use async_trait;
use hyper::{Body, Request, Response};
use tokio::sync::mpsc::UnboundedSender;
use mmids_core::http_api::routing::RouteHandler;
use serde::Deserialize;
use crate::endpoints::webrtc_server::WebrtcServerRequest;

pub struct RequestWebRtcConnectionHandler {
    webrtc_server: UnboundedSender<WebrtcServerRequest>,
}

#[derive(Deserialize)]
pub enum RequestType { Publish, Watch }

#[derive(Deserialize)]
pub struct WebRtcRequest {
    pub request_type: RequestType,
    pub application_name: String,
    pub stream_name: String,
    pub offer_sdp: String,
}

#[derive(Serialize)]
pub struct SuccessfulResult {
    pub sdp: String
}

#[derive(Serialize)]
pub struct FailureResult {
    pub message: String
}

impl RequestWebRtcConnectionHandler {
    pub fn new(webrtc_server: UnboundedSender<WebrtcServerRequest>) -> Self {
        Self {
            webrtc_server,
        }
    }
}

#[async_trait]
impl RouteHandler for RequestWebRtcConnectionHandler {
    async fn execute(
        &self,
        request: &mut Request<Body>,
        path_parameters: HashMap<String, String>,
        request_id: String,
    ) -> Result<Response<Body>, hyper::Error> {
        todo!()
    }
}