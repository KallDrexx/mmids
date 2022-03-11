use crate::endpoints::webrtc_server::{
    WebrtcServerRequest, WebrtcStreamPublisherNotification, WebrtcStreamWatcherNotification,
};
use async_trait::async_trait;
use hyper::http::HeaderValue;
use hyper::{header, Body, Request, Response, StatusCode};
use mmids_core::http_api::routing::RouteHandler;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tracing::log::warn;
use tracing::{error, info, instrument};

pub struct RequestWebRtcConnectionHandler {
    webrtc_server: UnboundedSender<WebrtcServerRequest>,
}

#[derive(Deserialize)]
pub enum RequestType {
    Unknown,
    Publish,
    Watch,
}

#[derive(Deserialize)]
pub struct WebRtcRequest {
    pub request_type: RequestType,
    pub application_name: String,
    pub stream_name: String,
    pub offer_sdp: String,
}

#[derive(Serialize)]
pub struct SuccessfulResult {
    pub sdp: String,
}

#[derive(Serialize)]
pub struct FailureResult {
    pub message: String,
}

impl RequestWebRtcConnectionHandler {
    pub fn new(webrtc_server: UnboundedSender<WebrtcServerRequest>) -> Self {
        RequestWebRtcConnectionHandler { webrtc_server }
    }

    #[instrument(skip(self, offer_sdp))]
    async fn request_publish(
        &self,
        application_name: String,
        stream_name: String,
        offer_sdp: String,
    ) -> Response<Body> {
        info!("Requesting WebRTC publish stream");

        let (response_sender, mut response_receiver) = unbounded_channel();
        let _ = self
            .webrtc_server
            .send(WebrtcServerRequest::StreamPublishRequested {
                application_name,
                stream_name,
                offer_sdp,
                notification_channel: response_sender,
            });

        match response_receiver.recv().await {
            Some(WebrtcStreamPublisherNotification::PublishRequestAccepted { answer_sdp }) => {
                info!("WebRTC publish request accepted");
                return_success(answer_sdp)
            }

            Some(WebrtcStreamPublisherNotification::PublishRequestRejected) => {
                info!("WebRTC publish request rejected");

                return_error(
                    "Publish request rejected".to_string(),
                    StatusCode::BAD_REQUEST,
                )
            }

            None => {
                error!("WebRTC server dropped channel before sending response");

                return_error(
                    "Internal Server Failure".to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        }
    }

    #[instrument(skip(self, offer_sdp))]
    async fn request_watch(
        &self,
        application_name: String,
        stream_name: String,
        offer_sdp: String,
    ) -> Response<Body> {
        info!("WebRTC stream watch requested");

        let (response_sender, mut response_receiver) = unbounded_channel();
        let _ = self
            .webrtc_server
            .send(WebrtcServerRequest::StreamWatchRequested {
                application_name,
                stream_name,
                offer_sdp,
                notification_channel: response_sender,
            });

        match response_receiver.recv().await {
            Some(WebrtcStreamWatcherNotification::WatchRequestAccepted { answer_sdp }) => {
                info!("Request accepted");
                return_success(answer_sdp)
            }

            Some(WebrtcStreamWatcherNotification::WatchRequestRejected) => {
                info!("Request rejected");
                return_error("Request rejected".to_string(), StatusCode::BAD_REQUEST)
            }

            None => {
                error!("WebRTC endpoint dropped the response sender");
                return_error(
                    "Internal server error".to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        }
    }
}

#[async_trait]
impl RouteHandler for RequestWebRtcConnectionHandler {
    #[instrument(
        name = "Execute WebRTC Connection Request",
        skip(self, request, _path_parameters)
    )]
    async fn execute(
        &self,
        request: &mut Request<Body>,
        _path_parameters: HashMap<String, String>,
        _request_id: String,
    ) -> Result<Response<Body>, hyper::Error> {
        let body = hyper::body::to_bytes(request.body_mut()).await?;
        let body_string = match String::from_utf8(body.to_vec()) {
            Ok(content) => content,
            Err(utf8_error) => {
                error!("Could not read content as text: {:?}", utf8_error);

                return Ok(return_error(
                    "Could not read body content".to_string(),
                    StatusCode::BAD_REQUEST,
                ));
            }
        };

        let request = match serde_json::from_str::<WebRtcRequest>(body_string.as_str()) {
            Ok(request) => request,
            Err(error) => {
                error!("Could not read content as json: {:?}", error);

                return Ok(return_error(
                    "Deserialization failure".to_string(),
                    StatusCode::BAD_REQUEST,
                ));
            }
        };

        let response = match request.request_type {
            RequestType::Publish => {
                self.request_publish(
                    request.application_name,
                    request.stream_name,
                    request.offer_sdp,
                )
                .await
            }

            RequestType::Watch => {
                self.request_watch(
                    request.application_name,
                    request.stream_name,
                    request.offer_sdp,
                )
                .await
            }

            _ => {
                warn!("Invalid request type specified");
                return_error(
                    "Invalid request type specified. Value is required and must be either \
                    'publish' or 'watch'."
                        .to_string(),
                    StatusCode::BAD_REQUEST,
                )
            }
        };

        Ok(response)
    }
}

fn return_error(message: String, status_code: StatusCode) -> Response<Body> {
    let data = FailureResult { message };

    let (result_content, mime_type) = match serde_json::to_string_pretty(&data) {
        Ok(json) => (json, "application/json"),
        Err(error) => {
            error!(
                "Failed to create failure response json, returning raw message as raw string: {:?}",
                error,
            );
            (data.message, "application/text")
        }
    };

    let mut response = Response::new(Body::from(result_content));
    *response.status_mut() = status_code;

    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(mime_type));

    response
}

fn return_success(answer_sdp: String) -> Response<Body> {
    let result = SuccessfulResult { sdp: answer_sdp };
    let json = match serde_json::to_string_pretty(&result) {
        Ok(json) => json,
        Err(error) => {
            error!("Failed to serialize success result to json: {:?}", error);
            return return_error(
                "Serialization of success result failed to serialize".to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };

    Response::new(Body::from(json))
}
