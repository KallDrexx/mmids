use std::collections::HashMap;
use async_trait;
use hyper::{Body, header, Request, Response, StatusCode};
use hyper::http::HeaderValue;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use mmids_core::http_api::routing::RouteHandler;
use serde::Deserialize;
use tracing::{instrument, error, info};
use tracing::log::warn;
use crate::endpoints::webrtc_server::{WebrtcServerRequest, WebrtcStreamPublisherNotification};

pub struct RequestWebRtcConnectionHandler {
    webrtc_server: UnboundedSender<WebrtcServerRequest>,
}

#[derive(Deserialize)]
pub enum RequestType { Unknown, Publish, Watch }

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
        RequestWebRtcConnectionHandler {
            webrtc_server,
        }
    }
}

#[async_trait]
impl RouteHandler for RequestWebRtcConnectionHandler {
    #[instrument(name = "Execute WebRTC Connection Request", skip(self, request, _path_parameters))]
    async fn execute(
        &self,
        request: &mut Request<Body>,
        _path_parameters: HashMap<String, String>,
        request_id: String,
    ) -> Result<Response<Body>, hyper::Error> {
        let body = hyper::body::to_bytes(request.body_mut()).await?;
        let body_string = match String::from_utf8(body.to_vec()) {
            Ok(content) => content,
            Err(utf8_error) => {
                error!("Could not read content as text: {:?}", utf8_error);

                return Ok(return_error(
                    "Could not read body content".to_string(),
                    StatusCode::BAD_REQUEST
                ));
            },
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
                info!(
                    application_name = %request.application_name,
                    stream_name = %request.stream_name,
                    "Requesting publishing to webrtc application {} and stream {}",
                    request.application_name, request.stream_name
                );

                let (response_sender, mut response_receiver) = unbounded_channel();
                let _ = self.webrtc_server.send(WebrtcServerRequest::StreamPublishRequested {
                    application_name: request.application_name,
                    stream_name: request.stream_name,
                    offer_sdp: request.offer_sdp,
                    notification_channel: response_sender,
                });

                match response_receiver.recv().await {
                    Some(WebrtcStreamPublisherNotification::PublishRequestAccepted {answer_sdp}) => {
                        info!()
                    }
                }

            },

            RequestType::Watch => {
                let (response_sender, response_receiver) = unbounded_channel();
                let webrtc_request = WebrtcServerRequest::StreamWatchRequested {
                    application_name: request.application_name,
                    stream_name: request.stream_name,
                    offer_sdp: request.offer_sdp,
                    notification_channel: response_sender,
                };
            },

            _ => {
                warn!("Invalid request type specified");
                return_error(
                    "Invalid request type specified. Value is required and must be either \
                    'publish' or 'watch'.".to_string(),
                    StatusCode::BAD_REQUEST,
                )
            }
        };

        Ok(response)
    }
}

fn return_error(message: String, status_code: StatusCode) -> Response<Body> {
    let data = FailureResult {
        message,
    };

    let (result_content, mime_type) = match serde_json::to_string_pretty(&data) {
        Ok(json) => (json, "application/json"),
        Err(error) => {
            error!("Failed to create failure response json, returning raw message as raw string");
            (data.message, "application/text")
        }
    };

    let mut response = Response::new(Body::from(result_content));
    *response.status_mut() = status_code;

    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(mime_type));

    response
}