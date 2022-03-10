use std::collections::HashMap;
use anyhow::anyhow;
use futures::FutureExt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info, warn};
use mmids_core::codecs::{AudioCodec, VideoCodec};
use mmids_core::net::ConnectionId;
use mmids_core::StreamId;
use mmids_core::workflows::definitions::WorkflowStepDefinition;
use mmids_core::workflows::steps::factory::StepGenerator;
use mmids_core::workflows::steps::{StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep};
use mmids_core::workflows::{MediaNotification, MediaNotificationContent};
use crate::endpoints::webrtc_server::{RequestType, StreamNameRegistration, WebrtcServerPublisherRegistrantNotification, WebrtcServerRequest};

pub const APP_NAME_PARAM: &str = "app_name";
pub const STREAM_NAME_PARAM: &str = "stream_name";
pub const AUDIO_CODEC_PARAM: &str = "audio_codec";
pub const VIDEO_CODEC_PARAM: &str = "video_codec";

pub struct WebRtcReceiveStepGenerator {
    webrtc_server: UnboundedSender<WebrtcServerRequest>,
}

impl WebRtcReceiveStepGenerator {
    pub fn new(
        webrtc_server: UnboundedSender<WebrtcServerRequest>,
    ) -> Self {
        WebRtcReceiveStepGenerator {
            webrtc_server,
        }
    }
}

impl StepGenerator for WebRtcReceiveStepGenerator {
    fn generate(&self, definition: WorkflowStepDefinition) -> StepCreationResult {
        let app_name = definition.parameters
            .get(APP_NAME_PARAM)
            .unwrap_or(&None)
            .ok_or(anyhow!("No webrtc application name was specified, but is required"))?;

        let stream_name = definition.parameters
            .get(STREAM_NAME_PARAM)
            .unwrap_or(&None)
            .map(|x| if x == "*" {
                StreamNameRegistration::Any
            } else {
                StreamNameRegistration::Exact(x)}
            )
            .ok_or(anyhow!("No webrtc stream name provided, but is required"))?;

        let audio_codec = match definition.parameters
            .get(AUDIO_CODEC_PARAM)
            .unwrap_or(&None)
            .map(|value| value.to_lowercase().as_str()) {

            Some("none") => None,
            // todo: Add opus here when implemented
            _ => Err(anyhow!(
                "Invalid audio codec parameter. Parameter is required and must be 'none'"
            ))?
        };

        let video_codec = match definition.parameters
            .get(VIDEO_CODEC_PARAM)
            .unwrap_or(&None)
            .map(|value| value.to_lowercase().as_str()) {

            Some("none") => None,
            Some("h264") => Some(VideoCodec::H264),
            _ => Err(anyhow!(
                "Invalid audio codec parameter.  Parameter is required and must be 'h264' or 'none'"
            ))?
        };

        if video_codec.is_none() && audio_codec.is_none() {
            Err(anyhow!(
                "No audio or video codec specified.  At least one must be an actual codec"
            ))?;
        }

        let step = WebRtcReceiveStep {
            webrtc_server: self.webrtc_server.clone(),
            audio_codec,
            video_codec,
            application_name: app_name.clone(),
            stream_name_registration: stream_name.clone(),
            definition: definition.clone(),
            status: StepStatus::Created,
            connection_id_to_stream_id_map: HashMap::new(),
        };

        let (notification_sender, notification_receiver) = unbounded_channel();
        let _ = self.webrtc_server.send(WebrtcServerRequest::ListenForPublishers {
            application_name: app_name,
            stream_name,
            video_codec,
            audio_codec,
            requires_registrant_approval: false,
            notification_channel: notification_sender
        });

        let futures = vec![
            notify_on_response_received(notification_receiver).boxed(),
        ];

        Ok((Box::new(step), futures))
    }
}

enum WebRtcReceiveFutureResult {
    WebRtcServerGone,
    WebRtcServerNotification {
        notification: WebrtcServerPublisherRegistrantNotification,
        receiver: UnboundedReceiver<WebrtcServerPublisherRegistrantNotification>,
    },
}

impl StepFutureResult for WebRtcReceiveFutureResult { }

struct WebRtcReceiveStep {
    definition: WorkflowStepDefinition,
    status: StepStatus,
    webrtc_server: UnboundedSender<WebrtcServerRequest>,
    application_name: String,
    stream_name_registration: StreamNameRegistration,
    audio_codec: Option<AudioCodec>,
    video_codec: Option<VideoCodec>,
    connection_id_to_stream_id_map: HashMap<ConnectionId, StreamId>,
}

impl WebRtcReceiveStep {
    fn handle_webrtc_notification(
        &mut self,
        notification: WebrtcServerPublisherRegistrantNotification,
        outputs: &mut StepOutputs,
    ) {
        match notification {
            WebrtcServerPublisherRegistrantNotification::RegistrationFailed {} => {
                warn!("WebRTC registration failed");
                self.status = StepStatus::Error {
                    message: "Failed to register to listen for WebRTC publishers".to_string(),
                };
            }

            WebrtcServerPublisherRegistrantNotification::RegistrationSuccessful => {
                info!("WebRTC registration succeeded");
                self.status = StepStatus::Active;
            }

            WebrtcServerPublisherRegistrantNotification::NewPublisherConnected {
                connection_id,
                stream_name,
                stream_id,
                media_channel,
                reactor_update_channel,
            } => {
                info!(
                    connection_id = ?connection_id,
                    stream_id = ?stream_id,
                    "New publisher connected",
                );

                if let Some(old_stream_id) = self.connection_id_to_stream_id_map
                    .insert(connection_id.clone(), stream_id.clone()) {
                    if old_stream_id == stream_id {
                        warn!(
                            connection_id = ?connection_id,
                            stream_id = ?stream_id,
                            "Duplicate publisher connection notification received",
                        );
                    } else {
                        error!(
                            connection_id = ?connection_id,
                            old_stream_id = ?old_stream_id,
                            new_stream_id = ?stream_id,
                            "WebRTC server reports new publisher with the same connection id but \
                            an new stream id.  This shouldn't happen"
                        );
                    }
                }

                outputs.media.push(MediaNotification {
                    stream_id,
                    content: MediaNotificationContent::NewIncomingStream {
                        stream_name,
                    }
                });
            }

            WebrtcServerPublisherRegistrantNotification::PublisherDisconnected {connection_id, ..} => {
                info!(
                    connection_id = ?connection_id,
                    "Publisher disconnected"
                );

                if let Some(stream_id) = self.connection_id_to_stream_id_map.remove(&connection_id) {
                    outputs.media.push(MediaNotification {
                        stream_id,
                        content: MediaNotificationContent::StreamDisconnected,
                    });
                } else {
                    warn!(
                        connection_id = ?connection_id,
                        "Disconnected connection id was not originally tracked!"
                    );
                }
            }

            WebrtcServerPublisherRegistrantNotification::PublisherRequiringApproval {
                connection_id,
                response_channel,
                stream_name,
            } => {
                unimplemented!()
            }
        }
    }
}

impl WorkflowStep for WebRtcReceiveStep {
    fn get_status(&self) -> &StepStatus {
        &self.status
    }

    fn get_definition(&self) -> &WorkflowStepDefinition {
        &self.definition
    }

    fn execute(&mut self, inputs: &mut StepInputs, outputs: &mut StepOutputs) {
        for future_result in inputs.notifications.drain(..) {
            let future_result = match future_result.downcast::<WebRtcReceiveFutureResult>() {
                Ok(result) => *result,
                Err(_) => {
                    error!(
                        "WebRTC receive step got a future result that is not a \
                        'WebRtcReceiveFutureResult"
                    );

                    self.status = StepStatus::Error {
                        message: "Invalid future result type received".to_string(),
                    };

                    return;
                }
            };

            match future_result {
                WebRtcReceiveFutureResult::WebRtcServerGone => {
                    error!("WebRTC server endpoint disappeared");
                    self.status = StepStatus::Error {
                        message: "WebRTC server disappeared".to_string(),
                    };

                    return;
                }

                WebRtcReceiveFutureResult::WebRtcServerNotification {notification, receiver} => {
                    outputs.futures.push(
                        notify_on_response_received(receiver).boxed(),
                    );

                    self.handle_webrtc_notification(notification, outputs);
                }
            }
        }
    }

    fn shutdown(&mut self) {
        self.status = StepStatus::Shutdown;
        let _ = self.webrtc_server.send(WebrtcServerRequest::RemoveRegistration {
            application_name: self.application_name.clone(),
            stream_name: self.stream_name_registration.clone(),
            registration_type: RequestType::Publisher,
        });
    }
}

async fn notify_on_response_received(
    mut receiver: UnboundedReceiver<WebrtcServerPublisherRegistrantNotification>,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(notification) =>
            WebRtcReceiveFutureResult::WebRtcServerNotification {notification, receiver},

        None =>
            WebRtcReceiveFutureResult::WebRtcServerGone,
    };

    Box::new(result)
}