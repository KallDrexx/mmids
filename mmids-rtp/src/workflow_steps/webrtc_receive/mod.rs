use crate::endpoints::webrtc_server::{
    RequestType, StreamNameRegistration, WebrtcServerPublisherRegistrantNotification,
    WebrtcServerRequest,
};
use anyhow::anyhow;
use futures::FutureExt;
use mmids_core::codecs::VideoCodec;
use mmids_core::net::ConnectionId;
use mmids_core::workflows::definitions::WorkflowStepDefinition;
use mmids_core::workflows::steps::factory::StepGenerator;
use mmids_core::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use mmids_core::workflows::{MediaNotification, MediaNotificationContent};
use mmids_core::StreamId;
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info, warn};

pub const APP_NAME_PARAM: &str = "app_name";
pub const STREAM_NAME_PARAM: &str = "stream_name";
pub const AUDIO_CODEC_PARAM: &str = "audio_codec";
pub const VIDEO_CODEC_PARAM: &str = "video_codec";

pub struct WebRtcReceiveStepGenerator {
    webrtc_server: UnboundedSender<WebrtcServerRequest>,
}

impl WebRtcReceiveStepGenerator {
    pub fn new(webrtc_server: UnboundedSender<WebrtcServerRequest>) -> Self {
        WebRtcReceiveStepGenerator { webrtc_server }
    }
}

impl StepGenerator for WebRtcReceiveStepGenerator {
    fn generate(&self, definition: WorkflowStepDefinition) -> StepCreationResult {
        let app_name = match definition.parameters.get(APP_NAME_PARAM) {
            Some(Some(string)) => string.clone(),
            _ => Err(anyhow!(
                "No webrtc application name was specified, but is required"
            ))?,
        };

        let stream_name = match definition.parameters.get(STREAM_NAME_PARAM) {
            Some(Some(string)) => match string.as_str() {
                "*" => StreamNameRegistration::Any,
                x => StreamNameRegistration::Exact(x.to_string()),
            },
            _ => Err(anyhow!("No webrtc stream name provided, but is required"))?,
        };

        const AUDIO_CODEC_ERROR: &str =
            "Invalid webrtc audio codec specified. Parameter is required and must be 'none'";

        let audio_codec = match definition.parameters.get(AUDIO_CODEC_PARAM) {
            Some(Some(string)) => match string.to_lowercase().as_str() {
                "none" => None,
                _ => Err(anyhow!(AUDIO_CODEC_ERROR))?,
            },

            _ => Err(anyhow!(AUDIO_CODEC_ERROR))?,
        };

        const VIDEO_CODEC_ERROR: &str =
            "Invalid audio codec parameter.  Parameter is required and must be 'h264' or 'none'";

        let video_codec = match definition.parameters.get(VIDEO_CODEC_PARAM) {
            Some(Some(string)) => match string.to_lowercase().as_str() {
                "h264" => Some(VideoCodec::H264),
                "none" => None,
                _ => Err(anyhow!(VIDEO_CODEC_ERROR))?,
            },

            _ => Err(anyhow!(VIDEO_CODEC_ERROR))?,
        };

        if video_codec.is_none() && audio_codec.is_none() {
            Err(anyhow!(
                "No audio or video codec specified.  At least one must be an actual codec"
            ))?;
        }

        let step = WebRtcReceiveStep {
            webrtc_server: self.webrtc_server.clone(),
            application_name: app_name.clone(),
            stream_name_registration: stream_name.clone(),
            definition: definition.clone(),
            status: StepStatus::Created,
            connection_id_to_stream_id_map: HashMap::new(),
        };

        let (notification_sender, notification_receiver) = unbounded_channel();
        let _ = self
            .webrtc_server
            .send(WebrtcServerRequest::ListenForPublishers {
                application_name: app_name,
                stream_name,
                video_codec,
                audio_codec,
                requires_registrant_approval: false,
                notification_channel: notification_sender,
            });

        let futures = vec![notify_on_response_received(notification_receiver).boxed()];

        Ok((Box::new(step), futures))
    }
}

enum WebRtcReceiveFutureResult {
    WebRtcServerGone,
    WebRtcServerNotification {
        notification: WebrtcServerPublisherRegistrantNotification,
        receiver: UnboundedReceiver<WebrtcServerPublisherRegistrantNotification>,
    },

    MediaSenderGone {
        stream_id: StreamId,
    },

    MediaReceived {
        stream_id: StreamId,
        media: MediaNotificationContent,
        receiver: UnboundedReceiver<MediaNotificationContent>,
    },
}

impl StepFutureResult for WebRtcReceiveFutureResult {}

struct WebRtcReceiveStep {
    definition: WorkflowStepDefinition,
    status: StepStatus,
    webrtc_server: UnboundedSender<WebrtcServerRequest>,
    application_name: String,
    stream_name_registration: StreamNameRegistration,
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

                if let Some(old_stream_id) = self
                    .connection_id_to_stream_id_map
                    .insert(connection_id.clone(), stream_id.clone())
                {
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
                    stream_id: stream_id.clone(),
                    content: MediaNotificationContent::NewIncomingStream { stream_name },
                });

                outputs
                    .futures
                    .push(notify_on_media_received(stream_id, media_channel).boxed());
            }

            WebrtcServerPublisherRegistrantNotification::PublisherDisconnected {
                connection_id,
                ..
            } => {
                info!(
                    connection_id = ?connection_id,
                    "Publisher disconnected"
                );

                if let Some(stream_id) = self.connection_id_to_stream_id_map.remove(&connection_id)
                {
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

    fn handle_media(
        &self,
        received_by: StreamId,
        media: MediaNotificationContent,
        outputs: &mut StepOutputs,
    ) {
        outputs.media.push(MediaNotification {
            stream_id: received_by,
            content: media,
        });
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

                WebRtcReceiveFutureResult::MediaSenderGone { stream_id } => {
                    error!(
                        stream_id = ?stream_id,
                        "Media receiver for stream {:?} gone. No more media will be received by \
                        this publisher", stream_id
                    );

                    // Unsure if we should send a disconnection message down the line. This should
                    // only happen if the webrtc server loses track of it without sending a
                    // publisher disconnected signal, which shouldn't happen.
                }

                WebRtcReceiveFutureResult::WebRtcServerNotification {
                    notification,
                    receiver,
                } => {
                    outputs
                        .futures
                        .push(notify_on_response_received(receiver).boxed());

                    self.handle_webrtc_notification(notification, outputs);
                }

                WebRtcReceiveFutureResult::MediaReceived {
                    stream_id,
                    media,
                    receiver,
                } => {
                    outputs
                        .futures
                        .push(notify_on_media_received(stream_id.clone(), receiver).boxed());

                    self.handle_media(stream_id, media, outputs);
                }
            }
        }
    }

    fn shutdown(&mut self) {
        self.status = StepStatus::Shutdown;
        let _ = self
            .webrtc_server
            .send(WebrtcServerRequest::RemoveRegistration {
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
        Some(notification) => WebRtcReceiveFutureResult::WebRtcServerNotification {
            notification,
            receiver,
        },

        None => WebRtcReceiveFutureResult::WebRtcServerGone,
    };

    Box::new(result)
}

async fn notify_on_media_received(
    stream_id: StreamId,
    mut receiver: UnboundedReceiver<MediaNotificationContent>,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(media) => WebRtcReceiveFutureResult::MediaReceived {
            stream_id,
            media,
            receiver,
        },

        None => WebRtcReceiveFutureResult::MediaSenderGone { stream_id },
    };

    Box::new(result)
}
