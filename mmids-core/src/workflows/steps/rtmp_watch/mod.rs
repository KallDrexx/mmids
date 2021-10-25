#[cfg(test)]
mod tests;

use crate::endpoints::rtmp_server::{
    RtmpEndpointMediaData, RtmpEndpointMediaMessage, RtmpEndpointRequest,
    RtmpEndpointWatcherNotification, StreamKeyRegistration,
};
use crate::utils::hash_map_to_stream_metadata;
use crate::workflows::definitions::WorkflowStepDefinition;
use crate::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use crate::workflows::{MediaNotification, MediaNotificationContent};
use crate::StreamId;
use futures::future::BoxFuture;
use futures::FutureExt;
use log::{error, info, warn};
use rml_rtmp::time::RtmpTimestamp;
use std::collections::HashMap;
use thiserror::Error as ThisError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub const PORT_PROPERTY_NAME: &'static str = "port";
pub const APP_PROPERTY_NAME: &'static str = "rtmp_app";
pub const STREAM_KEY_PROPERTY_NAME: &'static str = "stream_key";

pub struct RtmpWatchStep {
    definition: WorkflowStepDefinition,
    port: u16,
    rtmp_app: String,
    stream_key: StreamKeyRegistration,
    status: StepStatus,
    rtmp_endpoint_sender: UnboundedSender<RtmpEndpointRequest>,
    media_channel: UnboundedSender<RtmpEndpointMediaMessage>,
    stream_id_to_name_map: HashMap<StreamId, String>,
}

impl StepFutureResult for RtmpWatchStepFutureResult {}

enum RtmpWatchStepFutureResult {
    RtmpWatchNotificationReceived(
        RtmpEndpointWatcherNotification,
        UnboundedReceiver<RtmpEndpointWatcherNotification>,
    ),
    RtmpEndpointGone,
}

#[derive(ThisError, Debug)]
enum StepStartupError {
    #[error(
        "No RTMP app specified.  A non-empty parameter of '{}' is required",
        PORT_PROPERTY_NAME
    )]
    NoRtmpAppSpecified,

    #[error(
        "No stream key specified.  A non-empty parameter of '{}' is required",
        APP_PROPERTY_NAME
    )]
    NoStreamKeySpecified,

    #[error(
        "Invalid port value of '{0}' specified.  A number from 0 to 65535 should be specified"
    )]
    InvalidPortSpecified(String),
}

impl RtmpWatchStep {
    pub fn create_factory_fn(
        rtmp_endpoint_sender: UnboundedSender<RtmpEndpointRequest>,
    ) -> Box<dyn Fn(&WorkflowStepDefinition) -> StepCreationResult + Send + Sync> {
        Box::new(move |definition| {
            match RtmpWatchStep::new(definition, rtmp_endpoint_sender.clone()) {
                Ok((step, futures)) => Ok((Box::new(step), futures)),
                Err(e) => Err(e),
            }
        })
    }

    pub fn new<'a>(
        definition: &WorkflowStepDefinition,
        rtmp_endpoint_sender: UnboundedSender<RtmpEndpointRequest>,
    ) -> Result<
        (Self, Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>),
        Box<dyn std::error::Error + Sync + Send>,
    > {
        let port = match definition.parameters.get(PORT_PROPERTY_NAME) {
            Some(value) => match value.parse::<u16>() {
                Ok(num) => num,
                Err(_) => {
                    return Err(Box::new(StepStartupError::InvalidPortSpecified(
                        value.clone(),
                    )));
                }
            },

            None => 1935,
        };

        let app = match definition.parameters.get(APP_PROPERTY_NAME) {
            Some(x) => x.trim(),
            None => return Err(Box::new(StepStartupError::NoRtmpAppSpecified)),
        };

        let stream_key = match definition.parameters.get(STREAM_KEY_PROPERTY_NAME) {
            Some(x) => x.trim(),
            None => return Err(Box::new(StepStartupError::NoStreamKeySpecified)),
        };

        let stream_key = if stream_key == "*" {
            StreamKeyRegistration::Any
        } else {
            StreamKeyRegistration::Exact(stream_key.to_string())
        };

        let (media_sender, media_receiver) = unbounded_channel();

        let step = RtmpWatchStep {
            definition: definition.clone(),
            status: StepStatus::Created,
            port,
            rtmp_app: app.to_string(),
            rtmp_endpoint_sender,
            media_channel: media_sender,
            stream_key,
            stream_id_to_name_map: HashMap::new(),
        };

        let (notification_sender, notification_receiver) = unbounded_channel();
        let _ = step
            .rtmp_endpoint_sender
            .send(RtmpEndpointRequest::ListenForWatchers {
                port: step.port,
                rtmp_app: step.rtmp_app.clone(),
                rtmp_stream_key: step.stream_key.clone(),
                media_channel: media_receiver,
                notification_channel: notification_sender,
            });

        Ok((
            step,
            vec![wait_for_endpoint_notification(notification_receiver).boxed()],
        ))
    }

    fn handle_endpoint_notification(&mut self, notification: RtmpEndpointWatcherNotification) {
        match notification {
            RtmpEndpointWatcherNotification::WatcherRegistrationFailed => {
                error!("Registration for RTMP watchers was denied");
                self.status = StepStatus::Error;
            }

            RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful => {
                info!("Registration for RTMP watchers was accepted");
                self.status = StepStatus::Active;
            }

            RtmpEndpointWatcherNotification::StreamKeyBecameActive { stream_key } => {
                info!(
                    "At least one watcher became active for stream key '{}'",
                    stream_key
                );
            }

            RtmpEndpointWatcherNotification::StreamKeyBecameInactive { stream_key } => {
                info!("All watchers left stream key '{}'", stream_key);
            }
        }
    }

    fn handle_media(&mut self, media: MediaNotification, outputs: &mut StepOutputs) {
        if self.status == StepStatus::Active {
            match &media.content {
                MediaNotificationContent::NewIncomingStream { stream_name } => {
                    info!("New incoming stream notification found for stream id {:?} and stream name '{}",
                media.stream_id, stream_name);

                    match self.stream_id_to_name_map.get(&media.stream_id) {
                        None => (),
                        Some(current_stream_name) => {
                            if current_stream_name == stream_name {
                                warn!("New incoming stream notification for stream id {:?} is already mapped \
                            to this same stream name.", media.stream_id);
                            } else {
                                warn!("New incoming stream notification for stream id {:?} is already mapped \
                            to the stream name '{}'", media.stream_id, current_stream_name);
                            }
                        }
                    }

                    self.stream_id_to_name_map
                        .insert(media.stream_id.clone(), stream_name.clone());
                }

                MediaNotificationContent::StreamDisconnected => {
                    info!(
                        "Stream disconnected notification received for stream id {:?}",
                        media.stream_id
                    );
                    match self.stream_id_to_name_map.remove(&media.stream_id) {
                        Some(_) => (),
                        None => {
                            warn!(
                                "Disconnected stream {:?} was not mapped to a stream name",
                                media.stream_id
                            );
                        }
                    }
                }

                MediaNotificationContent::Metadata { data } => {
                    let stream_key = match self.stream_id_to_name_map.get(&media.stream_id) {
                        Some(key) => key,
                        None => return,
                    };

                    let metadata = hash_map_to_stream_metadata(data);
                    let rtmp_media = RtmpEndpointMediaMessage {
                        stream_key: stream_key.clone(),
                        data: RtmpEndpointMediaData::NewStreamMetaData { metadata },
                    };

                    let _ = self.media_channel.send(rtmp_media);
                }

                MediaNotificationContent::Video {
                    is_keyframe,
                    is_sequence_header,
                    codec,
                    timestamp,
                    data,
                } => {
                    let stream_key = match self.stream_id_to_name_map.get(&media.stream_id) {
                        Some(key) => key,
                        None => return,
                    };

                    let rtmp_media = RtmpEndpointMediaMessage {
                        stream_key: stream_key.clone(),
                        data: RtmpEndpointMediaData::NewVideoData {
                            is_keyframe: *is_keyframe,
                            is_sequence_header: *is_sequence_header,
                            codec: codec.clone(),
                            data: data.clone(),
                            timestamp: RtmpTimestamp::new(timestamp.as_millis() as u32),
                        },
                    };

                    let _ = self.media_channel.send(rtmp_media);
                }

                MediaNotificationContent::Audio {
                    is_sequence_header,
                    codec,
                    timestamp,
                    data,
                } => {
                    let stream_key = match self.stream_id_to_name_map.get(&media.stream_id) {
                        Some(key) => key,
                        None => return,
                    };

                    let rtmp_media = RtmpEndpointMediaMessage {
                        stream_key: stream_key.clone(),
                        data: RtmpEndpointMediaData::NewAudioData {
                            is_sequence_header: *is_sequence_header,
                            codec: codec.clone(),
                            data: data.clone(),
                            timestamp: RtmpTimestamp::new(timestamp.as_millis() as u32),
                        },
                    };

                    let _ = self.media_channel.send(rtmp_media);
                }
            }
        }

        outputs.media.push(media);
    }
}

impl WorkflowStep for RtmpWatchStep {
    fn get_status(&self) -> &StepStatus {
        &self.status
    }

    fn get_definition(&self) -> &WorkflowStepDefinition {
        &self.definition
    }

    fn execute(&mut self, inputs: &mut StepInputs, outputs: &mut StepOutputs) {
        if self.status == StepStatus::Error {
            return;
        }

        for notification in inputs.notifications.drain(..) {
            let future_result = match notification.downcast::<RtmpWatchStepFutureResult>() {
                Ok(x) => *x,
                Err(_) => {
                    error!("Rtmp receive step received a notification that is not an 'RtmpReceiveFutureResult' type");
                    self.status = StepStatus::Error;

                    return;
                }
            };

            match future_result {
                RtmpWatchStepFutureResult::RtmpEndpointGone => {
                    error!("Rtmp endpoint gone, shutting step down");
                    self.status = StepStatus::Error;

                    return;
                }

                RtmpWatchStepFutureResult::RtmpWatchNotificationReceived(
                    notification,
                    receiver,
                ) => {
                    outputs
                        .futures
                        .push(wait_for_endpoint_notification(receiver).boxed());
                    self.handle_endpoint_notification(notification);
                }
            }
        }

        for media in inputs.media.drain(..) {
            self.handle_media(media, outputs);
        }
    }
}

async fn wait_for_endpoint_notification(
    mut receiver: UnboundedReceiver<RtmpEndpointWatcherNotification>,
) -> Box<dyn StepFutureResult> {
    let future_result = match receiver.recv().await {
        Some(message) => {
            RtmpWatchStepFutureResult::RtmpWatchNotificationReceived(message, receiver)
        }
        None => RtmpWatchStepFutureResult::RtmpEndpointGone,
    };

    Box::new(future_result)
}
