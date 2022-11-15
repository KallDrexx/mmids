//! The RTMP watch step registers with the RTMP server endpoint to allow for RTMP clients to connect
//! and watch media streams based on the specified port, application name, and stream key
//! combinations.  When the workflow step is passed in media notifications it passes them to
//! the RTMP endpoint for distribution for waiting clients.
//!
//! When a stream key of `*` is specified, this allows for RTMP clients to connect on any stream key
//! for the rtmp application to watch video.  Media packets will be routed to clients that connected
//! on stream key that matches the name of the stream in the pipeline.
//!
//! If an exact stream key is configured, then the first media stream that comes into the step will
//! be surfaced on that stream key.
//!
//! All media notifications that are passed into this step are passed onto the next step.

#[cfg(test)]
mod tests;

use crate::rtmp_server::{
    IpRestriction, RegistrationType, RtmpEndpointMediaData, RtmpEndpointMediaMessage,
    RtmpEndpointRequest, RtmpEndpointWatcherNotification, StreamKeyRegistration,
    ValidationResponse,
};
use mmids_core::net::{IpAddress, IpAddressParseError};
use mmids_core::reactors::manager::ReactorManagerRequest;
use mmids_core::reactors::ReactorWorkflowUpdate;
use crate::utils::hash_map_to_stream_metadata;
use mmids_core::workflows::definitions::WorkflowStepDefinition;
use mmids_core::workflows::steps::factory::StepGenerator;
use mmids_core::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use mmids_core::workflows::{MediaNotification, MediaNotificationContent};
use mmids_core::StreamId;
use futures::FutureExt;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::HashMap;
use thiserror::Error as ThisError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tracing::{error, info, warn};

pub const PORT_PROPERTY_NAME: &str = "port";
pub const APP_PROPERTY_NAME: &str = "rtmp_app";
pub const STREAM_KEY_PROPERTY_NAME: &str = "stream_key";
pub const IP_ALLOW_PROPERTY_NAME: &str = "allow_ips";
pub const IP_DENY_PROPERTY_NAME: &str = "deny_ips";
pub const RTMPS_FLAG: &str = "rtmps";
pub const REACTOR_NAME: &str = "reactor";

/// Generates new rtmp watch workflow step instances based on a given step definition.
pub struct RtmpWatchStepGenerator {
    rtmp_endpoint_sender: UnboundedSender<RtmpEndpointRequest>,
    reactor_manager: UnboundedSender<ReactorManagerRequest>,
}

struct StreamWatchers {
    // Use an unbounded channel for this instead of a one shot, as we risk losing the cancellation
    // channel when a reactor update comes through. We can work around this by recreating the
    // cancellation token each time, but it's easier to just use an `UnboundedSender` instead.
    _reactor_cancel_channel: Option<UnboundedSender<()>>,
}

struct RtmpWatchStep {
    definition: WorkflowStepDefinition,
    port: u16,
    rtmp_app: String,
    stream_key: StreamKeyRegistration,
    reactor_name: Option<String>,
    status: StepStatus,
    rtmp_endpoint_sender: UnboundedSender<RtmpEndpointRequest>,
    reactor_manager: UnboundedSender<ReactorManagerRequest>,
    media_channel: UnboundedSender<RtmpEndpointMediaMessage>,
    stream_id_to_name_map: HashMap<StreamId, String>,
    stream_watchers: HashMap<String, StreamWatchers>,
}

impl StepFutureResult for RtmpWatchStepFutureResult {}

enum RtmpWatchStepFutureResult {
    RtmpEndpointGone,
    ReactorManagerGone,
    ReactorGone,
    RtmpWatchNotificationReceived(
        RtmpEndpointWatcherNotification,
        UnboundedReceiver<RtmpEndpointWatcherNotification>,
    ),

    ReactorWorkflowResponse {
        is_valid: bool,
        validation_channel: Sender<ValidationResponse>,
        reactor_update_channel: UnboundedReceiver<ReactorWorkflowUpdate>,
    },

    ReactorUpdateReceived {
        stream_name: String,
        update: ReactorWorkflowUpdate,
        reactor_update_channel: UnboundedReceiver<ReactorWorkflowUpdate>,
        cancellation_channel: UnboundedReceiver<()>,
    },

    ReactorReceiverCanceled {
        stream_name: String,
    },
}

#[derive(ThisError, Debug)]
enum StepStartupError {
    #[error(
        "No RTMP app specified.  A non-empty parameter of '{}' is required",
        PORT_PROPERTY_NAME
    )]
    NoRtmpApp,

    #[error(
        "No stream key specified.  A non-empty parameter of '{}' is required",
        APP_PROPERTY_NAME
    )]
    NoStreamKey,

    #[error(
        "Invalid port value of '{0}' specified.  A number from 0 to 65535 should be specified"
    )]
    InvalidPort(String),

    #[error("Failed to parse ip address")]
    InvalidIpAddress(#[from] IpAddressParseError),

    #[error(
        "Both {} and {} were specified, but only one is allowed",
        IP_ALLOW_PROPERTY_NAME,
        IP_DENY_PROPERTY_NAME
    )]
    BothDenyAndAllowIpRestrictions,
}

impl RtmpWatchStepGenerator {
    pub fn new(
        rtmp_endpoint_sender: UnboundedSender<RtmpEndpointRequest>,
        reactor_manager: UnboundedSender<ReactorManagerRequest>,
    ) -> Self {
        RtmpWatchStepGenerator {
            rtmp_endpoint_sender,
            reactor_manager,
        }
    }
}

impl StepGenerator for RtmpWatchStepGenerator {
    fn generate(&self, definition: WorkflowStepDefinition) -> StepCreationResult {
        let use_rtmps = definition.parameters.get(RTMPS_FLAG).is_some();
        let port = match definition.parameters.get(PORT_PROPERTY_NAME) {
            Some(Some(value)) => match value.parse::<u16>() {
                Ok(num) => num,
                Err(_) => {
                    return Err(Box::new(StepStartupError::InvalidPort(value.clone())));
                }
            },

            _ => {
                if use_rtmps {
                    443
                } else {
                    1935
                }
            }
        };

        let app = match definition.parameters.get(APP_PROPERTY_NAME) {
            Some(Some(x)) => x.trim(),
            _ => return Err(Box::new(StepStartupError::NoRtmpApp)),
        };

        let stream_key = match definition.parameters.get(STREAM_KEY_PROPERTY_NAME) {
            Some(Some(x)) => x.trim(),
            _ => return Err(Box::new(StepStartupError::NoStreamKey)),
        };

        let stream_key = if stream_key == "*" {
            StreamKeyRegistration::Any
        } else {
            StreamKeyRegistration::Exact(stream_key.to_string())
        };

        let allowed_ips = match definition.parameters.get(IP_ALLOW_PROPERTY_NAME) {
            Some(Some(value)) => IpAddress::parse_comma_delimited_list(Some(value))?,
            _ => Vec::new(),
        };

        let denied_ips = match definition.parameters.get(IP_DENY_PROPERTY_NAME) {
            Some(Some(value)) => IpAddress::parse_comma_delimited_list(Some(value))?,
            _ => Vec::new(),
        };

        let ip_restriction = match (!allowed_ips.is_empty(), !denied_ips.is_empty()) {
            (true, true) => {
                return Err(Box::new(StepStartupError::BothDenyAndAllowIpRestrictions));
            }
            (true, false) => IpRestriction::Allow(allowed_ips),
            (false, true) => IpRestriction::Deny(denied_ips),
            (false, false) => IpRestriction::None,
        };

        let reactor_name = match definition.parameters.get(REACTOR_NAME) {
            Some(Some(value)) => Some(value.clone()),
            _ => None,
        };

        let (media_sender, media_receiver) = unbounded_channel();

        let app = app.to_owned();
        let step = RtmpWatchStep {
            definition,
            status: StepStatus::Created,
            port,
            rtmp_app: app,
            rtmp_endpoint_sender: self.rtmp_endpoint_sender.clone(),
            reactor_manager: self.reactor_manager.clone(),
            media_channel: media_sender,
            stream_key,
            stream_id_to_name_map: HashMap::new(),
            reactor_name,
            stream_watchers: HashMap::new(),
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
                ip_restrictions: ip_restriction,
                use_tls: use_rtmps,
                requires_registrant_approval: step.reactor_name.is_some(),
            });

        Ok((
            Box::new(step),
            vec![
                wait_for_endpoint_notification(notification_receiver).boxed(),
                notify_on_reactor_manager_close(self.reactor_manager.clone()).boxed(),
            ],
        ))
    }
}

impl RtmpWatchStep {
    fn handle_endpoint_notification(
        &mut self,
        notification: RtmpEndpointWatcherNotification,
        outputs: &mut StepOutputs,
    ) {
        match notification {
            RtmpEndpointWatcherNotification::WatcherRegistrationFailed => {
                error!("Registration for RTMP watchers was denied");
                self.status = StepStatus::Error {
                    message: "Registration for watchers failed".to_string(),
                };
            }

            RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful => {
                info!("Registration for RTMP watchers was accepted");
                self.status = StepStatus::Active;
            }

            RtmpEndpointWatcherNotification::StreamKeyBecameActive {
                stream_key,
                reactor_update_channel,
            } => {
                info!(
                    stream_key = %stream_key,
                    "At least one watcher became active for stream key '{}'", stream_key
                );

                let cancellation_channel =
                    if let Some(reactor_update_channel) = reactor_update_channel {
                        let (cancellation_sender, cancellation_receiver) = unbounded_channel();
                        let future = wait_for_reactor_update(
                            stream_key.clone(),
                            reactor_update_channel,
                            cancellation_receiver,
                        )
                        .boxed();

                        outputs.futures.push(future);
                        Some(cancellation_sender)
                    } else {
                        None
                    };

                self.stream_watchers.insert(
                    stream_key,
                    StreamWatchers {
                        _reactor_cancel_channel: cancellation_channel,
                    },
                );
            }

            RtmpEndpointWatcherNotification::StreamKeyBecameInactive { stream_key } => {
                info!(
                    stream_key = %stream_key,
                    "All watchers left stream key '{}'", stream_key
                );

                self.stream_watchers.remove(&stream_key);
            }

            RtmpEndpointWatcherNotification::WatcherRequiringApproval {
                connection_id,
                stream_key,
                response_channel,
            } => {
                if let Some(reactor) = &self.reactor_name {
                    let (sender, receiver) = unbounded_channel();
                    let _ = self.reactor_manager.send(
                        ReactorManagerRequest::CreateWorkflowForStreamName {
                            reactor_name: reactor.clone(),
                            stream_name: stream_key,
                            response_channel: sender,
                        },
                    );

                    outputs
                        .futures
                        .push(wait_for_reactor_response(receiver, response_channel).boxed());
                } else {
                    error!(
                        connection_id = %connection_id,
                        stream_key = %stream_key,
                        "Watcher requires approval for stream key {} but no reactor name was set",
                        stream_key
                    );

                    let _ = response_channel.send(ValidationResponse::Reject);
                }
            }
        }
    }

    fn handle_media(&mut self, media: MediaNotification, outputs: &mut StepOutputs) {
        outputs.media.push(media.clone());

        if self.status == StepStatus::Active {
            match &media.content {
                MediaNotificationContent::NewIncomingStream { stream_name } => {
                    // If this step was registered with an exact stream name, then we don't care
                    // what stream name this was originally published as.  For watch purposes treat
                    // it as the configured stream key
                    let stream_name = match &self.stream_key {
                        StreamKeyRegistration::Any => stream_name,
                        StreamKeyRegistration::Exact(configured_stream_name) => {
                            configured_stream_name
                        }
                    };

                    info!(
                        stream_id = ?media.stream_id,
                        stream_name = %stream_name,
                        "New incoming stream notification found for stream id {:?} and stream name '{}", media.stream_id, stream_name
                    );

                    match self.stream_id_to_name_map.get(&media.stream_id) {
                        None => (),
                        Some(current_stream_name) => {
                            if current_stream_name == stream_name {
                                warn!(
                                    stream_id = ?media.stream_id,
                                    stream_name = %stream_name,
                                    "New incoming stream notification for stream id {:?} is already mapped \
                                        to this same stream name.", media.stream_id
                                );
                            } else {
                                warn!(
                                    stream_id = ?media.stream_id,
                                    new_stream_name = %stream_name,
                                    active_stream_name = %current_stream_name,
                                    "New incoming stream notification for stream id {:?} is already mapped \
                                        to the stream name '{}'", media.stream_id, current_stream_name
                                );
                            }
                        }
                    }

                    self.stream_id_to_name_map
                        .insert(media.stream_id.clone(), stream_name.clone());
                }

                MediaNotificationContent::StreamDisconnected => {
                    info!(
                        stream_id = ?media.stream_id,
                        "Stream disconnected notification received for stream id {:?}", media.stream_id
                    );
                    match self.stream_id_to_name_map.remove(&media.stream_id) {
                        Some(_) => (),
                        None => {
                            warn!(
                                stream_id = ?media.stream_id,
                                "Disconnected stream {:?} was not mapped to a stream name", media.stream_id
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
                            codec: *codec,
                            data: data.clone(),
                            timestamp: RtmpTimestamp::new(timestamp.dts().as_millis() as u32),
                            composition_time_offset: timestamp.pts_offset(),
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
                            codec: *codec,
                            data: data.clone(),
                            timestamp: RtmpTimestamp::new(timestamp.as_millis() as u32),
                        },
                    };

                    let _ = self.media_channel.send(rtmp_media);
                }
            }
        }
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
        for notification in inputs.notifications.drain(..) {
            let future_result = match notification.downcast::<RtmpWatchStepFutureResult>() {
                Ok(x) => *x,
                Err(_) => {
                    error!("Rtmp receive step received a notification that is not an 'RtmpReceiveFutureResult' type");
                    self.status = StepStatus::Error {
                        message: "Received invalid future result type".to_string(),
                    };

                    return;
                }
            };

            match future_result {
                RtmpWatchStepFutureResult::RtmpEndpointGone => {
                    error!("Rtmp endpoint gone, shutting step down");
                    self.status = StepStatus::Error {
                        message: "Rtmp endpoint gone".to_string(),
                    };

                    return;
                }

                RtmpWatchStepFutureResult::ReactorManagerGone => {
                    error!("Reactor manager gone");
                    self.status = StepStatus::Error {
                        message: "Reactor manager gone".to_string(),
                    };

                    return;
                }

                RtmpWatchStepFutureResult::ReactorGone => {
                    if let Some(reactor_name) = &self.reactor_name {
                        error!("The {} reactor is gone", reactor_name);
                    } else {
                        error!("Received notice that the reactor is gone, but this step doesn't use one");
                    }

                    self.status = StepStatus::Error {
                        message: "Reactor gone".to_string(),
                    };

                    return;
                }

                RtmpWatchStepFutureResult::RtmpWatchNotificationReceived(
                    notification,
                    receiver,
                ) => {
                    outputs
                        .futures
                        .push(wait_for_endpoint_notification(receiver).boxed());

                    self.handle_endpoint_notification(notification, outputs);
                }

                RtmpWatchStepFutureResult::ReactorWorkflowResponse {
                    is_valid,
                    validation_channel,
                    reactor_update_channel,
                } => {
                    if is_valid {
                        let _ = validation_channel.send(ValidationResponse::Approve {
                            reactor_update_channel,
                        });
                    } else {
                        let _ = validation_channel.send(ValidationResponse::Reject);
                    }
                }

                RtmpWatchStepFutureResult::ReactorUpdateReceived {
                    stream_name,
                    update,
                    reactor_update_channel,
                    cancellation_channel,
                } => {
                    if update.is_valid {
                        // No action needed as this is still a valid stream name
                        let future = wait_for_reactor_update(
                            stream_name,
                            reactor_update_channel,
                            cancellation_channel,
                        );

                        outputs.futures.push(future.boxed());
                    } else {
                        info!(
                            stream_key = %stream_name,
                            "Received update that stream {} is no longer tied to a workflow",
                            stream_name
                        );

                        // TODO: Need some way to disconnect watchers
                    }
                }

                RtmpWatchStepFutureResult::ReactorReceiverCanceled { stream_name } => {
                    if self.stream_watchers.remove(&stream_name).is_some() {
                        info!(
                            "Stream {}'s reactor updating has been cancelled",
                            stream_name
                        );
                    }
                }
            }
        }

        for media in inputs.media.drain(..) {
            self.handle_media(media, outputs);
        }
    }

    fn shutdown(&mut self) {
        self.status = StepStatus::Shutdown;
        let _ = self
            .rtmp_endpoint_sender
            .send(RtmpEndpointRequest::RemoveRegistration {
                registration_type: RegistrationType::Watcher,
                port: self.port,
                rtmp_app: self.rtmp_app.clone(),
                rtmp_stream_key: self.stream_key.clone(),
            });
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

async fn wait_for_reactor_response(
    mut receiver: UnboundedReceiver<ReactorWorkflowUpdate>,
    response_channel: Sender<ValidationResponse>,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(result) => result.is_valid,
        None => false, // Treat the channel being closed as no workflow
    };

    let result = RtmpWatchStepFutureResult::ReactorWorkflowResponse {
        is_valid: result,
        validation_channel: response_channel,
        reactor_update_channel: receiver,
    };

    Box::new(result)
}

async fn wait_for_reactor_update(
    stream_name: String,
    mut update_receiver: UnboundedReceiver<ReactorWorkflowUpdate>,
    mut cancellation_receiver: UnboundedReceiver<()>,
) -> Box<dyn StepFutureResult> {
    let result = tokio::select! {
        update = update_receiver.recv() => {
            match update {
                Some(update) => RtmpWatchStepFutureResult::ReactorUpdateReceived{
                    stream_name,
                    update,
                    reactor_update_channel: update_receiver,
                    cancellation_channel: cancellation_receiver,
                },

                None => RtmpWatchStepFutureResult::ReactorGone,
            }
        }

        _ = cancellation_receiver.recv() => RtmpWatchStepFutureResult::ReactorReceiverCanceled {
            stream_name,
        }
    };

    Box::new(result)
}

async fn notify_on_reactor_manager_close(
    sender: UnboundedSender<ReactorManagerRequest>,
) -> Box<dyn StepFutureResult> {
    sender.closed().await;
    Box::new(RtmpWatchStepFutureResult::ReactorManagerGone)
}
