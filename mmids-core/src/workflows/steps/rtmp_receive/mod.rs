//! The RTMP Receive step registers with the RTMP server endpoint to allow publishers to connect with
//! the specified port, application name, and stream key combination.  Any media packets that
//! RTMP publishers send in will be sent to the next steps.
//!
//! All media packets that come in from previous workflow steps are ignored.
#[cfg(test)]
mod tests;

use crate::endpoints::rtmp_server::{
    IpRestriction, RegistrationType, RtmpEndpointPublisherMessage, RtmpEndpointRequest,
    StreamKeyRegistration, ValidationResponse,
};

use crate::net::{ConnectionId, IpAddress, IpAddressParseError};
use crate::workflows::definitions::WorkflowStepDefinition;
use crate::workflows::steps::factory::StepGenerator;
use crate::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};

use crate::reactors::manager::ReactorManagerRequest;
use crate::reactors::ReactorWorkflowUpdate;
use crate::workflows::{MediaNotification, MediaNotificationContent};
use crate::StreamId;
use futures::FutureExt;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error as ThisError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tracing::{error, info};

pub const PORT_PROPERTY_NAME: &'static str = "port";
pub const APP_PROPERTY_NAME: &'static str = "rtmp_app";
pub const STREAM_KEY_PROPERTY_NAME: &'static str = "stream_key";
pub const IP_ALLOW_PROPERTY_NAME: &'static str = "allow_ips";
pub const IP_DENY_PROPERTY_NAME: &'static str = "deny_ips";
pub const RTMPS_FLAG: &'static str = "rtmps";
pub const REACTOR_NAME: &'static str = "reactor";

/// Generates new rtmp receiver workflow step instances based on specified step definitions.
pub struct RtmpReceiverStepGenerator {
    rtmp_endpoint_sender: UnboundedSender<RtmpEndpointRequest>,
    reactor_manager: UnboundedSender<ReactorManagerRequest>,
}

struct ConnectionDetails {
    stream_id: StreamId,

    // Used to cancel the reactor update future. When a stream disconnects, this cancellation
    // channel will be dropped causing the future waiting for reactor updates to be closed. This
    // will inform the reactor that this step is no longer interested in whatever workflow it was
    // managing for it. Not using a one shot, as the channel needs to live across multiple futures
    // if updates come in.
    _cancellation_channel: Option<UnboundedSender<()>>,
}

struct RtmpReceiverStep {
    definition: WorkflowStepDefinition,
    rtmp_endpoint_sender: UnboundedSender<RtmpEndpointRequest>,
    reactor_manager: UnboundedSender<ReactorManagerRequest>,
    port: u16,
    rtmp_app: String,
    stream_key: StreamKeyRegistration,
    status: StepStatus,
    connection_details: HashMap<ConnectionId, ConnectionDetails>,
    reactor_name: Option<String>,
}

impl StepFutureResult for FutureResult {}

enum FutureResult {
    RtmpEndpointGone,
    ReactorManagerGone,
    ReactorGone,
    RtmpEndpointResponseReceived(
        RtmpEndpointPublisherMessage,
        UnboundedReceiver<RtmpEndpointPublisherMessage>,
    ),

    ReactorWorkflowReturned {
        workflow_name: Option<String>,
        reactor_receiver: UnboundedReceiver<ReactorWorkflowUpdate>,
        response_channel: Sender<ValidationResponse>,
    },

    ReactorUpdateReceived {
        connection_id: ConnectionId,
        update: ReactorWorkflowUpdate,
        reactor_receiver: UnboundedReceiver<ReactorWorkflowUpdate>,
        cancellation_channel: UnboundedReceiver<()>,
    },

    ReactorCancellationReceived,
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

    #[error("Failed to parse ip address")]
    InvalidIpAddressSpecified(#[from] IpAddressParseError),

    #[error(
        "Both {} and {} were specified, but only one is allowed",
        IP_ALLOW_PROPERTY_NAME,
        IP_DENY_PROPERTY_NAME
    )]
    BothDenyAndAllowIpRestrictionsSpecified,
}

impl RtmpReceiverStepGenerator {
    pub fn new(
        rtmp_endpoint_sender: UnboundedSender<RtmpEndpointRequest>,
        reactor_manager: UnboundedSender<ReactorManagerRequest>,
    ) -> Self {
        RtmpReceiverStepGenerator {
            rtmp_endpoint_sender,
            reactor_manager,
        }
    }
}

impl StepGenerator for RtmpReceiverStepGenerator {
    fn generate(&self, definition: WorkflowStepDefinition) -> StepCreationResult {
        let use_rtmps = match definition.parameters.get(RTMPS_FLAG) {
            Some(_) => true,
            None => false,
        };

        let port = match definition.parameters.get(PORT_PROPERTY_NAME) {
            Some(Some(value)) => match value.parse::<u16>() {
                Ok(num) => num,
                Err(_) => {
                    return Err(Box::new(StepStartupError::InvalidPortSpecified(
                        value.clone(),
                    )));
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
            _ => return Err(Box::new(StepStartupError::NoRtmpAppSpecified)),
        };

        let stream_key = match definition.parameters.get(STREAM_KEY_PROPERTY_NAME) {
            Some(Some(x)) => x.trim(),
            _ => return Err(Box::new(StepStartupError::NoStreamKeySpecified)),
        };

        let allowed_ips = match definition.parameters.get(IP_ALLOW_PROPERTY_NAME) {
            Some(Some(value)) => IpAddress::parse_comma_delimited_list(Some(value))?,
            _ => Vec::new(),
        };

        let denied_ips = match definition.parameters.get(IP_DENY_PROPERTY_NAME) {
            Some(Some(value)) => IpAddress::parse_comma_delimited_list(Some(value))?,
            _ => Vec::new(),
        };

        let ip_restriction = match (allowed_ips.len() > 0, denied_ips.len() > 0) {
            (true, true) => {
                return Err(Box::new(
                    StepStartupError::BothDenyAndAllowIpRestrictionsSpecified,
                ));
            }
            (true, false) => IpRestriction::Allow(allowed_ips),
            (false, true) => IpRestriction::Deny(denied_ips),
            (false, false) => IpRestriction::None,
        };

        let reactor_name = match definition.parameters.get(REACTOR_NAME) {
            Some(Some(value)) => Some(value.clone()),
            _ => None,
        };

        let step = RtmpReceiverStep {
            definition: definition.clone(),
            status: StepStatus::Created,
            rtmp_endpoint_sender: self.rtmp_endpoint_sender.clone(),
            reactor_manager: self.reactor_manager.clone(),
            port,
            rtmp_app: app.to_string(),
            connection_details: HashMap::new(),
            reactor_name,
            stream_key: if stream_key == "*" {
                StreamKeyRegistration::Any
            } else {
                StreamKeyRegistration::Exact(stream_key.to_string())
            },
        };

        let (sender, receiver) = unbounded_channel();
        let _ = step
            .rtmp_endpoint_sender
            .send(RtmpEndpointRequest::ListenForPublishers {
                message_channel: sender,
                port: step.port,
                rtmp_app: step.rtmp_app.clone(),
                rtmp_stream_key: step.stream_key.clone(),
                stream_id: None,
                ip_restrictions: ip_restriction,
                use_tls: use_rtmps,
                requires_registrant_approval: step.reactor_name.is_some(),
            });

        Ok((
            Box::new(step),
            vec![
                wait_for_rtmp_endpoint_response(receiver).boxed(),
                notify_reactor_manager_gone(self.reactor_manager.clone()).boxed(),
            ],
        ))
    }
}

impl RtmpReceiverStep {
    fn handle_rtmp_publisher_message(
        &mut self,
        outputs: &mut StepOutputs,
        message: RtmpEndpointPublisherMessage,
    ) {
        match message {
            RtmpEndpointPublisherMessage::PublisherRegistrationFailed => {
                error!("Rtmp receive step failed to register for publish registration");
                self.status = StepStatus::Error;

                return;
            }

            RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => {
                info!("Rtmp receive step successfully registered for publishing");
                self.status = StepStatus::Active;

                return;
            }

            RtmpEndpointPublisherMessage::NewPublisherConnected {
                stream_id,
                connection_id,
                stream_key,
                reactor_update_channel,
            } => {
                info!(
                    stream_id = ?stream_id,
                    connection_id = ?connection_id,
                    stream_key = %stream_key,
                    "Rtmp receive step seen new publisher: {:?}, {:?}, {:?}", stream_id, connection_id, stream_key
                );

                let cancellation_token = if let Some(update_channel) = reactor_update_channel {
                    let (cancellation_sender, cancellation_receiver) = unbounded_channel();
                    let future = wait_for_reactor_update(
                        connection_id.clone(),
                        update_channel,
                        cancellation_receiver,
                    );

                    outputs.futures.push(future.boxed());
                    Some(cancellation_sender)
                } else {
                    None
                };

                self.connection_details.insert(
                    connection_id,
                    ConnectionDetails {
                        stream_id: stream_id.clone(),
                        _cancellation_channel: cancellation_token,
                    },
                );

                outputs.media.push(MediaNotification {
                    stream_id,
                    content: MediaNotificationContent::NewIncomingStream {
                        stream_name: stream_key,
                    },
                });
            }

            RtmpEndpointPublisherMessage::PublishingStopped { connection_id } => {
                match self.connection_details.remove(&connection_id) {
                    None => (),
                    Some(connection) => {
                        info!(
                            stream_id = ?connection.stream_id,
                            connection_id = ?connection_id,
                            "Rtmp receive step notified that connection {:?} is no longer publishing stream {:?}",
                            connection_id, connection.stream_id
                        );

                        outputs.media.push(MediaNotification {
                            stream_id: connection.stream_id,
                            content: MediaNotificationContent::StreamDisconnected,
                        });
                    }
                }
            }

            RtmpEndpointPublisherMessage::StreamMetadataChanged {
                publisher,
                metadata,
            } => match self.connection_details.get(&publisher) {
                None => (),
                Some(connection) => outputs.media.push(MediaNotification {
                    stream_id: connection.stream_id.clone(),
                    content: MediaNotificationContent::Metadata {
                        data: crate::utils::stream_metadata_to_hash_map(metadata),
                    },
                }),
            },

            RtmpEndpointPublisherMessage::NewVideoData {
                publisher,
                data,
                timestamp,
                codec,
                is_sequence_header,
                is_keyframe,
            } => match self.connection_details.get(&publisher) {
                None => (),
                Some(connection) => {
                    outputs.media.push(MediaNotification {
                        stream_id: connection.stream_id.clone(),
                        content: MediaNotificationContent::Video {
                            is_keyframe,
                            is_sequence_header,
                            data,
                            codec,
                            timestamp: Duration::from_millis(timestamp.value as u64),
                        },
                    });
                }
            },

            RtmpEndpointPublisherMessage::NewAudioData {
                publisher,
                is_sequence_header,
                data,
                codec,
                timestamp,
            } => match self.connection_details.get(&publisher) {
                None => (),
                Some(connection) => {
                    outputs.media.push(MediaNotification {
                        stream_id: connection.stream_id.clone(),
                        content: MediaNotificationContent::Audio {
                            is_sequence_header,
                            data,
                            codec,
                            timestamp: Duration::from_millis(timestamp.value as u64),
                        },
                    });
                }
            },

            RtmpEndpointPublisherMessage::PublisherRequiringApproval {
                connection_id,
                stream_key,
                response_channel,
            } => {
                if let Some(name) = &self.reactor_name {
                    let (sender, receiver) = unbounded_channel();
                    let _ = self.reactor_manager.send(
                        ReactorManagerRequest::CreateWorkflowForStreamName {
                            reactor_name: name.clone(),
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
                        "Publisher requires approval for stream key {} but no reactor name was set",
                        stream_key
                    );

                    let _ = response_channel.send(ValidationResponse::Reject);
                }
            }
        }
    }
}

unsafe impl Send for RtmpReceiverStep {}

unsafe impl Sync for RtmpReceiverStep {}

impl WorkflowStep for RtmpReceiverStep {
    fn get_status(&self) -> &StepStatus {
        &self.status
    }

    fn get_definition(&self) -> &WorkflowStepDefinition {
        &self.definition
    }

    fn execute(&mut self, inputs: &mut StepInputs, outputs: &mut StepOutputs) {
        for future_result in inputs.notifications.drain(..) {
            let future_result = match future_result.downcast::<FutureResult>() {
                Ok(result) => *result,
                Err(_) => {
                    error!("Rtmp receive step received a notification that is not an 'RtmpReceiveFutureResult' type");
                    self.status = StepStatus::Error;

                    return;
                }
            };

            match future_result {
                FutureResult::RtmpEndpointGone => {
                    error!("Rtmp receive step stopping as the rtmp endpoint is gone");
                    self.status = StepStatus::Error;

                    return;
                }

                FutureResult::ReactorManagerGone => {
                    error!("Reactor manager gone");
                    self.status = StepStatus::Error;

                    return;
                }

                FutureResult::ReactorGone => {
                    if let Some(name) = &self.reactor_name {
                        error!("Reactor {} is gone", name);
                    } else {
                        error!("Got reactor gone signal but step is not using a reactor");
                    }

                    self.status = StepStatus::Error;

                    return;
                }

                FutureResult::RtmpEndpointResponseReceived(message, receiver) => {
                    outputs
                        .futures
                        .push(wait_for_rtmp_endpoint_response(receiver).boxed());

                    self.handle_rtmp_publisher_message(outputs, message);
                }

                FutureResult::ReactorWorkflowReturned {
                    workflow_name,
                    reactor_receiver,
                    response_channel,
                } => {
                    // If a workflow was returned, then that means the the stream key is allowed
                    if workflow_name.is_some() {
                        let _ = response_channel.send(ValidationResponse::Approve {
                            reactor_update_channel: reactor_receiver,
                        });
                    } else {
                        let _ = response_channel.send(ValidationResponse::Reject);
                    }
                }

                FutureResult::ReactorUpdateReceived {
                    connection_id,
                    update,
                    reactor_receiver,
                    cancellation_channel,
                } => {
                    if let Some(workflow_name) = update.workflow_name {
                        info!(
                            connection_id = %connection_id,
                            "Received update that {}'s workflow has been changed to {}",
                            connection_id, workflow_name
                        );

                        // No action needed as this is still a valid stream name
                        let future = wait_for_reactor_update(
                            connection_id,
                            reactor_receiver,
                            cancellation_channel,
                        );

                        outputs.futures.push(future.boxed());
                    } else {
                        info!(
                            connection_id = %connection_id,
                            "Received update that stream {} is no longer tied to a workflow",
                            connection_id
                        );

                        // TODO: Need some way to disconnect publishers
                    }
                }

                FutureResult::ReactorCancellationReceived => {}
            }
        }
    }

    fn shutdown(&mut self) {
        self.status = StepStatus::Shutdown;
        let _ = self
            .rtmp_endpoint_sender
            .send(RtmpEndpointRequest::RemoveRegistration {
                registration_type: RegistrationType::Publisher,
                port: self.port,
                rtmp_app: self.rtmp_app.clone(),
                rtmp_stream_key: self.stream_key.clone(),
            });
    }
}

async fn wait_for_rtmp_endpoint_response(
    mut receiver: UnboundedReceiver<RtmpEndpointPublisherMessage>,
) -> Box<dyn StepFutureResult> {
    let notification = match receiver.recv().await {
        None => FutureResult::RtmpEndpointGone,
        Some(message) => FutureResult::RtmpEndpointResponseReceived(message, receiver),
    };

    Box::new(notification)
}

async fn wait_for_reactor_response(
    mut reactor_receiver: UnboundedReceiver<ReactorWorkflowUpdate>,
    connection_response_channel: Sender<ValidationResponse>,
) -> Box<dyn StepFutureResult> {
    let result = match reactor_receiver.recv().await {
        Some(response) => response.workflow_name,
        None => None, // reactor closed, treat it the same as no workflow scenario
    };

    let result = FutureResult::ReactorWorkflowReturned {
        workflow_name: result,
        reactor_receiver,
        response_channel: connection_response_channel,
    };

    Box::new(result)
}

async fn wait_for_reactor_update(
    connection_id: ConnectionId,
    mut reactor_receiver: UnboundedReceiver<ReactorWorkflowUpdate>,
    mut cancellation_receiver: UnboundedReceiver<()>,
) -> Box<dyn StepFutureResult> {
    let result = tokio::select! {
        update = reactor_receiver.recv() => {
            match update {
                Some(update) => FutureResult::ReactorUpdateReceived{
                    connection_id,
                    update,
                    reactor_receiver: reactor_receiver,
                    cancellation_channel: cancellation_receiver,
                },

                None => FutureResult::ReactorGone,
            }
        }

        _ = cancellation_receiver.recv() => FutureResult::ReactorCancellationReceived,
    };

    Box::new(result)
}

async fn notify_reactor_manager_gone(
    sender: UnboundedSender<ReactorManagerRequest>,
) -> Box<dyn StepFutureResult> {
    sender.closed().await;
    Box::new(FutureResult::ReactorManagerGone)
}
