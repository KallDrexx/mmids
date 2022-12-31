//! The RTMP Receive step registers with the RTMP server endpoint to allow publishers to connect with
//! the specified port, application name, and stream key combination.  Any media packets that
//! RTMP publishers send in will be sent to the next steps.
//!
//! All media packets that come in from previous workflow steps are ignored.
#[cfg(test)]
mod tests;

use crate::rtmp_server::{
    IpRestriction, RegistrationType, RtmpEndpointPublisherMessage, RtmpEndpointRequest,
    StreamKeyRegistration, ValidationResponse,
};
use bytes::BytesMut;
use futures::FutureExt;
use mmids_core::codecs::{AUDIO_CODEC_AAC_RAW, VIDEO_CODEC_H264_AVC};
use mmids_core::net::{ConnectionId, IpAddress, IpAddressParseError};
use mmids_core::reactors::manager::ReactorManagerRequest;
use mmids_core::reactors::ReactorWorkflowUpdate;
use mmids_core::workflows::definitions::WorkflowStepDefinition;
use mmids_core::workflows::metadata::{
    MediaPayloadMetadataCollection, MetadataEntry, MetadataKey, MetadataValue,
};
use mmids_core::workflows::steps::factory::StepGenerator;
use mmids_core::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use mmids_core::workflows::{MediaNotification, MediaNotificationContent, MediaType};
use mmids_core::StreamId;
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error as ThisError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tracing::{error, info};

pub const PORT_PROPERTY_NAME: &str = "port";
pub const APP_PROPERTY_NAME: &str = "rtmp_app";
pub const STREAM_KEY_PROPERTY_NAME: &str = "stream_key";
pub const IP_ALLOW_PROPERTY_NAME: &str = "allow_ips";
pub const IP_DENY_PROPERTY_NAME: &str = "deny_ips";
pub const RTMPS_FLAG: &str = "rtmps";
pub const REACTOR_NAME: &str = "reactor";

/// Generates new rtmp receiver workflow step instances based on specified step definitions.
pub struct RtmpReceiverStepGenerator {
    rtmp_endpoint_sender: UnboundedSender<RtmpEndpointRequest>,
    reactor_manager: UnboundedSender<ReactorManagerRequest>,
    is_keyframe_metadata_key: MetadataKey,
    pts_offset_metadata_key: MetadataKey,
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
    rtmp_app: Arc<String>,
    stream_key: StreamKeyRegistration,
    status: StepStatus,
    connection_details: HashMap<ConnectionId, ConnectionDetails>,
    reactor_name: Option<Arc<String>>,
    metadata_buffer: BytesMut,
    is_keyframe_metadata_key: MetadataKey,
    pts_offset_metadata_key: MetadataKey,
}

impl StepFutureResult for FutureResult {}

enum FutureResult {
    RtmpEndpointDroppedRegistration,
    ReactorManagerGone,
    ReactorGone,
    RtmpEndpointResponseReceived(
        RtmpEndpointPublisherMessage,
        UnboundedReceiver<RtmpEndpointPublisherMessage>,
    ),

    ReactorWorkflowReturned {
        is_valid: bool,
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

impl RtmpReceiverStepGenerator {
    pub fn new(
        rtmp_endpoint_sender: UnboundedSender<RtmpEndpointRequest>,
        reactor_manager: UnboundedSender<ReactorManagerRequest>,
        is_keyframe_metadata_key: MetadataKey,
        pts_offset_metadata_key: MetadataKey,
    ) -> Self {
        RtmpReceiverStepGenerator {
            rtmp_endpoint_sender,
            reactor_manager,
            is_keyframe_metadata_key,
            pts_offset_metadata_key,
        }
    }
}

impl StepGenerator for RtmpReceiverStepGenerator {
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
            Some(Some(x)) => Arc::new(x.trim().to_string()),
            _ => return Err(Box::new(StepStartupError::NoRtmpApp)),
        };

        let stream_key = match definition.parameters.get(STREAM_KEY_PROPERTY_NAME) {
            Some(Some(x)) => Arc::new(x.trim().to_string()),
            _ => return Err(Box::new(StepStartupError::NoStreamKey)),
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
            Some(Some(value)) => Some(Arc::new(value.clone())),
            _ => None,
        };

        let step = RtmpReceiverStep {
            definition,
            status: StepStatus::Created,
            rtmp_endpoint_sender: self.rtmp_endpoint_sender.clone(),
            reactor_manager: self.reactor_manager.clone(),
            port,
            rtmp_app: app,
            connection_details: HashMap::new(),
            reactor_name,
            stream_key: if stream_key.as_str() == "*" {
                StreamKeyRegistration::Any
            } else {
                StreamKeyRegistration::Exact(stream_key)
            },
            metadata_buffer: BytesMut::new(),
            is_keyframe_metadata_key: self.is_keyframe_metadata_key,
            pts_offset_metadata_key: self.pts_offset_metadata_key,
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
                self.status = StepStatus::Error {
                    message: "Rtmp receive step failed to register for publish registration"
                        .to_string(),
                };
            }

            RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => {
                info!("Rtmp receive step successfully registered for publishing");
                self.status = StepStatus::Active;
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
                is_sequence_header,
                is_keyframe,
                composition_time_offset,
            } => match self.connection_details.get(&publisher) {
                None => (),
                Some(connection) => {
                    let is_keyframe_metadata = MetadataEntry::new(
                        self.is_keyframe_metadata_key,
                        MetadataValue::Bool(is_keyframe),
                        &mut self.metadata_buffer,
                    )
                    .unwrap(); // Should only happen if type mismatch occurs

                    let pts_offset_metadata = MetadataEntry::new(
                        self.pts_offset_metadata_key,
                        MetadataValue::I32(composition_time_offset),
                        &mut self.metadata_buffer,
                    )
                    .unwrap(); // Should only happen if type mismatch occurs

                    let metadata = MediaPayloadMetadataCollection::new(
                        [is_keyframe_metadata, pts_offset_metadata].into_iter(),
                        &mut self.metadata_buffer,
                    );

                    outputs.media.push(MediaNotification {
                        stream_id: connection.stream_id.clone(),
                        content: MediaNotificationContent::MediaPayload {
                            media_type: MediaType::Video,
                            payload_type: VIDEO_CODEC_H264_AVC.clone(),
                            is_required_for_decoding: is_sequence_header,
                            timestamp: Duration::from_millis(timestamp.value.into()),
                            metadata,
                            data,
                        },
                    });
                }
            },

            RtmpEndpointPublisherMessage::NewAudioData {
                publisher,
                is_sequence_header,
                data,
                timestamp,
            } => match self.connection_details.get(&publisher) {
                None => (),
                Some(connection) => {
                    outputs.media.push(MediaNotification {
                        stream_id: connection.stream_id.clone(),
                        content: MediaNotificationContent::MediaPayload {
                            payload_type: AUDIO_CODEC_AAC_RAW.clone(),
                            media_type: MediaType::Audio,
                            timestamp: Duration::from_millis(timestamp.value as u64),
                            metadata: MediaPayloadMetadataCollection::new(
                                iter::empty(),
                                &mut self.metadata_buffer,
                            ),
                            is_required_for_decoding: is_sequence_header,
                            data,
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
                    self.status = StepStatus::Error {
                        message: "Rtmp receive step received a notification that is not an 'RtmpReceiveFutureResult' type".to_string(),
                    };

                    return;
                }
            };

            match future_result {
                FutureResult::RtmpEndpointDroppedRegistration => {
                    error!(
                        "Rtmp receive step stopping as the rtmp endpoint dropped the registration"
                    );
                    self.status = StepStatus::Error {
                        message: "Rtmp receive step stopping as the rtmp endpoint dropped the registration"
                            .to_string(),
                    };

                    return;
                }

                FutureResult::ReactorManagerGone => {
                    error!("Reactor manager gone");
                    self.status = StepStatus::Error {
                        message: "Reactor manager gone".to_string(),
                    };

                    return;
                }

                FutureResult::ReactorGone => {
                    if let Some(name) = &self.reactor_name {
                        error!("Reactor {} is gone", name);
                    } else {
                        error!("Got reactor gone signal but step is not using a reactor");
                    }

                    self.status = StepStatus::Error {
                        message: "Reactor gone".to_string(),
                    };

                    return;
                }

                FutureResult::RtmpEndpointResponseReceived(message, receiver) => {
                    outputs
                        .futures
                        .push(wait_for_rtmp_endpoint_response(receiver).boxed());

                    self.handle_rtmp_publisher_message(outputs, message);
                }

                FutureResult::ReactorWorkflowReturned {
                    is_valid,
                    reactor_receiver,
                    response_channel,
                } => {
                    if is_valid {
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
                    if update.is_valid {
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
        None => FutureResult::RtmpEndpointDroppedRegistration,
        Some(message) => FutureResult::RtmpEndpointResponseReceived(message, receiver),
    };

    Box::new(notification)
}

async fn wait_for_reactor_response(
    mut reactor_receiver: UnboundedReceiver<ReactorWorkflowUpdate>,
    connection_response_channel: Sender<ValidationResponse>,
) -> Box<dyn StepFutureResult> {
    let result = match reactor_receiver.recv().await {
        Some(response) => response.is_valid,
        None => false, // reactor closed, treat it the same as no workflow scenario
    };

    let result = FutureResult::ReactorWorkflowReturned {
        is_valid: result,
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
                    reactor_receiver,
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
