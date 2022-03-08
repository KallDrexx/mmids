mod actor;
pub mod publisher_connection_handler;
pub mod watcher_connection_handler;

use mmids_core::codecs::{AudioCodec, VideoCodec};
use mmids_core::net::ConnectionId;
use mmids_core::reactors::ReactorWorkflowUpdate;
use std::fmt::{Debug, Formatter};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;

pub use actor::start_webrtc_server;
use mmids_core::workflows::{MediaNotification, MediaNotificationContent};
use mmids_core::StreamId;

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum StreamNameRegistration {
    Any,
    Exact(String),
}

pub enum WebrtcServerRequest {
    ListenForPublishers {
        application_name: String,
        stream_name: StreamNameRegistration,
        video_codec: Option<VideoCodec>,
        audio_codec: Option<AudioCodec>,
        requires_registrant_approval: bool,
        notification_channel: UnboundedSender<WebrtcServerPublisherRegistrantNotification>,
    },

    ListenForWatchers {
        application_name: String,
        stream_name: StreamNameRegistration,
        video_codec: Option<VideoCodec>,
        audio_codec: Option<AudioCodec>,
        requires_registrant_approval: bool,
        notification_channel: UnboundedSender<WebrtcServerWatcherRegistrantNotification>,
        media_channel: UnboundedReceiver<MediaNotification>,
    },

    RemoveRegistration {
        registration_type: RequestType,
        application_name: String,
        stream_name: StreamNameRegistration,
    },

    StreamPublishRequested {
        application_name: String,
        stream_name: String,
        offer_sdp: String,
        notification_channel: UnboundedSender<WebrtcStreamPublisherNotification>,
    },

    StreamWatchRequested {
        application_name: String,
        stream_name: String,
        offer_sdp: String,
        notification_channel: UnboundedSender<WebrtcStreamWatcherNotification>,
    },
}

#[derive(Debug)]
pub enum WebrtcServerPublisherRegistrantNotification {
    RegistrationFailed {},
    RegistrationSuccessful,
    PublisherRequiringApproval {
        connection_id: ConnectionId,
        stream_name: String,
        response_channel: Sender<ValidationResponse>,
    },

    NewPublisherConnected {
        connection_id: ConnectionId,
        stream_id: StreamId,
        stream_name: String,
        reactor_update_channel: Option<UnboundedReceiver<ReactorWorkflowUpdate>>,
        media_channel: UnboundedReceiver<MediaNotificationContent>,
    },

    PublisherDisconnected {
        connection_id: ConnectionId,
        stream_name: String,
    },
}

#[derive(Debug)]
pub enum WebrtcServerWatcherRegistrantNotification {
    RegistrationFailed,
    RegistrationSuccessful,

    WatcherRequiringApproval {
        connection_id: ConnectionId,
        stream_name: String,
        response_channel: Sender<ValidationResponse>,
    },

    StreamNameBecameActive {
        stream_name: String,
        reactor_update_channel: Option<UnboundedReceiver<ReactorWorkflowUpdate>>,
    },

    StreamNameBecameInactive {
        stream_name: String,
    },
}

pub enum WebrtcStreamPublisherNotification {
    PublishRequestRejected,
    PublishRequestAccepted { answer_sdp: String },
}

pub enum WebrtcStreamWatcherNotification {
    WatchRequestRejected,
    WatchRequestAccepted { answer_sdp: String },
}

pub enum ValidationResponse {
    Reject,
    Approve {
        reactor_update_channel: UnboundedReceiver<ReactorWorkflowUpdate>,
    },
}

#[derive(Debug)]
pub enum RequestType {
    Publisher,
    Watcher,
}

impl Debug for ValidationResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationResponse::Reject => write!(f, "Reject"),
            ValidationResponse::Approve { .. } => write!(f, "Approve"),
        }
    }
}

// Manually implement Debug to not log raw SDP
impl Debug for WebrtcStreamWatcherNotification {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WebrtcStreamWatcherNotification::WatchRequestRejected => {
                write!(f, "WatchRequestRejected")
            }

            WebrtcStreamWatcherNotification::WatchRequestAccepted { .. } => {
                write!(f, "WatchRequestAccepted")
            }
        }
    }
}

// Manually implement Debug to not log raw SDP
impl Debug for WebrtcStreamPublisherNotification {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WebrtcStreamPublisherNotification::PublishRequestAccepted { .. } => {
                write!(f, "PublishRequestAccepted")
            }

            WebrtcStreamPublisherNotification::PublishRequestRejected => {
                write!(f, "PublishRequestRejected")
            }
        }
    }
}
