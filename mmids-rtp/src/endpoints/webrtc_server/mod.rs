mod actor;
pub mod publisher_connection_handler;
pub mod watcher_connection_handler;

use mmids_core::codecs::{AudioCodec, VideoCodec};
use mmids_core::net::ConnectionId;
use mmids_core::reactors::ReactorWorkflowUpdate;
use bytes::Bytes;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;

pub use actor::start_webrtc_server;
use mmids_core::{StreamId, VideoTimestamp};
use mmids_core::workflows::MediaNotificationContent;

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
}

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
    WatchRequestAccepted {
        answer_sdp: String,
    },
}

#[derive(Debug)]
pub enum ValidationResponse {
    Reject,
    Approve {
        reactor_update_channel: UnboundedReceiver<ReactorWorkflowUpdate>,
    },
}

pub enum RequestType {
    Publisher,
    Watcher,
}
