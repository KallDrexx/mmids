mod actor;

use crate::codecs::{AudioCodec, VideoCodec};
use crate::net::ConnectionId;
use crate::reactors::ReactorWorkflowUpdate;
use crate::{StreamId, VideoTimestamp};
use bytes::Bytes;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Sender;

pub use actor::start_webrtc_server;

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
        reactor_update_channel: Option<UnboundedSender<ReactorWorkflowUpdate>>,
    },

    NewVideoData {
        publisher: ConnectionId,
        codec: VideoCodec,
        is_keyframe: bool,
        is_sequence_header: bool,
        data: Bytes,
        timestamp: VideoTimestamp,
    },

    NewAudioData {
        publisher: ConnectionId,
        codec: AudioCodec,
        is_sequence_header: bool,
        data: Bytes,
        timestamp: Duration,
    },
}

pub enum WebrtcServerWatcherRegistrantNotification {
    RegistrationFailed,
    RegistrationSuccessful,
    WatcherRequiringApproval {
        connection_id: ConnectionId,
        stream_name: String,
        response_channel: UnboundedSender<ValidationResponse>,
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

    NewVideoData {
        codec: VideoCodec,
        data: Bytes,
        timestamp: VideoTimestamp,
        is_sequence_header: bool,
        is_keyframe: bool,
    },

    NewAudioData {
        codec: AudioCodec,
        data: Bytes,
        timestamp: Duration,
        is_sequence_header: bool,
    },
}

#[derive(Debug)]
pub enum ValidationResponse {
    Reject,
    Approve {
        reactor_update_channel: UnboundedSender<ReactorWorkflowUpdate>,
    },
}

pub enum RequestType {
    Publisher,
    Watcher,
}
