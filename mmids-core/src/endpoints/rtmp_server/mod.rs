//! This endpoint acts as a server for RTMP clients that want to publish or watch RTMP live streams.
//! Workflow steps send a message requesting to allow RTMP publishers or watchers for specific
//! port, RTMP application and stream key combinations.  The RTMP server endpoint will register the
//! specified port with the networking infrastructure for listening for connections, and any
//! networked traffic over that port will be forwarded to this endpoint.
//!
//! It will then perform handshaking and all other RTMP protocol actions, disconnecting clients if
//! they don't conform to the RTMP protocol correctly, or if they attempt to publish or watch an
//! application name and stream key combination that isn't actively registered.
//!
//! Incoming publish actions (such as new metadata, media packets, etc...) are passed to the workflow
//! steps that were registered for that application/stream key combination.  Likewise, when the
//! endpoint receives media from workflow steps it will route that media to the correct RTMP watcher
//! clients

mod actor;

use crate::codecs::{AudioCodec, VideoCodec};
use crate::net::tcp::TcpSocketRequest;
use crate::net::{ConnectionId, IpAddress};
use crate::reactors::ReactorWorkflowUpdate;
use crate::StreamId;
use actor::actor_types::RtmpServerEndpointActor;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use rml_rtmp::sessions::StreamMetadata;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;

/// Starts a new RTMP server endpoint, returning a channel that can be used to send notifications
/// and requests to it.
pub fn start_rtmp_server_endpoint(
    socket_request_sender: UnboundedSender<TcpSocketRequest>,
) -> UnboundedSender<RtmpEndpointRequest> {
    let (endpoint_sender, endpoint_receiver) = unbounded_channel();

    let endpoint = RtmpServerEndpointActor {
        futures: FuturesUnordered::new(),
        ports: HashMap::new(),
    };

    tokio::spawn(endpoint.run(endpoint_receiver, socket_request_sender));

    endpoint_sender
}

/// Specifies how a stream key should be registered for playback or publishing
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub enum StreamKeyRegistration {
    /// All stream keys for the the rtmp application should be registered
    Any,

    /// Only set up registration for the exact stream key
    Exact(String),
}

/// Specifies if there are any IP address restrictions as part of an RTMP server registration
#[derive(Debug, PartialEq)]
pub enum IpRestriction {
    /// All IP addresses are allowed
    None,

    /// Only the specified IP addresses are allowed.
    Allow(Vec<IpAddress>),

    /// All IP addresses are allowed except for the ones specified.
    Deny(Vec<IpAddress>),
}

/// Type of registration the request is related to
#[derive(Debug)]
pub enum RegistrationType {
    Publisher,
    Watcher,
}

/// Operations the rtmp server endpoint is being requested to make
#[derive(Debug)]
pub enum RtmpEndpointRequest {
    /// Requests the RTMP server to allow publishers on the given port, app, and stream key
    /// combinations.
    ListenForPublishers {
        /// Port to listen for RTMP publisher connections on
        port: u16,

        /// Name of the RTMP application publishers will connect to
        rtmp_app: String,

        /// What stream key publishers should be using
        rtmp_stream_key: StreamKeyRegistration,

        /// Channel that the rtmp server endpoint should respond with
        message_channel: UnboundedSender<RtmpEndpointPublisherMessage>,

        /// If specified, new media streams being published from this registration will be given
        /// the stream id specified.  If no id is given than one will be generated.  This is useful
        /// to correlate media streams that may have been pulled, processed externally, then brought
        /// back in for later workflow steps (e.g. an external transcoding workflow).
        stream_id: Option<StreamId>,

        /// What IP restriction rules should be in place for this registration
        ip_restrictions: IpRestriction,

        /// If true, this port should be on a TLS socket (i.e. RTMPS)
        use_tls: bool,

        /// If true, then publishers will not be automatically accepted even if they connect to
        /// the correct app/stream key combination and pass ip restrictions. Instead the registrant
        /// should be asked for final verification if the publisher should be allowed or not.
        requires_registrant_approval: bool,
    },

    /// Requests the RTMP server to allow clients to receive video on the given port, app,
    /// and stream key combinations
    ListenForWatchers {
        /// Port to listen on
        port: u16,

        /// Name of the RTMP application playback clients will connect to
        rtmp_app: String,

        /// Stream keys clients can receive video on
        rtmp_stream_key: StreamKeyRegistration,

        /// The channel that the rtmp server endpoint will send notifications to
        notification_channel: UnboundedSender<RtmpEndpointWatcherNotification>,

        /// The channel that the registrant will send updated media data to the rtmp endpoint on
        media_channel: UnboundedReceiver<RtmpEndpointMediaMessage>,

        /// What IP restriction rules should be in place for this registration
        ip_restrictions: IpRestriction,

        /// If true, this port should be on a TLS socket (i.e. RTMPS)
        use_tls: bool,

        /// If true, then watchers will not be automatically accepted even if they connect to
        /// the correct app/stream key combination and pass ip restrictions. Instead the registrant
        /// should be asked for final verification if the watcher should be allowed or not.
        requires_registrant_approval: bool,
    },

    /// Requests the specified registration should be removed
    RemoveRegistration {
        /// The type of registration that is being removed
        registration_type: RegistrationType,

        /// Port the removed registrant was listening on
        port: u16,

        /// The RTMP application name that the registrant was listening on
        rtmp_app: String,

        /// The stream key the registrant had registered for
        rtmp_stream_key: StreamKeyRegistration,
    },
}

/// Response to approval/validation requests
#[derive(Debug)]
pub enum ValidationResponse {
    Approve {
        reactor_update_channel: UnboundedReceiver<ReactorWorkflowUpdate>,
    },

    Reject,
}

/// Messages the rtmp server endpoint will send to publisher registrants.
#[derive(Debug)]
pub enum RtmpEndpointPublisherMessage {
    /// Notification that the publisher registration failed.  No further messages will be sent
    /// if this is sent.
    PublisherRegistrationFailed,

    /// Notification that the publisher registration succeeded.
    PublisherRegistrationSuccessful,

    /// Notification that a new RTMP connection has been made and they have requested to be a
    /// publisher on a stream key, but they require validation before being approved.
    PublisherRequiringApproval {
        /// Unique identifier for the TCP connection that's requesting to be a publisher
        connection_id: ConnectionId,

        /// The stream key that the connection is requesting to be a publisher to
        stream_key: String,

        /// Channel to send the approval or rejection response to
        response_channel: Sender<ValidationResponse>,
    },

    /// Notification that a new RTMP connection has been made and is publishing media
    NewPublisherConnected {
        /// Unique identifier for the TCP connection that's publishing
        connection_id: ConnectionId,

        /// Unique identifier for the stream.
        stream_id: StreamId,

        /// Actual stream key that this stream is coming in from.  Mostly used if the registrant
        /// specified that Any stream key would be allowed.
        stream_key: String,

        /// If provided, this is a channel which will receive workflow updates from a reactor
        /// tied to this publisher
        reactor_update_channel: Option<UnboundedReceiver<ReactorWorkflowUpdate>>,
    },

    /// Notification that a publisher has stopped publishing.  It may still be connected to the
    /// server, but it is no longer in a publisher state.
    PublishingStopped {
        /// Unique identifier for the TCP connection that stopped publishing
        connection_id: ConnectionId,
    },

    /// An RTMP publisher has sent in new stream metadata information
    StreamMetadataChanged {
        publisher: ConnectionId,
        metadata: StreamMetadata,
    },

    /// An RTMP publisher has sent in new video data
    NewVideoData {
        publisher: ConnectionId,
        codec: VideoCodec,
        is_keyframe: bool,
        is_sequence_header: bool,
        data: Bytes,
        timestamp: RtmpTimestamp,
        composition_time_offset: i32,
    },

    /// An RTMP publisher has sent in new audio data
    NewAudioData {
        publisher: ConnectionId,
        codec: AudioCodec,
        is_sequence_header: bool,
        data: Bytes,
        timestamp: RtmpTimestamp,
    },
}

/// Messages the rtmp server endpoint will send to watcher registrants
#[derive(Debug)]
pub enum RtmpEndpointWatcherNotification {
    /// The request to register for watchers has failed.  No further messages will be sent
    /// afterwards.
    WatcherRegistrationFailed,

    /// The request to register for watchers was successful
    WatcherRegistrationSuccessful,

    /// Notification that a new RTMP connection has been made and they have requested to be a
    /// watcher on a stream key, but they require validation before being approved.
    WatcherRequiringApproval {
        /// Unique identifier for the TCP connection that's requesting to be a watcher
        connection_id: ConnectionId,

        /// The stream key that the connection is requesting to be a watcher of
        stream_key: String,

        /// Channel to send the approval or rejection response to
        response_channel: Sender<ValidationResponse>,
    },

    /// Notifies the registrant that at least one watcher is now watching on a particular
    /// stream key,
    StreamKeyBecameActive {
        stream_key: String,
        reactor_update_channel: Option<UnboundedReceiver<ReactorWorkflowUpdate>>,
    },

    /// Notifies the registrant that the last watcher has disconnected on the stream key, and
    /// there are no longer anyone watching
    StreamKeyBecameInactive { stream_key: String },
}

/// Message watcher registrants send to announce new media data that should be sent to watchers
#[derive(Debug)]
pub struct RtmpEndpointMediaMessage {
    pub stream_key: String,
    pub data: RtmpEndpointMediaData,
}

/// New media data that should be sent to watchers
#[derive(Debug, Clone, PartialEq)]
pub enum RtmpEndpointMediaData {
    NewStreamMetaData {
        metadata: StreamMetadata,
    },

    NewVideoData {
        codec: VideoCodec,
        is_keyframe: bool,
        is_sequence_header: bool,
        data: Bytes,
        timestamp: RtmpTimestamp,
        composition_time_offset: i32,
    },

    NewAudioData {
        codec: AudioCodec,
        is_sequence_header: bool,
        data: Bytes,
        timestamp: RtmpTimestamp,
    },
}
