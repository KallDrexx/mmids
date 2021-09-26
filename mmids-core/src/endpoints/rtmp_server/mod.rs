mod actor;

use crate::codecs::{AudioCodec, VideoCodec};
use crate::net::tcp::TcpSocketRequest;
use crate::net::ConnectionId;
use crate::StreamId;
use actor::actor_types::RtmpServerEndpointActor;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use rml_rtmp::sessions::StreamMetadata;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

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
#[derive(Clone, Hash, Eq, PartialEq)]
pub enum StreamKeyRegistration {
    /// All stream keys for the the rtmp application should be registered
    Any,

    /// Only set up registration for the exact stream key
    Exact(String),
}

/// Operations the rtmp server endpoint is being requested to make
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
    },
}

/// Messages the rtmp server endpoint will send to publisher registrants.
#[derive(Debug)]
pub enum RtmpEndpointPublisherMessage {
    /// Notification that the publisher registration failed.  No further messages will be sent
    /// if this is sent.
    PublisherRegistrationFailed,

    /// Notification that the publisher registration succeeded.
    PublisherRegistrationSuccessful,

    /// Notification that a new RTMP connection has been made and is publishing media
    NewPublisherConnected {
        /// Unique identifier for the TCP connection that's publishing
        connection_id: ConnectionId,

        /// Unique identifier for the stream.
        stream_id: StreamId,

        /// Actual stream key that this stream is coming in from.  Mostly used if the registrant
        /// specified that Any stream key would be allowed.
        stream_key: String,
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
    ReceiverRegistrationFailed,

    /// The request to register for watchers was successful
    ReceiverRegistrationSuccessful,

    /// Notifies the registrant that at least one watcher is now watching on a particular
    /// stream key,
    StreamKeyBecameActive { stream_key: String },

    /// Notifies the registrant that the last watcher has disconnected on the stream key, and
    /// there are no longer anyone watching
    StreamKeyBecameInactive { stream_key: String },
}

/// Message watcher registrants send to announce new media data that should be sent to watchers
pub struct RtmpEndpointMediaMessage {
    pub stream_key: String,
    pub data: RtmpEndpointMediaData,
}

/// New media data that should be sent to watchers
#[derive(Debug, Clone)]
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
    },

    NewAudioData {
        codec: AudioCodec,
        is_sequence_header: bool,
        data: Bytes,
        timestamp: RtmpTimestamp,
    },
}
