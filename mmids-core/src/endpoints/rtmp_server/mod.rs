mod actor;

use crate::net::tcp::TcpSocketRequest;
use crate::net::ConnectionId;
use crate::StreamId;
use actor::actor_types::RtmpServerEndpointActor;
use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use rml_rtmp::sessions::StreamMetadata;
use bytes::Bytes;
use rml_rtmp::time::RtmpTimestamp;

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
}

/// Messages the rtmp server endpoint will send to registrants.
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
        data: Bytes,
        timestamp: RtmpTimestamp,
    },

    /// An RTMP publisher has sent in new audio data
    NewAudioData {
        publisher: ConnectionId,
        data: Bytes,
        timestamp: RtmpTimestamp,
    }
}
