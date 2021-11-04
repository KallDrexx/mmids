use super::connection_handler::{ConnectionRequest, ConnectionResponse};
use super::{RtmpEndpointPublisherMessage, RtmpEndpointRequest, StreamKeyRegistration};
use crate::codecs::{AudioCodec, VideoCodec};
use crate::endpoints::rtmp_server::{
    IpRestriction, RtmpEndpointMediaData, RtmpEndpointMediaMessage, RtmpEndpointWatcherNotification,
};

use crate::net::tcp::TcpSocketResponse;
use crate::net::ConnectionId;
use crate::StreamId;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub enum FutureResult {
    EndpointRequestReceived {
        request: RtmpEndpointRequest,
        receiver: UnboundedReceiver<RtmpEndpointRequest>,
    },

    SocketResponseReceived {
        port: u16,
        response: TcpSocketResponse,
        receiver: UnboundedReceiver<TcpSocketResponse>,
    },

    PublishingRegistrantGone {
        port: u16,
        app: String,
        stream_key: StreamKeyRegistration,
        id: PublisherRegistrantId,
    },

    ConnectionHandlerRequestReceived {
        port: u16,
        connection_id: ConnectionId,
        request: ConnectionRequest,
        receiver: UnboundedReceiver<ConnectionRequest>,
    },

    ConnectionHandlerGone {
        port: u16,
        connection_id: ConnectionId,
    },

    WatcherRegistrantGone {
        port: u16,
        app: String,
        stream_key: StreamKeyRegistration,
    },

    WatcherMediaDataReceived {
        data: RtmpEndpointMediaData,
        port: u16,
        app: String,
        stream_key: String,
        stream_key_registration: StreamKeyRegistration,
        receiver: UnboundedReceiver<RtmpEndpointMediaMessage>,
    },

    NoMoreEndpointRequesters,
    SocketManagerClosed,
}

/// Value which can identify a specific publisher registrant.  This prevents a race condition
/// where 2 futures cause a registrant to be removed but in between a registrant gets added for
/// the same app/stream key combination.  With the identifier we can make sure we don't accidentally
/// close the newly added publisher registrant.
#[derive(Clone, Debug, PartialEq)]
pub struct PublisherRegistrantId(pub String);

pub struct PublishingRegistrant {
    pub id: PublisherRegistrantId,
    pub response_channel: UnboundedSender<RtmpEndpointPublisherMessage>,
    pub stream_id: Option<StreamId>,
    pub ip_restrictions: IpRestriction,
}

pub struct WatcherRegistrant {
    pub response_channel: UnboundedSender<RtmpEndpointWatcherNotification>,
    pub ip_restrictions: IpRestriction,
}

pub struct VideoSequenceHeader {
    pub codec: VideoCodec,
    pub data: Bytes,
}

pub struct AudioSequenceHeader {
    pub codec: AudioCodec,
    pub data: Bytes,
}

pub struct WatcherDetails {
    pub media_sender: UnboundedSender<RtmpEndpointMediaData>,
}

pub struct StreamKeyConnections {
    pub publisher: Option<ConnectionId>,
    pub watchers: HashMap<ConnectionId, WatcherDetails>,
    pub latest_video_sequence_header: Option<VideoSequenceHeader>,
    pub latest_audio_sequence_header: Option<AudioSequenceHeader>,
}

pub struct RtmpAppMapping {
    pub publisher_registrants: HashMap<StreamKeyRegistration, PublishingRegistrant>,
    pub watcher_registrants: HashMap<StreamKeyRegistration, WatcherRegistrant>,
    pub active_stream_keys: HashMap<String, StreamKeyConnections>,
}

#[derive(PartialEq)]
pub enum PortStatus {
    Requested,
    Open,
}

pub struct RtmpServerEndpointActor<'a> {
    pub futures: FuturesUnordered<BoxFuture<'a, FutureResult>>,
    pub ports: HashMap<u16, PortMapping>,
}

pub enum ListenerRequest {
    Publisher {
        channel: UnboundedSender<RtmpEndpointPublisherMessage>,
        stream_id: Option<StreamId>,
    },

    Watcher {
        notification_channel: UnboundedSender<RtmpEndpointWatcherNotification>,
        media_channel: UnboundedReceiver<RtmpEndpointMediaMessage>,
    },
}

pub enum ConnectionState {
    None,

    Publishing {
        rtmp_app: String,
        stream_key: String,
    },

    Watching {
        rtmp_app: String,
        stream_key: String,
    },
}

pub struct Connection {
    pub response_channel: UnboundedSender<ConnectionResponse>,
    pub state: ConnectionState,
    pub socket_address: SocketAddr,
}

pub struct PortMapping {
    pub rtmp_applications: HashMap<String, RtmpAppMapping>,
    pub status: PortStatus,
    pub connections: HashMap<ConnectionId, Connection>,
}
