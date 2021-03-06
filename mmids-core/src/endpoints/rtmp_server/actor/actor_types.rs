use super::connection_handler::{ConnectionRequest, ConnectionResponse};
use super::{RtmpEndpointPublisherMessage, RtmpEndpointRequest, StreamKeyRegistration};
use crate::codecs::{AudioCodec, VideoCodec};
use crate::endpoints::rtmp_server::{
    IpRestriction, RtmpEndpointMediaData, RtmpEndpointMediaMessage,
    RtmpEndpointWatcherNotification, ValidationResponse,
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

    PortGone {
        port: u16,
    },

    NoMoreEndpointRequesters,
    SocketManagerClosed,
    ValidationApprovalResponseReceived(u16, ConnectionId, ValidationResponse),
}

pub struct PublishingRegistrant {
    pub response_channel: UnboundedSender<RtmpEndpointPublisherMessage>,
    pub stream_id: Option<StreamId>,
    pub ip_restrictions: IpRestriction,
    pub requires_registrant_approval: bool,
    pub cancellation_notifier: UnboundedReceiver<()>,
}

pub struct WatcherRegistrant {
    pub response_channel: UnboundedSender<RtmpEndpointWatcherNotification>,
    pub ip_restrictions: IpRestriction,
    pub requires_registrant_approval: bool,
    pub cancellation_notifier: UnboundedReceiver<()>,
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

pub struct RtmpServerEndpointActor {
    pub futures: FuturesUnordered<BoxFuture<'static, FutureResult>>,
    pub ports: HashMap<u16, PortMapping>,
}

pub enum ListenerRequest {
    Publisher {
        channel: UnboundedSender<RtmpEndpointPublisherMessage>,
        stream_id: Option<StreamId>,
        requires_registrant_approval: bool,
    },

    Watcher {
        notification_channel: UnboundedSender<RtmpEndpointWatcherNotification>,
        media_channel: UnboundedReceiver<RtmpEndpointMediaMessage>,
        requires_registrant_approval: bool,
    },
}

pub enum ConnectionState {
    None,

    WaitingForPublishValidation {
        rtmp_app: String,
        stream_key: String,
    },

    WaitingForWatchValidation {
        rtmp_app: String,
        stream_key: String,
    },

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
    pub received_registrant_approval: bool,
}

pub struct PortMapping {
    pub rtmp_applications: HashMap<String, RtmpAppMapping>,
    pub status: PortStatus,
    pub connections: HashMap<ConnectionId, Connection>,
    pub tls: bool,
}
