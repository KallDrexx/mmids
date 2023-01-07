use super::connection_handler::{ConnectionRequest, ConnectionResponse};
use super::{RtmpEndpointPublisherMessage, RtmpEndpointRequest, StreamKeyRegistration};
use crate::rtmp_server::{
    IpRestriction, RtmpEndpointMediaData, RtmpEndpointMediaMessage,
    RtmpEndpointWatcherNotification, ValidationResponse,
};
use bytes::Bytes;
use mmids_core::net::tcp::TcpSocketResponse;
use mmids_core::net::ConnectionId;
use mmids_core::StreamId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub enum FutureResult {
    EndpointRequestReceived(RtmpEndpointRequest),

    SocketResponseReceived {
        port: u16,
        response: TcpSocketResponse,
    },

    PublishingRegistrantGone {
        port: u16,
        app: Arc<String>,
        stream_key: StreamKeyRegistration,
    },

    ConnectionHandlerRequestReceived {
        port: u16,
        connection_id: ConnectionId,
        request: ConnectionRequest,
    },

    ConnectionHandlerGone {
        port: u16,
        connection_id: ConnectionId,
    },

    WatcherRegistrantGone {
        port: u16,
        app: Arc<String>,
        stream_key: StreamKeyRegistration,
    },

    WatcherMediaDataReceived {
        data: RtmpEndpointMediaData,
        port: u16,
        app: Arc<String>,
        stream_key: Arc<String>,
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
    pub data: Bytes,
}

pub struct AudioSequenceHeader {
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
    pub active_stream_keys: HashMap<Arc<String>, StreamKeyConnections>,
}

#[derive(PartialEq, Eq)]
pub enum PortStatus {
    Requested,
    Open,
}

pub struct RtmpServerEndpointActor {
    pub internal_actor: UnboundedSender<FutureResult>,
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
        rtmp_app: Arc<String>,
        stream_key: Arc<String>,
    },

    WaitingForWatchValidation {
        rtmp_app: Arc<String>,
        stream_key: Arc<String>,
    },

    Publishing {
        rtmp_app: Arc<String>,
        stream_key: Arc<String>,
    },

    Watching {
        rtmp_app: Arc<String>,
        stream_key: Arc<String>,
    },
}

pub struct Connection {
    pub response_channel: UnboundedSender<ConnectionResponse>,
    pub state: ConnectionState,
    pub socket_address: SocketAddr,
    pub received_registrant_approval: bool,
}

pub struct PortMapping {
    pub rtmp_applications: HashMap<Arc<String>, RtmpAppMapping>,
    pub status: PortStatus,
    pub connections: HashMap<ConnectionId, Connection>,
    pub tls: bool,
}
