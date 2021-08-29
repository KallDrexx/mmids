use super::{
    RtmpEndpointPublisherMessage, RtmpEndpointRequest, StreamKeyRegistration,
};

use super::connection_handler::{ConnectionRequest, ConnectionResponse};

use crate::net::tcp::TcpSocketResponse;
use crate::net::ConnectionId;

use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use crate::StreamId;

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
}

pub struct StreamKeyConnections {
    pub publisher: Option<ConnectionId>,
}

pub struct RtmpAppMapping {
    pub publisher_registrants: HashMap<StreamKeyRegistration, PublishingRegistrant>,
    pub active_stream_keys: HashMap<String, StreamKeyConnections>,
}

#[derive(PartialEq)]
pub enum PortStatus {
    Requested,
    Open,
}

pub struct PortMapping {
    pub rtmp_applications: HashMap<String, RtmpAppMapping>,
    pub status: PortStatus,
    pub connections: HashMap<ConnectionId, Connection>,
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
}

pub struct Connection {
    pub response_channel: UnboundedSender<ConnectionResponse>,
    pub state: ConnectionState,
}

pub enum ConnectionState {
    None,
    Publishing {
        rtmp_app: String,
        stream_key: String,
    },
}
