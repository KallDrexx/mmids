mod listener;
mod socket_manager;

use super::ConnectionId;
use bytes::Bytes;
use tokio::sync::mpsc;

pub use listener::OutboundPacket;
pub use socket_manager::start as start_socket_manager;

#[derive(Debug)]
pub enum RequestFailureReason {
    PortInUse,
}

/// Requests by callers to the TCP socket manager
pub enum TcpSocketRequest {
    /// Request for the server to start listening on a specific TCP port
    OpenPort {
        /// Unique identification token that is passed back with socket responses, so the
        /// caller knows which of their requests was accepted or denied.
        request_id: u32,

        /// TCP port to be opened
        port: u16,

        /// The channel in which responses should be sent.  If the port is successfully opened
        /// then all state changes for the port (such as new connections) will use this channel
        /// for notifications
        response_channel: mpsc::UnboundedSender<TcpSocketResponse>,

        /// Channel so the socket manager knows of the owner of this port request ends up closing
        /// down.
        disconnection_signal: mpsc::UnboundedSender<()>,
    },
}

#[derive(Debug)]
/// Response messages that the TCP socket manager may send back
pub enum TcpSocketResponse {
    /// Notification that the specified request that was previously made was accepted
    RequestAccepted {
        /// Identifier the original requester gave the request that was accepted
        request_id: u32,
    },

    /// Notification that the specified request that was previously made was denied
    RequestDenied {
        /// Identifier that hte original requester gave the request that was denied
        request_id: u32,
        /// Reason why the request was denied
        reason: RequestFailureReason,
    },

    /// Notification to system that requested a port be opened that the port has been
    /// forced closed.  This is mostly due to an error listening onto the socket.
    PortForciblyClosed {
        port: u16,
    },

    /// Notification that a client has connected to a TCP port opened by the receiver of this
    /// notification.
    NewConnection {
        /// The port the TCP connection came in on
        port: u16,

        /// Unique identifier for this new connection
        connection_id: ConnectionId,

        /// Channel the owner can use to receive bytes sent from the client
        incoming_bytes: mpsc::UnboundedReceiver<Bytes>,

        /// Channel the owner can use to send bytes to the client
        outgoing_bytes: mpsc::UnboundedSender<OutboundPacket>,
    },

    Disconnection {
        connection_id: ConnectionId,
    },
}
