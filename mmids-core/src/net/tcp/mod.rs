//! A TCP socket manager actor that allows other systems to request TCP connections.  The socket
//! manager will manage listeners for different ports, accept connections, unwrap SSL sessions (if
//! requested), and pass networked data to requesters.
mod listener;
mod socket_manager;

use super::ConnectionId;
use bytes::Bytes;
use std::net::SocketAddr;
use tokio::sync::mpsc;

pub use listener::OutboundPacket;
pub use socket_manager::start as start_socket_manager;

/// Reasons why the request to listen for TCP connections can fail
#[derive(Debug)]
pub enum RequestFailureReason {
    /// The port being requested has already been opened for another requester
    PortInUse,

    /// A TLS port was requested to be opened, but the certificate could not be opened
    InvalidCertificate(String),

    /// A TLS port was requested to be opened, but the password provided did not unlock the
    /// provided certificate.
    CertPasswordIncorrect,

    /// A TLS port was requested to be opened, but the TCP socket manager was not given any
    /// details required to accept TLS sessions.
    NoTlsDetailsGiven,
}

/// Options required for TLS session handling
pub struct TlsOptions {
    pub pfx_file_location: String,
    pub cert_password: String,
}

/// Requests by callers to the TCP socket manager
pub enum TcpSocketRequest {
    /// Request for the server to start listening on a specific TCP port
    OpenPort {
        /// TCP port to be opened
        port: u16,

        /// If the port should be accepting TLS connections or not
        use_tls: bool,

        /// The channel in which responses should be sent.  If the port is successfully opened
        /// then all state changes for the port (such as new connections) will use this channel
        /// for notifications
        response_channel: mpsc::UnboundedSender<TcpSocketResponse>,
    },
}

#[derive(Debug)]
/// Response messages that the TCP socket manager may send back
pub enum TcpSocketResponse {
    /// Notification that the specified request that was previously made was accepted
    RequestAccepted {},

    /// Notification that the specified request that was previously made was denied
    RequestDenied {
        /// Reason why the request was denied
        reason: RequestFailureReason,
    },

    /// Notification to system that requested a port be opened that the port has been
    /// forced closed.  This is mostly due to an error listening onto the socket.
    PortForciblyClosed { port: u16 },

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

        /// The socket address the client connected from
        socket_address: SocketAddr,
    },

    /// Notification that a client has disconnected from a TCP port
    Disconnection {
        /// Unique identifier of the connection that disconnected
        connection_id: ConnectionId,
    },
}
