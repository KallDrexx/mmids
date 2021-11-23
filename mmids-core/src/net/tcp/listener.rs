use super::TcpSocketResponse;
use crate::net::tcp::TlsOptions;
use crate::net::ConnectionId;
use bytes::{Bytes, BytesMut};
use futures::future::FutureExt;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_native_tls::TlsAcceptor;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

/// Set of bytes that should be sent over a TCP socket
pub struct OutboundPacket {
    /// The bytes to send over the network
    pub bytes: Bytes,

    /// If the connection to the client is backlogged, then any packet that's marked as
    /// droppable will be dropped
    pub can_be_dropped: bool,
}

/// Parameters to start listening on a TCP port
pub struct ListenerParams {
    /// The port to listen on
    pub port: u16,

    /// Should this port accept TLS connections
    pub use_tls: bool,

    /// Options for TLS. Required if use_tls is true
    pub tls_options: Arc<Option<TlsOptions>>,

    /// The channel in which to send notifications of port activity to
    pub response_channel: UnboundedSender<TcpSocketResponse>,
}

enum ReadSocket {
    Bare(ReadHalf<TcpStream>),
    Tls(ReadHalf<tokio_native_tls::TlsStream<TcpStream>>),
}

enum WriteSocket {
    Bare(WriteHalf<TcpStream>),
    Tls(WriteHalf<tokio_native_tls::TlsStream<TcpStream>>),
}

/// Starts listening for TCP connections on the specified port.  It returns a channel which
/// callers can use to know if the listener has shut down unexpectedly.
pub fn start(params: ListenerParams) -> UnboundedSender<()> {
    let (self_disconnect_sender, self_disconnect_receiver) = unbounded_channel();
    tokio::spawn(listen(params, self_disconnect_receiver));

    self_disconnect_sender
}

#[instrument(skip(params, _self_disconnection_signal), fields(port = params.port, use_tls = params.use_tls))]
async fn listen(params: ListenerParams, _self_disconnection_signal: UnboundedReceiver<()>) {
    info!("Socket listener for port started");

    let ListenerParams {
        port,
        response_channel,
        use_tls,
        tls_options,
    } = params;

    let tls = if let Some(tls) = tls_options.as_ref() {
        let identity = tls.certificate.clone();
        let tls_acceptor = match native_tls::TlsAcceptor::builder(identity).build() {
            Ok(x) => x,
            Err(e) => {
                error!("Failed to build tls acceptor: {:?}", e);
                return;
            }
        };

        Some(tokio_native_tls::TlsAcceptor::from(tls_acceptor))
    } else {
        None
    };

    let tls = if use_tls { tls } else { None };
    let tls = Arc::new(tls);

    let bind_address = "0.0.0.0:".to_string() + &port.to_string();
    let listener = match TcpListener::bind(bind_address.clone()).await {
        Ok(x) => x,
        Err(e) => {
            error!("Error occurred binding socket to {}: {:?}", bind_address, e);
            return;
        }
    };

    loop {
        let disconnect = response_channel.clone();
        tokio::select! {
            result = listener.accept() => {
                let (socket, client_info) = match result {
                    Ok(x) => x,
                    Err(e) => {
                        error!("Error accepting connection for listener on port {}: {:?}", port, e);
                        return;
                    }
                };

                let connection_id = ConnectionId(Uuid::new_v4().to_string());
                tokio::spawn(handle_new_connection(socket, client_info, response_channel.clone(), port, connection_id, tls.clone()));
            },

            _ = disconnect.closed() => {
                break;
            }
        }
    }
    info!("Socket listener for port {} closing", port);
}

#[instrument(skip(tls_acceptor, response_channel, socket, client_info))]
async fn handle_new_connection(
    socket: TcpStream,
    client_info: SocketAddr,
    response_channel: UnboundedSender<TcpSocketResponse>,
    port: u16,
    connection_id: ConnectionId,
    tls_acceptor: Arc<Option<TlsAcceptor>>,
) {
    info!(
        ip = %client_info.ip(),
        "Tcp Listener: new connection from {}, given id {}",
        client_info.ip(),
        connection_id
    );

    let (incoming_sender, incoming_receiver) = unbounded_channel();
    let (outgoing_sender, outgoing_receiver) = unbounded_channel();

    let message = TcpSocketResponse::NewConnection {
        port,
        connection_id: connection_id.clone(),
        incoming_bytes: incoming_receiver,
        outgoing_bytes: outgoing_sender,
        socket_address: client_info,
    };

    if let Err(_) = response_channel.send(message) {
        info!("Port owner disconnected before connection was handled");

        return;
    }

    let (reader, writer) = match split_socket(socket, tls_acceptor).await {
        Ok(x) => x,
        Err(e) => {
            error!("Error splitting socket: {:?}", e);
            return;
        }
    };

    tokio::spawn(socket_reader(
        connection_id.clone(),
        reader,
        incoming_sender,
        response_channel,
    ));

    tokio::spawn(socket_writer(connection_id, writer, outgoing_receiver));
}

#[instrument(skip(reader, incoming_sender, tcp_response_sender))]
async fn socket_reader(
    connection_id: ConnectionId,
    mut reader: ReadSocket,
    incoming_sender: UnboundedSender<Bytes>,
    tcp_response_sender: UnboundedSender<TcpSocketResponse>,
) {
    let mut buffer = BytesMut::with_capacity(4096);
    loop {
        tokio::select! {
            bytes_read = read_buf(&mut reader, &mut buffer) => {
                let bytes_read = match bytes_read {
                    Ok(x) => x,
                    Err(e) => {
                        error!("Error reading from byte buffer: {:?}", e);
                        return;
                    }
                };

                if bytes_read == 0 {
                    break;
                }

                let bytes = buffer.split_off(bytes_read);
                let received_bytes = buffer.freeze();

                if let Err(_) = incoming_sender.send(received_bytes) {
                    break;
                }

                buffer = bytes;
            },

            () = incoming_sender.closed() => {
                break;
            },
        }
    }

    info!("reader task closed");
    let _ = tcp_response_sender.send(TcpSocketResponse::Disconnection { connection_id });
}

#[instrument(skip(writer, outgoing_receiver))]
async fn socket_writer(
    connection_id: ConnectionId,
    mut writer: WriteSocket,
    mut outgoing_receiver: UnboundedReceiver<OutboundPacket>,
) {
    const INITIAL_BACKLOG_THRESHOLD: usize = 100;
    const LETHAL_BACKLOG_THRESHOLD: usize = 1000;

    let mut send_queue = VecDeque::new();

    loop {
        let packet = outgoing_receiver.recv().await;
        if packet.is_none() {
            break;
        }

        let packet = packet.unwrap();

        // Since this is a TCP connection, we can only send so many packets before we have to wait
        // for acknowledgements.  If we don't have enough bandwidth to the client for the current
        // batch of packets it's possible we get backlogged.  The end result of this is the
        // outgoing packet channel constantly filling with new packets.  Left uncontrolled we'll
        // run out of memory.
        //
        // We can't actually see how many items are in a MPSC, we need continually need to read
        // items from the channel and place them in a queue, then when the channel is empty see
        // how many packets we have in the queue.  If we are above the lethal amount assume we
        // will never catch up and kill the writer.  If we are only above an initial threshold then
        // only send packets not marked as droppable.
        send_queue.push_back(packet);
        while let Some(Some(packet)) = outgoing_receiver.recv().now_or_never() {
            send_queue.push_back(packet);
        }

        if send_queue.len() >= LETHAL_BACKLOG_THRESHOLD {
            warn!(
                "{} outbound packets in the queue.  Killing writer",
                send_queue.len()
            );
            break;
        }

        let queue_length = send_queue.len();
        let drop_optional_packets = send_queue.len() >= INITIAL_BACKLOG_THRESHOLD;
        let mut dropped_packet_count = 0;
        for packet in send_queue.drain(..) {
            if !packet.can_be_dropped || !drop_optional_packets {
                if let Err(e) = write_packet(&mut writer, packet).await {
                    error!("Error when writing packet bytes: {:?}", e);
                    return;
                }
            } else {
                dropped_packet_count += 1;
            }
        }

        if drop_optional_packets {
            warn!(
                "send queue was backlogged with {} packets ({} dropped)",
                queue_length, dropped_packet_count
            );
        }
    }

    info!("writer task closed");
}

async fn split_socket(
    socket: TcpStream,
    tls_acceptor: Arc<Option<TlsAcceptor>>,
) -> Result<(ReadSocket, WriteSocket), Box<dyn std::error::Error + Sync + Send>> {
    match tls_acceptor.as_ref() {
        None => {
            let (reader, writer) = tokio::io::split(socket);
            Ok((ReadSocket::Bare(reader), WriteSocket::Bare(writer)))
        }

        Some(tls) => {
            let tls_stream = tls.accept(socket).await?;
            let (reader, writer) = tokio::io::split(tls_stream);
            Ok((ReadSocket::Tls(reader), WriteSocket::Tls(writer)))
        }
    }
}

async fn read_buf(reader: &mut ReadSocket, buffer: &mut BytesMut) -> std::io::Result<usize> {
    match reader {
        ReadSocket::Bare(socket) => socket.read_buf(buffer).await,
        ReadSocket::Tls(socket) => socket.read_buf(buffer).await,
    }
}

async fn write_packet(writer: &mut WriteSocket, packet: OutboundPacket) -> std::io::Result<()> {
    match writer {
        WriteSocket::Bare(socket) => socket.write_all(packet.bytes.as_ref()).await,
        WriteSocket::Tls(socket) => socket.write_all(packet.bytes.as_ref()).await,
    }
}
