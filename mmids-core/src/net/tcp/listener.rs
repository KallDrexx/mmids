use super::TcpSocketResponse;
use crate::net::ConnectionId;
use crate::{send, spawn_and_log};
use bytes::{Bytes, BytesMut};
use futures::future::FutureExt;
use log::{info, warn, debug};
use std::collections::VecDeque;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
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

    /// The channel in which to send notifications of port activity to
    pub response_channel: UnboundedSender<TcpSocketResponse>,
}

/// Starts listening for TCP connections on the specified port.  It returns a channel which
/// callers can use to know if the listener has shut down unexpectedly.
pub fn start(params: ListenerParams) -> UnboundedSender<()> {
    let (self_disconnect_sender, self_disconnect_receiver) = unbounded_channel();
    spawn_and_log(listen(params, self_disconnect_receiver));

    self_disconnect_sender
}

async fn listen(
    params: ListenerParams,
    _self_disconnection_signal: UnboundedReceiver<()>,
) -> Result<(), std::io::Error> {
    debug!("Socket listener for port {} started", params.port);

    let ListenerParams {
        port,
        response_channel,
    } = params;
    let bind_address = "0.0.0.0:".to_string() + &port.to_string();
    let listener = TcpListener::bind(bind_address).await?;

    debug!("Listener for port {} started", port);
    loop {
        let disconnect = response_channel.clone();
        tokio::select! {
            result = listener.accept() => {
                let (socket, client_info) = result?;
                handle_new_connection(socket, client_info, response_channel.clone(), port);
            },

            _ = disconnect.closed() => {
                break;
            }
        }
    }
    debug!("Listener for port {} stopped", port);

    debug!("Socket listener for port {} closing", port);
    Ok(())
}

fn handle_new_connection(
    socket: TcpStream,
    client_info: SocketAddr,
    mut response_channel: UnboundedSender<TcpSocketResponse>,
    port: u16,
) {
    let connection_id = ConnectionId(Uuid::new_v4().to_string());
    info!(
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
    };

    if !send(&mut response_channel, message) {
        info!(
            "Connection {}: Port owner disconnected before connection was handled",
            connection_id
        );
        return;
    }

    let (reader, writer) = tokio::io::split(socket);
    spawn_and_log(socket_reader(
        connection_id.clone(),
        reader,
        incoming_sender,
        response_channel,
    ));

    spawn_and_log(socket_writer(connection_id, writer, outgoing_receiver));
}

async fn socket_reader(
    connection_id: ConnectionId,
    mut reader: ReadHalf<TcpStream>,
    mut incoming_sender: UnboundedSender<Bytes>,
    tcp_response_sender: UnboundedSender<TcpSocketResponse>,
) -> Result<(), std::io::Error> {
    let mut buffer = BytesMut::with_capacity(4096);
    loop {
        tokio::select! {
            bytes_read = reader.read_buf(&mut buffer) => {
                let bytes_read = bytes_read?;
                if bytes_read == 0 {
                    break;
                }

                let bytes = buffer.split_off(bytes_read);
                let received_bytes = buffer.freeze();
                if !send(&mut incoming_sender, received_bytes) {
                    break;
                }

                buffer = bytes;
            },

            () = incoming_sender.closed() => {
                break;
            },
        }
    }

    info!("Connection {}: reader task closed", connection_id);
    let _ = tcp_response_sender.send(TcpSocketResponse::Disconnection {
        connection_id,
    });

    Ok(())
}

async fn socket_writer(
    connection_id: ConnectionId,
    mut writer: WriteHalf<TcpStream>,
    mut outgoing_receiver: UnboundedReceiver<OutboundPacket>,
) -> Result<(), std::io::Error> {
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
                "Connection {}: {} outbound packets in the queue.  Killing writer",
                connection_id,
                send_queue.len()
            );
            break;
        }

        let queue_length = send_queue.len();
        let drop_optional_packets = send_queue.len() >= INITIAL_BACKLOG_THRESHOLD;
        let mut dropped_packet_count = 0;
        for packet in send_queue.drain(..) {
            if !packet.can_be_dropped || !drop_optional_packets {
                writer.write_all(packet.bytes.as_ref()).await?;
            } else {
                dropped_packet_count += 1;
            }
        }

        if drop_optional_packets {
            warn!(
                "Connection {}: send queue was backlogged with {} packets ({} dropped)",
                connection_id, queue_length, dropped_packet_count
            );
        }
    }

    info!("Connection {}: writer task closed", connection_id);

    Ok(())
}
