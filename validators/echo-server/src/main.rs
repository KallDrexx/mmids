use bytes::{Bytes, BytesMut};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use log::{debug, error, info, warn};
use mmids_core::net::tcp::{
    start_socket_manager, OutboundPacket, TcpSocketRequest, TcpSocketResponse,
};
use mmids_core::net::ConnectionId;
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

struct Connection {
    sender: UnboundedSender<OutboundPacket>,
}

enum FutureResult {
    TcpSocketResponse(TcpSocketResponse, UnboundedReceiver<TcpSocketResponse>),
    BytesReceived(ConnectionId, Bytes, UnboundedReceiver<Bytes>),
    ReceiverClosed,
    ConnectionClosed(ConnectionId),
}

#[tokio::main]
pub async fn main() {
    env_logger::init();

    info!("Test");

    let socket_manager_sender = start_socket_manager();
    let (response_sender, mut response_receiver) = unbounded_channel();
    let message = TcpSocketRequest::OpenPort {
        port: 8888,
        response_channel: response_sender,
    };

    debug!("Opening port 8888");

    match socket_manager_sender.send(message) {
        Ok(_) => (),
        Err(e) => panic!("Failed to send open port request: {}", e),
    };

    debug!("Waiting for response");

    let response = match response_receiver.recv().await {
        Some(response) => response,
        None => panic!("No senders for tcp socket responses"),
    };

    match response {
        TcpSocketResponse::RequestAccepted {} => (),
        x => panic!("Unexpected response: {:?}", x),
    };

    let mut connections = HashMap::new();
    let mut futures = FuturesUnordered::new();
    futures.push(wait_for_responses(response_receiver).boxed());

    let mut pending_received = BytesMut::new();

    while let Some(result) = futures.next().await {
        match result {
            FutureResult::ReceiverClosed => {
                error!("Receiver has no more senders");
                break;
            }

            FutureResult::TcpSocketResponse(response, receiver) => {
                futures.push(wait_for_responses(receiver).boxed());

                match response {
                    TcpSocketResponse::NewConnection {
                        port: _,
                        connection_id,
                        outgoing_bytes,
                        incoming_bytes,
                    } => {
                        info!("New connection {:?}", connection_id);

                        let packet = OutboundPacket {
                            bytes: Bytes::from("Welcome!\n".to_string()),
                            can_be_dropped: false,
                        };
                        let _ = outgoing_bytes.send(packet);
                        let connection = Connection {
                            sender: outgoing_bytes,
                        };

                        connections.insert(connection_id.clone(), connection);
                        futures.push(wait_for_bytes(connection_id, incoming_bytes).boxed())
                    }

                    TcpSocketResponse::Disconnection { connection_id } => {
                        info!("Connection {:?} disconnected", connection_id);
                        connections.remove(&connection_id);
                    }

                    TcpSocketResponse::PortForciblyClosed { port: _ } => {
                        error!("Port was forcibly closed");
                        break;
                    }

                    x => warn!("Unexpected tcp response: {:?}", x),
                }
            }

            FutureResult::ConnectionClosed(connection_id) => {
                info!("Connection {:?} closed", connection_id);
                connections.remove(&connection_id);
            }

            FutureResult::BytesReceived(connection_id, bytes, receiver) => {
                futures.push(wait_for_bytes(connection_id.clone(), receiver).boxed());

                pending_received.extend(bytes);
                let mut index = None;
                for x in 0..pending_received.len() {
                    if pending_received[x] == 10 {
                        index = Some(x);
                        break;
                    }
                }

                if let Some(index) = index {
                    let received = pending_received.split_to(index + 1);
                    if let Ok(string) = std::str::from_utf8(&received) {
                        info!(
                            "Received data from connection {:?}: {}",
                            connection_id,
                            string.trim()
                        );

                        let sender = match connections.get(&connection_id) {
                            Some(connection) => &connection.sender,
                            None => {
                                error!(
                                    "Received packet for non-cataloged connection {:?}",
                                    connection_id
                                );
                                break;
                            }
                        };

                        let response = format!("You said: {}", string);
                        let packet = OutboundPacket {
                            bytes: Bytes::from(response),
                            can_be_dropped: false,
                        };
                        let _ = sender.send(packet);
                    }
                }
            }
        }
    }

    info!("Closing");
}

async fn wait_for_responses(mut receiver: UnboundedReceiver<TcpSocketResponse>) -> FutureResult {
    match receiver.recv().await {
        None => FutureResult::ReceiverClosed,
        Some(x) => FutureResult::TcpSocketResponse(x, receiver),
    }
}

async fn wait_for_bytes(
    connection_id: ConnectionId,
    mut receiver: UnboundedReceiver<Bytes>,
) -> FutureResult {
    match receiver.recv().await {
        None => FutureResult::ConnectionClosed(connection_id),
        Some(bytes) => FutureResult::BytesReceived(connection_id, bytes, receiver),
    }
}
