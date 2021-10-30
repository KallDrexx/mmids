use super::listener::{start as start_listener, ListenerParams};
use super::{TcpSocketRequest, TcpSocketResponse};
use crate::net::tcp::RequestFailureReason;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use log::{debug, info};
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

/// Starts a new instance of a socket manager task.  A socket manager can be requested to open
/// ports on behalf of another system.  If the port is successfully opened it will begin listening
/// for TCP connections on that port, and then manage the reading and writing of network traffic
/// for that connection.
pub fn start() -> UnboundedSender<TcpSocketRequest> {
    let (request_sender, request_receiver) = unbounded_channel();

    let manager = SocketManager::new();
    tokio::spawn(manager.run(request_receiver));

    request_sender
}

enum SocketManagerFutureResult {
    IncomingRequest {
        request: Option<TcpSocketRequest>,
        receiver: UnboundedReceiver<TcpSocketRequest>,
    },
    ListenerShutdown {
        port: u16,
    },
}

struct OpenPort {
    response_channel: UnboundedSender<TcpSocketResponse>,
}

struct SocketManager<'a> {
    open_ports: HashMap<u16, OpenPort>,
    futures: FuturesUnordered<BoxFuture<'a, SocketManagerFutureResult>>,
}

impl<'a> SocketManager<'a> {
    fn new() -> Self {
        SocketManager {
            open_ports: HashMap::new(),
            futures: FuturesUnordered::new(),
        }
    }

    async fn run(mut self, request_receiver: UnboundedReceiver<TcpSocketRequest>) {
        info!("Starting TCP socket manager");

        self.futures
            .push(request_receiver_future(request_receiver).boxed());

        while let Some(future_result) = self.futures.next().await {
            match future_result {
                SocketManagerFutureResult::IncomingRequest { request, receiver } => {
                    self.futures.push(request_receiver_future(receiver).boxed());

                    match request {
                        Some(request) => self.handle_request(request),
                        None => break, // no more senders of requests
                    }
                }

                SocketManagerFutureResult::ListenerShutdown { port } => {
                    match self.open_ports.remove(&port) {
                        None => (),
                        Some(details) => {
                            let _ = details
                                .response_channel
                                .send(TcpSocketResponse::PortForciblyClosed { port });
                        }
                    }
                }
            }
        }

        debug!("Socket manager closing");
    }

    fn handle_request(&mut self, request: TcpSocketRequest) {
        match request {
            TcpSocketRequest::OpenPort {
                port,
                response_channel,
            } => {
                if self.open_ports.contains_key(&port) {
                    debug!("Port {} is already in use!", port);
                    let message = TcpSocketResponse::RequestDenied {
                        reason: RequestFailureReason::PortInUse,
                    };

                    let _ = response_channel.send(message);
                } else {
                    debug!("TCP port {} being opened", port);
                    let details = OpenPort {
                        response_channel: response_channel.clone(),
                    };

                    self.open_ports.insert(port, details);

                    let listener_shutdown = start_listener(ListenerParams {
                        port,
                        response_channel: response_channel.clone(),
                    });

                    self.futures
                        .push(listener_shutdown_future(port, listener_shutdown).boxed());

                    let _ = response_channel.send(TcpSocketResponse::RequestAccepted {});
                }
            }
        }
    }
}

async fn request_receiver_future(
    mut receiver: UnboundedReceiver<TcpSocketRequest>,
) -> SocketManagerFutureResult {
    let result = receiver.recv().await;

    SocketManagerFutureResult::IncomingRequest {
        request: result,
        receiver,
    }
}

async fn listener_shutdown_future(
    port: u16,
    signal: UnboundedSender<()>,
) -> SocketManagerFutureResult {
    signal.closed().await;

    SocketManagerFutureResult::ListenerShutdown { port }
}
