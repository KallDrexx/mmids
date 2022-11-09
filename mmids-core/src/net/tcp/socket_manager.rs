use super::listener::{start as start_listener, ListenerParams};
use super::{TcpSocketRequest, TcpSocketResponse};
use crate::net::tcp::{RequestFailureReason, TlsOptions};
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{debug, error, info};

/// Starts a new instance of a socket manager task.  A socket manager can be requested to open
/// ports on behalf of another system.  If the port is successfully opened it will begin listening
/// for TCP connections on that port, and then manage the reading and writing of network traffic
/// for that connection.
pub fn start(tls_options: Option<TlsOptions>) -> UnboundedSender<TcpSocketRequest> {
    let (request_sender, request_receiver) = unbounded_channel();

    let manager = SocketManager::new();
    tokio::spawn(manager.run(request_receiver, tls_options));

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

struct SocketManager {
    open_ports: HashMap<u16, OpenPort>,
    futures: FuturesUnordered<BoxFuture<'static, SocketManagerFutureResult>>,
}

impl SocketManager {
    fn new() -> Self {
        SocketManager {
            open_ports: HashMap::new(),
            futures: FuturesUnordered::new(),
        }
    }

    async fn run(
        mut self,
        request_receiver: UnboundedReceiver<TcpSocketRequest>,
        tls_options: Option<TlsOptions>,
    ) {
        info!("Starting TCP socket manager");
        let tls_options = Arc::new(tls_options);

        self.futures
            .push(request_receiver_future(request_receiver).boxed());

        while let Some(future_result) = self.futures.next().await {
            match future_result {
                SocketManagerFutureResult::IncomingRequest { request, receiver } => {
                    self.futures.push(request_receiver_future(receiver).boxed());

                    match request {
                        Some(request) => self.handle_request(request, tls_options.clone()),
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

        info!("Socket manager closing");
    }

    fn handle_request(&mut self, request: TcpSocketRequest, tls_options: Arc<Option<TlsOptions>>) {
        match request {
            TcpSocketRequest::OpenPort {
                port,
                response_channel,
                use_tls,
            } => {
                if use_tls && tls_options.as_ref().is_none() {
                    error!(
                        port = port,
                        "Request to open port with tls, but we have no tls options"
                    );
                    let _ = response_channel.send(TcpSocketResponse::RequestDenied {
                        reason: RequestFailureReason::NoTlsDetailsGiven,
                    });

                    return;
                }

                if let Entry::Vacant(entry) = self.open_ports.entry(port) {
                    debug!(port = port, use_tls = use_tls, "TCP port being opened");
                    let details = OpenPort {
                        response_channel: response_channel.clone(),
                    };

                    entry.insert(details);

                    let listener_shutdown = start_listener(ListenerParams {
                        port,
                        response_channel: response_channel.clone(),
                        use_tls,
                        tls_options,
                    });

                    self.futures
                        .push(listener_shutdown_future(port, listener_shutdown).boxed());

                    let _ = response_channel.send(TcpSocketResponse::RequestAccepted {});
                } else {
                    debug!(port = port, "Port is already in use!");
                    let message = TcpSocketResponse::RequestDenied {
                        reason: RequestFailureReason::PortInUse,
                    };

                    let _ = response_channel.send(message);
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
