use super::listener::{start as start_listener, ListenerParams};
use super::{TcpSocketRequest, TcpSocketResponse};
use crate::actor_utils::notify_on_unbounded_recv;
use crate::net::tcp::{RequestFailureReason, TlsOptions};
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
    let (actor_sender, actor_receiver) = unbounded_channel();

    let manager = SocketManager::new(request_receiver, actor_sender);
    tokio::spawn(manager.run(actor_receiver, tls_options));

    request_sender
}

enum SocketManagerFutureResult {
    AllRequestSendersGone,
    IncomingRequest(TcpSocketRequest),
    ListenerShutdown { port: u16 },
}

struct OpenPort {
    response_channel: UnboundedSender<TcpSocketResponse>,
}

struct SocketManager {
    internal_sender: UnboundedSender<SocketManagerFutureResult>,
    open_ports: HashMap<u16, OpenPort>,
}

impl SocketManager {
    fn new(
        request_receiver: UnboundedReceiver<TcpSocketRequest>,
        actor_sender: UnboundedSender<SocketManagerFutureResult>,
    ) -> Self {
        notify_on_unbounded_recv(
            request_receiver,
            actor_sender.clone(),
            SocketManagerFutureResult::IncomingRequest,
            || SocketManagerFutureResult::AllRequestSendersGone,
        );

        SocketManager {
            internal_sender: actor_sender,
            open_ports: HashMap::new(),
        }
    }

    async fn run(
        mut self,
        mut actor_receiver: UnboundedReceiver<SocketManagerFutureResult>,
        tls_options: Option<TlsOptions>,
    ) {
        info!("Starting TCP socket manager");
        let tls_options = Arc::new(tls_options);

        while let Some(future_result) = actor_receiver.recv().await {
            match future_result {
                SocketManagerFutureResult::AllRequestSendersGone => {
                    info!("All TCP socket manager requesters gone");
                    break;
                }

                SocketManagerFutureResult::IncomingRequest(request) => {
                    self.handle_request(request, tls_options.clone());
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

                    notify_on_listener_shutdown(
                        port,
                        listener_shutdown,
                        self.internal_sender.clone(),
                    );

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

fn notify_on_listener_shutdown(
    port: u16,
    signal: UnboundedSender<()>,
    actor_sender: UnboundedSender<SocketManagerFutureResult>,
) {
    tokio::spawn(async move {
        tokio::select! {
            _ = signal.closed() => {
                let _ = actor_sender.send(SocketManagerFutureResult::ListenerShutdown { port });
            }

            _ = actor_sender.closed() => {}
        }
    });
}
