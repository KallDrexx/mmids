pub mod actor_types;
mod connection_handler;

use super::{RtmpEndpointPublisherMessage, RtmpEndpointRequest, StreamKeyRegistration};
use crate::endpoints::rtmp_server::actor::connection_handler::ConnectionResponse;
use crate::endpoints::rtmp_server::RtmpEndpointWatcherNotification;
use crate::net::tcp::{TcpSocketRequest, TcpSocketResponse};
use crate::net::ConnectionId;
use crate::StreamId;
use actor_types::*;
use connection_handler::{ConnectionRequest, RtmpServerConnectionHandler};
use futures::future::FutureExt;
use futures::StreamExt;
use log::{error, info, warn};
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

impl<'a> RtmpServerEndpointActor<'a> {
    pub async fn run(
        mut self,
        endpoint_receiver: UnboundedReceiver<RtmpEndpointRequest>,
        socket_request_sender: UnboundedSender<TcpSocketRequest>,
    ) {
        info!("Starting RTMP server endpoint");

        self.futures
            .push(internal_futures::wait_for_endpoint_request(endpoint_receiver).boxed());

        while let Some(result) = self.futures.next().await {
            match result {
                FutureResult::NoMoreEndpointRequesters => {
                    info!("No endpoint requesters exist");
                    break;
                }

                FutureResult::SocketManagerClosed => {
                    info!("Socket manager closed");
                    break;
                }

                FutureResult::EndpointRequestReceived { request, receiver } => {
                    self.futures
                        .push(internal_futures::wait_for_endpoint_request(receiver).boxed());
                    self.handle_endpoint_request(request, socket_request_sender.clone());
                }

                FutureResult::PublishingRegistrantGone {
                    port,
                    app,
                    stream_key,
                    id,
                } => {
                    let port_map = match self.ports.get_mut(&port) {
                        Some(x) => x,
                        None => continue,
                    };

                    let app_map = match port_map.rtmp_applications.get_mut(app.as_str()) {
                        Some(x) => x,
                        None => continue,
                    };

                    {
                        let registrant = match app_map.publisher_registrants.get_mut(&stream_key) {
                            Some(x) => x,
                            None => continue,
                        };

                        if registrant.id != id {
                            continue;
                        }
                    }

                    app_map.publisher_registrants.remove(&stream_key);
                }

                FutureResult::WatcherRegistrantGone {
                    port,
                    app,
                    stream_key,
                } => {
                    let port_map = match self.ports.get_mut(&port) {
                        Some(x) => x,
                        None => continue,
                    };

                    let app_map = match port_map.rtmp_applications.get_mut(app.as_str()) {
                        Some(x) => x,
                        None => continue,
                    };

                    app_map.watcher_registrants.remove(&stream_key);
                }

                FutureResult::SocketResponseReceived {
                    port,
                    response,
                    receiver,
                } => {
                    self.handle_socket_response(port, response);
                    self.futures
                        .push(internal_futures::wait_for_socket_response(receiver, port).boxed());
                }

                FutureResult::ConnectionHandlerRequestReceived {
                    port,
                    connection_id,
                    request,
                    receiver,
                } => {
                    self.futures.push(
                        internal_futures::wait_for_connection_request(
                            port,
                            connection_id.clone(),
                            receiver,
                        )
                        .boxed(),
                    );

                    self.handle_connection_handler_request(port, connection_id, request);
                }

                FutureResult::ConnectionHandlerGone {
                    port,
                    connection_id,
                } => {
                    let port_map = match self.ports.get_mut(&port) {
                        Some(x) => x,
                        None => continue,
                    };

                    port_map.connections.remove(&connection_id);
                }

                FutureResult::WatcherMediaDataReceived {
                    port,
                    app,
                    stream_key,
                    stream_key_registration,
                    data,
                    receiver,
                } => {
                    self.futures.push(
                        internal_futures::wait_for_watcher_media(
                            receiver,
                            port,
                            app.clone(),
                            stream_key_registration,
                        )
                        .boxed(),
                    );

                    let port_map = match self.ports.get(&port) {
                        Some(x) => x,
                        None => continue,
                    };

                    let app_map = match port_map.rtmp_applications.get(app.as_str()) {
                        Some(x) => x,
                        None => continue,
                    };

                    let key_details = match app_map.active_stream_keys.get(stream_key.as_str()) {
                        Some(x) => x,
                        None => continue,
                    };

                    for (_, watcher_details) in &key_details.watchers {
                        let _ = watcher_details.media_sender.send(data.clone());
                    }
                }
            }
        }

        info!("Rtmp server endpoint closing");
    }

    fn handle_endpoint_request(
        &mut self,
        request: RtmpEndpointRequest,
        socket_request_sender: UnboundedSender<TcpSocketRequest>,
    ) {
        match request {
            RtmpEndpointRequest::ListenForPublishers {
                port,
                rtmp_app,
                rtmp_stream_key,
                message_channel,
                stream_id,
            } => {
                self.register_listener(
                    port,
                    rtmp_app,
                    rtmp_stream_key,
                    socket_request_sender,
                    ListenerRequest::Publisher {
                        channel: message_channel,
                        stream_id,
                    },
                );
            }

            RtmpEndpointRequest::ListenForWatchers {
                port,
                rtmp_app,
                rtmp_stream_key,
                media_channel,
                notification_channel,
            } => {
                self.register_listener(
                    port,
                    rtmp_app,
                    rtmp_stream_key,
                    socket_request_sender,
                    ListenerRequest::Watcher {
                        notification_channel,
                        media_channel,
                    },
                );
            }
        }
    }

    fn register_listener(
        &mut self,
        port: u16,
        rtmp_app: String,
        stream_key: StreamKeyRegistration,
        socket_sender: UnboundedSender<TcpSocketRequest>,
        listener: ListenerRequest,
    ) {
        let mut new_port_requested = false;
        let port_map = self.ports.entry(port).or_insert_with(|| {
            let port_map = PortMapping {
                rtmp_applications: HashMap::new(),
                status: PortStatus::Requested,
                connections: HashMap::new(),
            };

            new_port_requested = true;

            port_map
        });

        if new_port_requested {
            let (sender, receiver) = unbounded_channel();
            let request = TcpSocketRequest::OpenPort {
                port,
                response_channel: sender,
            };

            let _ = socket_sender.send(request);
            self.futures
                .push(internal_futures::wait_for_socket_response(receiver, port).boxed());
        }

        let app_map = port_map
            .rtmp_applications
            .entry(rtmp_app.clone())
            .or_insert(RtmpAppMapping {
                publisher_registrants: HashMap::new(),
                watcher_registrants: HashMap::new(),
                active_stream_keys: HashMap::new(),
            });

        match listener {
            ListenerRequest::Publisher { channel, stream_id } => {
                let can_be_added = match &stream_key {
                    StreamKeyRegistration::Any => {
                        if !app_map.publisher_registrants.is_empty() {
                            warn!("Rtmp server publish request registration failed for port {}, app '{}', all stream keys': \
                                    Another system is registered for at least one stream key on this port and app", port, rtmp_app);

                            false
                        } else {
                            true
                        }
                    }

                    StreamKeyRegistration::Exact(key) => {
                        if app_map
                            .publisher_registrants
                            .contains_key(&StreamKeyRegistration::Any)
                        {
                            warn!("Rtmp server publish request registration failed for port {}, app '{}', stream key '{}': \
                                    Another system is registered for all stream keys on this port/app", port, rtmp_app, key);

                            false
                        } else if app_map
                            .publisher_registrants
                            .contains_key(&StreamKeyRegistration::Exact(key.clone()))
                        {
                            warn!("Rtmp server publish request registration failed for port {}, app '{}', stream key '{}': \
                                    Another system is registered for this port/app/stream key combo", port, rtmp_app, key);

                            false
                        } else {
                            true
                        }
                    }
                };

                if !can_be_added {
                    let _ =
                        channel.send(RtmpEndpointPublisherMessage::PublisherRegistrationFailed {});

                    return;
                }

                let id = PublisherRegistrantId(Uuid::new_v4().to_string());
                app_map.publisher_registrants.insert(
                    stream_key.clone(),
                    PublishingRegistrant {
                        id: id.clone(),
                        response_channel: channel.clone(),
                        stream_id,
                    },
                );

                self.futures.push(
                    internal_futures::wait_for_publisher_channel_closed(
                        channel.clone(),
                        port,
                        rtmp_app,
                        stream_key,
                        id,
                    )
                    .boxed(),
                );

                // If the port isn't in a listening mode, we don't want to claim that
                // registration was successful yet
                if port_map.status == PortStatus::Open {
                    let _ = channel
                        .send(RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful {});
                }
            }

            ListenerRequest::Watcher {
                media_channel,
                notification_channel,
            } => {
                let can_be_added = match &stream_key {
                    StreamKeyRegistration::Any => {
                        if !app_map.watcher_registrants.is_empty() {
                            warn!("Rtmp server watcher registration failed for port {}, app '{}', all stream keys': \
                                    Another system is registered for at least one stream key on this port and app", port, rtmp_app);

                            false
                        } else {
                            true
                        }
                    }

                    StreamKeyRegistration::Exact(key) => {
                        if app_map
                            .watcher_registrants
                            .contains_key(&StreamKeyRegistration::Any)
                        {
                            warn!("Rtmp server watcher registration failed for port {}, app '{}', stream key '{}': \
                                    Another system is registered for all stream keys on this port/app", port, rtmp_app, key);

                            false
                        } else if app_map
                            .watcher_registrants
                            .contains_key(&StreamKeyRegistration::Exact(key.clone()))
                        {
                            warn!("Rtmp server watcher registration failed for port {}, app '{}', stream key '{}': \
                                    Another system is registered for this port/app/stream key combo", port, rtmp_app, key);

                            false
                        } else {
                            true
                        }
                    }
                };

                if !can_be_added {
                    let _ = notification_channel
                        .send(RtmpEndpointWatcherNotification::ReceiverRegistrationFailed);

                    return;
                }

                app_map.watcher_registrants.insert(
                    stream_key.clone(),
                    WatcherRegistrant {
                        response_channel: notification_channel.clone(),
                    },
                );

                self.futures.push(
                    internal_futures::wait_for_watcher_notification_channel_closed(
                        notification_channel.clone(),
                        port,
                        rtmp_app.clone(),
                        stream_key.clone(),
                    )
                    .boxed(),
                );

                self.futures.push(
                    internal_futures::wait_for_watcher_media(
                        media_channel,
                        port,
                        rtmp_app,
                        stream_key,
                    )
                    .boxed(),
                );

                // If the port isn't open yet, we don't want to claim registration was successful yet
                if port_map.status == PortStatus::Open {
                    let _ = notification_channel
                        .send(RtmpEndpointWatcherNotification::ReceiverRegistrationSuccessful);
                }
            }
        }
    }

    fn handle_socket_response(&mut self, port: u16, response: TcpSocketResponse) {
        let mut remove_port = false;
        {
            let port_map = match self.ports.get_mut(&port) {
                Some(x) => x,
                None => {
                    error!("Received socket response for port {} but that port has not been registered", port);

                    return;
                }
            };

            match response {
                TcpSocketResponse::RequestDenied { reason } => {
                    warn!("Port {} could not be opened: {:?}", port, reason);

                    for (_, app_map) in &port_map.rtmp_applications {
                        for (_, publisher) in &app_map.publisher_registrants {
                            let _ = publisher
                                .response_channel
                                .send(RtmpEndpointPublisherMessage::PublisherRegistrationFailed {});
                        }

                        for (_, watcher) in &app_map.watcher_registrants {
                            let _ = watcher
                                .response_channel
                                .send(RtmpEndpointWatcherNotification::ReceiverRegistrationFailed);
                        }
                    }

                    remove_port = true;
                }

                TcpSocketResponse::PortForciblyClosed { port: _ } => {
                    warn!("Port {} closed", port);

                    remove_port = true;
                }

                TcpSocketResponse::RequestAccepted {} => {
                    info!("Port {} successfully opened", port);

                    // Since the port was successfully opened, any pending registrants need to be
                    // informed that their registration has now been successful
                    for (_, app_map) in &port_map.rtmp_applications {
                        for (_, publisher) in &app_map.publisher_registrants {
                            let _ = publisher.response_channel.send(
                                RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful {},
                            );
                        }

                        for (_, watcher) in &app_map.watcher_registrants {
                            let _ = watcher.response_channel.send(
                                RtmpEndpointWatcherNotification::ReceiverRegistrationSuccessful,
                            );
                        }
                    }

                    port_map.status = PortStatus::Open;
                }

                TcpSocketResponse::NewConnection {
                    port: _,
                    connection_id,
                    outgoing_bytes,
                    incoming_bytes,
                } => {
                    let (request_sender, request_receiver) = unbounded_channel();
                    let (response_sender, response_receiver) = unbounded_channel();
                    let handler = RtmpServerConnectionHandler::new(
                        connection_id.clone(),
                        outgoing_bytes,
                        request_sender,
                    );
                    tokio::spawn(handler.run_async(response_receiver, incoming_bytes));

                    port_map.connections.insert(
                        connection_id.clone(),
                        Connection {
                            response_channel: response_sender,
                            state: ConnectionState::None,
                        },
                    );

                    self.futures.push(
                        internal_futures::wait_for_connection_request(
                            port,
                            connection_id,
                            request_receiver,
                        )
                        .boxed(),
                    );
                }

                TcpSocketResponse::Disconnection { connection_id } => {
                    // Clean this connection up
                    if let Some(connection) = port_map.connections.remove(&connection_id) {
                        info!("Connection {} disconnected.  Cleaning it up", connection_id);

                        match connection.state {
                            ConnectionState::None => (),
                            ConnectionState::Publishing {
                                rtmp_app,
                                stream_key,
                            } => {
                                if let Some(application) =
                                    port_map.rtmp_applications.get_mut(rtmp_app.as_str())
                                {
                                    if let Some(active_key) =
                                        application.active_stream_keys.get_mut(&stream_key)
                                    {
                                        let remove = match &active_key.publisher {
                                            Some(x) => *x == connection_id,
                                            None => false,
                                        };

                                        if remove {
                                            active_key.publisher = None;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if remove_port {
            self.ports.remove(&port);
        }
    }

    fn handle_connection_handler_request(
        &mut self,
        port: u16,
        connection_id: ConnectionId,
        request: ConnectionRequest,
    ) {
        let port_map = match self.ports.get_mut(&port) {
            Some(x) => x,
            None => {
                error!(
                    "Connection handler for connection {:?} sent {:?} on port {}, but that \
                port isn't managed yet!",
                    connection_id, request, port
                );

                return;
            }
        };

        let connection = match port_map.connections.get_mut(&connection_id) {
            Some(x) => x,
            None => {
                warn!(
                    "Connection handler for connection {:?} sent {:?} on port {}, but that \
                connection isn't being tracked.",
                    connection_id, request, port
                );

                return;
            }
        };

        match request {
            ConnectionRequest::RequestConnectToApp { rtmp_app } => {
                let response = if !port_map.rtmp_applications.contains_key(rtmp_app.as_str()) {
                    info!("Connection {} requested connection to RTMP app '{}' which isn't registered yet", connection_id, rtmp_app);

                    ConnectionResponse::RequestRejected
                } else {
                    info!(
                        "Connection {} accepted connection for RTMP app '{}'",
                        connection_id, rtmp_app
                    );

                    ConnectionResponse::AppConnectRequestAccepted
                };

                let _ = connection.response_channel.send(response);
            }

            ConnectionRequest::RequestPublish {
                rtmp_app,
                stream_key,
            } => {
                // Has this RTMP application been registered yet?
                let application = match port_map.rtmp_applications.get_mut(rtmp_app.as_str()) {
                    Some(x) => x,
                    None => {
                        info!(
                            "Connection {} requested publishing to '{}/{}', but the RTMP app '{}' \
                        isn't registered yet",
                            connection_id, rtmp_app, stream_key, rtmp_app
                        );

                        let _ = connection
                            .response_channel
                            .send(ConnectionResponse::RequestRejected);

                        return;
                    }
                };

                // Has this stream key been registered yet?
                let registrant = match application
                    .publisher_registrants
                    .get(&StreamKeyRegistration::Any)
                {
                    Some(x) => x,
                    None => {
                        match application
                            .publisher_registrants
                            .get(&StreamKeyRegistration::Exact(stream_key.clone()))
                        {
                            Some(x) => x,
                            None => {
                                error!("Connection {} requested publishing to '{}/{}', but no one has registered \
                                to support publishers on that stream key", connection_id, rtmp_app, stream_key);

                                let _ = connection
                                    .response_channel
                                    .send(ConnectionResponse::RequestRejected);
                                return;
                            }
                        }
                    }
                };

                // app/stream key combination is valid and we have a registrant for itk
                let stream_key_connections = application
                    .active_stream_keys
                    .entry(stream_key.clone())
                    .or_insert(StreamKeyConnections {
                        publisher: None,
                        watchers: HashMap::new(),
                    });

                // Is someone already publishing on this stream key?
                if let Some(id) = &stream_key_connections.publisher {
                    error!(
                        "Connection {} requested publishing to '{}/{}', but connection {} is \
                    already publishing to this stream key",
                        connection_id, rtmp_app, stream_key, id
                    );

                    let _ = connection
                        .response_channel
                        .send(ConnectionResponse::RequestRejected);
                    return;
                }

                // All good to publish
                stream_key_connections.publisher = Some(connection_id.clone());
                connection.state = ConnectionState::Publishing {
                    rtmp_app: rtmp_app.clone(),
                    stream_key: stream_key.clone(),
                };

                let stream_id = if let Some(id) = &registrant.stream_id {
                    (*id).clone()
                } else {
                    StreamId(Uuid::new_v4().to_string())
                };

                let _ =
                    connection
                        .response_channel
                        .send(ConnectionResponse::PublishRequestAccepted {
                            channel: registrant.response_channel.clone(),
                        });

                let _ = registrant.response_channel.send(
                    RtmpEndpointPublisherMessage::NewPublisherConnected {
                        connection_id,
                        stream_key,
                        stream_id,
                    },
                );
            }

            ConnectionRequest::RequestWatch {
                rtmp_app,
                stream_key,
            } => {
                // Has this app been registered yet?
                let application = match port_map.rtmp_applications.get_mut(rtmp_app.as_str()) {
                    Some(x) => x,
                    None => {
                        info!("Connection {} requested watching '{}/{}' but that app is not registered \
                        to accept watchers", connection_id, rtmp_app, stream_key);

                        let _ = connection
                            .response_channel
                            .send(ConnectionResponse::RequestRejected);
                        return;
                    }
                };

                // Is this stream key registered for watching
                let _ = match application
                    .watcher_registrants
                    .get(&StreamKeyRegistration::Any)
                {
                    Some(x) => x,
                    None => {
                        match application
                            .watcher_registrants
                            .get(&StreamKeyRegistration::Exact(stream_key.clone()))
                        {
                            Some(x) => x,
                            None => {
                                info!("Connection {} requested watching '{}/{}' but that stream key is \
                                not registered to accept watchers", connection_id, rtmp_app, stream_key);

                                let _ = connection
                                    .response_channel
                                    .send(ConnectionResponse::RequestRejected);
                                return;
                            }
                        }
                    }
                };

                let active_stream_key = application.active_stream_keys.entry(stream_key).or_insert(
                    StreamKeyConnections {
                        watchers: HashMap::new(),
                        publisher: None,
                    },
                );

                let (media_sender, media_receiver) = unbounded_channel();
                active_stream_key
                    .watchers
                    .insert(connection_id, WatcherDetails { media_sender });

                let _ =
                    connection
                        .response_channel
                        .send(ConnectionResponse::WatchRequestAccepted {
                            channel: media_receiver,
                        });
            }
        }
    }
}

mod internal_futures {
    use super::actor_types::PublisherRegistrantId;
    use super::{
        FutureResult, RtmpEndpointPublisherMessage, RtmpEndpointRequest, StreamKeyRegistration,
    };
    use crate::endpoints::rtmp_server::actor::connection_handler::ConnectionRequest;
    use crate::endpoints::rtmp_server::{
        RtmpEndpointMediaMessage, RtmpEndpointWatcherNotification,
    };
    use crate::net::tcp::TcpSocketResponse;
    use crate::net::ConnectionId;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

    pub(super) async fn wait_for_endpoint_request(
        mut endpoint_receiver: UnboundedReceiver<RtmpEndpointRequest>,
    ) -> FutureResult {
        match endpoint_receiver.recv().await {
            None => FutureResult::NoMoreEndpointRequesters,
            Some(request) => FutureResult::EndpointRequestReceived {
                request,
                receiver: endpoint_receiver,
            },
        }
    }

    pub(super) async fn wait_for_socket_response(
        mut socket_receiver: UnboundedReceiver<TcpSocketResponse>,
        port: u16,
    ) -> FutureResult {
        match socket_receiver.recv().await {
            None => FutureResult::SocketManagerClosed,
            Some(response) => FutureResult::SocketResponseReceived {
                port,
                response,
                receiver: socket_receiver,
            },
        }
    }

    pub(super) async fn wait_for_publisher_channel_closed(
        sender: UnboundedSender<RtmpEndpointPublisherMessage>,
        port: u16,
        app_name: String,
        stream_key: StreamKeyRegistration,
        id: PublisherRegistrantId,
    ) -> FutureResult {
        sender.closed().await;

        FutureResult::PublishingRegistrantGone {
            port,
            app: app_name,
            stream_key,
            id,
        }
    }

    pub(super) async fn wait_for_connection_request(
        port: u16,
        connection_id: ConnectionId,
        mut receiver: UnboundedReceiver<ConnectionRequest>,
    ) -> FutureResult {
        match receiver.recv().await {
            Some(request) => FutureResult::ConnectionHandlerRequestReceived {
                port,
                receiver,
                connection_id,
                request,
            },

            None => FutureResult::ConnectionHandlerGone {
                port,
                connection_id,
            },
        }
    }

    pub(super) async fn wait_for_watcher_notification_channel_closed(
        sender: UnboundedSender<RtmpEndpointWatcherNotification>,
        port: u16,
        app_name: String,
        stream_key: StreamKeyRegistration,
    ) -> FutureResult {
        sender.closed().await;

        FutureResult::WatcherRegistrantGone {
            port,
            app: app_name,
            stream_key,
        }
    }

    pub(super) async fn wait_for_watcher_media(
        mut receiver: UnboundedReceiver<RtmpEndpointMediaMessage>,
        port: u16,
        app_name: String,
        stream_key_registration: StreamKeyRegistration,
    ) -> FutureResult {
        match receiver.recv().await {
            None => FutureResult::WatcherRegistrantGone {
                port,
                app: app_name,
                stream_key: stream_key_registration,
            },
            Some(message) => FutureResult::WatcherMediaDataReceived {
                port,
                app: app_name,
                stream_key: message.stream_key,
                stream_key_registration,
                data: message.data,
                receiver,
            },
        }
    }
}
