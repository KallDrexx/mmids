pub mod actor_types;
mod connection_handler;

use super::{
    RtmpEndpointMediaData, RtmpEndpointPublisherMessage, RtmpEndpointRequest, StreamKeyRegistration,
};
use crate::endpoints::rtmp_server::actor::connection_handler::ConnectionResponse;
use crate::endpoints::rtmp_server::actor::internal_futures::wait_for_validation;
use crate::endpoints::rtmp_server::{
    IpRestriction, RegistrationType, RtmpEndpointWatcherNotification, ValidationResponse,
};
use crate::net::tcp::{TcpSocketRequest, TcpSocketResponse};
use crate::net::ConnectionId;
use crate::StreamId;
use actor_types::*;
use connection_handler::{ConnectionRequest, RtmpServerConnectionHandler};
use futures::future::{BoxFuture, FutureExt};
use futures::StreamExt;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::{channel, Sender};
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

impl RtmpServerEndpointActor {
    #[instrument(
        name = "RtmpServer Endpoint Execution",
        skip(self, endpoint_receiver, socket_request_sender)
    )]
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
                } => {
                    self.remove_publish_registration(port, app, stream_key);
                }

                FutureResult::WatcherRegistrantGone {
                    port,
                    app,
                    stream_key,
                } => {
                    self.remove_watcher_registration(port, app, stream_key);
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

                    clean_disconnected_connection(connection_id, port_map);
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

                    self.handle_watcher_media_received(port, app, stream_key, data);
                }

                FutureResult::ValidationApprovalResponseReceived(port, connection_id, response) => {
                    self.handle_validation_response(port, connection_id, response);
                }
            }
        }

        info!("Rtmp server endpoint closing");
    }

    #[instrument(skip(self))]
    fn handle_validation_response(
        &mut self,
        port: u16,
        connection_id: ConnectionId,
        response: ValidationResponse,
    ) {
        let port_map = match self.ports.get_mut(&port) {
            Some(ports) => ports,
            None => {
                return;
            } // Port has been closed prior to this response
        };

        let connection = match port_map.connections.get_mut(&connection_id) {
            Some(connection) => connection,
            None => {
                return;
            } // Disconnected before this response came in
        };

        match response {
            ValidationResponse::Approve {
                reactor_keep_alive_channel,
            } => {
                match &connection.state {
                    ConnectionState::None => {
                        warn!("Unexpected approval for connection in None state");
                    }

                    ConnectionState::Watching { .. } => {
                        warn!("Unexpected approval for connection in the Watching state");
                    }

                    ConnectionState::Publishing { .. } => {
                        warn!("Unexpected approval for connection in the publishing state");
                    }

                    ConnectionState::WaitingForPublishValidation {
                        rtmp_app,
                        stream_key,
                    } => {
                        info!(
                            rtmp_app = %rtmp_app,
                            stream_key = %stream_key,
                            "Request to publish was approved"
                        );

                        // Redefine as clones due to borrow checker
                        let rtmp_app = rtmp_app.clone();
                        let stream_key = stream_key.clone();

                        connection.received_registrant_approval = true;
                        let future = handle_connection_request_publish(
                            &connection_id,
                            port_map,
                            port,
                            rtmp_app,
                            &stream_key,
                            reactor_keep_alive_channel,
                        );

                        if let Some(future) = future {
                            self.futures.push(future);
                        }
                    }

                    ConnectionState::WaitingForWatchValidation {
                        rtmp_app,
                        stream_key,
                    } => {
                        info!(
                            rtmp_app = %rtmp_app,
                            stream_key = %stream_key,
                            "Request to watch was approved",
                        );

                        // Redefine with clones due to borrow checker
                        let rtmp_app = rtmp_app.clone();
                        let stream_key = stream_key.clone();

                        connection.received_registrant_approval = true;
                        let future = handle_connection_request_watch(
                            connection_id,
                            port_map,
                            port,
                            rtmp_app,
                            &stream_key,
                            reactor_keep_alive_channel,
                        );

                        if let Some(future) = future {
                            self.futures.push(future);
                        }
                    }
                }
            }

            ValidationResponse::Reject => {
                match &connection.state {
                    ConnectionState::None => {
                        warn!("Unexpected approval for connection in None state");
                    }

                    ConnectionState::Watching { .. } => {
                        warn!("Unexpected approval for connection in the Watching state");
                    }

                    ConnectionState::Publishing { .. } => {
                        warn!("Unexpected approval for connection in the publishing state");
                    }

                    ConnectionState::WaitingForPublishValidation {
                        rtmp_app,
                        stream_key,
                    } => {
                        info!(
                            rtmp_app = %rtmp_app,
                            stream_key = %stream_key,
                            "Request to publish was rejected"
                        );
                    }

                    ConnectionState::WaitingForWatchValidation {
                        rtmp_app,
                        stream_key,
                    } => {
                        info!(
                            rtmp_app = %rtmp_app,
                            stream_key = %stream_key,
                            "Request to watch was rejected"
                        );
                    }
                }

                let _ = connection
                    .response_channel
                    .send(ConnectionResponse::RequestRejected);
            }
        }
    }

    fn handle_watcher_media_received(
        &mut self,
        port: u16,
        app: String,
        stream_key: String,
        data: RtmpEndpointMediaData,
    ) {
        let port_map = match self.ports.get_mut(&port) {
            Some(x) => x,
            None => return,
        };

        let app_map = match port_map.rtmp_applications.get_mut(app.as_str()) {
            Some(x) => x,
            None => return,
        };

        let key_details = app_map
            .active_stream_keys
            .entry(stream_key.clone())
            .or_insert(StreamKeyConnections {
                watchers: HashMap::new(),
                publisher: None,
                latest_video_sequence_header: None,
                latest_audio_sequence_header: None,
            });

        match &data {
            RtmpEndpointMediaData::NewVideoData {
                data,
                codec,
                is_sequence_header,
                ..
            } => {
                if *is_sequence_header {
                    key_details.latest_video_sequence_header = Some(VideoSequenceHeader {
                        codec: codec.clone(),
                        data: data.clone(),
                    });
                }
            }

            RtmpEndpointMediaData::NewAudioData {
                data,
                codec,
                is_sequence_header,
                ..
            } => {
                if *is_sequence_header {
                    key_details.latest_audio_sequence_header = Some(AudioSequenceHeader {
                        codec: codec.clone(),
                        data: data.clone(),
                    });
                }
            }

            _ => (),
        };

        for (_, watcher_details) in &key_details.watchers {
            let _ = watcher_details.media_sender.send(data.clone());
        }
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
                ip_restrictions: ip_restriction,
                use_tls,
                requires_registrant_approval,
            } => {
                self.register_listener(
                    port,
                    rtmp_app,
                    rtmp_stream_key,
                    socket_request_sender,
                    ListenerRequest::Publisher {
                        channel: message_channel,
                        stream_id,
                        requires_registrant_approval: requires_registrant_approval,
                    },
                    ip_restriction,
                    use_tls,
                );
            }

            RtmpEndpointRequest::ListenForWatchers {
                port,
                rtmp_app,
                rtmp_stream_key,
                media_channel,
                notification_channel,
                ip_restrictions,
                use_tls,
                requires_registrant_approval,
            } => {
                self.register_listener(
                    port,
                    rtmp_app,
                    rtmp_stream_key,
                    socket_request_sender,
                    ListenerRequest::Watcher {
                        notification_channel,
                        media_channel,
                        requires_registrant_approval: requires_registrant_approval,
                    },
                    ip_restrictions,
                    use_tls,
                );
            }

            RtmpEndpointRequest::RemoveRegistration {
                registration_type,
                port,
                rtmp_app,
                rtmp_stream_key,
            } => {
                info!(
                    port = %port,
                    rtmp_app = %rtmp_app,
                    stream_key = ?rtmp_stream_key,
                    registration_type = ?registration_type,
                    "{:?} Registration removal requested for port {}, app {}, and stream key {:?}",
                    registration_type, port, rtmp_app, rtmp_stream_key
                );

                match registration_type {
                    RegistrationType::Publisher => {
                        self.remove_publish_registration(port, rtmp_app, rtmp_stream_key)
                    }
                    RegistrationType::Watcher => {
                        self.remove_watcher_registration(port, rtmp_app, rtmp_stream_key)
                    }
                }
            }
        }
    }

    #[instrument(skip(self, socket_sender, listener))]
    fn register_listener(
        &mut self,
        port: u16,
        rtmp_app: String,
        stream_key: StreamKeyRegistration,
        socket_sender: UnboundedSender<TcpSocketRequest>,
        listener: ListenerRequest,
        ip_restrictions: IpRestriction,
        use_tls: bool,
    ) {
        let mut new_port_requested = false;
        let port_map = self.ports.entry(port).or_insert_with(|| {
            let port_map = PortMapping {
                rtmp_applications: HashMap::new(),
                status: PortStatus::Requested,
                connections: HashMap::new(),
                tls: use_tls,
            };

            new_port_requested = true;

            port_map
        });

        if port_map.tls != use_tls {
            error!(
                "Request to open port {} with tls set to {} failed, as the port is already mapped \
            with tls set to {}",
                port, use_tls, port_map.tls
            );

            match listener {
                ListenerRequest::Publisher { channel, .. } => {
                    let _ = channel.send(RtmpEndpointPublisherMessage::PublisherRegistrationFailed);
                }

                ListenerRequest::Watcher {
                    notification_channel,
                    ..
                } => {
                    let _ = notification_channel
                        .send(RtmpEndpointWatcherNotification::WatcherRegistrationFailed);
                }
            }

            return;
        }

        if new_port_requested {
            let (sender, receiver) = unbounded_channel();
            let request = TcpSocketRequest::OpenPort {
                port,
                response_channel: sender,
                use_tls,
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
            ListenerRequest::Publisher {
                channel,
                stream_id,
                requires_registrant_approval,
            } => {
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

                app_map.publisher_registrants.insert(
                    stream_key.clone(),
                    PublishingRegistrant {
                        response_channel: channel.clone(),
                        stream_id,
                        ip_restrictions,
                        requires_registrant_approval,
                    },
                );

                self.futures.push(
                    internal_futures::wait_for_publisher_channel_closed(
                        channel.clone(),
                        port,
                        rtmp_app,
                        stream_key,
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
                requires_registrant_approval,
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
                        .send(RtmpEndpointWatcherNotification::WatcherRegistrationFailed);

                    return;
                }

                app_map.watcher_registrants.insert(
                    stream_key.clone(),
                    WatcherRegistrant {
                        response_channel: notification_channel.clone(),
                        ip_restrictions,
                        requires_registrant_approval,
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
                        .send(RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful);
                }
            }
        }
    }

    #[instrument(skip(self))]
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
                                .send(RtmpEndpointWatcherNotification::WatcherRegistrationFailed);
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
                                RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful,
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
                    socket_address,
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
                            socket_address,
                            received_registrant_approval: false,
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
                    clean_disconnected_connection(connection_id, port_map);
                }
            }
        }

        if remove_port {
            self.ports.remove(&port);
        }
    }

    #[instrument(skip(self))]
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

        match request {
            ConnectionRequest::RequestConnectToApp { rtmp_app } => {
                handle_connection_request_connect_to_app(&connection_id, port_map, port, rtmp_app);
            }

            ConnectionRequest::RequestPublish {
                rtmp_app,
                stream_key,
            } => {
                let future = handle_connection_request_publish(
                    &connection_id,
                    port_map,
                    port,
                    rtmp_app,
                    &stream_key,
                    None,
                );

                if let Some(future) = future {
                    self.futures.push(future);
                }
            }

            ConnectionRequest::RequestWatch {
                rtmp_app,
                stream_key,
            } => {
                let future = handle_connection_request_watch(
                    connection_id,
                    port_map,
                    port,
                    rtmp_app,
                    &stream_key,
                    None,
                );

                if let Some(future) = future {
                    self.futures.push(future);
                }
            }
        }
    }

    fn remove_publish_registration(
        &mut self,
        port: u16,
        app: String,
        stream_key: StreamKeyRegistration,
    ) {
        let port_map = match self.ports.get_mut(&port) {
            Some(x) => x,
            None => return,
        };

        let app_map = match port_map.rtmp_applications.get_mut(app.as_str()) {
            Some(x) => x,
            None => return,
        };

        if let None = app_map.publisher_registrants.remove(&stream_key) {
            return;
        }

        // Remove all publishers tied to this registrant
        let mut keys_to_remove = Vec::new();
        if let StreamKeyRegistration::Exact(key) = stream_key {
            keys_to_remove.push(key);
        } else {
            keys_to_remove.extend(app_map.active_stream_keys.keys().map(|x| x.clone()));
        }

        for key in keys_to_remove {
            if let Some(connection) = app_map.active_stream_keys.get_mut(&key) {
                if let Some(id) = &connection.publisher {
                    if let Some(connection) = port_map.connections.get(id) {
                        let _ = connection
                            .response_channel
                            .send(ConnectionResponse::Disconnect);
                    }
                }

                connection.publisher = None;
            }
        }

        if app_map.publisher_registrants.is_empty() && app_map.watcher_registrants.is_empty() {
            port_map.rtmp_applications.remove(&app);
        }
    }

    fn remove_watcher_registration(
        &mut self,
        port: u16,
        app: String,
        stream_key: StreamKeyRegistration,
    ) {
        let port_map = match self.ports.get_mut(&port) {
            Some(x) => x,
            None => return,
        };

        let app_map = match port_map.rtmp_applications.get_mut(app.as_str()) {
            Some(x) => x,
            None => return,
        };

        if let None = app_map.watcher_registrants.remove(&stream_key) {
            return;
        }

        // Remove all watchers tied to this registrant
        let mut keys_to_remove = Vec::new();
        if let StreamKeyRegistration::Exact(key) = stream_key {
            keys_to_remove.push(key);
        } else {
            keys_to_remove.extend(app_map.active_stream_keys.keys().map(|x| x.clone()));
        }

        for key in keys_to_remove {
            if let Some(connection) = app_map.active_stream_keys.get_mut(&key) {
                for id in connection.watchers.keys() {
                    if let Some(connection) = port_map.connections.get(id) {
                        let _ = connection
                            .response_channel
                            .send(ConnectionResponse::Disconnect);
                    }
                }

                connection.watchers.clear();
            }
        }

        if app_map.watcher_registrants.is_empty() && app_map.publisher_registrants.is_empty() {
            port_map.rtmp_applications.remove(&app);
        }
    }
}

#[instrument(skip(port_map))]
fn handle_connection_request_watch(
    connection_id: ConnectionId,
    port_map: &mut PortMapping,
    port: u16,
    rtmp_app: String,
    stream_key: &String,
    reactor_keep_alive_channel: Option<Sender<()>>,
) -> Option<BoxFuture<'static, FutureResult>> {
    let connection = match port_map.connections.get_mut(&connection_id) {
        Some(x) => x,
        None => {
            warn!("Connection handler for connection {:?} sent request to watch on port {}, but that \
                connection isn't being tracked.", connection_id, port);

            return None;
        }
    };

    // Has this app been registered yet?
    let application = match port_map.rtmp_applications.get_mut(rtmp_app.as_str()) {
        Some(x) => x,
        None => {
            info!(
                "Connection {} requested watching '{}/{}' but that app is not registered \
                        to accept watchers",
                connection_id, rtmp_app, stream_key
            );

            let _ = connection
                .response_channel
                .send(ConnectionResponse::RequestRejected);

            return None;
        }
    };

    // Is this stream key registered for watching
    let registrant = match application
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
                    info!(
                        "Connection {} requested watching '{}/{}' but that stream key is \
                                not registered to accept watchers",
                        connection_id, rtmp_app, stream_key
                    );

                    let _ = connection
                        .response_channel
                        .send(ConnectionResponse::RequestRejected);

                    return None;
                }
            }
        }
    };

    if !is_ip_allowed(&connection.socket_address, &registrant.ip_restrictions) {
        error!(
            "Connection {} requested watching to '{}/{}', but the client's ip address of '{}' \
        is not allowed",
            connection_id,
            rtmp_app,
            stream_key,
            connection.socket_address.ip()
        );

        let _ = connection
            .response_channel
            .send(ConnectionResponse::RequestRejected);

        return None;
    }

    if registrant.requires_registrant_approval && !connection.received_registrant_approval {
        info!(
            "Connection {} requested watching to '{}/{}' but requires approval from the \
            registrant first",
            connection_id, rtmp_app, stream_key
        );

        connection.state = ConnectionState::WaitingForWatchValidation {
            rtmp_app,
            stream_key: stream_key.clone(),
        };

        let (sender, receiver) = channel();
        let _ = registrant.response_channel.send(
            RtmpEndpointWatcherNotification::WatcherRequiringApproval {
                stream_key: stream_key.clone(),
                connection_id: connection_id.clone(),
                response_channel: sender,
            },
        );

        let future = wait_for_validation(port, connection_id.clone(), receiver).boxed();

        return Some(future);
    }

    let active_stream_key = application
        .active_stream_keys
        .entry(stream_key.clone())
        .or_insert(StreamKeyConnections {
            watchers: HashMap::new(),
            publisher: None,
            latest_video_sequence_header: None,
            latest_audio_sequence_header: None,
        });

    connection.state = ConnectionState::Watching {
        rtmp_app,
        stream_key: stream_key.clone(),
    };

    if active_stream_key.watchers.is_empty() {
        let _ = registrant.response_channel.send(
            RtmpEndpointWatcherNotification::StreamKeyBecameActive {
                stream_key: stream_key.clone(),
                reactor_keep_alive_channel,
            },
        );
    }

    let (media_sender, media_receiver) = unbounded_channel();

    // If we have a sequence headers available, send it to the client so they can immediately
    // start decoding video
    if let Some(sequence_header) = &active_stream_key.latest_video_sequence_header {
        let _ = media_sender.send(RtmpEndpointMediaData::NewVideoData {
            codec: sequence_header.codec.clone(),
            is_sequence_header: true,
            is_keyframe: true,
            data: sequence_header.data.clone(),
            timestamp: RtmpTimestamp::new(0),
        });
    }

    if let Some(sequence_header) = &active_stream_key.latest_audio_sequence_header {
        let _ = media_sender.send(RtmpEndpointMediaData::NewAudioData {
            codec: sequence_header.codec.clone(),
            data: sequence_header.data.clone(),
            is_sequence_header: true,
            timestamp: RtmpTimestamp::new(0),
        });
    }

    active_stream_key
        .watchers
        .insert(connection_id, WatcherDetails { media_sender });

    let _ = connection
        .response_channel
        .send(ConnectionResponse::WatchRequestAccepted {
            channel: media_receiver,
        });

    return None;
}

#[instrument(skip(port_map))]
fn handle_connection_request_publish(
    connection_id: &ConnectionId,
    port_map: &mut PortMapping,
    port: u16,
    rtmp_app: String,
    stream_key: &String,
    reactor_keep_alive_channel: Option<Sender<()>>,
) -> Option<BoxFuture<'static, FutureResult>> {
    let connection = match port_map.connections.get_mut(&connection_id) {
        Some(x) => x,
        None => {
            warn!("Connection handler for connection {:?} sent a request to publish on port {}, but that \
                connection isn't being tracked.", connection_id, port);

            return None;
        }
    };

    // Has this RTMP application been registered yet?
    let application = match port_map.rtmp_applications.get_mut(rtmp_app.as_str()) {
        Some(x) => x,
        None => {
            info!("Connection {} requested publishing to '{}/{}', but the RTMP app '{}' isn't registered yet",
                    connection_id, rtmp_app, stream_key, rtmp_app);

            let _ = connection
                .response_channel
                .send(ConnectionResponse::RequestRejected);

            return None;
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
                    error!(
                        "Connection {} requested publishing to '{}/{}', but no one has registered \
                            to support publishers on that stream key",
                        connection_id, rtmp_app, stream_key
                    );

                    let _ = connection
                        .response_channel
                        .send(ConnectionResponse::RequestRejected);

                    return None;
                }
            }
        }
    };

    // app/stream key combination is valid and we have a registrant for it
    let stream_key_connections = application
        .active_stream_keys
        .entry(stream_key.clone())
        .or_insert(StreamKeyConnections {
            publisher: None,
            watchers: HashMap::new(),
            latest_video_sequence_header: None,
            latest_audio_sequence_header: None,
        });

    // Is someone already publishing on this stream key?
    if let Some(id) = &stream_key_connections.publisher {
        error!(
            "Connection {} requested publishing to '{}/{}', but connection {} is already \
        publishing to this stream key",
            connection_id, rtmp_app, stream_key, id
        );

        let _ = connection
            .response_channel
            .send(ConnectionResponse::RequestRejected);

        return None;
    }

    if !is_ip_allowed(&connection.socket_address, &registrant.ip_restrictions) {
        error!(
            "Connection {} requested publishing to '{}/{}', but the client's ip address of '{}' \
        is not allowed",
            connection_id,
            rtmp_app,
            stream_key,
            connection.socket_address.ip()
        );

        let _ = connection
            .response_channel
            .send(ConnectionResponse::RequestRejected);

        return None;
    }

    if registrant.requires_registrant_approval && !connection.received_registrant_approval {
        info!(
            "Connection {} requested publishing to '{}/{}' but requires approval from the \
            registrant first",
            connection_id, rtmp_app, stream_key
        );

        connection.state = ConnectionState::WaitingForPublishValidation {
            rtmp_app,
            stream_key: stream_key.clone(),
        };

        let (sender, receiver) = channel();
        let _ = registrant.response_channel.send(
            RtmpEndpointPublisherMessage::PublisherRequiringApproval {
                stream_key: stream_key.clone(),
                connection_id: connection_id.clone(),
                response_channel: sender,
            },
        );

        let future = wait_for_validation(port, connection_id.clone(), receiver).boxed();

        return Some(future);
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

    let _ = connection
        .response_channel
        .send(ConnectionResponse::PublishRequestAccepted {
            channel: registrant.response_channel.clone(),
        });

    let _ = registrant
        .response_channel
        .send(RtmpEndpointPublisherMessage::NewPublisherConnected {
            connection_id: connection_id.clone(),
            stream_key: stream_key.clone(),
            stream_id,
            reactor_keep_alive_channel,
        });

    return None;
}

#[instrument(skip(port_map))]
fn handle_connection_request_connect_to_app(
    connection_id: &ConnectionId,
    port_map: &mut PortMapping,
    port: u16,
    rtmp_app: String,
) {
    let connection = match port_map.connections.get_mut(&connection_id) {
        Some(x) => x,
        None => {
            warn!("Connection handler for connection {} sent a request to connect to an rtmp app on port {}, \
            but that connection isn't being tracked.", connection_id, port);

            return;
        }
    };
    let response = if !port_map.rtmp_applications.contains_key(rtmp_app.as_str()) {
        info!(
            "Connection {} requested connection to RTMP app '{}' which isn't registered yet",
            connection_id, rtmp_app
        );

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

#[instrument(skip(port_map))]
fn clean_disconnected_connection(connection_id: ConnectionId, port_map: &mut PortMapping) {
    let connection = match port_map.connections.remove(&connection_id) {
        Some(x) => x,
        None => return,
    };

    info!("Connection {} disconnected.  Cleaning it up", connection_id);
    match connection.state {
        ConnectionState::None => (),
        ConnectionState::WaitingForPublishValidation { .. } => (),
        ConnectionState::WaitingForWatchValidation { .. } => (),
        ConnectionState::Publishing {
            rtmp_app,
            stream_key,
        } => match port_map.rtmp_applications.get_mut(rtmp_app.as_str()) {
            None => (),
            Some(app_map) => match app_map.active_stream_keys.get_mut(stream_key.as_str()) {
                None => (),
                Some(active_key) => {
                    match &active_key.publisher {
                        None => (),
                        Some(publisher_id) => {
                            if *publisher_id == connection_id {
                                active_key.publisher = None;
                                active_key.latest_video_sequence_header = None;
                                active_key.latest_audio_sequence_header = None;

                                let registrant = match app_map
                                    .publisher_registrants
                                    .get(&StreamKeyRegistration::Any)
                                {
                                    Some(x) => Some(x),
                                    None => app_map
                                        .publisher_registrants
                                        .get(&StreamKeyRegistration::Exact(stream_key.clone())),
                                };

                                if let Some(registrant) = registrant {
                                    let _ = registrant.response_channel.send(
                                        RtmpEndpointPublisherMessage::PublishingStopped {
                                            connection_id,
                                        },
                                    );
                                }
                            }
                        }
                    };
                }
            },
        },

        ConnectionState::Watching {
            rtmp_app,
            stream_key,
        } => match port_map.rtmp_applications.get_mut(rtmp_app.as_str()) {
            None => (),
            Some(app_map) => match app_map.active_stream_keys.get_mut(stream_key.as_str()) {
                None => (),
                Some(active_key) => {
                    active_key.watchers.remove(&connection_id);

                    if active_key.watchers.is_empty() {
                        let registrant =
                            match app_map.watcher_registrants.get(&StreamKeyRegistration::Any) {
                                Some(x) => Some(x),
                                None => app_map
                                    .watcher_registrants
                                    .get(&StreamKeyRegistration::Exact(stream_key.clone())),
                            };

                        if let Some(registrant) = registrant {
                            let _ = registrant.response_channel.send(
                                RtmpEndpointWatcherNotification::StreamKeyBecameInactive {
                                    stream_key,
                                },
                            );
                        }
                    }
                }
            },
        },
    };
}

mod internal_futures {
    use super::{
        FutureResult, RtmpEndpointPublisherMessage, RtmpEndpointRequest, StreamKeyRegistration,
    };
    use crate::endpoints::rtmp_server::actor::connection_handler::ConnectionRequest;
    use crate::endpoints::rtmp_server::{
        RtmpEndpointMediaMessage, RtmpEndpointWatcherNotification, ValidationResponse,
    };
    use crate::net::tcp::TcpSocketResponse;
    use crate::net::ConnectionId;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
    use tokio::sync::oneshot::Receiver;

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
    ) -> FutureResult {
        sender.closed().await;

        FutureResult::PublishingRegistrantGone {
            port,
            app: app_name,
            stream_key,
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

    pub(super) async fn wait_for_validation(
        port: u16,
        connection_id: ConnectionId,
        receiver: Receiver<ValidationResponse>,
    ) -> FutureResult {
        match receiver.await {
            Ok(response) => {
                FutureResult::ValidationApprovalResponseReceived(port, connection_id, response)
            }
            Err(_) => FutureResult::ValidationApprovalResponseReceived(
                port,
                connection_id,
                ValidationResponse::Reject,
            ),
        }
    }
}

fn is_ip_allowed(client_socket: &SocketAddr, ip_restrictions: &IpRestriction) -> bool {
    match ip_restrictions {
        IpRestriction::None => return true,
        IpRestriction::Allow(allowed_ips) => {
            if let SocketAddr::V4(client_ip) = client_socket {
                return allowed_ips.into_iter().any(|ip| ip.matches(client_ip.ip()));
            }

            return false; // ipv6 clients not supported atm
        }

        IpRestriction::Deny(denied_ips) => {
            if let SocketAddr::V4(client_ip) = client_socket {
                return denied_ips.into_iter().all(|ip| !ip.matches(client_ip.ip()));
            }

            return false; // ipv6
        }
    };
}
