use crate::net::ConnectionId;
use rml_rtmp::sessions::{
    ServerSession,
    ServerSessionConfig,
    ServerSessionResult,
    ServerSessionEvent,
    PublishMode,
};

use rml_rtmp::handshake::{Handshake, PeerType, HandshakeProcessResult};
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver};
use crate::net::tcp::OutboundPacket;
use futures::stream::FuturesUnordered;
use futures::future::BoxFuture;
use log::{debug, info, error};
use bytes::Bytes;
use futures::{StreamExt, FutureExt};
use super::RtmpEndpointPublisherMessage;

pub struct RtmpServerConnectionHandler<'a> {
    id: ConnectionId,
    state: ConnectionState,
    handshake: Handshake,
    rtmp_session: Option<ServerSession>,
    outgoing_byte_channel: UnboundedSender<OutboundPacket>,
    futures: FuturesUnordered<BoxFuture<'a, FutureResult>>,
    request_sender: UnboundedSender<ConnectionRequest>,
    force_disconnect: bool,
    published_event_channel: Option<UnboundedSender<RtmpEndpointPublisherMessage>>,
}

#[derive(Debug)]
pub enum ConnectionRequest {
    RequestConnectToApp {
        rtmp_app: String,
    },

    RequestPublish {
        rtmp_app: String,
        stream_key: String,
    },
}

pub enum ConnectionResponse {
    RequestRejected,
    AppConnectRequestAccepted,

    PublishRequestAccepted {
        channel: UnboundedSender<RtmpEndpointPublisherMessage>,
    },
}

#[derive(Debug, PartialEq)]
enum ConnectionState {
    Handshaking,
    RtmpSessionActive,
    RequestedAppConnection { rtmp_app: String, rtmp_request_id: u32 },
    ConnectedToApp { rtmp_app: String },
    RequestedPublishing { rtmp_app: String, stream_key: String, rtmp_request_id: u32 },
    Publishing { rtmp_app: String, stream_key: String },
}

enum FutureResult {
    ResponseReceived(ConnectionResponse, UnboundedReceiver<ConnectionResponse>),
    BytesReceived(Bytes, UnboundedReceiver<Bytes>),

    Disconnected,
    RtmpServerEndpointGone,
}

impl<'a> RtmpServerConnectionHandler<'a> {
    pub fn new(id: ConnectionId,
               outgoing_bytes: UnboundedSender<OutboundPacket>,
               request_sender: UnboundedSender<ConnectionRequest>) -> Self {
        RtmpServerConnectionHandler {
            id,
            state: ConnectionState::Handshaking,
            handshake: Handshake::new(PeerType::Server),
            rtmp_session: None,
            outgoing_byte_channel: outgoing_bytes,
            futures: FuturesUnordered::new(),
            request_sender,
            force_disconnect: false,
            published_event_channel: None,
        }
    }

    pub async fn run_async(mut self,
                           response_receiver: UnboundedReceiver<ConnectionResponse>,
                           incoming_bytes: UnboundedReceiver<Bytes>) {
        debug!("Starting new rtmp connection handler for {:?}", self.id);
        self.futures.push(
            internal_futures::wait_for_request_response(response_receiver).boxed(),
        );

        self.futures.push(
            internal_futures::wait_for_incoming_bytes(incoming_bytes).boxed(),
        );

        self.futures.push(
            internal_futures::wait_for_outbound_bytes_closed(self.outgoing_byte_channel.clone()).boxed(),
        );

        // Start the handshake process
        let p0_and_p1 = match self.handshake.generate_outbound_p0_and_p1() {
            Ok(x) => x,
            Err(error) => {
                error!("Connection {} failed to generate p0 and p1 handshake packets: {:?}", self.id, error);
                return;
            }
        };

        let _ = self.outgoing_byte_channel.send(OutboundPacket {
            bytes: Bytes::from(p0_and_p1),
            can_be_dropped: false,
        });

        while let Some(result) = self.futures.next().await {
            match result {
                FutureResult::Disconnected => {
                    info!("Connection {} has disconnected", self.id);
                    break;
                }

                FutureResult::RtmpServerEndpointGone => {
                    error!("Connection {}'s rtmp server endpoint is gone", self.id);
                    break;
                }

                FutureResult::BytesReceived(bytes, receiver) => {
                    self.futures.push(
                        internal_futures::wait_for_incoming_bytes(receiver).boxed(),
                    );

                    if self.handle_bytes(bytes).is_err() {
                        break;
                    }
                }

                FutureResult::ResponseReceived(response, receiver) => {
                    self.futures.push(
                        internal_futures::wait_for_request_response(receiver).boxed(),
                    );

                    self.handle_endpoint_response(response);
                }
            }

            if self.force_disconnect {
                break;
            }
        }

        info!("Connection {} rtmp server handler closing", self.id);
    }

    fn handle_bytes(&mut self, bytes: Bytes) -> Result<(), ()> {
        match &self.state {
            ConnectionState::Handshaking => {
                let result = match self.handshake.process_bytes(bytes.as_ref()) {
                    Ok(x) => x,
                    Err(error) => {
                        error!("Connection {} error handshaking: {:?}", self.id, error);
                        return Err(());
                    }
                };

                match result {
                    HandshakeProcessResult::InProgress { response_bytes } => {
                        let _ = self.outgoing_byte_channel.send(OutboundPacket {
                            bytes: Bytes::from(response_bytes),
                            can_be_dropped: false,
                        });
                    }

                    HandshakeProcessResult::Completed { response_bytes, remaining_bytes } => {
                        let _ = self.outgoing_byte_channel.send(OutboundPacket {
                            bytes: Bytes::from(response_bytes),
                            can_be_dropped: false,
                        });

                        let config = ServerSessionConfig::new();
                        let (session, results) = match ServerSession::new(config) {
                            Ok(x) => x,
                            Err(e) => {
                                error!("Connection {} failed to create an rtmp server session: {:?}", self.id, e);
                                return Err(());
                            }
                        };

                        self.rtmp_session = Some(session);
                        self.handle_rtmp_results(results);
                        self.state = ConnectionState::RtmpSessionActive;

                        let results = match self.rtmp_session.as_mut().unwrap().handle_input(&remaining_bytes) {
                            Ok(x) => x,
                            Err(e) => {
                                error!("Connection {} failed to handle initial post-handshake input: {:?}", self.id, e);
                                return Err(());
                            }
                        };

                        self.handle_rtmp_results(results);
                    }
                }
            }

            _ => {
                // Any other state means that we have a server session active
                let session_results = match self.rtmp_session.as_mut().unwrap().handle_input(bytes.as_ref()) {
                    Ok(x) => x,
                    Err(e) => {
                        error!("Connection {} sent invalid bytes: {:?}", self.id, e);
                        return Err(());
                    }
                };

                self.handle_rtmp_results(session_results);
            }
        };

        Ok(())
    }

    fn handle_rtmp_results(&mut self, results: Vec<ServerSessionResult>) {
        for result in results {
            match result {
                ServerSessionResult::OutboundResponse(packet) => {
                    let packet = OutboundPacket {
                        can_be_dropped: packet.can_be_dropped,
                        bytes: Bytes::from(packet.bytes),
                    };

                    let _ = self.outgoing_byte_channel.send(packet);
                }

                ServerSessionResult::RaisedEvent(event) => {
                    match event {
                        ServerSessionEvent::ConnectionRequested { request_id, app_name } => {
                            info!("Connection {} requesting connection to rtmp app '{}'", self.id, app_name);

                            let _ = self.request_sender.send(ConnectionRequest::RequestConnectToApp {
                                rtmp_app: app_name.clone()
                            });

                            self.state = ConnectionState::RequestedAppConnection {
                                rtmp_app: app_name,
                                rtmp_request_id: request_id,
                            };
                        }

                        ServerSessionEvent::PublishStreamRequested { request_id, app_name, stream_key, mode } => {
                            info!("Connection {} requesting publishing to '{}/{}'", self.id, app_name, stream_key);

                            if mode != PublishMode::Live {
                                error!("Connection {} requested publishing with publish mode {:?}, but \
                                only publish mode Live is supported", self.id, mode);

                                self.force_disconnect = true;
                                return;
                            }

                            match &self.state {
                                ConnectionState::ConnectedToApp { rtmp_app: connected_app } => {
                                    if *connected_app != app_name {
                                        error!("Connection {}'s publish request was for rtmp app '{}' but it's \
                                        already connected to rtmp app '{}'", self.id, connected_app, app_name);

                                        self.force_disconnect = true;
                                        return;
                                    }
                                }

                                _ => {
                                    error!("Connection {} was in state {:?}, which isn't meant for publishing",
                                    self.id, self.state);

                                    self.force_disconnect = true;
                                    return;
                                }
                            };

                            let _ = self.request_sender.send(ConnectionRequest::RequestPublish {
                                rtmp_app: app_name.clone(),
                                stream_key: stream_key.clone(),
                            });

                            self.state = ConnectionState::RequestedPublishing {
                                rtmp_app: app_name,
                                stream_key,
                                rtmp_request_id: request_id,
                            };
                        }

                        ServerSessionEvent::StreamMetadataChanged { app_name, stream_key, metadata } => {
                            match &self.state {
                                ConnectionState::Publishing { stream_key: current_stream_key, rtmp_app: current_rtmp_app } => {
                                    if *current_rtmp_app != app_name || *current_stream_key != stream_key {
                                        error!("Connection {} sent a stream metadata changed for '{}/{}', but \
                                        this connection is currently publishing on '{}/{}'",
                                            self.id, app_name, stream_key, current_rtmp_app, current_stream_key);

                                        self.force_disconnect = true;
                                        return;
                                    }

                                    info!("Connection {} sent new stream metadata: {:?}", self.id, metadata);

                                    let _ = self.published_event_channel.as_ref().unwrap().send(RtmpEndpointPublisherMessage::StreamMetadataChanged {
                                        publisher: self.id.clone(),
                                        metadata,
                                    });
                                }

                                _ => {
                                    error!("Conenction {} sent stream metadata but is not in a publishing state: {:?}",
                                        self.id, self.state);

                                    self.force_disconnect = true;
                                    return;
                                }
                            }
                        }

                        ServerSessionEvent::VideoDataReceived { app_name, stream_key, data, timestamp } => {
                            match &self.state {
                                ConnectionState::Publishing { stream_key: current_stream_key, rtmp_app: current_rtmp_app } => {
                                    if *current_rtmp_app != app_name || *current_stream_key != stream_key {
                                        error!("Connection {} sent video data for '{}/{}', but \
                                        this connection is currently publishing on '{}/{}'",
                                            self.id, app_name, stream_key, current_rtmp_app, current_stream_key);

                                        self.force_disconnect = true;
                                        return;
                                    }

                                    let _ = self.published_event_channel.as_ref().unwrap().send(RtmpEndpointPublisherMessage::NewVideoData {
                                        publisher: self.id.clone(),
                                        data,
                                        timestamp,
                                    });
                                }

                                _ => {
                                    error!("Conenction {} sent video data is not in a publishing state: {:?}",
                                        self.id, self.state);

                                    self.force_disconnect = true;
                                    return;
                                }
                            }
                        }

                        ServerSessionEvent::AudioDataReceived { app_name, stream_key, data, timestamp } => {
                            match &self.state {
                                ConnectionState::Publishing { stream_key: current_stream_key, rtmp_app: current_rtmp_app } => {
                                    if *current_rtmp_app != app_name || *current_stream_key != stream_key {
                                        error!("Connection {} sent audio data for '{}/{}', but \
                                        this connection is currently publishing on '{}/{}'",
                                            self.id, app_name, stream_key, current_rtmp_app, current_stream_key);

                                        self.force_disconnect = true;
                                        return;
                                    }

                                    let _ = self.published_event_channel.as_ref().unwrap().send(RtmpEndpointPublisherMessage::NewAudioData {
                                        publisher: self.id.clone(),
                                        data,
                                        timestamp,
                                    });
                                }

                                _ => {
                                    error!("Conenction {} sent audio data is not in a publishing state: {:?}",
                                        self.id, self.state);

                                    self.force_disconnect = true;
                                    return;
                                }
                            }
                        }

                        event => {
                            info!("Connection {} raised RTMP event: {:?}", self.id, event);
                        }
                    }
                }

                ServerSessionResult::UnhandleableMessageReceived(payload) => {
                    info!("Connection {} sent an unhandleable RTMP message: {:?}", self.id, payload);
                }
            }
        }
    }

    fn handle_endpoint_response(&mut self, response: ConnectionResponse) {
        match response {
            ConnectionResponse::RequestRejected => {
                info!("Disconnecting connection {:?} due to rejected request", self.id);
                self.force_disconnect = true;
            }

            ConnectionResponse::AppConnectRequestAccepted => {
                match &self.state {
                    ConnectionState::RequestedAppConnection { rtmp_request_id, rtmp_app } => {
                        info!("Connection {}'s request to connect to the rtmp app {} was accepted", self.id, rtmp_app);
                        let results = match self.rtmp_session.as_mut().unwrap().accept_request(*rtmp_request_id) {
                            Ok(x) => x,
                            Err(e) => {
                                error!("Error from connection {} when accepting app connection request: {:?}", self.id, e);
                                self.force_disconnect = true;

                                return;
                            }
                        };

                        self.state = ConnectionState::ConnectedToApp { rtmp_app: (*rtmp_app).clone() };
                        self.handle_rtmp_results(results);
                    }

                    state => {
                        error!("Connection {:?} had an rtmp app request accepted, but isn't in a requesting state \
                        (current state: {:?})", self.id, state);
                    }
                }
            }

            ConnectionResponse::PublishRequestAccepted { channel } => {
                match &self.state {
                    ConnectionState::RequestedPublishing { rtmp_app, stream_key, rtmp_request_id } => {
                        info!("Connection {}'s request to publish on '{}/{}' was accepted", self.id, rtmp_app, stream_key);
                        let results = match self.rtmp_session.as_mut().unwrap().accept_request(*rtmp_request_id) {
                            Ok(x) => x,
                            Err(e) => {
                                error!("Error from connection {} when accepting publish request: {:?}", self.id, e);
                                self.force_disconnect = true;
                                return;
                            }
                        };

                        self.published_event_channel = Some(channel);
                        self.state = ConnectionState::Publishing { rtmp_app: (*rtmp_app).clone(), stream_key: (*stream_key).clone() };
                        self.handle_rtmp_results(results);
                    }

                    state => {
                        error!("Connection {:?} had a request accepted, but isn't in a requesting state \
                        (current state: {:?})", self.id, state);
                    }
                }
            }
        }
    }
}

mod internal_futures {
    use bytes::Bytes;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
    use super::{FutureResult, ConnectionResponse};
    use crate::net::tcp::OutboundPacket;

    pub(super) async fn wait_for_request_response(mut receiver: UnboundedReceiver<ConnectionResponse>) -> super::FutureResult {
        match receiver.recv().await {
            None => FutureResult::RtmpServerEndpointGone,
            Some(x) => FutureResult::ResponseReceived(x, receiver),
        }
    }

    pub(super) async fn wait_for_incoming_bytes(mut receiver: UnboundedReceiver<Bytes>) -> super::FutureResult {
        match receiver.recv().await {
            None => FutureResult::Disconnected,
            Some(x) => FutureResult::BytesReceived(x, receiver),
        }
    }

    pub(super) async fn wait_for_outbound_bytes_closed(sender: UnboundedSender<OutboundPacket>) -> super::FutureResult {
        sender.closed().await;

        FutureResult::Disconnected
    }
}
