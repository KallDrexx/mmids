use crate::net::ConnectionId;
use rml_rtmp::sessions::{
    PublishMode, ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
    StreamMetadata,
};

use super::RtmpEndpointPublisherMessage;
use crate::endpoints::rtmp_server::RtmpEndpointMediaData;
use crate::net::tcp::OutboundPacket;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use log::{debug, error, info};
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::time::RtmpTimestamp;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

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

    RequestWatch {
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

    WatchRequestAccepted {
        channel: UnboundedReceiver<RtmpEndpointMediaData>,
    },
}

#[derive(Debug, PartialEq)]
enum ConnectionState {
    Handshaking,
    RtmpSessionActive,
    RequestedAppConnection {
        rtmp_app: String,
        rtmp_request_id: u32,
    },
    ConnectedToApp {
        rtmp_app: String,
    },
    RequestedPublishing {
        rtmp_app: String,
        stream_key: String,
        rtmp_request_id: u32,
    },
    Publishing {
        rtmp_app: String,
        stream_key: String,
    },
    RequestedWatch {
        rtmp_app: String,
        stream_key: String,
        rtmp_request_id: u32,
        stream_id: u32,
    },
    Watching {
        rtmp_app: String,
        stream_key: String,
        stream_id: u32,
    },
}

enum FutureResult {
    ResponseReceived(ConnectionResponse, UnboundedReceiver<ConnectionResponse>),
    BytesReceived(Bytes, UnboundedReceiver<Bytes>),
    WatchedMediaReceived(
        RtmpEndpointMediaData,
        UnboundedReceiver<RtmpEndpointMediaData>,
    ),

    Disconnected,
    RtmpServerEndpointGone,
}

impl<'a> RtmpServerConnectionHandler<'a> {
    pub fn new(
        id: ConnectionId,
        outgoing_bytes: UnboundedSender<OutboundPacket>,
        request_sender: UnboundedSender<ConnectionRequest>,
    ) -> Self {
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

    pub async fn run_async(
        mut self,
        response_receiver: UnboundedReceiver<ConnectionResponse>,
        incoming_bytes: UnboundedReceiver<Bytes>,
    ) {
        debug!("Starting new rtmp connection handler for {:?}", self.id);
        self.futures
            .push(internal_futures::wait_for_request_response(response_receiver).boxed());

        self.futures
            .push(internal_futures::wait_for_incoming_bytes(incoming_bytes).boxed());

        self.futures.push(
            internal_futures::wait_for_outbound_bytes_closed(self.outgoing_byte_channel.clone())
                .boxed(),
        );

        // Start the handshake process
        let p0_and_p1 = match self.handshake.generate_outbound_p0_and_p1() {
            Ok(x) => x,
            Err(error) => {
                error!(
                    "Connection {} failed to generate p0 and p1 handshake packets: {:?}",
                    self.id, error
                );
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
                    self.futures
                        .push(internal_futures::wait_for_incoming_bytes(receiver).boxed());

                    if self.handle_bytes(bytes).is_err() {
                        break;
                    }
                }

                FutureResult::ResponseReceived(response, receiver) => {
                    self.futures
                        .push(internal_futures::wait_for_request_response(receiver).boxed());

                    self.handle_endpoint_response(response);
                }

                FutureResult::WatchedMediaReceived(data, receiver) => {
                    self.futures
                        .push(internal_futures::wait_for_media_data(receiver).boxed());

                    self.handle_media_from_endpoint(data);
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

                    HandshakeProcessResult::Completed {
                        response_bytes,
                        remaining_bytes,
                    } => {
                        let _ = self.outgoing_byte_channel.send(OutboundPacket {
                            bytes: Bytes::from(response_bytes),
                            can_be_dropped: false,
                        });

                        let config = ServerSessionConfig::new();
                        let (session, results) = match ServerSession::new(config) {
                            Ok(x) => x,
                            Err(e) => {
                                error!(
                                    "Connection {} failed to create an rtmp server session: {:?}",
                                    self.id, e
                                );
                                return Err(());
                            }
                        };

                        self.rtmp_session = Some(session);
                        self.handle_rtmp_results(results);
                        self.state = ConnectionState::RtmpSessionActive;

                        let results = match self
                            .rtmp_session
                            .as_mut()
                            .unwrap()
                            .handle_input(&remaining_bytes)
                        {
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
                let session_results = match self
                    .rtmp_session
                    .as_mut()
                    .unwrap()
                    .handle_input(bytes.as_ref())
                {
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

                ServerSessionResult::RaisedEvent(event) => match event {
                    ServerSessionEvent::ConnectionRequested {
                        request_id,
                        app_name,
                    } => {
                        self.handle_rtmp_event_connection_requested(request_id, app_name);
                    }

                    ServerSessionEvent::PublishStreamRequested {
                        request_id,
                        app_name,
                        stream_key,
                        mode,
                    } => {
                        self.handle_rtmp_event_publish_stream_requested(
                            request_id, app_name, stream_key, mode,
                        );
                    }

                    ServerSessionEvent::StreamMetadataChanged {
                        app_name,
                        stream_key,
                        metadata,
                    } => self
                        .handle_rtmp_event_stream_metadata_changed(app_name, stream_key, metadata),

                    ServerSessionEvent::VideoDataReceived {
                        app_name,
                        stream_key,
                        data,
                        timestamp,
                    } => self.handle_rtmp_event_video_data_received(
                        app_name, stream_key, data, timestamp,
                    ),

                    ServerSessionEvent::AudioDataReceived {
                        app_name,
                        stream_key,
                        data,
                        timestamp,
                    } => self.handle_rtmp_event_audio_data_received(
                        app_name, stream_key, data, timestamp,
                    ),

                    ServerSessionEvent::PlayStreamRequested {
                        app_name,
                        stream_key,
                        stream_id,
                        request_id,
                        ..
                    } => self.handle_rtmp_event_play_stream_requested(
                        app_name, stream_key, stream_id, request_id,
                    ),

                    event => {
                        info!("Connection {} raised RTMP event: {:?}", self.id, event);
                    }
                },

                ServerSessionResult::UnhandleableMessageReceived(payload) => {
                    info!(
                        "Connection {} sent an unhandleable RTMP message: {:?}",
                        self.id, payload
                    );
                }
            }
        }
    }

    fn handle_rtmp_event_play_stream_requested(
        &mut self,
        app_name: String,
        stream_key: String,
        stream_id: u32,
        request_id: u32,
    ) {
        match &self.state {
            ConnectionState::ConnectedToApp {
                rtmp_app: current_rtmp_app,
            } => {
                if *current_rtmp_app != app_name {
                    error!("Connection {} requested playback on rtmp app {}, but it's currently connected \
                                        to rtmp app '{}'", self.id, app_name, current_rtmp_app);

                    self.force_disconnect = true;
                    return;
                }

                self.state = ConnectionState::RequestedWatch {
                    rtmp_app: app_name.clone(),
                    stream_key: stream_key.clone(),
                    stream_id,
                    rtmp_request_id: request_id,
                };

                let _ = self.request_sender.send(ConnectionRequest::RequestWatch {
                    rtmp_app: app_name,
                    stream_key,
                });
            }

            _ => {
                error!(
                    "Connection {} requested playback but was in an invalid state: {:?}",
                    self.id, self.state
                );
                self.force_disconnect = true;
                return;
            }
        }
    }

    fn handle_rtmp_event_audio_data_received(
        &mut self,
        app_name: String,
        stream_key: String,
        data: Bytes,
        timestamp: RtmpTimestamp,
    ) {
        match &self.state {
            ConnectionState::Publishing {
                stream_key: current_stream_key,
                rtmp_app: current_rtmp_app,
            } => {
                if *current_rtmp_app != app_name || *current_stream_key != stream_key {
                    error!(
                        "Connection {} sent audio data for '{}/{}', but \
                                        this connection is currently publishing on '{}/{}'",
                        self.id, app_name, stream_key, current_rtmp_app, current_stream_key
                    );

                    self.force_disconnect = true;
                    return;
                }

                let _ = self.published_event_channel.as_ref().unwrap().send(
                    RtmpEndpointPublisherMessage::NewAudioData {
                        publisher: self.id.clone(),
                        data,
                        timestamp,
                    },
                );
            }

            _ => {
                error!(
                    "Connection {} sent audio data is not in a publishing state: {:?}",
                    self.id, self.state
                );

                self.force_disconnect = true;
                return;
            }
        }
    }

    fn handle_rtmp_event_video_data_received(
        &mut self,
        app_name: String,
        stream_key: String,
        data: Bytes,
        timestamp: RtmpTimestamp,
    ) {
        match &self.state {
            ConnectionState::Publishing {
                stream_key: current_stream_key,
                rtmp_app: current_rtmp_app,
            } => {
                if *current_rtmp_app != app_name || *current_stream_key != stream_key {
                    error!(
                        "Connection {} sent video data for '{}/{}', but \
                                        this connection is currently publishing on '{}/{}'",
                        self.id, app_name, stream_key, current_rtmp_app, current_stream_key
                    );

                    self.force_disconnect = true;
                    return;
                }

                let _ = self.published_event_channel.as_ref().unwrap().send(
                    RtmpEndpointPublisherMessage::NewVideoData {
                        publisher: self.id.clone(),
                        data,
                        timestamp,
                    },
                );
            }

            _ => {
                error!(
                    "Conenction {} sent video data is not in a publishing state: {:?}",
                    self.id, self.state
                );

                self.force_disconnect = true;
                return;
            }
        }
    }

    fn handle_rtmp_event_stream_metadata_changed(
        &mut self,
        app_name: String,
        stream_key: String,
        metadata: StreamMetadata,
    ) {
        match &self.state {
            ConnectionState::Publishing {
                stream_key: current_stream_key,
                rtmp_app: current_rtmp_app,
            } => {
                if *current_rtmp_app != app_name || *current_stream_key != stream_key {
                    error!(
                        "Connection {} sent a stream metadata changed for '{}/{}', but \
                                        this connection is currently publishing on '{}/{}'",
                        self.id, app_name, stream_key, current_rtmp_app, current_stream_key
                    );

                    self.force_disconnect = true;
                    return;
                }

                info!(
                    "Connection {} sent new stream metadata: {:?}",
                    self.id, metadata
                );

                let _ = self.published_event_channel.as_ref().unwrap().send(
                    RtmpEndpointPublisherMessage::StreamMetadataChanged {
                        publisher: self.id.clone(),
                        metadata,
                    },
                );
            }

            _ => {
                error!(
                    "Conenction {} sent stream metadata but is not in a publishing state: {:?}",
                    self.id, self.state
                );

                self.force_disconnect = true;
                return;
            }
        }
    }

    fn handle_rtmp_event_publish_stream_requested(
        &mut self,
        request_id: u32,
        app_name: String,
        stream_key: String,
        mode: PublishMode,
    ) {
        info!(
            "Connection {} requesting publishing to '{}/{}'",
            self.id, app_name, stream_key
        );

        if mode != PublishMode::Live {
            error!(
                "Connection {} requested publishing with publish mode {:?}, but \
                                only publish mode Live is supported",
                self.id, mode
            );

            self.force_disconnect = true;
            return;
        }

        match &self.state {
            ConnectionState::ConnectedToApp {
                rtmp_app: connected_app,
            } => {
                if *connected_app != app_name {
                    error!(
                        "Connection {}'s publish request was for rtmp app '{}' but it's \
                                        already connected to rtmp app '{}'",
                        self.id, connected_app, app_name
                    );

                    self.force_disconnect = true;
                    return;
                }
            }

            _ => {
                error!(
                    "Connection {} was in state {:?}, which isn't meant for publishing",
                    self.id, self.state
                );

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

    fn handle_rtmp_event_connection_requested(&mut self, request_id: u32, app_name: String) {
        info!(
            "Connection {} requesting connection to rtmp app '{}'",
            self.id, app_name
        );

        let _ = self
            .request_sender
            .send(ConnectionRequest::RequestConnectToApp {
                rtmp_app: app_name.clone(),
            });

        self.state = ConnectionState::RequestedAppConnection {
            rtmp_app: app_name,
            rtmp_request_id: request_id,
        };
    }

    fn handle_endpoint_response(&mut self, response: ConnectionResponse) {
        match response {
            ConnectionResponse::RequestRejected => {
                info!(
                    "Disconnecting connection {:?} due to rejected request",
                    self.id
                );
                self.force_disconnect = true;
            }

            ConnectionResponse::AppConnectRequestAccepted => {
                self.handle_endpoint_app_connect_request_accepted();
            }

            ConnectionResponse::PublishRequestAccepted { channel } => {
                self.handle_endpoint_publish_request_accepted(channel);
            }

            ConnectionResponse::WatchRequestAccepted { channel } => {
                self.handle_endpoint_watch_request_accepted(channel);
            }
        }
    }

    fn handle_endpoint_watch_request_accepted(
        &mut self,
        media_channel: UnboundedReceiver<RtmpEndpointMediaData>,
    ) {
        self.futures
            .push(internal_futures::wait_for_media_data(media_channel).boxed());

        match &self.state {
            ConnectionState::RequestedWatch {
                rtmp_app,
                stream_key,
                rtmp_request_id,
                stream_id,
            } => {
                info!(
                    "Connection {}'s request to watch '{}/{}' was accepted",
                    self.id, rtmp_app, stream_key
                );
                let results = match self
                    .rtmp_session
                    .as_mut()
                    .unwrap()
                    .accept_request(*rtmp_request_id)
                {
                    Ok(x) => x,
                    Err(e) => {
                        error!(
                            "Error from connection {} when accepting watch request: {:?}",
                            self.id, e
                        );
                        self.force_disconnect = true;
                        return;
                    }
                };

                self.state = ConnectionState::Watching {
                    rtmp_app: (*rtmp_app).clone(),
                    stream_key: (*stream_key).clone(),
                    stream_id: *stream_id,
                };

                self.handle_rtmp_results(results);
            }

            state => {
                error!("Conenction {} had a watch request accepted, but it isn't in a valid requesting \
                        state (current state: {:?})", self.id, state);

                self.force_disconnect = true;
                return;
            }
        }
    }

    fn handle_endpoint_publish_request_accepted(
        &mut self,
        channel: UnboundedSender<RtmpEndpointPublisherMessage>,
    ) {
        match &self.state {
            ConnectionState::RequestedPublishing {
                rtmp_app,
                stream_key,
                rtmp_request_id,
            } => {
                info!(
                    "Connection {}'s request to publish on '{}/{}' was accepted",
                    self.id, rtmp_app, stream_key
                );
                let results = match self
                    .rtmp_session
                    .as_mut()
                    .unwrap()
                    .accept_request(*rtmp_request_id)
                {
                    Ok(x) => x,
                    Err(e) => {
                        error!(
                            "Error from connection {} when accepting publish request: {:?}",
                            self.id, e
                        );
                        self.force_disconnect = true;
                        return;
                    }
                };

                self.published_event_channel = Some(channel);
                self.state = ConnectionState::Publishing {
                    rtmp_app: (*rtmp_app).clone(),
                    stream_key: (*stream_key).clone(),
                };
                self.handle_rtmp_results(results);
            }

            state => {
                error!(
                    "Connection {:?} had a request accepted, but isn't in a requesting state \
                        (current state: {:?})",
                    self.id, state
                );

                self.force_disconnect = true;
                return;
            }
        }
    }

    fn handle_endpoint_app_connect_request_accepted(&mut self) {
        match &self.state {
            ConnectionState::RequestedAppConnection {
                rtmp_request_id,
                rtmp_app,
            } => {
                info!(
                    "Connection {}'s request to connect to the rtmp app {} was accepted",
                    self.id, rtmp_app
                );
                let results = match self
                    .rtmp_session
                    .as_mut()
                    .unwrap()
                    .accept_request(*rtmp_request_id)
                {
                    Ok(x) => x,
                    Err(e) => {
                        error!(
                            "Error from connection {} when accepting app connection request: {:?}",
                            self.id, e
                        );
                        self.force_disconnect = true;

                        return;
                    }
                };

                self.state = ConnectionState::ConnectedToApp {
                    rtmp_app: (*rtmp_app).clone(),
                };
                self.handle_rtmp_results(results);
            }

            state => {
                error!("Connection {:?} had an rtmp app request accepted, but isn't in a requesting state \
                        (current state: {:?})", self.id, state);
            }
        }
    }

    fn handle_media_from_endpoint(&mut self, media_data: RtmpEndpointMediaData) {
        let stream_id = match &self.state {
            ConnectionState::Watching { stream_id, .. } => *stream_id,
            _ => return, // Not in a state that can receive media
        };

        let session = self.rtmp_session.as_mut().unwrap();
        let session_results = match media_data {
            RtmpEndpointMediaData::NewStreamMetaData { metadata } => {
                session.send_metadata(stream_id, &metadata)
            }

            RtmpEndpointMediaData::NewVideoData { data, timestamp } =>
            // TODO: figure out if the video packet can be dropped
            {
                session.send_video_data(stream_id, data, timestamp, false)
            }

            RtmpEndpointMediaData::NewAudioData { data, timestamp } =>
            // TODO: figure out if the video packet can be dropped
            {
                session.send_audio_data(stream_id, data, timestamp, false)
            }
        };

        let packet = match session_results {
            Ok(x) => x,
            Err(e) => {
                error!(
                    "Connection {} failed to generate packet for media data: {:?}",
                    self.id, e
                );
                self.force_disconnect = true;
                return;
            }
        };

        let _ = self.outgoing_byte_channel.send(OutboundPacket {
            bytes: Bytes::from(packet.bytes),
            can_be_dropped: packet.can_be_dropped,
        });
    }
}

mod internal_futures {
    use super::{ConnectionResponse, FutureResult};
    use crate::endpoints::rtmp_server::RtmpEndpointMediaData;
    use crate::net::tcp::OutboundPacket;
    use bytes::Bytes;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

    pub(super) async fn wait_for_request_response(
        mut receiver: UnboundedReceiver<ConnectionResponse>,
    ) -> super::FutureResult {
        match receiver.recv().await {
            None => FutureResult::RtmpServerEndpointGone,
            Some(x) => FutureResult::ResponseReceived(x, receiver),
        }
    }

    pub(super) async fn wait_for_incoming_bytes(
        mut receiver: UnboundedReceiver<Bytes>,
    ) -> super::FutureResult {
        match receiver.recv().await {
            None => FutureResult::Disconnected,
            Some(x) => FutureResult::BytesReceived(x, receiver),
        }
    }

    pub(super) async fn wait_for_outbound_bytes_closed(
        sender: UnboundedSender<OutboundPacket>,
    ) -> super::FutureResult {
        sender.closed().await;

        FutureResult::Disconnected
    }

    pub(super) async fn wait_for_media_data(
        mut receiver: UnboundedReceiver<RtmpEndpointMediaData>,
    ) -> super::FutureResult {
        match receiver.recv().await {
            None => FutureResult::RtmpServerEndpointGone,
            Some(data) => FutureResult::WatchedMediaReceived(data, receiver),
        }
    }
}
