use super::RtmpEndpointPublisherMessage;
use crate::rtmp_server::RtmpEndpointMediaData;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{BufMut, Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use mmids_core::codecs::{AudioCodec, VideoCodec};
use mmids_core::net::tcp::OutboundPacket;
use mmids_core::net::ConnectionId;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::sessions::{
    PublishMode, ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
    StreamMetadata,
};
use rml_rtmp::time::RtmpTimestamp;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, error, info, instrument};

pub struct RtmpServerConnectionHandler {
    id: ConnectionId,
    state: ConnectionState,
    handshake: Handshake,
    rtmp_session: Option<ServerSession>,
    outgoing_byte_channel: UnboundedSender<OutboundPacket>,
    futures: FuturesUnordered<BoxFuture<'static, FutureResult>>,
    request_sender: UnboundedSender<ConnectionRequest>,
    force_disconnect: bool,
    published_event_channel: Option<UnboundedSender<RtmpEndpointPublisherMessage>>,
    video_parse_error_raised: bool,
    audio_parse_error_raised: bool,
}

#[derive(Debug)]
pub enum ConnectionRequest {
    RequestConnectToApp {
        rtmp_app: Arc<String>,
    },

    RequestPublish {
        rtmp_app: Arc<String>,
        stream_key: Arc<String>,
    },

    RequestWatch {
        rtmp_app: Arc<String>,
        stream_key: Arc<String>,
    },

    PublishFinished,
    PlaybackFinished,
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

    Disconnect,
}

#[derive(Debug, PartialEq)]
enum ConnectionState {
    Handshaking,
    RtmpSessionActive,
    RequestedAppConnection {
        rtmp_app: Arc<String>,
        rtmp_request_id: u32,
    },
    ConnectedToApp {
        rtmp_app: Arc<String>,
    },
    RequestedPublishing {
        rtmp_app: Arc<String>,
        stream_key: Arc<String>,
        rtmp_request_id: u32,
    },
    Publishing {
        rtmp_app: Arc<String>,
        stream_key: Arc<String>,
    },
    RequestedWatch {
        rtmp_app: Arc<String>,
        stream_key: Arc<String>,
        rtmp_request_id: u32,
        stream_id: u32,
    },
    Watching {
        rtmp_app: Arc<String>,
        stream_key: Arc<String>,
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

struct UnwrappedVideo {
    codec: VideoCodec,
    is_keyframe: bool,
    is_sequence_header: bool,
    data: Bytes,
    composition_time_in_ms: i32,
}

struct UnwrappedAudio {
    codec: AudioCodec,
    is_sequence_header: bool,
    data: Bytes,
}

impl RtmpServerConnectionHandler {
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
            video_parse_error_raised: false,
            audio_parse_error_raised: false,
        }
    }

    #[instrument(name = "Connection Handler Execution",
        skip(self, response_receiver, incoming_bytes),
        fields(connection_id = ?self.id))]
    pub async fn run_async(
        mut self,
        response_receiver: UnboundedReceiver<ConnectionResponse>,
        incoming_bytes: UnboundedReceiver<Bytes>,
    ) {
        debug!("Starting new rtmp connection handler");
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
                    "failed to generate p0 and p1 handshake packets: {:?}",
                    error
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
                    info!("Connection disconnected");
                    break;
                }

                FutureResult::RtmpServerEndpointGone => {
                    error!("Connection's rtmp server endpoint is gone");
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

        info!("Rtmp server handler closing");
    }

    fn handle_bytes(&mut self, bytes: Bytes) -> Result<(), ()> {
        match &self.state {
            ConnectionState::Handshaking => {
                let result = match self.handshake.process_bytes(bytes.as_ref()) {
                    Ok(x) => x,
                    Err(error) => {
                        error!("Error handshaking: {:?}", error);
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
                                error!("Failed to create an rtmp server session: {:?}", e);
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
                                error!("Failed to handle initial post-handshake input: {:?}", e);
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
                        error!("Connection Sent invalid bytes: {:?}", e);
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
                        self.handle_rtmp_event_connection_requested(request_id, Arc::new(app_name));
                    }

                    ServerSessionEvent::PublishStreamRequested {
                        request_id,
                        app_name,
                        stream_key,
                        mode,
                    } => {
                        self.handle_rtmp_event_publish_stream_requested(
                            request_id,
                            Arc::new(app_name),
                            Arc::new(stream_key),
                            mode,
                        );
                    }

                    ServerSessionEvent::StreamMetadataChanged {
                        app_name,
                        stream_key,
                        metadata,
                    } => self.handle_rtmp_event_stream_metadata_changed(
                        Arc::new(app_name),
                        Arc::new(stream_key),
                        metadata,
                    ),

                    ServerSessionEvent::VideoDataReceived {
                        app_name,
                        stream_key,
                        data,
                        timestamp,
                    } => self.handle_rtmp_event_video_data_received(
                        Arc::new(app_name),
                        Arc::new(stream_key),
                        data,
                        timestamp,
                    ),

                    ServerSessionEvent::AudioDataReceived {
                        app_name,
                        stream_key,
                        data,
                        timestamp,
                    } => self.handle_rtmp_event_audio_data_received(
                        Arc::new(app_name),
                        Arc::new(stream_key),
                        data,
                        timestamp,
                    ),

                    ServerSessionEvent::PlayStreamRequested {
                        app_name,
                        stream_key,
                        stream_id,
                        request_id,
                        reset: _,
                        duration: _,
                        start_at: _,
                    } => self.handle_rtmp_event_play_stream_requested(
                        Arc::new(app_name),
                        Arc::new(stream_key),
                        stream_id,
                        request_id,
                    ),

                    ServerSessionEvent::PublishStreamFinished {
                        app_name,
                        stream_key,
                    } => self.handle_rtmp_event_publish_finished(
                        Arc::new(app_name),
                        Arc::new(stream_key),
                    ),

                    ServerSessionEvent::PlayStreamFinished {
                        app_name,
                        stream_key,
                    } => self
                        .handle_rtmp_event_play_finished(Arc::new(app_name), Arc::new(stream_key)),

                    event => {
                        info!("Connection raised RTMP event: {:?}", event);
                    }
                },

                ServerSessionResult::UnhandleableMessageReceived(payload) => {
                    info!(
                        "Connection sent an unhandleable RTMP message: {:?}",
                        payload
                    );
                }
            }
        }
    }

    fn handle_rtmp_event_play_finished(&mut self, app_name: Arc<String>, stream_key: Arc<String>) {
        match &self.state {
            ConnectionState::Watching {
                rtmp_app: active_app,
                stream_key: active_key,
                stream_id: _,
            } => {
                if *active_app != app_name {
                    error!(
                        requested_app = %app_name,
                        active_app = %active_app,
                        "Connection requested to stop playback on an app it's not connected to"
                    );

                    self.force_disconnect = true;
                    return;
                }

                if *active_key != stream_key {
                    error!(
                        requested_key = %stream_key,
                        active_key = %active_key,
                        "Connection requested to stop playback on a stream key it's not watching"
                    );

                    self.force_disconnect = true;
                    return;
                }

                self.state = ConnectionState::ConnectedToApp { rtmp_app: app_name };
                let _ = self
                    .request_sender
                    .send(ConnectionRequest::PlaybackFinished);
            }

            _ => {
                error!(
                    "Connection {} requested to stop playback but was in an invalid state: {:?}",
                    self.id, self.state
                );

                self.force_disconnect = true;
            }
        }
    }

    fn handle_rtmp_event_publish_finished(
        &mut self,
        app_name: Arc<String>,
        stream_key: Arc<String>,
    ) {
        match &self.state {
            ConnectionState::Publishing {
                rtmp_app: current_app,
                stream_key: current_key,
            } => {
                if *current_app != app_name {
                    error!(
                        requested_app = %app_name,
                        active_app = %current_app,
                        "Connection requested to stop publishing on an app it's not connected to"
                    );

                    self.force_disconnect = true;
                    return;
                }

                if *current_key != stream_key {
                    error!(
                        requested_key = %stream_key,
                        active_key = %current_key,
                        "Connection requested to stop publishing on a stream key it's not publishing on"
                    );

                    self.force_disconnect = true;
                    return;
                }

                self.state = ConnectionState::ConnectedToApp { rtmp_app: app_name };

                let _ = self.request_sender.send(ConnectionRequest::PublishFinished);
            }

            _ => {
                error!(
                    "Connection {} requested to stop publishing but was in an invalid state: {:?}",
                    self.id, self.state
                );

                self.force_disconnect = true;
            }
        }
    }

    fn handle_rtmp_event_play_stream_requested(
        &mut self,
        app_name: Arc<String>,
        stream_key: Arc<String>,
        stream_id: u32,
        request_id: u32,
    ) {
        match &self.state {
            ConnectionState::ConnectedToApp {
                rtmp_app: current_rtmp_app,
            } => {
                if *current_rtmp_app != app_name {
                    error!("Connection requested playback on rtmp app {}, but it's currently connected \
                                        to rtmp app '{}'", app_name, current_rtmp_app);

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
            }
        }
    }

    fn handle_rtmp_event_audio_data_received(
        &mut self,
        app_name: Arc<String>,
        stream_key: Arc<String>,
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
                        "Connection sent audio data for '{}/{}', but this connection is \
                    currently publishing on '{}/{}'",
                        app_name, stream_key, current_rtmp_app, current_stream_key
                    );

                    self.force_disconnect = true;
                    return;
                }

                let UnwrappedAudio {
                    data,
                    is_sequence_header,
                    codec,
                } = unwrap_audio_from_flv(data);

                let _ = self.published_event_channel.as_ref().unwrap().send(
                    RtmpEndpointPublisherMessage::NewAudioData {
                        publisher: self.id.clone(),
                        codec,
                        data,
                        timestamp,
                        is_sequence_header,
                    },
                );
            }

            _ => {
                error!(
                    "Connection sent audio data is not in a publishing state: {:?}",
                    self.state
                );

                self.force_disconnect = true;
            }
        }
    }

    fn handle_rtmp_event_video_data_received(
        &mut self,
        app_name: Arc<String>,
        stream_key: Arc<String>,
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
                        "Connection sent video data for '{}/{}', but this connection is currently publishing on '{}/{}'",
                        app_name, stream_key, current_rtmp_app, current_stream_key
                    );

                    self.force_disconnect = true;
                    return;
                }

                let UnwrappedVideo {
                    data,
                    codec,
                    is_keyframe,
                    is_sequence_header: is_parameter_set,
                    composition_time_in_ms,
                } = unwrap_video_from_flv(data);

                let _ = self.published_event_channel.as_ref().unwrap().send(
                    RtmpEndpointPublisherMessage::NewVideoData {
                        publisher: self.id.clone(),
                        codec,
                        is_keyframe,
                        is_sequence_header: is_parameter_set,
                        data,
                        timestamp,
                        composition_time_offset: composition_time_in_ms,
                    },
                );
            }

            _ => {
                error!(
                    "Connection sent video data is not in a publishing state: {:?}",
                    self.state
                );

                self.force_disconnect = true;
            }
        }
    }

    fn handle_rtmp_event_stream_metadata_changed(
        &mut self,
        app_name: Arc<String>,
        stream_key: Arc<String>,
        metadata: StreamMetadata,
    ) {
        match &self.state {
            ConnectionState::Publishing {
                stream_key: current_stream_key,
                rtmp_app: current_rtmp_app,
            } => {
                if *current_rtmp_app != app_name || *current_stream_key != stream_key {
                    error!(
                        "Connection sent a stream metadata changed for '{}/{}', but \
                    this connection is currently publishing on '{}/{}'",
                        app_name, stream_key, current_rtmp_app, current_stream_key
                    );

                    self.force_disconnect = true;
                    return;
                }

                info!("Connection sent new stream metadata: {:?}", metadata);

                let _ = self.published_event_channel.as_ref().unwrap().send(
                    RtmpEndpointPublisherMessage::StreamMetadataChanged {
                        publisher: self.id.clone(),
                        metadata,
                    },
                );
            }

            _ => {
                error!(
                    "Connection sent stream metadata but is not in a publishing state: {:?}",
                    self.state
                );

                self.force_disconnect = true;
            }
        }
    }

    fn handle_rtmp_event_publish_stream_requested(
        &mut self,
        request_id: u32,
        app_name: Arc<String>,
        stream_key: Arc<String>,
        mode: PublishMode,
    ) {
        info!(
            "Connection requesting publishing to '{}/{}'",
            app_name, stream_key
        );

        if mode != PublishMode::Live {
            error!("Connection requested publishing with publish mode {:?}, but only publish mode Live is supported", mode);

            self.force_disconnect = true;
            return;
        }

        match &self.state {
            ConnectionState::ConnectedToApp {
                rtmp_app: connected_app,
            } => {
                if *connected_app != app_name {
                    error!(
                        "Connection's publish request was for rtmp app '{}' but it's already connected to rtmp app '{}'",
                        connected_app, app_name
                    );

                    self.force_disconnect = true;
                    return;
                }
            }

            _ => {
                error!(
                    "Connection was in state {:?}, which isn't meant for publishing",
                    self.state
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

    fn handle_rtmp_event_connection_requested(&mut self, request_id: u32, app_name: Arc<String>) {
        info!(
            "Connection requesting connection to rtmp app '{}'",
            app_name
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
                info!("Disconnecting connection due to rejected request");
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

            ConnectionResponse::Disconnect => {
                info!("Disconnect requested");
                self.force_disconnect = true;
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
                    "Connections request to watch '{}/{}' was accepted",
                    rtmp_app, stream_key
                );
                let results = match self
                    .rtmp_session
                    .as_mut()
                    .unwrap()
                    .accept_request(*rtmp_request_id)
                {
                    Ok(x) => x,
                    Err(e) => {
                        error!("Error when accepting watch request: {:?}", e);
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
                error!(
                    "Connection had a watch request accepted, but it isn't in a valid requesting \
                        state (current state: {:?})",
                    state
                );

                self.force_disconnect = true;
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
                    "Connections request to publish on '{}/{}' was accepted",
                    rtmp_app, stream_key
                );
                let results = match self
                    .rtmp_session
                    .as_mut()
                    .unwrap()
                    .accept_request(*rtmp_request_id)
                {
                    Ok(x) => x,
                    Err(e) => {
                        error!("Error when accepting publish request: {:?}", e);
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
                error!("Connection had a request accepted, but isn't in a requesting state (current state: {:?})", state);

                self.force_disconnect = true;
            }
        }
    }

    #[instrument(skip(self), fields(connection_id = ?self.id))]
    fn handle_endpoint_app_connect_request_accepted(&mut self) {
        match &self.state {
            ConnectionState::RequestedAppConnection {
                rtmp_request_id,
                rtmp_app,
            } => {
                info!(
                    "Connection's request to connect to the rtmp app {} was accepted",
                    rtmp_app
                );
                let results = match self
                    .rtmp_session
                    .as_mut()
                    .unwrap()
                    .accept_request(*rtmp_request_id)
                {
                    Ok(x) => x,
                    Err(e) => {
                        error!("Error when accepting app connection request: {:?}", e);
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
                error!(
                    "Connection had an rtmp app request accepted, but isn't in a requesting state \
                        (current state: {:?})",
                    state
                );
            }
        }
    }

    #[instrument(skip(self), fields(connection_id = ?self.id))]
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

            RtmpEndpointMediaData::NewVideoData {
                data,
                timestamp,
                is_keyframe,
                is_sequence_header,
                codec,
                composition_time_offset,
            } => {
                let flv_video = match wrap_video_into_flv(
                    data,
                    codec,
                    is_keyframe,
                    is_sequence_header,
                    composition_time_offset,
                ) {
                    Ok(x) => x,
                    Err(()) => {
                        if !self.video_parse_error_raised {
                            error!(
                                "Connection received video that could not be wrapped in FLV format"
                            );
                            self.video_parse_error_raised = true;
                        }

                        return;
                    }
                };

                session.send_video_data(stream_id, flv_video, timestamp, !is_keyframe)
            }

            RtmpEndpointMediaData::NewAudioData {
                data,
                timestamp,
                codec,
                is_sequence_header,
            } => {
                let flv_audio = match wrap_audio_into_flv(data, codec, is_sequence_header) {
                    Ok(x) => x,
                    Err(()) => {
                        if !self.audio_parse_error_raised {
                            error!(
                                "Connection received audio that could not be wrapped in FLV format"
                            );
                            self.audio_parse_error_raised = true;
                        }

                        return;
                    }
                };

                session.send_audio_data(stream_id, flv_audio, timestamp, is_sequence_header)
            }
        };

        let packet = match session_results {
            Ok(x) => x,
            Err(e) => {
                error!(
                    "Connection failed to generate packet for media data: {:?}",
                    e
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

fn unwrap_video_from_flv(mut data: Bytes) -> UnwrappedVideo {
    if data.len() < 2 {
        return UnwrappedVideo {
            codec: VideoCodec::Unknown,
            is_keyframe: false,
            is_sequence_header: false,
            data,
            composition_time_in_ms: 0,
        };
    }

    let flv_tag = data.split_to(1);
    let avc_header = data.split_to(4);

    let is_sequence_header;
    let codec = if flv_tag[0] & 0x07 == 0x07 {
        is_sequence_header = avc_header[0] == 0x00;
        VideoCodec::H264
    } else {
        is_sequence_header = false;
        VideoCodec::Unknown
    };

    let is_keyframe = flv_tag[0] & 0x10 == 0x10;

    let composition_time = Cursor::new(&avc_header[1..]).read_i24::<BigEndian>();
    let composition_time = if let Ok(offset) = composition_time {
        offset
    } else {
        error!("Failed to read composition time offset for some reason.  This shouldn't happen.  Assuming 0");
        0
    };

    UnwrappedVideo {
        codec,
        is_keyframe,
        is_sequence_header,
        data,
        composition_time_in_ms: composition_time,
    }
}

fn wrap_video_into_flv(
    data: Bytes,
    codec: VideoCodec,
    is_keyframe: bool,
    is_sequence_header: bool,
    composition_time_offset: i32,
) -> Result<Bytes, ()> {
    match codec {
        VideoCodec::H264 => {
            let flv_tag = if is_keyframe { 0x17 } else { 0x27 };
            let avc_type = if is_sequence_header { 0 } else { 1 };

            let mut header = vec![flv_tag, avc_type];
            if let Err(error) = header.write_i24::<BigEndian>(composition_time_offset) {
                error!("Failed to write composition time offset: {error:?}");
                return Err(());
            }

            let mut wrapped = BytesMut::new();
            wrapped.extend(header);
            wrapped.extend(data);

            Ok(wrapped.freeze())
        }

        VideoCodec::Unknown => {
            // Can't wrap unknown codec into FLV
            Err(())
        }
    }
}

fn unwrap_audio_from_flv(mut data: Bytes) -> UnwrappedAudio {
    if data.len() < 2 {
        return UnwrappedAudio {
            codec: AudioCodec::Unknown,
            is_sequence_header: false,
            data,
        };
    }

    let flv_tag = data.split_to(1);
    let packet_type = data.split_to(1);
    let is_sequence_header = packet_type[0] == 0;
    let codec = if flv_tag[0] & 0xa0 == 0xa0 {
        AudioCodec::Aac
    } else {
        AudioCodec::Unknown
    };

    UnwrappedAudio {
        codec,
        is_sequence_header,
        data,
    }
}

fn wrap_audio_into_flv(
    data: Bytes,
    codec: AudioCodec,
    is_sequence_header: bool,
) -> Result<Bytes, ()> {
    match codec {
        AudioCodec::Aac => {
            let flv_tag = 0xaf;
            let packet_type = if is_sequence_header { 0 } else { 1 };
            let mut wrapped = BytesMut::new();
            wrapped.put_u8(flv_tag);
            wrapped.put_u8(packet_type);
            wrapped.extend(data);

            Ok(wrapped.freeze())
        }

        AudioCodec::Unknown => {
            // Need to know the codec to wrap it into flv
            Err(())
        }
    }
}

mod internal_futures {
    use super::{ConnectionResponse, FutureResult};
    use crate::rtmp_server::RtmpEndpointMediaData;
    use bytes::Bytes;
    use mmids_core::net::tcp::OutboundPacket;
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
