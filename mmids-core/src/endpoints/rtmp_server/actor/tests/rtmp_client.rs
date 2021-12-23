use crate::net::tcp::{OutboundPacket, RequestFailureReason, TcpSocketRequest, TcpSocketResponse};
use crate::net::ConnectionId;
use bytes::Bytes;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::sessions::{
    ClientSession, ClientSessionConfig, ClientSessionError, ClientSessionEvent,
    ClientSessionResult, PublishRequestType, StreamMetadata,
};
use rml_rtmp::time::RtmpTimestamp;
use std::net::{SocketAddr, SocketAddrV4};
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time::timeout;

pub const CONNECTION_ID: &'static str = "test-1234";

pub struct RtmpTestClient {
    socket_manager_receiver: UnboundedReceiver<TcpSocketRequest>,
    socket_manager_response_sender: Option<UnboundedSender<TcpSocketResponse>>,
    port: Option<u16>,
    connection: Option<Connection>,
}

struct Connection {
    incoming_bytes: UnboundedSender<Bytes>,
    outgoing_bytes: UnboundedReceiver<OutboundPacket>,
    session: ClientSession,
}

impl RtmpTestClient {
    pub fn new() -> (Self, UnboundedSender<TcpSocketRequest>) {
        let (sender, receiver) = unbounded_channel();
        let client = RtmpTestClient {
            socket_manager_receiver: receiver,
            socket_manager_response_sender: None,
            port: None,
            connection: None,
        };

        (client, sender)
    }

    pub async fn accept_port_request(&mut self, port: u16, use_tls: bool) {
        let request = match timeout(
            Duration::from_millis(10),
            self.socket_manager_receiver.recv(),
        )
        .await
        {
            Ok(Some(request)) => request,
            Ok(None) => panic!("Channel closed"),
            Err(_) => panic!("Accept port request timed out"),
        };

        match request {
            TcpSocketRequest::OpenPort {
                port: requested_port,
                use_tls: requested_tls,
                response_channel,
            } => {
                assert_eq!(
                    requested_port, port,
                    "Requested port was not the expected port"
                );
                assert_eq!(
                    requested_tls, use_tls,
                    "Requested TLS flag was not expected"
                );

                if response_channel.is_closed() {
                    panic!("Response channel was closed");
                }

                if self.socket_manager_response_sender.is_some() {
                    panic!("Port already registered");
                }

                let _ = response_channel.send(TcpSocketResponse::RequestAccepted {});
                self.socket_manager_response_sender = Some(response_channel);
                self.port = Some(port);
            }
        }
    }

    pub async fn deny_port_request(&mut self, port: u16, use_tls: bool) {
        let request = match timeout(
            Duration::from_millis(10),
            self.socket_manager_receiver.recv(),
        )
        .await
        {
            Ok(Some(request)) => request,
            Ok(None) => panic!("Channel closed"),
            Err(_) => panic!("Accept port request timed out"),
        };

        match request {
            TcpSocketRequest::OpenPort {
                port: requested_port,
                use_tls: requested_tls,
                response_channel,
            } => {
                assert_eq!(
                    requested_port, port,
                    "Requested port was not the expected port"
                );
                assert_eq!(
                    requested_tls, use_tls,
                    "Requested TLS flag was not expected"
                );

                if response_channel.is_closed() {
                    panic!("Response channel was closed");
                }

                if self.socket_manager_response_sender.is_some() {
                    panic!("Port already registered");
                }

                let _ = response_channel.send(TcpSocketResponse::RequestDenied {
                    reason: RequestFailureReason::PortInUse,
                });
            }
        }
    }

    pub async fn expect_empty_request_channel(&mut self) {
        match timeout(
            Duration::from_millis(10),
            self.socket_manager_receiver.recv(),
        )
        .await
        {
            Err(_) => (),
            _ => panic!("Expected timeout, got request instead"),
        };
    }

    pub async fn assert_connection_sender_closed(&mut self) {
        let connection = self
            .connection
            .as_mut()
            .expect("Connection not established yet");

        match timeout(
            Duration::from_millis(10),
            connection.incoming_bytes.closed(),
        )
        .await
        {
            Ok(()) => return,
            Err(_) => panic!("Response sender not closed as expected (not disconnected"),
        }
    }

    pub async fn perform_handshake(&mut self) {
        if self.connection.is_some() {
            panic!("Only one connection is supported at a time");
        }

        let connection_id = ConnectionId(CONNECTION_ID.to_string());
        let (incoming_sender, incoming_receiver) = unbounded_channel();
        let (outgoing_sender, mut outgoing_receiver) = unbounded_channel();

        self.socket_manager_response_sender
            .as_ref()
            .unwrap()
            .send(TcpSocketResponse::NewConnection {
                port: self.port.unwrap(),
                connection_id: connection_id.clone(),
                incoming_bytes: incoming_receiver,
                outgoing_bytes: outgoing_sender,
                socket_address: SocketAddr::V4(SocketAddrV4::new([127, 0, 0, 1].into(), 1234)),
            })
            .expect("Failed to send new connection signal");

        let mut handshake = Handshake::new(PeerType::Client);
        let p0_and_p1 = handshake
            .generate_outbound_p0_and_p1()
            .expect("Failed to generate p0 and p1");

        incoming_sender
            .send(Bytes::from(p0_and_p1))
            .expect("incoming bytes channel closed");
        let response = match timeout(Duration::from_millis(100), outgoing_receiver.recv()).await {
            Ok(Some(response)) => response,
            _ => panic!("Failed to get outgoing bytes"),
        };

        let result = handshake
            .process_bytes(&response.bytes)
            .expect("Failed to process received p0 and p1 packet");

        let response_bytes = match result {
            HandshakeProcessResult::InProgress { response_bytes } => response_bytes,
            HandshakeProcessResult::Completed { .. } => {
                panic!("Did not expect to be completed after first packet")
            }
        };

        incoming_sender
            .send(Bytes::from(response_bytes))
            .expect("Incoming bytes channel closed");
        let response = match timeout(Duration::from_millis(100), outgoing_receiver.recv()).await {
            Ok(Some(response)) => response,
            _ => panic!("Failed to get outgoing bytes"),
        };

        let result = handshake
            .process_bytes(&response.bytes)
            .expect("Failed to process p2 packet");

        match result {
            HandshakeProcessResult::InProgress { .. } => {
                panic!("Did not expect to still be in progress after 2nd packet")
            }
            HandshakeProcessResult::Completed {
                remaining_bytes, ..
            } => {
                if remaining_bytes.len() > 0 {
                    panic!("Expected no leftover bytes after handshake completed");
                }
            }
        }

        let (mut session, client_results) = ClientSession::new(ClientSessionConfig::new())
            .expect("Failed to generate client session");

        for result in client_results {
            match result {
                ClientSessionResult::OutboundResponse(packet) => {
                    incoming_sender
                        .send(Bytes::from(packet.bytes))
                        .expect("Incoming bytes channel closed");
                }

                x => panic!("Unexpected session result of {:?}", x),
            }
        }

        // Handle any initial messages the server may send (like chunks size)
        loop {
            let packet = match timeout(Duration::from_millis(10), outgoing_receiver.recv()).await {
                Ok(Some(packet)) => packet,
                Ok(None) => panic!("outgoing receiver sender closed"),
                Err(_) => break,
            };

            let results = session
                .handle_input(&packet.bytes)
                .expect("Error processing bytes");

            for result in results {
                match result {
                    ClientSessionResult::OutboundResponse(packet) => {
                        incoming_sender
                            .send(Bytes::from(packet.bytes))
                            .expect("Incoming bytes channel closed");
                    }

                    _ => (),
                }
            }
        }

        self.connection = Some(Connection {
            session,
            incoming_bytes: incoming_sender,
            outgoing_bytes: outgoing_receiver,
        })
    }

    pub async fn connect_to_app(&mut self, app: String, should_succeed: bool) {
        self.execute_session_method_single_result(|session| session.request_connection(app));

        if should_succeed {
            let connection = self.connection.as_mut().unwrap();
            let response =
                match timeout(Duration::from_millis(10), connection.outgoing_bytes.recv()).await {
                    Ok(Some(response)) => response,
                    _ => panic!("No response for connection request was given"),
                };

            let results = connection
                .session
                .handle_input(&response.bytes)
                .expect("Failed to process results");

            // Client will send back an event and a window acknowledgement message
            let mut event_raised = false;
            for result in results {
                match result {
                    ClientSessionResult::RaisedEvent(
                        ClientSessionEvent::ConnectionRequestAccepted,
                    ) => event_raised = true,

                    _ => (),
                }
            }

            if !event_raised {
                panic!("No connection request accepted event raised");
            }
        }
    }

    pub async fn publish_to_stream_key(&mut self, stream_key: String, should_succeed: bool) {
        self.execute_session_method_single_result(|session| {
            session.request_publishing(stream_key, PublishRequestType::Live)
        });

        // `createStream` should always succeed
        let response = match timeout(
            Duration::from_millis(10),
            self.connection.as_mut().unwrap().outgoing_bytes.recv(),
        )
        .await
        {
            Ok(Some(response)) => response,
            _ => panic!("No response for connection request was given"),
        };

        // handle create stream response
        self.execute_session_method_vec_result(|session| session.handle_input(&response.bytes));

        if should_succeed {
            let connection = self.connection.as_mut().unwrap();
            let mut all_results = Vec::new();
            loop {
                let response = match timeout(
                    Duration::from_millis(10),
                    connection.outgoing_bytes.recv(),
                )
                .await
                {
                    Ok(Some(response)) => response,
                    Ok(None) => panic!("Outgoing bytes channel closed"),
                    Err(_) => break, // no more packets coming in
                };

                let results = connection
                    .session
                    .handle_input(&response.bytes)
                    .expect("Failed to process results");

                all_results.extend(results);
            }

            assert_eq!(all_results.len(), 1, "Only one result expected");
            match all_results.remove(0) {
                ClientSessionResult::RaisedEvent(ClientSessionEvent::PublishRequestAccepted) => (),
                result => panic!("Unexpected result seen: {:?}", result),
            }
        }
    }

    pub async fn watch_stream_key(&mut self, stream_key: String, should_succeed: bool) {
        self.execute_session_method_single_result(|session| session.request_playback(stream_key));

        // `createStream` should always succeed
        let response = match timeout(
            Duration::from_millis(10),
            self.connection.as_mut().unwrap().outgoing_bytes.recv(),
        )
        .await
        {
            Ok(Some(response)) => response,
            _ => panic!("No response for connection request was given"),
        };

        // handle create stream response
        self.execute_session_method_vec_result(|session| session.handle_input(&response.bytes));

        if should_succeed {
            let connection = self.connection.as_mut().unwrap();
            let mut all_results = Vec::new();
            loop {
                let response = match timeout(
                    Duration::from_millis(10),
                    connection.outgoing_bytes.recv(),
                )
                .await
                {
                    Ok(Some(response)) => response,
                    Ok(None) => panic!("Outgoing bytes channel closed"),
                    Err(_) => break, // no more packets coming in
                };

                let results = connection
                    .session
                    .handle_input(&response.bytes)
                    .expect("Failed to process results");

                all_results.extend(results);
            }

            let mut accepted_event_received = false;
            for result in all_results {
                match result {
                    ClientSessionResult::RaisedEvent(
                        ClientSessionEvent::PlaybackRequestAccepted,
                    ) => accepted_event_received = true,

                    _ => (),
                }
            }

            assert!(
                accepted_event_received,
                "PlaybackRequestAccepted event not raised"
            );
        }
    }

    pub async fn stop_watching(&mut self) {
        self.execute_session_method_vec_result(|session| session.stop_playback());
    }

    pub fn disconnect(&mut self) {
        self.connection = None;
    }

    pub async fn stop_publishing(&mut self) {
        self.execute_session_method_vec_result(|session| session.stop_publishing());
    }

    pub fn publish_metadata(&mut self, metadata: StreamMetadata) {
        self.execute_session_method_single_result(|session| session.publish_metadata(&metadata));
    }

    pub fn publish_video(&mut self, data: Bytes, timestamp: RtmpTimestamp) {
        self.execute_session_method_single_result(|session| {
            session.publish_video_data(data, timestamp, false)
        });
    }

    pub fn publish_audio(&mut self, data: Bytes, timestamp: RtmpTimestamp) {
        self.execute_session_method_single_result(|session| {
            session.publish_audio_data(data, timestamp, false)
        });
    }

    fn execute_session_method_single_result(
        &mut self,
        function: impl FnOnce(&mut ClientSession) -> Result<ClientSessionResult, ClientSessionError>,
    ) {
        let connection = self
            .connection
            .as_mut()
            .expect("Connection not established yet");

        let result = function(&mut connection.session).expect("Client session returned error");

        match result {
            ClientSessionResult::OutboundResponse(packet) => connection
                .incoming_bytes
                .send(Bytes::from(packet.bytes))
                .expect("Failed to send stop publishing command"),

            x => panic!("Unexpected session result: {:?}", x),
        }
    }

    fn execute_session_method_vec_result(
        &mut self,
        function: impl FnOnce(
            &mut ClientSession,
        ) -> Result<Vec<ClientSessionResult>, ClientSessionError>,
    ) {
        let connection = self
            .connection
            .as_mut()
            .expect("Connection not established yet");

        let results = function(&mut connection.session).expect("Client session returned error");

        for result in results {
            match result {
                ClientSessionResult::OutboundResponse(packet) => connection
                    .incoming_bytes
                    .send(Bytes::from(packet.bytes))
                    .expect("Failed to send packet"),

                x => panic!("Unexpected session result: {:?}", x),
            }
        }
    }

    pub async fn get_next_event(&mut self) -> Option<ClientSessionEvent> {
        let connection = self
            .connection
            .as_mut()
            .expect("Connection not established yet");

        loop {
            let packet =
                match timeout(Duration::from_millis(10), connection.outgoing_bytes.recv()).await {
                    Ok(Some(packet)) => packet,
                    _ => break,
                };

            let results = connection
                .session
                .handle_input(&packet.bytes)
                .expect("Failed to handle packet");

            for result in results {
                match result {
                    ClientSessionResult::RaisedEvent(event) => return Some(event),
                    _ => (),
                }
            }
        }

        return None;
    }
}
