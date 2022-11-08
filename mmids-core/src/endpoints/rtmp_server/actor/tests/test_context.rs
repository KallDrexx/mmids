use crate::endpoints::rtmp_server::actor::tests::rtmp_client::RtmpTestClient;
use crate::endpoints::rtmp_server::{
    start_rtmp_server_endpoint, IpRestriction, RtmpEndpointMediaMessage,
    RtmpEndpointPublisherMessage, RtmpEndpointRequest, RtmpEndpointWatcherNotification,
    StreamKeyRegistration,
};
use crate::{test_utils, StreamId};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

const RTMP_APP: &str = "app";

pub struct TestContextBuilder {
    port: Option<u16>,
    use_tls: Option<bool>,
    requires_registrant_approval: Option<bool>,
    stream_id: Option<Option<StreamId>>,
    ip_restriction: Option<IpRestriction>,
    rtmp_app: Option<String>,
    rtmp_stream_key: Option<StreamKeyRegistration>,
}

pub struct TestContext {
    pub endpoint: UnboundedSender<RtmpEndpointRequest>,
    pub client: RtmpTestClient,
    pub publish_receiver: Option<UnboundedReceiver<RtmpEndpointPublisherMessage>>,
    pub watch_receiver: Option<UnboundedReceiver<RtmpEndpointWatcherNotification>>,
    pub media_sender: Option<UnboundedSender<RtmpEndpointMediaMessage>>,
    pub rtmp_app: String,
}

impl TestContextBuilder {
    pub fn new() -> Self {
        Self {
            port: None,
            use_tls: None,
            requires_registrant_approval: None,
            stream_id: None,
            ip_restriction: None,
            rtmp_app: None,
            rtmp_stream_key: None,
        }
    }

    pub fn set_stream_key(mut self, stream_key: StreamKeyRegistration) -> Self {
        self.rtmp_stream_key = Some(stream_key);
        self
    }

    pub fn set_requires_registrant_approval(mut self, requires_approval: bool) -> Self {
        self.requires_registrant_approval = Some(requires_approval);
        self
    }

    pub async fn into_publisher(self) -> TestContext {
        let (sender, receiver) = unbounded_channel();
        let request = RtmpEndpointRequest::ListenForPublishers {
            port: self.port.unwrap_or(9999),
            use_tls: self.use_tls.unwrap_or(false),
            requires_registrant_approval: self.requires_registrant_approval.unwrap_or(false),
            stream_id: self.stream_id.unwrap_or(None),
            ip_restrictions: self.ip_restriction.unwrap_or(IpRestriction::None),
            rtmp_app: self.rtmp_app.unwrap_or_else(|| RTMP_APP.to_string()),
            rtmp_stream_key: self.rtmp_stream_key.unwrap_or(StreamKeyRegistration::Any),
            message_channel: sender,
        };

        TestContext::new_publisher(request, receiver).await
    }

    pub async fn into_watcher(self) -> TestContext {
        let (notification_sender, notification_receiver) = unbounded_channel();
        let (media_sender, media_receiver) = unbounded_channel();
        let request = RtmpEndpointRequest::ListenForWatchers {
            port: self.port.unwrap_or(9999),
            use_tls: self.use_tls.unwrap_or(false),
            requires_registrant_approval: self.requires_registrant_approval.unwrap_or(false),
            ip_restrictions: self.ip_restriction.unwrap_or(IpRestriction::None),
            rtmp_app: self.rtmp_app.unwrap_or_else(|| RTMP_APP.to_string()),
            rtmp_stream_key: self.rtmp_stream_key.unwrap_or(StreamKeyRegistration::Any),
            notification_channel: notification_sender,
            media_channel: media_receiver,
        };

        TestContext::new_watcher(request, notification_receiver, media_sender).await
    }
}

impl TestContext {
    pub async fn set_as_active_publisher(&mut self) {
        self.client.perform_handshake().await;
        self.client
            .connect_to_app(self.rtmp_app.clone(), true)
            .await;

        self.client
            .publish_to_stream_key("key".to_string(), true)
            .await;

        let receiver = self.publish_receiver.as_mut().unwrap();
        let response = test_utils::expect_mpsc_response(receiver).await;
        match response {
            RtmpEndpointPublisherMessage::NewPublisherConnected { .. } => (),
            message => panic!("Unexpected publisher message received: {:?}", message),
        };
    }

    pub async fn set_as_active_watcher(&mut self) {
        self.client.perform_handshake().await;
        self.client
            .connect_to_app(self.rtmp_app.clone(), true)
            .await;

        self.client.watch_stream_key("key".to_string(), true).await;

        let receiver = self.watch_receiver.as_mut().unwrap();
        let response = test_utils::expect_mpsc_response(receiver).await;
        match response {
            RtmpEndpointWatcherNotification::StreamKeyBecameActive { .. } => (),
            message => panic!("Unexpected publisher message received: {:?}", message),
        };
    }

    async fn new_publisher(
        request: RtmpEndpointRequest,
        mut receiver: UnboundedReceiver<RtmpEndpointPublisherMessage>,
    ) -> TestContext {
        let (mut client, sender) = RtmpTestClient::new();
        let endpoint = start_rtmp_server_endpoint(sender);

        endpoint
            .send(request)
            .expect("Endpoint request failed to send");

        client.accept_port_request(9999, false).await;

        let response = test_utils::expect_mpsc_response(&mut receiver).await;
        match response {
            RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => (),
            x => panic!("Unexpected endpoint response: {:?}", x),
        }

        TestContext {
            client,
            endpoint,
            publish_receiver: Some(receiver),
            watch_receiver: None,
            media_sender: None,
            rtmp_app: RTMP_APP.to_string(),
        }
    }

    async fn new_watcher(
        request: RtmpEndpointRequest,
        mut notification_receiver: UnboundedReceiver<RtmpEndpointWatcherNotification>,
        media_sender: UnboundedSender<RtmpEndpointMediaMessage>,
    ) -> TestContext {
        let (mut client, sender) = RtmpTestClient::new();
        let endpoint = start_rtmp_server_endpoint(sender);

        endpoint
            .send(request)
            .expect("Endpoint request failed to send");

        client.accept_port_request(9999, false).await;

        let response = test_utils::expect_mpsc_response(&mut notification_receiver).await;
        match response {
            RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful => (),
            x => panic!("Unexpected endpoint response: {:?}", x),
        }

        TestContext {
            client,
            endpoint,
            publish_receiver: None,
            watch_receiver: Some(notification_receiver),
            media_sender: Some(media_sender),
            rtmp_app: RTMP_APP.to_string(),
        }
    }
}
