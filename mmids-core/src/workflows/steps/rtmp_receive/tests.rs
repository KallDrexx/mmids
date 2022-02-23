use super::*;
use crate::codecs::{AudioCodec, VideoCodec};
use crate::net::ConnectionId;
use crate::workflows::definitions::WorkflowStepType;
use crate::workflows::steps::StepTestContext;
use crate::workflows::MediaNotificationContent::StreamDisconnected;
use crate::workflows::{MediaNotification, MediaNotificationContent};
use crate::{test_utils, StreamId};
use anyhow::Result;
use bytes::Bytes;
use rml_rtmp::sessions::StreamMetadata;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::sync::oneshot::channel;

struct TestContext {
    step_context: StepTestContext,
    rtmp_endpoint: UnboundedReceiver<RtmpEndpointRequest>,
    reactor_manager: UnboundedReceiver<ReactorManagerRequest>,
}

struct DefinitionBuilder {
    port: Option<u16>,
    app: Option<String>,
    key: Option<String>,
    reactor: Option<String>,
}

impl DefinitionBuilder {
    fn new() -> Self {
        DefinitionBuilder {
            port: None,
            app: None,
            key: None,
            reactor: None,
        }
    }

    fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    fn app(mut self, app: &str) -> Self {
        self.app = Some(app.to_string());
        self
    }

    fn key(mut self, key: &str) -> Self {
        self.key = Some(key.to_string());
        self
    }

    fn reactor_name(mut self, name: &str) -> Self {
        self.reactor = Some(name.to_string());
        self
    }

    fn build(self) -> WorkflowStepDefinition {
        let mut definition = WorkflowStepDefinition {
            step_type: WorkflowStepType("rtmp_receive".to_string()),
            parameters: HashMap::new(),
        };

        if let Some(port) = self.port {
            definition
                .parameters
                .insert(PORT_PROPERTY_NAME.to_string(), Some(port.to_string()));
        }

        if let Some(app) = self.app {
            definition
                .parameters
                .insert(APP_PROPERTY_NAME.to_string(), Some(app));
        } else {
            definition
                .parameters
                .insert(APP_PROPERTY_NAME.to_string(), Some("app".to_string()));
        }

        if let Some(key) = self.key {
            definition
                .parameters
                .insert(STREAM_KEY_PROPERTY_NAME.to_string(), Some(key));
        } else {
            definition
                .parameters
                .insert(STREAM_KEY_PROPERTY_NAME.to_string(), Some("*".to_string()));
        }

        if let Some(reactor) = self.reactor {
            definition
                .parameters
                .insert(REACTOR_NAME.to_string(), Some(reactor));
        }

        definition
    }
}

impl TestContext {
    fn new(definition: WorkflowStepDefinition) -> Result<Self> {
        let (reactor_sender, reactor_receiver) = unbounded_channel();
        let (rtmp_sender, rtmp_receiver) = unbounded_channel();

        let generator = RtmpReceiverStepGenerator {
            reactor_manager: reactor_sender,
            rtmp_endpoint_sender: rtmp_sender,
        };

        let step_context = StepTestContext::new(Box::new(generator), definition)?;

        Ok(TestContext {
            step_context,
            rtmp_endpoint: rtmp_receiver,
            reactor_manager: reactor_receiver,
        })
    }

    async fn accept_registration(&mut self) -> UnboundedSender<RtmpEndpointPublisherMessage> {
        let request = test_utils::expect_mpsc_response(&mut self.rtmp_endpoint).await;
        let channel = match request {
            RtmpEndpointRequest::ListenForPublishers {
                message_channel, ..
            } => {
                message_channel
                    .send(RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful)
                    .expect("Failed to send registration response");

                message_channel
            }

            request => panic!("Unexpected rtmp request seen: {:?}", request),
        };

        self.step_context.execute_pending_notifications().await;

        channel
    }

    async fn get_reactor_channel(&mut self) -> UnboundedSender<ReactorWorkflowUpdate> {
        let request = test_utils::expect_mpsc_response(&mut self.reactor_manager).await;
        match request {
            ReactorManagerRequest::CreateWorkflowForStreamName {
                response_channel, ..
            } => response_channel,
            request => panic!("Unexpected request: {:?}", request),
        }
    }
}

#[tokio::test]
async fn requests_registration_for_publishers() {
    let definition = DefinitionBuilder::new()
        .port(1234)
        .app("some_app")
        .key("some_key")
        .build();

    let mut context = TestContext::new(definition).unwrap();

    let response = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
    match response {
        RtmpEndpointRequest::ListenForPublishers {
            port,
            rtmp_app,
            rtmp_stream_key,
            ..
        } => {
            assert_eq!(port, 1234, "Unexpected port");
            assert_eq!(&rtmp_app, "some_app", "Unexpected rtmp app");
            assert_eq!(
                rtmp_stream_key,
                StreamKeyRegistration::Exact("some_key".to_string()),
                "Unexpected stream key"
            );
        }

        response => panic!("Unexpected rtmp request: {:?}", response),
    }
}

#[tokio::test]
async fn no_port_specified_defaults_to_1935() {
    let mut definition = DefinitionBuilder::new().key("app").key("key").build();

    definition.parameters.remove(PORT_PROPERTY_NAME);
    let mut context = TestContext::new(definition).unwrap();

    let response = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
    match response {
        RtmpEndpointRequest::ListenForPublishers { port, .. } => {
            assert_eq!(port, 1935, "Unexpected port");
        }

        response => panic!("Unexpected rtmp request: {:?}", response),
    }
}

#[tokio::test]
async fn asterisk_stream_key_acts_as_wildcard() {
    let definition = DefinitionBuilder::new().key("*").build();
    let mut context = TestContext::new(definition).unwrap();

    let response = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
    match response {
        RtmpEndpointRequest::ListenForPublishers {
            rtmp_stream_key, ..
        } => {
            assert_eq!(
                rtmp_stream_key,
                StreamKeyRegistration::Any,
                "Unexpected stream key"
            );
        }

        response => panic!("Unexpected rtmp request: {:?}", response),
    }
}

#[tokio::test]
async fn error_if_no_app_specified() {
    let mut definition = DefinitionBuilder::new().build();
    definition.parameters.remove(APP_PROPERTY_NAME);

    match TestContext::new(definition) {
        Ok(_) => panic!("Expecected failure"),
        Err(_) => (),
    }
}

#[tokio::test]
async fn error_if_no_key_specified() {
    let mut definition = DefinitionBuilder::new().build();
    definition.parameters.remove(STREAM_KEY_PROPERTY_NAME);

    match TestContext::new(definition) {
        Ok(_) => panic!("Expecected failure"),
        Err(_) => (),
    }
}

#[test]
fn step_starts_in_created_state() {
    let definition = DefinitionBuilder::new().build();
    let context = TestContext::new(definition).unwrap();

    let status = context.step_context.step.get_status();
    assert_eq!(status, &StepStatus::Created, "Unexpected step status");
}

#[tokio::test]
async fn registration_failure_sets_status_to_error() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    let request = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
    let _channel = match request {
        RtmpEndpointRequest::ListenForPublishers {
            message_channel, ..
        } => {
            message_channel
                .send(RtmpEndpointPublisherMessage::PublisherRegistrationFailed)
                .expect("Failed to send registration response");

            message_channel
        }

        request => panic!("Unexpected rtmp request seen: {:?}", request),
    };

    context.step_context.execute_pending_notifications().await;

    let status = context.step_context.step.get_status();
    match status {
        StepStatus::Error { message: _ } => (),
        _ => panic!("Unexpected status: {:?}", status),
    }
}

#[tokio::test]
async fn registration_success_sets_status_to_active() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    let request = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
    let _channel = match request {
        RtmpEndpointRequest::ListenForPublishers {
            message_channel, ..
        } => {
            message_channel
                .send(RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful)
                .expect("Failed to send registration response");

            message_channel
        }

        request => panic!("Unexpected rtmp request seen: {:?}", request),
    };

    context.step_context.execute_pending_notifications().await;

    let status = context.step_context.step.get_status();
    match status {
        StepStatus::Active => (),
        _ => panic!("Unexpected status: {:?}", status),
    }
}

#[tokio::test]
async fn stream_started_notification_raised_when_publisher_connects() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let channel = context.accept_registration().await;

    channel
        .send(RtmpEndpointPublisherMessage::NewPublisherConnected {
            stream_id: StreamId("test".to_string()),
            stream_key: "abc".to_string(),
            connection_id: ConnectionId("connection".to_string()),
            reactor_update_channel: None,
        })
        .expect("Failed to send publisher connected message");

    context.step_context.execute_pending_notifications().await;

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(&media.stream_id.0, "test", "Unexpected stream id");

    match &media.content {
        MediaNotificationContent::NewIncomingStream { stream_name } => {
            assert_eq!(stream_name, "abc", "Unexpected stream name");
        }

        content => panic!("Unexpected media content: {:?}", content),
    }
}

#[tokio::test]
async fn stream_disconnected_notification_raised_when_publisher_disconnects() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let channel = context.accept_registration().await;

    channel
        .send(RtmpEndpointPublisherMessage::NewPublisherConnected {
            stream_id: StreamId("test".to_string()),
            stream_key: "abc".to_string(),
            connection_id: ConnectionId("connection".to_string()),
            reactor_update_channel: None,
        })
        .expect("Failed to send publisher connected message");

    context.step_context.execute_pending_notifications().await;
    context.step_context.media_outputs.clear();

    channel
        .send(RtmpEndpointPublisherMessage::PublishingStopped {
            connection_id: ConnectionId("connection".to_string()),
        })
        .expect("Failed to send disconnected message");

    context.step_context.execute_pending_notifications().await;

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(&media.stream_id.0, "test", "Unexpected stream id");

    match &media.content {
        MediaNotificationContent::StreamDisconnected => (),
        content => panic!("Unexpected media content: {:?}", content),
    }
}

#[tokio::test]
async fn metadata_notification_raised_when_publisher_sends_one() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let channel = context.accept_registration().await;

    channel
        .send(RtmpEndpointPublisherMessage::NewPublisherConnected {
            stream_id: StreamId("test".to_string()),
            stream_key: "abc".to_string(),
            connection_id: ConnectionId("connection".to_string()),
            reactor_update_channel: None,
        })
        .expect("Failed to send publisher connected message");

    context.step_context.execute_pending_notifications().await;

    let mut metadata = StreamMetadata::new();
    metadata.video_width = Some(1920);

    channel
        .send(RtmpEndpointPublisherMessage::StreamMetadataChanged {
            metadata,
            publisher: ConnectionId("connection".to_string()),
        })
        .expect("Failed to send metadata message");

    context.step_context.execute_pending_notifications().await;

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(&media.stream_id.0, "test", "Unexpected stream id");

    match &media.content {
        MediaNotificationContent::Metadata { data } => {
            assert_eq!(
                data.get("width"),
                Some(&"1920".to_string()),
                "Unexpected width"
            );
        }

        content => panic!("Unexpected media content: {:?}", content),
    }
}

#[tokio::test]
async fn video_notification_received_when_publisher_sends_video() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let channel = context.accept_registration().await;

    channel
        .send(RtmpEndpointPublisherMessage::NewPublisherConnected {
            stream_id: StreamId("test".to_string()),
            stream_key: "abc".to_string(),
            connection_id: ConnectionId("connection".to_string()),
            reactor_update_channel: None,
        })
        .expect("Failed to send publisher connected message");

    context.step_context.execute_pending_notifications().await;

    channel
        .send(RtmpEndpointPublisherMessage::NewVideoData {
            publisher: ConnectionId("connection".to_string()),
            data: Bytes::from(vec![1, 2, 3]),
            codec: VideoCodec::H264,
            timestamp: RtmpTimestamp::new(5),
            is_keyframe: true,
            is_sequence_header: true,
            composition_time_offset: 123,
        })
        .expect("Failed to send video message");

    context.step_context.execute_pending_notifications().await;

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(&media.stream_id.0, "test", "Unexpected stream id");

    match &media.content {
        MediaNotificationContent::Video {
            data,
            timestamp,
            codec,
            is_keyframe,
            is_sequence_header,
        } => {
            assert_eq!(data, &vec![1, 2, 3], "Unexpected video data");
            assert_eq!(timestamp.dts(), Duration::from_millis(5), "Unexpected dts");
            assert_eq!(timestamp.pts_offset(), 123, "Unexpected pts offset");
            assert_eq!(codec, &VideoCodec::H264, "Unexpected codec");
            assert!(is_keyframe, "Expected is_keyframe to be true");
            assert!(is_sequence_header, "Expected is_sequence_header to be true");
        }

        content => panic!("Unexpected media content: {:?}", content),
    }
}

#[tokio::test]
async fn audio_notification_received_when_publisher_sends_audio() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let channel = context.accept_registration().await;

    channel
        .send(RtmpEndpointPublisherMessage::NewPublisherConnected {
            stream_id: StreamId("test".to_string()),
            stream_key: "abc".to_string(),
            connection_id: ConnectionId("connection".to_string()),
            reactor_update_channel: None,
        })
        .expect("Failed to send publisher connected message");

    context.step_context.execute_pending_notifications().await;

    channel
        .send(RtmpEndpointPublisherMessage::NewAudioData {
            publisher: ConnectionId("connection".to_string()),
            data: Bytes::from(vec![1, 2, 3]),
            codec: AudioCodec::Aac,
            timestamp: RtmpTimestamp::new(5),
            is_sequence_header: true,
        })
        .expect("Failed to send audio message");

    context.step_context.execute_pending_notifications().await;

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(&media.stream_id.0, "test", "Unexpected stream id");

    match &media.content {
        MediaNotificationContent::Audio {
            data,
            timestamp,
            codec,
            is_sequence_header,
        } => {
            assert_eq!(data, &vec![1, 2, 3], "Unexpected video data");
            assert_eq!(timestamp, &Duration::from_millis(5), "Unexpected timestamp");
            assert_eq!(codec, &AudioCodec::Aac, "Unexpected codec");
            assert!(is_sequence_header, "Expected is_sequence_header to be true");
        }

        content => panic!("Unexpected media content: {:?}", content),
    }
}

#[test]
fn stream_started_notification_passed_as_input_does_not_get_passed_as_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context
        .step_context
        .assert_media_not_passed_through(MediaNotification {
            stream_id: StreamId("test".to_string()),
            content: MediaNotificationContent::NewIncomingStream {
                stream_name: "name".to_string(),
            },
        });
}

#[test]
fn stream_disconnected_notification_passed_as_input_does_not_get_passed_as_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context
        .step_context
        .assert_media_not_passed_through(MediaNotification {
            stream_id: StreamId("test".to_string()),
            content: StreamDisconnected,
        });
}

#[test]
fn metadata_notification_passed_as_input_does_not_get_passed_as_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context
        .step_context
        .assert_media_not_passed_through(MediaNotification {
            stream_id: StreamId("test".to_string()),
            content: MediaNotificationContent::Metadata {
                data: HashMap::new(),
            },
        });
}

#[test]
fn video_notification_passed_as_input_does_not_get_passed_as_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context
        .step_context
        .assert_media_not_passed_through(MediaNotification {
            stream_id: StreamId("test".to_string()),
            content: MediaNotificationContent::Video {
                data: Bytes::from(vec![1, 2]),
                codec: VideoCodec::H264,
                timestamp: VideoTimestamp::from_durations(Duration::new(0, 0), Duration::new(0, 0)),
                is_keyframe: true,
                is_sequence_header: true,
            },
        });
}

#[test]
fn audio_notification_passed_as_input_does_not_get_passed_as_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context
        .step_context
        .assert_media_not_passed_through(MediaNotification {
            stream_id: StreamId("test".to_string()),
            content: MediaNotificationContent::Audio {
                data: Bytes::from(vec![1, 2]),
                codec: AudioCodec::Aac,
                timestamp: Duration::from_millis(5),
                is_sequence_header: true,
            },
        });
}

#[tokio::test]
async fn approval_required_requested_when_reactor_specified() {
    let definition = DefinitionBuilder::new().reactor_name("abc").build();
    let mut context = TestContext::new(definition).unwrap();
    let request = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
    match request {
        RtmpEndpointRequest::ListenForPublishers {
            requires_registrant_approval,
            ..
        } => {
            assert!(
                requires_registrant_approval,
                "Expected requires approval to be true"
            );
        }

        request => panic!("Unexpected rtmp request seen: {:?}", request),
    };
}

#[tokio::test]
async fn reactor_queried_for_stream_key_when_approval_required() {
    let definition = DefinitionBuilder::new().reactor_name("abc").build();
    let mut context = TestContext::new(definition).unwrap();
    let publish_channel = context.accept_registration().await;

    let (sender, _receiver) = channel();
    publish_channel
        .send(RtmpEndpointPublisherMessage::PublisherRequiringApproval {
            stream_key: "ab123".to_string(),
            connection_id: ConnectionId("connection".to_string()),
            response_channel: sender,
        })
        .expect("Failed to send publisher message");

    context.step_context.execute_pending_notifications().await;

    let request = test_utils::expect_mpsc_response(&mut context.reactor_manager).await;
    match request {
        ReactorManagerRequest::CreateWorkflowForStreamName {
            reactor_name,
            stream_name,
            ..
        } => {
            assert_eq!(&reactor_name, "abc", "Unexpected reactor name");
            assert_eq!(&stream_name, "ab123", "Unexpected stream name");
        }

        request => panic!("Unexpected request received: {:?}", request),
    }
}

#[tokio::test]
async fn rejection_sent_when_reactor_says_stream_is_not_valid() {
    let definition = DefinitionBuilder::new().reactor_name("reactor").build();
    let mut context = TestContext::new(definition).unwrap();
    let publish_channel = context.accept_registration().await;

    let (sender, receiver) = channel();
    publish_channel
        .send(RtmpEndpointPublisherMessage::PublisherRequiringApproval {
            stream_key: "ab123".to_string(),
            connection_id: ConnectionId("connection".to_string()),
            response_channel: sender,
        })
        .expect("Failed to send publisher message");

    context.step_context.execute_pending_notifications().await;
    let reactor_channel = context.get_reactor_channel().await;

    reactor_channel
        .send(ReactorWorkflowUpdate {
            is_valid: false,
            routable_workflow_names: HashSet::new(),
        })
        .expect("Failed to send reactor response");

    context.step_context.execute_pending_notifications().await;

    let response = test_utils::expect_oneshot_response(receiver).await;
    match response {
        ValidationResponse::Reject => (),
        response => panic!("Unexpected response: {:?}", response),
    }
}

#[tokio::test]
async fn approval_sent_when_reactor_says_stream_is_valid() {
    let definition = DefinitionBuilder::new().reactor_name("reactor").build();
    let mut context = TestContext::new(definition).unwrap();
    let publish_channel = context.accept_registration().await;

    let (sender, receiver) = channel();
    publish_channel
        .send(RtmpEndpointPublisherMessage::PublisherRequiringApproval {
            stream_key: "ab123".to_string(),
            connection_id: ConnectionId("connection".to_string()),
            response_channel: sender,
        })
        .expect("Failed to send publisher message");

    context.step_context.execute_pending_notifications().await;
    let reactor_channel = context.get_reactor_channel().await;

    reactor_channel
        .send(ReactorWorkflowUpdate {
            is_valid: true,
            routable_workflow_names: HashSet::new(),
        })
        .expect("Failed to send reactor response");

    context.step_context.execute_pending_notifications().await;

    let response = test_utils::expect_oneshot_response(receiver).await;
    match response {
        ValidationResponse::Approve { .. } => (),
        response => panic!("Unexpected response: {:?}", response),
    }
}
