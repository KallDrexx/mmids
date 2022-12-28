use super::*;
use anyhow::Result;
use bytes::Bytes;
use mmids_core::codecs::VideoCodec;
use mmids_core::net::ConnectionId;
use mmids_core::workflows::definitions::WorkflowStepType;
use mmids_core::workflows::steps::StepTestContext;
use mmids_core::workflows::MediaNotificationContent::StreamDisconnected;
use mmids_core::workflows::{MediaNotification, MediaNotificationContent};
use mmids_core::{test_utils, StreamId, VideoTimestamp};
use rml_rtmp::sessions::StreamMetadata;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot::channel;
use mmids_core::workflows::metadata::common_metadata::{get_is_keyframe_metadata_key, get_pts_offset_metadata_key};
use mmids_core::workflows::metadata::MetadataKeyMap;

struct TestContext {
    step_context: StepTestContext,
    rtmp_endpoint: UnboundedReceiver<RtmpEndpointRequest>,
    reactor_manager: UnboundedReceiver<ReactorManagerRequest>,
    is_keyframe_metadata_key: MetadataKey,
    pts_offset_metadata_key: MetadataKey,
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

        let mut metadata_key_map = MetadataKeyMap::new();
        let is_keyframe_metadata_key = get_is_keyframe_metadata_key(&mut metadata_key_map);
        let pts_offset_metadata_key = get_pts_offset_metadata_key(&mut metadata_key_map);

        let generator = RtmpReceiverStepGenerator {
            reactor_manager: reactor_sender,
            rtmp_endpoint_sender: rtmp_sender,
            is_keyframe_metadata_key,
            pts_offset_metadata_key,
        };

        let step_context = StepTestContext::new(Box::new(generator), definition)?;

        Ok(TestContext {
            step_context,
            rtmp_endpoint: rtmp_receiver,
            reactor_manager: reactor_receiver,
            is_keyframe_metadata_key,
            pts_offset_metadata_key,
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
            assert_eq!(rtmp_app.as_str(), "some_app", "Unexpected rtmp app");
            assert_eq!(
                rtmp_stream_key,
                StreamKeyRegistration::Exact(Arc::new("some_key".to_string())),
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

    if TestContext::new(definition).is_ok() {
        panic!("Expected failure");
    }
}

#[tokio::test]
async fn error_if_no_key_specified() {
    let mut definition = DefinitionBuilder::new().build();
    definition.parameters.remove(STREAM_KEY_PROPERTY_NAME);

    if TestContext::new(definition).is_ok() {
        panic!("Expected failure");
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
            stream_id: StreamId(Arc::new("test".to_string())),
            stream_key: Arc::new("abc".to_string()),
            connection_id: ConnectionId(Arc::new("connection".to_string())),
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
    assert_eq!(media.stream_id.0.as_str(), "test", "Unexpected stream id");

    match &media.content {
        MediaNotificationContent::NewIncomingStream { stream_name } => {
            assert_eq!(stream_name.as_str(), "abc", "Unexpected stream name");
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
            stream_id: StreamId(Arc::new("test".to_string())),
            stream_key: Arc::new("abc".to_string()),
            connection_id: ConnectionId(Arc::new("connection".to_string())),
            reactor_update_channel: None,
        })
        .expect("Failed to send publisher connected message");

    context.step_context.execute_pending_notifications().await;
    context.step_context.media_outputs.clear();

    channel
        .send(RtmpEndpointPublisherMessage::PublishingStopped {
            connection_id: ConnectionId(Arc::new("connection".to_string())),
        })
        .expect("Failed to send disconnected message");

    context.step_context.execute_pending_notifications().await;

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(media.stream_id.0.as_str(), "test", "Unexpected stream id");

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
            stream_id: StreamId(Arc::new("test".to_string())),
            stream_key: Arc::new("abc".to_string()),
            connection_id: ConnectionId(Arc::new("connection".to_string())),
            reactor_update_channel: None,
        })
        .expect("Failed to send publisher connected message");

    context.step_context.execute_pending_notifications().await;

    let mut metadata = StreamMetadata::new();
    metadata.video_width = Some(1920);

    channel
        .send(RtmpEndpointPublisherMessage::StreamMetadataChanged {
            metadata,
            publisher: ConnectionId(Arc::new("connection".to_string())),
        })
        .expect("Failed to send metadata message");

    context.step_context.execute_pending_notifications().await;

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(media.stream_id.0.as_str(), "test", "Unexpected stream id");

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
            stream_id: StreamId(Arc::new("test".to_string())),
            stream_key: Arc::new("abc".to_string()),
            connection_id: ConnectionId(Arc::new("connection".to_string())),
            reactor_update_channel: None,
        })
        .expect("Failed to send publisher connected message");

    context.step_context.execute_pending_notifications().await;

    channel
        .send(RtmpEndpointPublisherMessage::NewVideoData {
            publisher: ConnectionId(Arc::new("connection".to_string())),
            data: Bytes::from(vec![1, 2, 3]),
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
    assert_eq!(media.stream_id.0.as_str(), "test", "Unexpected stream id");

    match &media.content {
        MediaNotificationContent::MediaPayload {
            media_type,
            payload_type,
            data,
            timestamp,
            is_required_for_decoding,
            metadata
        } => {
            let is_keyframe = metadata.iter()
                .filter(|m| m.key() == context.is_keyframe_metadata_key)
                .filter_map(|m| match m.value() {
                    MetadataValue::Bool(val) => Some(val),
                    _ => None,
                })
                .next()
                .unwrap_or_default();

            let pts_offset = metadata.iter()
                .filter(|m| m.key() == context.pts_offset_metadata_key)
                .filter_map(|m| match m.value() {
                    MetadataValue::I32(val) => Some(val),
                    _ => None,
                })
                .next()
                .unwrap_or_default();

            assert_eq!(*media_type, MediaType::Video);
            assert_eq!(*payload_type, *VIDEO_CODEC_H264_AVC, "Unexpected payload type");
            assert_eq!(data, &vec![1, 2, 3], "Unexpected bytes");
            assert_eq!(timestamp, &Duration::from_millis(5), "Unexpected dts");
            assert!(is_required_for_decoding, "Expected is_required_for_decoding to be true");
            assert!(is_keyframe, "Expected is_keyframe to be true");
            assert_eq!(pts_offset, 123, "Unexpected pts offset");
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
            stream_id: StreamId(Arc::new("test".to_string())),
            stream_key: Arc::new("abc".to_string()),
            connection_id: ConnectionId(Arc::new("connection".to_string())),
            reactor_update_channel: None,
        })
        .expect("Failed to send publisher connected message");

    context.step_context.execute_pending_notifications().await;

    channel
        .send(RtmpEndpointPublisherMessage::NewAudioData {
            publisher: ConnectionId(Arc::new("connection".to_string())),
            data: Bytes::from(vec![1, 2, 3]),
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
    assert_eq!(media.stream_id.0.as_str(), "test", "Unexpected stream id");

    let expected_content = MediaNotificationContent::MediaPayload {
        timestamp: Duration::from_millis(5),
        data: Bytes::from_static(&[1, 2, 3]),
        is_required_for_decoding: true,
        media_type: MediaType::Audio,
        payload_type: AUDIO_CODEC_AAC_RAW.clone(),
        metadata: MediaPayloadMetadataCollection::new(iter::empty(), &mut BytesMut::new()),
    };

    assert_eq!(media.content, expected_content, "Unexpected media content");
}

#[test]
fn stream_started_notification_passed_as_input_does_not_get_passed_as_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context
        .step_context
        .assert_media_not_passed_through(MediaNotification {
            stream_id: StreamId(Arc::new("test".to_string())),
            content: MediaNotificationContent::NewIncomingStream {
                stream_name: Arc::new("name".to_string()),
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
            stream_id: StreamId(Arc::new("test".to_string())),
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
            stream_id: StreamId(Arc::new("test".to_string())),
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
            stream_id: StreamId(Arc::new("test".to_string())),
            content: MediaNotificationContent::MediaPayload {
                media_type: MediaType::Video,
                payload_type: VIDEO_CODEC_H264_AVC.clone(),
                data: Bytes::from(vec![1, 2]),
                timestamp: Duration::new(0, 0),
                is_required_for_decoding: true,
                metadata: MediaPayloadMetadataCollection::new(iter::empty(), &mut BytesMut::new()),
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
            stream_id: StreamId(Arc::new("test".to_string())),
            content: MediaNotificationContent::MediaPayload {
                data: Bytes::from(vec![1, 2]),
                timestamp: Duration::from_millis(5),
                is_required_for_decoding: true,
                media_type: MediaType::Audio,
                payload_type: AUDIO_CODEC_AAC_RAW.clone(),
                metadata: MediaPayloadMetadataCollection::new(iter::empty(), &mut BytesMut::new()),
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
            stream_key: Arc::new("ab123".to_string()),
            connection_id: ConnectionId(Arc::new("connection".to_string())),
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
            assert_eq!(reactor_name.as_str(), "abc", "Unexpected reactor name");
            assert_eq!(stream_name.as_str(), "ab123", "Unexpected stream name");
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
            stream_key: Arc::new("ab123".to_string()),
            connection_id: ConnectionId(Arc::new("connection".to_string())),
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
            stream_key: Arc::new("ab123".to_string()),
            connection_id: ConnectionId(Arc::new("connection".to_string())),
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
