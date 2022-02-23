use super::*;
use crate::codecs::{AudioCodec, VideoCodec};
use crate::endpoints::rtmp_server::{
    RtmpEndpointMediaData, RtmpEndpointMediaMessage, RtmpEndpointWatcherNotification,
};
use crate::net::ConnectionId;
use crate::test_utils::expect_mpsc_response;
use crate::workflows::definitions::WorkflowStepType;
use crate::workflows::steps::StepTestContext;
use crate::workflows::{MediaNotification, MediaNotificationContent};
use crate::{test_utils, StreamId, VideoTimestamp};
use anyhow::Result;
use bytes::Bytes;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
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
            step_type: WorkflowStepType("rtmp_watch".to_string()),
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

        let generator = RtmpWatchStepGenerator {
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

    async fn accept_registration(
        &mut self,
    ) -> (
        UnboundedSender<RtmpEndpointWatcherNotification>,
        UnboundedReceiver<RtmpEndpointMediaMessage>,
    ) {
        let request = test_utils::expect_mpsc_response(&mut self.rtmp_endpoint).await;
        let channel = match request {
            RtmpEndpointRequest::ListenForWatchers {
                media_channel,
                notification_channel,
                ..
            } => {
                notification_channel
                    .send(RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful)
                    .expect("Failed to send registration response");

                (notification_channel, media_channel)
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
async fn requests_registration_for_watchers() {
    let definition = DefinitionBuilder::new()
        .port(1234)
        .app("some_app")
        .key("some_key")
        .build();

    let mut context = TestContext::new(definition).unwrap();

    let response = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
    match response {
        RtmpEndpointRequest::ListenForWatchers {
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

        response => panic!("Unexpected response: {:?}", response),
    }
}

#[tokio::test]
async fn no_port_specified_defaults_to_1935() {
    let mut definition = DefinitionBuilder::new().build();
    definition.parameters.remove(PORT_PROPERTY_NAME);

    let mut context = TestContext::new(definition).unwrap();

    let response = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
    match response {
        RtmpEndpointRequest::ListenForWatchers { port, .. } => {
            assert_eq!(port, 1935, "Unexpected port");
        }

        response => panic!("Unexpected response: {:?}", response),
    }
}

#[tokio::test]
async fn asterisk_stream_key_acts_as_wildcard() {
    let mut definition = DefinitionBuilder::new().build();
    definition
        .parameters
        .insert(STREAM_KEY_PROPERTY_NAME.to_string(), Some("*".to_string()));

    let mut context = TestContext::new(definition).unwrap();

    let response = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
    match response {
        RtmpEndpointRequest::ListenForWatchers {
            rtmp_stream_key, ..
        } => {
            assert_eq!(
                rtmp_stream_key,
                StreamKeyRegistration::Any,
                "Unexpected stream key"
            );
        }

        response => panic!("Unexpected response: {:?}", response),
    }
}

#[test]
fn error_if_no_app_provided() {
    let mut definition = DefinitionBuilder::new().build();
    definition.parameters.remove(APP_PROPERTY_NAME);

    match TestContext::new(definition) {
        Ok(_) => panic!("Expecected failure"),
        Err(_) => (),
    }
}

#[test]
fn error_if_no_stream_key_provided() {
    let mut definition = DefinitionBuilder::new().build();
    definition.parameters.remove(STREAM_KEY_PROPERTY_NAME);

    match TestContext::new(definition) {
        Ok(_) => panic!("Expecected failure"),
        Err(_) => (),
    }
}

#[test]
fn new_step_is_in_created_status() {
    let definition = DefinitionBuilder::new().build();
    let context = TestContext::new(definition).unwrap();

    let status = context.step_context.step.get_status();
    assert_eq!(status, &StepStatus::Created, "Unexpected step status");
}

#[tokio::test]
async fn registration_failure_changes_status_to_error() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    let response = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
    let _channel = match response {
        RtmpEndpointRequest::ListenForWatchers {
            notification_channel,
            ..
        } => {
            notification_channel
                .send(RtmpEndpointWatcherNotification::WatcherRegistrationFailed)
                .expect("Failed to send failure response");

            notification_channel
        }

        response => panic!("Unexpected response: {:?}", response),
    };

    context.step_context.execute_pending_notifications().await;

    let status = context.step_context.step.get_status();
    match status {
        StepStatus::Error { message: _ } => (),
        _ => panic!("Unexpected status: {:?}", status),
    }
}

#[tokio::test]
async fn registration_success_changes_status_to_active() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    let response = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
    let _channel = match response {
        RtmpEndpointRequest::ListenForWatchers {
            notification_channel,
            ..
        } => {
            notification_channel
                .send(RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful)
                .expect("Failed to send failure response");

            notification_channel
        }

        response => panic!("Unexpected response: {:?}", response),
    };

    context.step_context.execute_pending_notifications().await;

    let status = context.step_context.step.get_status();
    match status {
        StepStatus::Active => (),
        _ => panic!("Unexpected status: {:?}", status),
    }
}

#[tokio::test]
async fn video_packet_not_sent_to_media_channel_if_new_stream_message_not_received() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let (_notification_channel, mut media_channel) = context.accept_registration().await;

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::Video {
            codec: VideoCodec::H264,
            data: Bytes::from(vec![3, 4]),
            is_keyframe: true,
            is_sequence_header: true,
            timestamp: VideoTimestamp::from_durations(Duration::new(0, 0), Duration::new(0, 0)),
        },
    });

    test_utils::expect_mpsc_timeout(&mut media_channel).await;
}

#[tokio::test]
async fn video_packet_sent_to_media_channel_after_new_stream_message_received() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let (_notification_channel, mut media_channel) = context.accept_registration().await;

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::Video {
            codec: VideoCodec::H264,
            data: Bytes::from(vec![3, 4]),
            is_keyframe: true,
            is_sequence_header: true,
            timestamp: VideoTimestamp::from_durations(
                Duration::from_millis(5),
                Duration::from_millis(15),
            ),
        },
    });

    let media = expect_mpsc_response(&mut media_channel).await;
    assert_eq!(&media.stream_key, "def");

    match &media.data {
        RtmpEndpointMediaData::NewVideoData {
            data,
            codec,
            timestamp,
            is_keyframe,
            is_sequence_header,
            composition_time_offset,
        } => {
            assert_eq!(data, &vec![3, 4], "Unexpected video bytes");
            assert_eq!(codec, &VideoCodec::H264, "Unexpected video codec");
            assert_eq!(timestamp, &RtmpTimestamp::new(5), "Unexpected timestamp");
            assert!(is_keyframe, "Expected is_keyframe to be true");
            assert!(is_sequence_header, "Expected is_sequence_header to be true");
            assert_eq!(
                composition_time_offset, &10,
                "Unexpected composition time offset"
            );
        }

        _ => panic!("Unexpected media data: {:?}", media.data),
    }
}

#[tokio::test]
async fn video_packet_not_sent_to_media_channel_after_stream_disconnection_message_received() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let (_notification_channel, mut media_channel) = context.accept_registration().await;

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::StreamDisconnected,
    });

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::Video {
            codec: VideoCodec::H264,
            data: Bytes::from(vec![3, 4]),
            is_keyframe: true,
            is_sequence_header: true,
            timestamp: VideoTimestamp::from_durations(
                Duration::from_millis(5),
                Duration::from_millis(15),
            ),
        },
    });

    test_utils::expect_mpsc_timeout(&mut media_channel).await;
}

#[tokio::test]
async fn video_packet_not_sent_to_media_channel_when_new_stream_is_for_different_stream_id() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let (_notification_channel, mut media_channel) = context.accept_registration().await;

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId("def".to_string()),
        content: MediaNotificationContent::Video {
            codec: VideoCodec::H264,
            data: Bytes::from(vec![3, 4]),
            is_keyframe: true,
            is_sequence_header: true,
            timestamp: VideoTimestamp::from_durations(
                Duration::from_millis(5),
                Duration::from_millis(15),
            ),
        },
    });

    test_utils::expect_mpsc_timeout(&mut media_channel).await;
}

#[tokio::test]
async fn audio_packet_sent_to_media_channel_after_new_stream_message_received() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let (_notification_channel, mut media_channel) = context.accept_registration().await;

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::Audio {
            codec: AudioCodec::Aac,
            data: Bytes::from(vec![3, 4]),
            is_sequence_header: true,
            timestamp: Duration::from_millis(1),
        },
    });

    let media = expect_mpsc_response(&mut media_channel).await;
    assert_eq!(&media.stream_key, "def");

    match &media.data {
        RtmpEndpointMediaData::NewAudioData {
            data,
            codec,
            timestamp,
            is_sequence_header,
        } => {
            assert_eq!(data, &vec![3, 4], "Unexpected video bytes");
            assert_eq!(codec, &AudioCodec::Aac, "Unexpected video codec");
            assert_eq!(timestamp, &RtmpTimestamp::new(1), "Unexpected timestamp");
            assert!(is_sequence_header, "Expected is_sequence_header to be true");
        }

        _ => panic!("Unexpected media data: {:?}", media.data),
    }
}

#[tokio::test]
async fn metadata_packet_sent_to_media_channel_after_new_stream_message_received() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let (_notification_channel, mut media_channel) = context.accept_registration().await;

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    let mut metadata = HashMap::new();
    metadata.insert("width".to_string(), "1920".to_string());

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::Metadata { data: metadata },
    });

    let media = expect_mpsc_response(&mut media_channel).await;
    assert_eq!(&media.stream_key, "def");

    match &media.data {
        RtmpEndpointMediaData::NewStreamMetaData { metadata } => {
            assert_eq!(metadata.video_width, Some(1920), "Unexpected video width");
        }

        _ => panic!("Unexpected media data: {:?}", media.data),
    }
}

#[tokio::test]
async fn media_message_uses_strict_stream_key_when_exact_key_registered() {
    let definition = DefinitionBuilder::new().key("specific_key").build();
    let mut context = TestContext::new(definition).unwrap();
    let (_notification_channel, mut media_channel) = context.accept_registration().await;

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::Video {
            codec: VideoCodec::H264,
            data: Bytes::from(vec![3, 4]),
            is_keyframe: true,
            is_sequence_header: true,
            timestamp: VideoTimestamp::from_durations(
                Duration::from_millis(5),
                Duration::from_millis(15),
            ),
        },
    });

    let media = expect_mpsc_response(&mut media_channel).await;
    assert_eq!(&media.stream_key, "specific_key", "Unexpected stream key");
}

#[tokio::test]
async fn new_stream_message_passed_as_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let (_notification_channel, _media_channel) = context.accept_registration().await;

    context
        .step_context
        .assert_media_passed_through(MediaNotification {
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::NewIncomingStream {
                stream_name: "def".to_string(),
            },
        });
}

#[tokio::test]
async fn stream_disconnected_message_passed_as_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let (_notification_channel, _media_channel) = context.accept_registration().await;

    context
        .step_context
        .assert_media_passed_through(MediaNotification {
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::StreamDisconnected,
        });
}

#[tokio::test]
async fn video_message_passed_as_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let (_notification_channel, _media_channel) = context.accept_registration().await;

    context
        .step_context
        .assert_media_passed_through(MediaNotification {
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::Video {
                codec: VideoCodec::H264,
                data: Bytes::from(vec![3, 4]),
                is_keyframe: true,
                is_sequence_header: true,
                timestamp: VideoTimestamp::from_durations(
                    Duration::from_millis(5),
                    Duration::from_millis(15),
                ),
            },
        });
}

#[tokio::test]
async fn audio_message_passed_as_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let (_notification_channel, _media_channel) = context.accept_registration().await;

    context
        .step_context
        .assert_media_passed_through(MediaNotification {
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::Audio {
                codec: AudioCodec::Aac,
                data: Bytes::from(vec![3, 4]),
                is_sequence_header: true,
                timestamp: Duration::from_millis(1),
            },
        });
}

#[tokio::test]
async fn metadata_message_passed_as_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();
    let (_notification_channel, _media_channel) = context.accept_registration().await;

    let mut metadata = HashMap::new();
    metadata.insert("a".to_string(), "b".to_string());

    context
        .step_context
        .assert_media_passed_through(MediaNotification {
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::Metadata { data: metadata },
        });
}

#[tokio::test]
async fn watchers_requiring_approval_sends_request_to_reactor() {
    let definition = DefinitionBuilder::new()
        .reactor_name("some_reactor")
        .build();
    let mut context = TestContext::new(definition).unwrap();
    let (notification_channel, _media_channel) = context.accept_registration().await;

    let (sender, _receiver) = channel();
    notification_channel
        .send(RtmpEndpointWatcherNotification::WatcherRequiringApproval {
            stream_key: "abc".to_string(),
            connection_id: ConnectionId("def".to_string()),
            response_channel: sender,
        })
        .expect("Failed to send approval request");

    context.step_context.execute_pending_notifications().await;

    let request = test_utils::expect_mpsc_response(&mut context.reactor_manager).await;
    match request {
        ReactorManagerRequest::CreateWorkflowForStreamName {
            reactor_name,
            stream_name,
            ..
        } => {
            assert_eq!(&reactor_name, "some_reactor", "Unexpected reactor name");
            assert_eq!(&stream_name, "abc", "Unexpected stream name");
        }

        request => panic!("Unexpected request: {:?}", request),
    }
}

#[tokio::test]
async fn reactor_responding_with_invalid_sends_rejection_response() {
    let definition = DefinitionBuilder::new()
        .reactor_name("some_reactor")
        .build();
    let mut context = TestContext::new(definition).unwrap();
    let (notification_channel, _media_channel) = context.accept_registration().await;

    let (sender, receiver) = channel();
    notification_channel
        .send(RtmpEndpointWatcherNotification::WatcherRequiringApproval {
            stream_key: "abc".to_string(),
            connection_id: ConnectionId("def".to_string()),
            response_channel: sender,
        })
        .expect("Failed to send approval request");

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
async fn reactor_responding_with_valid_sends_approved_response() {
    let definition = DefinitionBuilder::new()
        .reactor_name("some_reactor")
        .build();
    let mut context = TestContext::new(definition).unwrap();
    let (notification_channel, _media_channel) = context.accept_registration().await;

    let (sender, receiver) = channel();
    notification_channel
        .send(RtmpEndpointWatcherNotification::WatcherRequiringApproval {
            stream_key: "abc".to_string(),
            connection_id: ConnectionId("def".to_string()),
            response_channel: sender,
        })
        .expect("Failed to send approval request");

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
