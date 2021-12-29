use super::*;
use crate::codecs::{AudioCodec, VideoCodec};
use crate::test_utils;
use crate::workflows::definitions::WorkflowStepType;
use crate::workflows::steps::StepTestContext;
use bytes::Bytes;
use std::time::Duration;

struct TestContext {
    reactor_manager: UnboundedReceiver<ReactorManagerRequest>,
    _event_hub: UnboundedReceiver<SubscriptionRequest>,
    step_context: StepTestContext,
    workflow_sender: UnboundedSender<WorkflowRequest>,
    workflow_receiver: UnboundedReceiver<WorkflowRequest>,
    workflow_event_channel: UnboundedSender<WorkflowStartedOrStoppedEvent>,
}

impl TestContext {
    async fn new(specific_workflow: Option<&str>, reactor: Option<&str>) -> Self {
        if specific_workflow.is_some() && reactor.is_some() {
            panic!("Both workflow and reactor names specified. Only one should be");
        }

        if specific_workflow.is_none() && reactor.is_none() {
            panic!("Neither workflow or reactor name specified. One must be");
        }

        let (reactor_sender, reactor_receiver) = unbounded_channel();
        let (workflow_sender, workflow_receiver) = unbounded_channel();
        let (sub_sender, mut sub_receiver) = unbounded_channel();

        let generator = WorkflowForwarderStepGenerator::new(sub_sender, reactor_sender);
        let mut definition = WorkflowStepDefinition {
            step_type: WorkflowStepType("".to_string()),
            parameters: HashMap::new(),
        };

        if let Some(reactor) = reactor {
            definition
                .parameters
                .insert(REACTOR_NAME.to_string(), Some(reactor.to_string()));
        }

        if let Some(workflow) = specific_workflow {
            definition
                .parameters
                .insert(TARGET_WORKFLOW.to_string(), Some(workflow.to_string()));
        }

        let step_context = StepTestContext::new(Box::new(generator), definition);

        // It must send a subscription event on startup
        let event = test_utils::expect_mpsc_response(&mut sub_receiver).await;
        let channel = match event {
            SubscriptionRequest::WorkflowStartedOrStopped { channel } => channel,
            event => panic!("Unexpected event: {:?}", event),
        };

        TestContext {
            step_context,
            workflow_sender,
            workflow_receiver,
            _event_hub: sub_receiver,
            reactor_manager: reactor_receiver,
            workflow_event_channel: channel,
        }
    }

    async fn send_workflow_started_event(&mut self, name: &str) {
        self.workflow_event_channel
            .send(WorkflowStartedOrStoppedEvent::WorkflowStarted {
                name: name.to_string(),
                channel: self.workflow_sender.clone(),
            })
            .expect("Failed to send workflow started event");

        let result = test_utils::expect_future_resolved(&mut self.step_context.futures).await;
        self.step_context.execute_notification(result);
    }

    async fn send_workflow_stopped_event(&mut self, name: &str) {
        self.workflow_event_channel
            .send(WorkflowStartedOrStoppedEvent::WorkflowEnded {
                name: name.to_string(),
            })
            .expect("Failed to send workflow ended event");

        let result = test_utils::expect_future_resolved(&mut self.step_context.futures).await;
        self.step_context.execute_notification(result);
    }
}

#[tokio::test]
async fn new_stream_message_sent_to_global_workflow() {
    let mut context = TestContext::new(Some("test"), None).await;
    context.send_workflow_started_event("test").await;

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    let response = test_utils::expect_mpsc_response(&mut context.workflow_receiver).await;
    match response.operation {
        WorkflowRequestOperation::MediaNotification { media } => {
            assert_eq!(&media.stream_id.0, "abc", "Unexpected stream id");
            match media.content {
                MediaNotificationContent::NewIncomingStream { stream_name } => {
                    assert_eq!(&stream_name, "def", "Unexpected stream name");
                }

                content => panic!("Unexpected media content: {:?}", content),
            }
        }

        operation => panic!("Unexpected workflow operation: {:?}", operation),
    }
}

#[tokio::test]
async fn new_stream_message_sent_if_workflow_started_after_message_comes_in() {
    let mut context = TestContext::new(Some("test"), None).await;
    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    test_utils::expect_mpsc_timeout(&mut context.workflow_receiver).await;
    context.send_workflow_started_event("test").await;

    let response = test_utils::expect_mpsc_response(&mut context.workflow_receiver).await;
    match response.operation {
        WorkflowRequestOperation::MediaNotification { media } => {
            assert_eq!(&media.stream_id.0, "abc", "Unexpected stream id");
            match media.content {
                MediaNotificationContent::NewIncomingStream { stream_name } => {
                    assert_eq!(&stream_name, "def", "Unexpected stream name");
                }

                content => panic!("Unexpected media content: {:?}", content),
            }
        }

        operation => panic!("Unexpected workflow operation: {:?}", operation),
    }
}

#[tokio::test]
async fn no_message_passed_if_workflow_has_different_name_than_global_name() {
    let mut context = TestContext::new(Some("test"), None).await;
    context.send_workflow_started_event("test2").await;

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    test_utils::expect_mpsc_timeout(&mut context.workflow_receiver).await;
}

#[tokio::test]
async fn no_message_passed_if_workflow_stopped_before_media_sent() {
    let mut context = TestContext::new(Some("test"), None).await;
    context.send_workflow_started_event("test").await;
    context.send_workflow_stopped_event("test").await;

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    test_utils::expect_mpsc_timeout(&mut context.workflow_receiver).await;
}

#[tokio::test]
async fn no_message_passed_if_stream_disconnected_before_workflow_started() {
    let mut context = TestContext::new(Some("test"), None).await;

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::StreamDisconnected,
    });

    context.send_workflow_started_event("test").await;
    test_utils::expect_mpsc_timeout(&mut context.workflow_receiver).await;
}

#[tokio::test]
async fn new_stream_media_passed_as_output_immediately() {
    let mut context = TestContext::new(Some("test"), None).await;

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(media.stream_id.0, "abc", "Unexpected stream id");

    match &media.content {
        MediaNotificationContent::NewIncomingStream { stream_name } => {
            assert_eq!(stream_name, "def", "Unexpected stream name");
        }

        content => panic!("Unexpected media content: {:?}", content),
    }
}

#[tokio::test]
async fn stream_disconnected_media_passed_as_output_immediately() {
    let mut context = TestContext::new(Some("test"), None).await;

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::StreamDisconnected,
    });

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(media.stream_id.0, "abc", "Unexpected stream id");

    match &media.content {
        MediaNotificationContent::StreamDisconnected => (),

        content => panic!("Unexpected media content: {:?}", content),
    }
}

#[tokio::test]
async fn video_media_passed_as_output_immediately() {
    let mut context = TestContext::new(Some("test"), None).await;

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::Video {
            data: Bytes::from(vec![1, 2, 3]),
            codec: VideoCodec::H264,
            timestamp: Duration::from_millis(5),
            is_keyframe: true,
            is_sequence_header: true,
        },
    });

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(media.stream_id.0, "abc", "Unexpected stream id");

    match &media.content {
        MediaNotificationContent::Video {
            data,
            codec,
            timestamp,
            is_keyframe,
            is_sequence_header,
        } => {
            assert_eq!(data, &vec![1, 2, 3], "Unexpected bytes");
            assert_eq!(codec, &VideoCodec::H264, "Unexpected codec");
            assert_eq!(timestamp, &Duration::from_millis(5), "Unexpected timestamp");
            assert!(is_keyframe, "Expected is_keyframe to be true");
            assert!(is_sequence_header, "Expected is_sequence_header to be true");
        }

        content => panic!("Unexpected media content: {:?}", content),
    }
}

#[tokio::test]
async fn audio_media_passed_as_output_immediately() {
    let mut context = TestContext::new(Some("test"), None).await;

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::Audio {
            data: Bytes::from(vec![1, 2, 3]),
            codec: AudioCodec::Aac,
            timestamp: Duration::from_millis(5),
            is_sequence_header: true,
        },
    });

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(media.stream_id.0, "abc", "Unexpected stream id");

    match &media.content {
        MediaNotificationContent::Audio {
            data,
            codec,
            timestamp,
            is_sequence_header,
        } => {
            assert_eq!(data, &vec![1, 2, 3], "Unexpected bytes");
            assert_eq!(codec, &AudioCodec::Aac, "Unexpected codec");
            assert_eq!(timestamp, &Duration::from_millis(5), "Unexpected timestamp");
            assert!(is_sequence_header, "Expected is_sequence_header to be true");
        }

        content => panic!("Unexpected media content: {:?}", content),
    }
}

#[tokio::test]
async fn metadata_media_passed_as_output_immediately() {
    let mut context = TestContext::new(Some("test"), None).await;

    let mut metadata = HashMap::new();
    metadata.insert("a".to_string(), "b".to_string());

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::Metadata {
            data: metadata.clone(),
        },
    });

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(media.stream_id.0, "abc", "Unexpected stream id");

    match &media.content {
        MediaNotificationContent::Metadata { data } => {
            assert_eq!(data, &metadata, "Unexpected metadata");
        }

        content => panic!("Unexpected media content: {:?}", content),
    }
}

#[tokio::test]
async fn video_sequence_headers_sent_to_workflow_when_received_before_workflow_starts() {
    let mut context = TestContext::new(Some("test"), None).await;
    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::Video {
            data: Bytes::from(vec![1, 2, 3]),
            codec: VideoCodec::H264,
            timestamp: Duration::from_millis(5),
            is_keyframe: true,
            is_sequence_header: true,
        },
    });

    test_utils::expect_mpsc_timeout(&mut context.workflow_receiver).await;
    context.send_workflow_started_event("test").await;

    let response = test_utils::expect_mpsc_response(&mut context.workflow_receiver).await;
    match response.operation {
        WorkflowRequestOperation::MediaNotification { media } => match media.content {
            MediaNotificationContent::NewIncomingStream { .. } => (),
            content => panic!("Unexpected media content: {:?}", content),
        },

        operation => panic!("Unexpected workflow operation: {:?}", operation),
    }

    let response = test_utils::expect_mpsc_response(&mut context.workflow_receiver).await;
    match response.operation {
        WorkflowRequestOperation::MediaNotification { media } => match media.content {
            MediaNotificationContent::Video { .. } => (),
            content => panic!("Unexpected media content: {:?}", content),
        },

        operation => panic!("Unexpected workflow operation: {:?}", operation),
    }
}

#[tokio::test]
async fn non_video_sequence_headers_not_sent_to_workflow_when_received_before_workflow_starts() {
    let mut context = TestContext::new(Some("test"), None).await;
    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::Video {
            data: Bytes::from(vec![1, 2, 3]),
            codec: VideoCodec::H264,
            timestamp: Duration::from_millis(5),
            is_keyframe: true,
            is_sequence_header: false,
        },
    });

    test_utils::expect_mpsc_timeout(&mut context.workflow_receiver).await;
    context.send_workflow_started_event("test").await;

    let response = test_utils::expect_mpsc_response(&mut context.workflow_receiver).await;
    match response.operation {
        WorkflowRequestOperation::MediaNotification { media } => match media.content {
            MediaNotificationContent::NewIncomingStream { .. } => (),
            content => panic!("Unexpected media content: {:?}", content),
        },

        operation => panic!("Unexpected workflow operation: {:?}", operation),
    }

    test_utils::expect_mpsc_timeout(&mut context.workflow_receiver).await;
}

#[tokio::test]
async fn audio_sequence_headers_sent_to_workflow_when_received_before_workflow_starts() {
    let mut context = TestContext::new(Some("test"), None).await;
    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::Audio {
            data: Bytes::from(vec![1, 2, 3]),
            codec: AudioCodec::Aac,
            timestamp: Duration::from_millis(5),
            is_sequence_header: true,
        },
    });

    test_utils::expect_mpsc_timeout(&mut context.workflow_receiver).await;
    context.send_workflow_started_event("test").await;

    let response = test_utils::expect_mpsc_response(&mut context.workflow_receiver).await;
    match response.operation {
        WorkflowRequestOperation::MediaNotification { media } => match media.content {
            MediaNotificationContent::NewIncomingStream { .. } => (),
            content => panic!("Unexpected media content: {:?}", content),
        },

        operation => panic!("Unexpected workflow operation: {:?}", operation),
    }

    let response = test_utils::expect_mpsc_response(&mut context.workflow_receiver).await;
    match response.operation {
        WorkflowRequestOperation::MediaNotification { media } => match media.content {
            MediaNotificationContent::Audio { .. } => (),
            content => panic!("Unexpected media content: {:?}", content),
        },

        operation => panic!("Unexpected workflow operation: {:?}", operation),
    }
}

#[tokio::test]
async fn non_audio_sequence_headers_not_sent_to_workflow_when_received_before_workflow_starts() {
    let mut context = TestContext::new(Some("test"), None).await;
    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::Audio {
            data: Bytes::from(vec![1, 2, 3]),
            codec: AudioCodec::Aac,
            timestamp: Duration::from_millis(5),
            is_sequence_header: false,
        },
    });

    test_utils::expect_mpsc_timeout(&mut context.workflow_receiver).await;
    context.send_workflow_started_event("test").await;

    let response = test_utils::expect_mpsc_response(&mut context.workflow_receiver).await;
    match response.operation {
        WorkflowRequestOperation::MediaNotification { media } => match media.content {
            MediaNotificationContent::NewIncomingStream { .. } => (),
            content => panic!("Unexpected media content: {:?}", content),
        },

        operation => panic!("Unexpected workflow operation: {:?}", operation),
    }

    test_utils::expect_mpsc_timeout(&mut context.workflow_receiver).await;
}

#[tokio::test]
async fn metadata_not_sent_when_received_before_workflow_starts() {
    let mut context = TestContext::new(Some("test"), None).await;
    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "def".to_string(),
        },
    });

    context.step_context.execute_media(MediaNotification {
        stream_id: StreamId("abc".to_string()),
        content: MediaNotificationContent::Metadata {
            data: HashMap::new(),
        },
    });

    test_utils::expect_mpsc_timeout(&mut context.workflow_receiver).await;
    context.send_workflow_started_event("test").await;

    let response = test_utils::expect_mpsc_response(&mut context.workflow_receiver).await;
    match response.operation {
        WorkflowRequestOperation::MediaNotification { media } => match media.content {
            MediaNotificationContent::NewIncomingStream { .. } => (),
            content => panic!("Unexpected media content: {:?}", content),
        },

        operation => panic!("Unexpected workflow operation: {:?}", operation),
    }

    test_utils::expect_mpsc_timeout(&mut context.workflow_receiver).await;
}
