use super::*;
use crate::codecs::{AudioCodec, VideoCodec};
use crate::endpoints::rtmp_server::{
    RtmpEndpointMediaData, RtmpEndpointMediaMessage, RtmpEndpointWatcherNotification,
};
use crate::workflows::definitions::WorkflowStepType;
use crate::workflows::steps::test_utils::{create_step_parameters, get_pending_future_result};
use crate::workflows::{MediaNotification, MediaNotificationContent};
use crate::StreamId;
use bytes::Bytes;
use futures::future::BoxFuture;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time::timeout;

const TEST_PORT: u16 = 9999;
const TEST_APP: &'static str = "some_app";
const TEST_KEY: &'static str = "test_key";

#[tokio::test]
async fn can_create_from_filled_out_workflow_definition() {
    let definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    let (sender, mut receiver) = unbounded_channel();

    let (_step, _futures) = RtmpWatchStepGenerator::new(sender)
        .generate(definition)
        .expect("Error returned creating step");

    let request = receiver
        .try_recv()
        .expect("Expected rtmp endpoint receiver to have a message");

    match request {
        RtmpEndpointRequest::ListenForWatchers {
            port,
            rtmp_app,
            rtmp_stream_key,
            ..
        } => {
            assert_eq!(port, TEST_PORT, "Unexpected port");
            assert_eq!(rtmp_app, TEST_APP.to_string(), "Unexpected rtmp app");
            assert_eq!(
                rtmp_stream_key,
                StreamKeyRegistration::Exact(TEST_KEY.to_string()),
                "Unexpected stream key registration"
            );
        }

        RtmpEndpointRequest::ListenForPublishers { .. } => {
            panic!(
                "Expected watch registration request, but publish registration request was found"
            )
        }
    }
}

#[tokio::test]
async fn asterisk_for_key_sets_key_to_any() {
    let definition = create_definition(TEST_PORT, TEST_APP, "*");
    let (sender, mut receiver) = unbounded_channel();

    let (_step, _futures) = RtmpWatchStepGenerator::new(sender)
        .generate(definition)
        .expect("Error returned creating step");

    let request = receiver
        .try_recv()
        .expect("Expected rtmp endpoint receiver to have a message");

    match request {
        RtmpEndpointRequest::ListenForWatchers {
            port,
            rtmp_app,
            rtmp_stream_key,
            ..
        } => {
            assert_eq!(port, TEST_PORT, "Unexpected port");
            assert_eq!(rtmp_app, TEST_APP.to_string(), "Unexpected rtmp app");
            assert_eq!(
                rtmp_stream_key,
                StreamKeyRegistration::Any,
                "Unexpected stream key registration"
            );
        }

        RtmpEndpointRequest::ListenForPublishers { .. } => {
            panic!(
                "Expected watch registration request, but publish registration request was found"
            )
        }
    }
}

#[tokio::test]
async fn port_is_1935_if_none_provided() {
    let mut definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    definition.parameters.remove(PORT_PROPERTY_NAME);

    let (sender, mut receiver) = unbounded_channel();

    let (_step, _futures) = RtmpWatchStepGenerator::new(sender)
        .generate(definition)
        .expect("Error returned creating step");

    let request = receiver
        .try_recv()
        .expect("Expected rtmp endpoint receiver to have a message");

    match request {
        RtmpEndpointRequest::ListenForWatchers { port, .. } => {
            assert_eq!(port, 1935, "Unexpected port");
        }

        RtmpEndpointRequest::ListenForPublishers { .. } => {
            panic!(
                "Expected watch registration request, but publish registration request was found"
            )
        }
    }
}

#[test]
fn error_if_no_app_provided() {
    let mut definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    definition.parameters.remove(APP_PROPERTY_NAME);

    let (mock_sender, _mock_receiver) = unbounded_channel();

    RtmpWatchStepGenerator::new(mock_sender)
        .generate(definition)
        .err()
        .unwrap();
}

#[test]
fn error_if_no_stream_key_provided() {
    let mut definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    definition.parameters.remove(STREAM_KEY_PROPERTY_NAME);

    let (mock_sender, _mock_receiver) = unbounded_channel();

    RtmpWatchStepGenerator::new(mock_sender)
        .generate(definition)
        .err()
        .unwrap();
}

#[tokio::test]
async fn rtmp_app_is_trimmed() {
    let mut definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    definition.parameters.insert(
        APP_PROPERTY_NAME.to_string(),
        " ".to_string() + TEST_APP + " ",
    );

    let (sender, mut receiver) = unbounded_channel();

    let (_step, _futures) = RtmpWatchStepGenerator::new(sender)
        .generate(definition)
        .expect("Error returned creating step");

    let request = receiver
        .try_recv()
        .expect("Expected rtmp endpoint receiver to have a message");

    match request {
        RtmpEndpointRequest::ListenForWatchers { rtmp_app, .. } => {
            assert_eq!(rtmp_app, TEST_APP, "Unexpected rtmp app");
        }

        RtmpEndpointRequest::ListenForPublishers { .. } => {
            panic!(
                "Expected watch registration request, but publish registration request was found"
            )
        }
    }
}

#[tokio::test]
async fn stream_key_is_trimmed() {
    let mut definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    definition.parameters.insert(
        STREAM_KEY_PROPERTY_NAME.to_string(),
        " ".to_string() + TEST_KEY + " ",
    );

    let (sender, mut receiver) = unbounded_channel();

    let (_step, _futures) = RtmpWatchStepGenerator::new(sender)
        .generate(definition)
        .expect("Error returned creating step");

    let request = receiver
        .try_recv()
        .expect("Expected rtmp endpoint receiver to have a message");

    match request {
        RtmpEndpointRequest::ListenForWatchers {
            rtmp_stream_key, ..
        } => {
            assert_eq!(
                rtmp_stream_key,
                StreamKeyRegistration::Exact(TEST_KEY.to_string()),
                "Unexpected stream key registration"
            );
        }

        RtmpEndpointRequest::ListenForPublishers { .. } => {
            panic!(
                "Expected watch registration request, but publish registration request was found"
            )
        }
    }
}

#[test]
fn new_step_is_in_created_status() {
    let mut definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    definition.parameters.insert(
        STREAM_KEY_PROPERTY_NAME.to_string(),
        " ".to_string() + TEST_KEY + " ",
    );

    let (mock_sender, _mock_receiver) = unbounded_channel();

    let (step, _futures) = RtmpWatchStepGenerator::new(mock_sender)
        .generate(definition)
        .expect("Error returned when creating rtmp receive step");

    let status = step.get_status();
    assert_eq!(status, &StepStatus::Created, "Unexpected status");
}

#[test]
fn new_requests_registration_for_watchers() {
    let definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    let (sender, mut receiver) = unbounded_channel();

    let (_step, _futures) = RtmpWatchStepGenerator::new(sender)
        .generate(definition)
        .expect("Error returned when creating rtmp receive step");

    let message = receiver
        .try_recv()
        .expect("Unexpected error reading from the receiver");

    match message {
        RtmpEndpointRequest::ListenForWatchers {
            port,
            rtmp_app,
            rtmp_stream_key,
            ..
        } => {
            assert_eq!(port, TEST_PORT, "Unexpected port in registration request");
            assert_eq!(
                rtmp_app,
                TEST_APP.to_string(),
                "Unexpected app in registration request"
            );
            assert_eq!(
                rtmp_stream_key,
                StreamKeyRegistration::Exact(TEST_KEY.to_string()),
                "Unexpected stream key in registration request"
            );
        }

        message => panic!("Unexpected endpoint request received: {:?}", message),
    };
}

#[tokio::test]
async fn initialized_step_returns_pending_future() {
    let (_step, mut futures, _media, _notification) = create_initialized_step();

    assert_eq!(futures.len(), 1, "Unexpected number of futures returned");

    let future = futures.remove(0);
    match timeout(Duration::from_millis(10), future).await {
        Ok(_) => panic!("Expected future to be pending, but instead it had a result"),
        Err(_) => (),
    };
}

#[tokio::test]
async fn registration_failure_changes_status_to_error() {
    let (mut step, futures, _media, notification_channel) = create_initialized_step();
    let _ = notification_channel.send(RtmpEndpointWatcherNotification::WatcherRegistrationFailed);
    let result = get_pending_future_result(futures).await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.notifications.push(result);

    step.execute(&mut inputs, &mut outputs);

    assert_eq!(
        step.get_status(),
        &StepStatus::Error,
        "Unexpected status for step"
    );
}

#[tokio::test]
async fn registration_success_changes_status_to_active() {
    let (mut step, futures, _media, notification_channel) = create_initialized_step();
    let _ =
        notification_channel.send(RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful);
    let result = get_pending_future_result(futures).await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.notifications.push(result);

    step.execute(&mut inputs, &mut outputs);

    assert_eq!(
        step.get_status(),
        &StepStatus::Active,
        "Unexpected status for step"
    );
}

#[tokio::test]
async fn video_packet_not_sent_to_media_channel_if_new_stream_message_not_received() {
    let stream_id = StreamId("stream_id".to_string());

    let (mut step, _futures, mut media_channel, _notification_channel) = create_active_step().await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.media.push(MediaNotification {
        stream_id,
        content: MediaNotificationContent::Video {
            codec: VideoCodec::H264,
            data: Bytes::from(vec![3, 4]),
            is_keyframe: true,
            is_sequence_header: true,
            timestamp: Duration::from_millis(1),
        },
    });

    step.execute(&mut inputs, &mut outputs);

    match timeout(Duration::from_millis(10), media_channel.recv()).await {
        Ok(_) => panic!("Expected no items in the media channel, but one was received"),
        Err(_) => (),
    }
}

#[tokio::test]
async fn video_packet_sent_to_media_channel_after_new_stream_message_received() {
    let stream_id = StreamId("stream_id".to_string());

    let (mut step, _futures, mut media_channel, _notification_channel) = create_active_step().await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.media.push(MediaNotification {
        stream_id: stream_id.clone(),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: TEST_KEY.to_string(),
        },
    });

    inputs.media.push(MediaNotification {
        stream_id,
        content: MediaNotificationContent::Video {
            codec: VideoCodec::H264,
            data: Bytes::from(vec![3, 4]),
            is_keyframe: true,
            is_sequence_header: true,
            timestamp: Duration::from_millis(1),
        },
    });

    step.execute(&mut inputs, &mut outputs);

    let media = match timeout(Duration::from_millis(10), media_channel.recv()).await {
        Ok(Some(message)) => message,
        _ => panic!("Expected items in the media channel, but none was received"),
    };

    assert_eq!(
        media.stream_key,
        TEST_KEY.to_string(),
        "Unexpected stream key in response"
    );

    match media.data {
        RtmpEndpointMediaData::NewVideoData {
            data,
            timestamp,
            codec,
            is_keyframe,
            is_sequence_header,
        } => {
            assert_eq!(timestamp, RtmpTimestamp::new(1), "Unexpected timestamp");
            assert_eq!(codec, VideoCodec::H264, "Unexpected video codec");
            assert!(is_keyframe, "Expected is_keyframe to be true");
            assert!(is_sequence_header, "Expected is_sequence_header to be true");
            assert_eq!(data.as_ref(), &[3, 4], "Unexpected data value");
        }

        x => panic!("Unexpected media result: {:?}", x),
    }
}

#[tokio::test]
async fn video_packet_not_sent_to_media_channel_after_stream_disconnection_message_received() {
    let stream_id = StreamId("stream_id".to_string());

    let (mut step, _futures, mut media_channel, _notification_channel) = create_active_step().await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.media.push(MediaNotification {
        stream_id: stream_id.clone(),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: TEST_KEY.to_string(),
        },
    });

    inputs.media.push(MediaNotification {
        stream_id: stream_id.clone(),
        content: MediaNotificationContent::StreamDisconnected,
    });

    inputs.media.push(MediaNotification {
        stream_id,
        content: MediaNotificationContent::Video {
            codec: VideoCodec::H264,
            data: Bytes::from(vec![3, 4]),
            is_keyframe: true,
            is_sequence_header: true,
            timestamp: Duration::from_millis(1),
        },
    });

    step.execute(&mut inputs, &mut outputs);

    match timeout(Duration::from_millis(10), media_channel.recv()).await {
        Ok(_) => panic!("Expected no items in the media channel, but one was received"),
        Err(_) => (),
    };
}

#[tokio::test]
async fn audio_packet_not_sent_to_media_channel_if_new_stream_message_not_received() {
    let stream_id = StreamId("stream_id".to_string());

    let (mut step, _futures, mut media_channel, _notification_channel) = create_active_step().await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.media.push(MediaNotification {
        stream_id,
        content: MediaNotificationContent::Audio {
            codec: AudioCodec::Aac,
            data: Bytes::from(vec![3, 4]),
            is_sequence_header: true,
            timestamp: Duration::from_millis(1),
        },
    });

    step.execute(&mut inputs, &mut outputs);

    match timeout(Duration::from_millis(10), media_channel.recv()).await {
        Ok(_) => panic!("Expected no items in the media channel, but one was received"),
        Err(_) => (),
    }
}

#[tokio::test]
async fn audio_packet_sent_to_media_channel_after_new_stream_message_received() {
    let stream_id = StreamId("stream_id".to_string());

    let (mut step, _futures, mut media_channel, _notification_channel) = create_active_step().await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.media.push(MediaNotification {
        stream_id: stream_id.clone(),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: TEST_KEY.to_string(),
        },
    });

    inputs.media.push(MediaNotification {
        stream_id,
        content: MediaNotificationContent::Audio {
            codec: AudioCodec::Aac,
            data: Bytes::from(vec![3, 4]),
            is_sequence_header: true,
            timestamp: Duration::from_millis(1),
        },
    });

    step.execute(&mut inputs, &mut outputs);

    let media = match timeout(Duration::from_millis(10), media_channel.recv()).await {
        Ok(Some(message)) => message,
        _ => panic!("Expected items in the media channel, but none was received"),
    };

    assert_eq!(
        media.stream_key,
        TEST_KEY.to_string(),
        "Unexpected stream key in response"
    );

    match media.data {
        RtmpEndpointMediaData::NewAudioData {
            data,
            timestamp,
            codec,
            is_sequence_header,
        } => {
            assert_eq!(timestamp, RtmpTimestamp::new(1), "Unexpected timestamp");
            assert_eq!(codec, AudioCodec::Aac, "Unexpected video codec");
            assert!(is_sequence_header, "Expected is_sequence_header to be true");
            assert_eq!(data.as_ref(), &[3, 4], "Unexpected data value");
        }

        x => panic!("Unexpected media result: {:?}", x),
    }
}

#[tokio::test]
async fn audio_packet_not_sent_to_media_channel_after_stream_disconnection_message_received() {
    let stream_id = StreamId("stream_id".to_string());

    let (mut step, _futures, mut media_channel, _notification_channel) = create_active_step().await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.media.push(MediaNotification {
        stream_id: stream_id.clone(),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: TEST_KEY.to_string(),
        },
    });

    inputs.media.push(MediaNotification {
        stream_id: stream_id.clone(),
        content: MediaNotificationContent::StreamDisconnected,
    });

    inputs.media.push(MediaNotification {
        stream_id,
        content: MediaNotificationContent::Audio {
            codec: AudioCodec::Aac,
            data: Bytes::from(vec![3, 4]),
            is_sequence_header: true,
            timestamp: Duration::from_millis(1),
        },
    });

    step.execute(&mut inputs, &mut outputs);

    match timeout(Duration::from_millis(10), media_channel.recv()).await {
        Ok(_) => panic!("Expected no items in the media channel, but one was received"),
        Err(_) => (),
    };
}

#[tokio::test]
async fn metadata_packet_not_sent_to_media_channel_if_new_stream_message_not_received() {
    let stream_id = StreamId("stream_id".to_string());

    let (mut step, _futures, mut media_channel, _notification_channel) = create_active_step().await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.media.push(MediaNotification {
        stream_id,
        content: MediaNotificationContent::Metadata {
            data: HashMap::new(),
        },
    });

    step.execute(&mut inputs, &mut outputs);

    match timeout(Duration::from_millis(10), media_channel.recv()).await {
        Ok(_) => panic!("Expected no items in the media channel, but one was received"),
        Err(_) => (),
    }
}

#[tokio::test]
async fn metadata_packet_sent_to_media_channel_after_new_stream_message_received() {
    let stream_id = StreamId("stream_id".to_string());

    let (mut step, _futures, mut media_channel, _notification_channel) = create_active_step().await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.media.push(MediaNotification {
        stream_id: stream_id.clone(),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: TEST_KEY.to_string(),
        },
    });

    let mut metadata = HashMap::new();
    metadata.insert("width".to_string(), "123".to_string());

    inputs.media.push(MediaNotification {
        stream_id,
        content: MediaNotificationContent::Metadata { data: metadata },
    });

    step.execute(&mut inputs, &mut outputs);

    let media = match timeout(Duration::from_millis(10), media_channel.recv()).await {
        Ok(Some(message)) => message,
        _ => panic!("Expected items in the media channel, but none was received"),
    };

    assert_eq!(
        media.stream_key,
        TEST_KEY.to_string(),
        "Unexpected stream key in response"
    );

    match media.data {
        RtmpEndpointMediaData::NewStreamMetaData { metadata } => {
            assert_eq!(
                metadata.video_width,
                Some(123),
                "Unexpected video width metadata"
            )
        }

        x => panic!("Unexpected media result: {:?}", x),
    }
}

#[tokio::test]
async fn metadata_packet_not_sent_to_media_channel_after_stream_disconnection_message_received() {
    let stream_id = StreamId("stream_id".to_string());

    let (mut step, _futures, mut media_channel, _notification_channel) = create_active_step().await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.media.push(MediaNotification {
        stream_id: stream_id.clone(),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: TEST_KEY.to_string(),
        },
    });

    inputs.media.push(MediaNotification {
        stream_id: stream_id.clone(),
        content: MediaNotificationContent::StreamDisconnected,
    });

    inputs.media.push(MediaNotification {
        stream_id,
        content: MediaNotificationContent::Metadata {
            data: HashMap::new(),
        },
    });

    step.execute(&mut inputs, &mut outputs);

    match timeout(Duration::from_millis(10), media_channel.recv()).await {
        Ok(_) => panic!("Expected no items in the media channel, but one was received"),
        Err(_) => (),
    };
}

#[tokio::test]
async fn media_message_passed_in_are_also_passed_on() {
    let stream1 = StreamId("stream1".to_string());
    let stream_key = "key".to_string();

    let (mut step, _futures, _media_channel, _notification_channel) = create_active_step().await;

    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.media.push(MediaNotification {
        stream_id: stream1.clone(),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: stream_key.clone(),
        },
    });

    step.execute(&mut inputs, &mut outputs);

    assert_eq!(outputs.media.len(), 1, "Unexpected number of media outputs");
    assert_eq!(
        outputs.media[0].stream_id, stream1,
        "Unexpected stream id of first media output"
    );

    match &outputs.media[0].content {
        MediaNotificationContent::NewIncomingStream { stream_name } => {
            assert_eq!(stream_name, &stream_key, "Unexpected stream name");
        }

        x => panic!("Unexpected media output content type: {:?}", x),
    }
}

fn create_definition(port: u16, app: &str, key: &str) -> WorkflowStepDefinition {
    let mut definition = WorkflowStepDefinition {
        step_type: WorkflowStepType("rtmp_watch".to_string()),
        parameters: HashMap::new(),
    };

    definition
        .parameters
        .insert(PORT_PROPERTY_NAME.to_string(), port.to_string());
    definition
        .parameters
        .insert(APP_PROPERTY_NAME.to_string(), app.to_string());
    definition
        .parameters
        .insert(STREAM_KEY_PROPERTY_NAME.to_string(), key.to_string());

    definition
}

fn create_initialized_step<'a>() -> (
    Box<dyn WorkflowStep>,
    Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>,
    UnboundedReceiver<RtmpEndpointMediaMessage>,
    UnboundedSender<RtmpEndpointWatcherNotification>,
) {
    let definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    let (sender, mut receiver) = unbounded_channel();

    let (step, init_results) = RtmpWatchStepGenerator::new(sender)
        .generate(definition)
        .expect("Error returned when creating rtmp receive step");

    let message = receiver
        .try_recv()
        .expect("Unexpected error reading from the receiver");

    let (media_channel, notification_channel) = match message {
        RtmpEndpointRequest::ListenForWatchers {
            media_channel,
            notification_channel,
            ..
        } => (media_channel, notification_channel),

        message => panic!("Unexpected endpoint request received: {:?}", message),
    };

    (step, init_results, media_channel, notification_channel)
}

async fn create_active_step<'a>() -> (
    Box<dyn WorkflowStep>,
    Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>,
    UnboundedReceiver<RtmpEndpointMediaMessage>,
    UnboundedSender<RtmpEndpointWatcherNotification>,
) {
    let (mut step, futures, media_channel, notification_channel) = create_initialized_step();
    let _ =
        notification_channel.send(RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful);
    let result = get_pending_future_result(futures).await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.notifications.push(result);

    step.execute(&mut inputs, &mut outputs);

    assert_eq!(
        step.get_status(),
        &StepStatus::Active,
        "Unexpected step result"
    );

    (step, outputs.futures, media_channel, notification_channel)
}
