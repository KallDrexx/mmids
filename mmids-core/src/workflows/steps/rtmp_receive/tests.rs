use super::super::test_utils::{create_step_parameters, get_pending_future_result};
use super::*;
use crate::codecs::{AudioCodec, VideoCodec};
use crate::net::ConnectionId;
use crate::workflows::definitions::WorkflowStepType;
use crate::workflows::{MediaNotification, MediaNotificationContent};
use crate::StreamId;
use bytes::Bytes;
use rml_rtmp::sessions::StreamMetadata;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::HashMap;
use std::time::Duration;
use futures::future::BoxFuture;
use tokio::time::timeout;

const TEST_PORT: u16 = 9999;
const TEST_APP: &'static str = "some_app";
const TEST_KEY: &'static str = "test_key";

#[test]
fn can_create_from_filled_out_workflow_definition() {
    let definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    let (mock_sender, _mock_receiver) = unbounded_channel();

    let (step, _futures) =
        RtmpReceiverStep::new(&definition, mock_sender).expect("Error returned creating step");

    assert_eq!(step.port, TEST_PORT, "Unexpected port");
    assert_eq!(step.rtmp_app, TEST_APP.to_string(), "Unexpected rtmp app");
    assert_eq!(
        step.stream_key,
        StreamKeyRegistration::Exact(TEST_KEY.to_string()),
        "Unexpected stream key registration"
    );
}

#[test]
fn asterisk_for_key_sets_key_to_any() {
    let definition = create_definition(TEST_PORT, TEST_APP, "*");
    let (mock_sender, _mock_receiver) = unbounded_channel();

    let (step, _futures) =
        RtmpReceiverStep::new(&definition, mock_sender).expect("Error returned creating step}");

    assert_eq!(step.port, TEST_PORT, "Unexpected port");
    assert_eq!(step.rtmp_app, TEST_APP.to_string(), "Unexpected rtmp app");
    assert_eq!(
        step.stream_key,
        StreamKeyRegistration::Any,
        "Unexpected stream key registration"
    );
}

#[test]
fn port_is_1935_if_none_provided() {
    let mut definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    definition.parameters.remove(PORT_PROPERTY_NAME);

    let (mock_sender, _mock_receiver) = unbounded_channel();

    let (step, _futures) = RtmpReceiverStep::new(&definition, mock_sender)
        .expect("Error returned when creating rtmp receive step");

    assert_eq!(step.port, 1935, "Unexpected port");
}

#[test]
fn error_if_no_app_provided() {
    let mut definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    definition.parameters.remove(APP_PROPERTY_NAME);

    let (mock_sender, _mock_receiver) = unbounded_channel();

    RtmpReceiverStep::new(&definition, mock_sender)
        .err()
        .unwrap();
}

#[test]
fn error_if_no_stream_key_provided() {
    let mut definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    definition.parameters.remove(STREAM_KEY_PROPERTY_NAME);

    let (mock_sender, _mock_receiver) = unbounded_channel();

    RtmpReceiverStep::new(&definition, mock_sender)
        .err()
        .unwrap();
}

#[test]
fn rtmp_app_is_trimmed() {
    let mut definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    definition.parameters.insert(
        APP_PROPERTY_NAME.to_string(),
        " ".to_string() + TEST_APP + " ",
    );

    let (mock_sender, _mock_receiver) = unbounded_channel();

    let (step, _futures) = RtmpReceiverStep::new(&definition, mock_sender)
        .expect("Error returned when creating rtmp receive step");

    assert_eq!(step.rtmp_app, TEST_APP, "Unexpected rtmp app");
}

#[test]
fn stream_key_is_trimmed() {
    let mut definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    definition.parameters.insert(
        STREAM_KEY_PROPERTY_NAME.to_string(),
        " ".to_string() + TEST_KEY + " ",
    );

    let (mock_sender, _mock_receiver) = unbounded_channel();

    let (step, _futures) = RtmpReceiverStep::new(&definition, mock_sender)
        .expect("Error returned when creating rtmp receive step");

    assert_eq!(
        step.stream_key,
        StreamKeyRegistration::Exact(TEST_KEY.to_string()),
        "Unexpected stream key registration"
    );
}

#[test]
fn new_step_is_in_created_status() {
    let mut definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    definition.parameters.insert(
        STREAM_KEY_PROPERTY_NAME.to_string(),
        " ".to_string() + TEST_KEY + " ",
    );

    let (mock_sender, _mock_receiver) = unbounded_channel();

    let (step, _futures) = RtmpReceiverStep::new(&definition, mock_sender)
        .expect("Error returned when creating rtmp receive step");

    let status = step.get_status();
    assert_eq!(status, &StepStatus::Created, "Unexpected status");
}

#[test]
fn new_registers_for_publishing() {
    let definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    let (sender, mut receiver) = unbounded_channel();

    let (_step, _futures) = RtmpReceiverStep::new(&definition, sender)
        .expect("Error returned when creating rtmp receive step");

    let message = receiver
        .try_recv()
        .expect("Unexpected error reading from the receiver");

    match message {
        RtmpEndpointRequest::ListenForPublishers {
            rtmp_app,
            port,
            rtmp_stream_key,
            stream_id,
            message_channel: _,
        } => {
            assert_eq!(port, TEST_PORT, "Unexpected port");
            assert_eq!(rtmp_app, TEST_APP.to_string(), "Unexpected rtmp app");
            assert_eq!(
                rtmp_stream_key,
                StreamKeyRegistration::Exact(TEST_KEY.to_string()),
                "Unexpected stream key"
            );
            assert_eq!(stream_id, None, "No stream id was expected to be provided");
        }

        message => panic!("Unexpected endpoint request received: {:?}", message),
    };
}

#[tokio::test]
async fn init_returns_future_that_is_pending() {
    let (_step, mut futures, _message_channel) = create_initialized_step();

    assert_eq!(futures.len(), 1, "Unexpected number of futures returned");

    let future = futures.remove(0);
    match timeout(Duration::from_millis(10), future).await {
        Ok(_) => panic!("Expected future to be pending, but instead it had a result"),
        Err(_) => (),
    };
}

#[tokio::test]
async fn publish_failure_sets_step_to_error_mode() {
    let (mut step, futures, message_channel) = create_initialized_step();
    let _ = message_channel.send(RtmpEndpointPublisherMessage::PublisherRegistrationFailed);
    let notification = get_pending_future_result(futures).await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.notifications.push(notification);

    step.execute(&mut inputs, &mut outputs);

    let status = step.get_status();
    assert_eq!(status, &StepStatus::Error, "Unexpected step status");
}

#[tokio::test]
async fn publish_success_sets_step_to_ready_status() {
    let (mut step, futures, message_channel) = create_initialized_step();
    let _ = message_channel.send(RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful);
    let notification = get_pending_future_result(futures).await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.notifications.push(notification);

    step.execute(&mut inputs, &mut outputs);

    let status = step.get_status();
    assert_eq!(status, &StepStatus::Active, "Unexpected step status");
}

#[tokio::test]
async fn stream_started_notification_raised_when_publisher_connects() {
    let (mut step, futures, message_channel) = create_ready_step().await;
    let _ = message_channel.send(RtmpEndpointPublisherMessage::NewPublisherConnected {
        stream_id: StreamId("stream-id".to_string()),
        connection_id: ConnectionId("conn-id".to_string()),
        stream_key: "stream_key".to_string(),
    });

    let (mut inputs, mut outputs) = create_step_parameters();
    inputs
        .notifications
        .push(get_pending_future_result(futures).await);

    step.execute(&mut inputs, &mut outputs);

    assert_eq!(
        step.get_status(),
        &StepStatus::Active,
        "Unexpected step status"
    );
    assert_eq!(outputs.media.len(), 1, "Unexpected number of output media");

    let media = outputs.media.remove(0);
    assert_eq!(media.stream_id, StreamId("stream-id".to_string()));

    let stream_name = match media.content {
        MediaNotificationContent::NewIncomingStream { stream_name } => stream_name,
        content => panic!("Unexpected media notification: {:?}", content),
    };

    assert_eq!(
        stream_name,
        "stream_key".to_string(),
        "Unexpected stream name"
    );

    assert!(outputs.futures.len() > 0,
            "Expected at least one future to be returned for more endpoint notifications, but none were seen")
}

#[tokio::test]
async fn stream_disconnected_notification_raised_when_publisher_disconnects() {
    let stream_id = StreamId("stream-id".to_string());
    let connection_id = ConnectionId("conn-id".to_string());

    let (mut step, futures, message_channel) = create_ready_step().await;
    let _ = message_channel.send(RtmpEndpointPublisherMessage::NewPublisherConnected {
        stream_id: stream_id.clone(),
        connection_id: connection_id.clone(),
        stream_key: "stream_key".to_string(),
    });

    let (mut inputs, mut outputs) = create_step_parameters();
    inputs
        .notifications
        .push(get_pending_future_result(futures).await);

    step.execute(&mut inputs, &mut outputs);

    let _ = message_channel.send(RtmpEndpointPublisherMessage::PublishingStopped { connection_id });

    let notification = get_pending_future_result(outputs.futures).await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.notifications.push(notification);

    step.execute(&mut inputs, &mut outputs);

    assert_eq!(
        step.get_status(),
        &StepStatus::Active,
        "Unexpected step status"
    );
    assert_eq!(outputs.media.len(), 1, "Unexpected number of output media");

    let media = outputs.media.remove(0);
    assert_eq!(media.stream_id, stream_id);

    match media.content {
        MediaNotificationContent::StreamDisconnected => (),
        content => panic!("Unexpected media notification: {:?}", content),
    }

    assert!(outputs.futures.len() > 0,
            "Expected at least one future to be returned for more endpoint notifications, but none were seen")
}

#[tokio::test]
async fn metadata_notification_raised_when_publisher_sends_one() {
    let stream_id = StreamId("stream-id".to_string());
    let connection_id = ConnectionId("conn-id".to_string());

    let (mut step, futures, message_channel) = create_ready_step().await;
    let _ = message_channel.send(RtmpEndpointPublisherMessage::NewPublisherConnected {
        stream_id: stream_id.clone(),
        connection_id: connection_id.clone(),
        stream_key: "stream_key".to_string(),
    });

    let (mut inputs, mut outputs) = create_step_parameters();
    inputs
        .notifications
        .push(get_pending_future_result(futures).await);

    step.execute(&mut inputs, &mut outputs);

    let _ = message_channel.send(RtmpEndpointPublisherMessage::StreamMetadataChanged {
        publisher: connection_id,
        metadata: StreamMetadata::new(),
    });

    let notification = get_pending_future_result(outputs.futures).await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.notifications.push(notification);

    step.execute(&mut inputs, &mut outputs);

    assert_eq!(
        step.get_status(),
        &StepStatus::Active,
        "Unexpected step status"
    );
    assert_eq!(outputs.media.len(), 1, "Unexpected number of output media");

    let media = outputs.media.remove(0);
    assert_eq!(media.stream_id, StreamId("stream-id".to_string()));

    match media.content {
        MediaNotificationContent::Metadata { data: _ } => (),
        content => panic!("Unexpected media notification: {:?}", content),
    }

    assert!(outputs.futures.len() > 0,
            "Expected at least one future to be returned for more endpoint notifications, but none were seen")
}

#[tokio::test]
async fn video_notification_received_when_publisher_sends_video() {
    let stream_id = StreamId("stream-id".to_string());
    let connection_id = ConnectionId("conn-id".to_string());

    let (mut step, futures, message_channel) = create_ready_step().await;
    let _ = message_channel.send(RtmpEndpointPublisherMessage::NewPublisherConnected {
        stream_id: stream_id.clone(),
        connection_id: connection_id.clone(),
        stream_key: "stream_key".to_string(),
    });

    let (mut inputs, mut outputs) = create_step_parameters();
    inputs
        .notifications
        .push(get_pending_future_result(futures).await);

    step.execute(&mut inputs, &mut outputs);

    let _ = message_channel.send(RtmpEndpointPublisherMessage::NewVideoData {
        publisher: connection_id,
        is_sequence_header: true,
        is_keyframe: true,
        codec: VideoCodec::H264,
        data: Bytes::from(vec![3_u8, 4_u8]),
        timestamp: RtmpTimestamp::new(53),
    });

    let notification = get_pending_future_result(outputs.futures).await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.notifications.push(notification);

    step.execute(&mut inputs, &mut outputs);

    assert_eq!(
        step.get_status(),
        &StepStatus::Active,
        "Unexpected step status"
    );
    assert_eq!(outputs.media.len(), 1, "Unexpected number of output media");

    let media = outputs.media.remove(0);
    assert_eq!(media.stream_id, StreamId("stream-id".to_string()));

    match media.content {
        MediaNotificationContent::Video {
            is_sequence_header,
            is_keyframe,
            codec,
            timestamp,
            data,
        } => {
            assert!(is_keyframe, "Expected is keyframe to be true");
            assert!(is_sequence_header, "Expected is_sequence_header to be true");
            assert_eq!(codec, VideoCodec::H264, "Expected codec to be h264");
            assert_eq!(timestamp, Duration::from_millis(53), "Unexpected timestamp");
            assert_eq!(data.len(), 2, "Expected 2 bytes of data");
            assert_eq!(data[0], 3, "Expected first byte to be a 3");
            assert_eq!(data[1], 4, "Expected second byte to be a 4");
        }

        content => panic!("Unexpected media notification: {:?}", content),
    }

    assert!(outputs.futures.len() > 0,
            "Expected at least one future to be returned for more endpoint notifications, but none were seen")
}

#[tokio::test]
async fn audio_notification_received_when_publisher_sends_audio() {
    let stream_id = StreamId("stream-id".to_string());
    let connection_id = ConnectionId("conn-id".to_string());

    let (mut step, futures, message_channel) = create_ready_step().await;
    let _ = message_channel.send(RtmpEndpointPublisherMessage::NewPublisherConnected {
        stream_id: stream_id.clone(),
        connection_id: connection_id.clone(),
        stream_key: "stream_key".to_string(),
    });

    let (mut inputs, mut outputs) = create_step_parameters();
    inputs
        .notifications
        .push(get_pending_future_result(futures).await);

    step.execute(&mut inputs, &mut outputs);

    let _ = message_channel.send(RtmpEndpointPublisherMessage::NewAudioData {
        publisher: connection_id,
        is_sequence_header: true,
        codec: AudioCodec::Aac,
        data: Bytes::from(vec![3_u8, 4_u8]),
        timestamp: RtmpTimestamp::new(53),
    });

    let notification = get_pending_future_result(outputs.futures).await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.notifications.push(notification);

    step.execute(&mut inputs, &mut outputs);

    assert_eq!(
        step.get_status(),
        &StepStatus::Active,
        "Unexpected step status"
    );
    assert_eq!(outputs.media.len(), 1, "Unexpected number of output media");

    let media = outputs.media.remove(0);
    assert_eq!(media.stream_id, StreamId("stream-id".to_string()));

    match media.content {
        MediaNotificationContent::Audio {
            is_sequence_header,
            codec,
            timestamp,
            data,
        } => {
            assert!(is_sequence_header, "Expected is_sequence_header to be true");
            assert_eq!(codec, AudioCodec::Aac, "Expected codec to be aac");
            assert_eq!(timestamp, Duration::from_millis(53), "Unexpected timestamp");
            assert_eq!(data.len(), 2, "Expected 2 bytes of data");
            assert_eq!(data[0], 3, "Expected first byte to be a 3");
            assert_eq!(data[1], 4, "Expected second byte to be a 4");
        }

        content => panic!("Unexpected media notification: {:?}", content),
    }

    assert!(outputs.futures.len() > 0,
            "Expected at least one future to be returned for more endpoint notifications, but none were seen")
}

#[tokio::test]
async fn media_input_does_not_get_passed_through() {
    let (mut step, _futures, _message_channel) = create_ready_step().await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.media.push(MediaNotification {
        stream_id: StreamId("a".to_string()),
        content: MediaNotificationContent::Video {
            codec: VideoCodec::H264,
            data: Bytes::new(),
            is_keyframe: true,
            timestamp: Duration::from_millis(0),
            is_sequence_header: true,
        },
    });

    inputs.media.push(MediaNotification {
        stream_id: StreamId("b".to_string()),
        content: MediaNotificationContent::Audio {
            is_sequence_header: true,
            codec: AudioCodec::Aac,
            data: Bytes::new(),
            timestamp: Duration::from_millis(0),
        },
    });

    inputs.media.push(MediaNotification {
        stream_id: StreamId("c".to_string()),
        content: MediaNotificationContent::Metadata {
            data: HashMap::new(),
        },
    });

    inputs.media.push(MediaNotification {
        stream_id: StreamId("d".to_string()),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: "something".to_string(),
        },
    });

    inputs.media.push(MediaNotification {
        stream_id: StreamId("e".to_string()),
        content: MediaNotificationContent::StreamDisconnected,
    });

    step.execute(&mut inputs, &mut outputs);

    assert_eq!(outputs.media.len(), 0, "Expected media outputs to be empty");
    assert_eq!(
        outputs.futures.len(),
        0,
        "Expected futures output to be empty"
    )
}

fn create_initialized_step<'a>() -> (
    RtmpReceiverStep,
    Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>,
    UnboundedSender<RtmpEndpointPublisherMessage>,
) {
    let definition = create_definition(TEST_PORT, TEST_APP, TEST_KEY);
    let (sender, mut receiver) = unbounded_channel();

    let (step, init_results) = RtmpReceiverStep::new(&definition, sender)
        .expect("Error returned when creating rtmp receive step");

    let message = receiver
        .try_recv()
        .expect("Unexpected error reading from the receiver");

    let message_channel = match message {
        RtmpEndpointRequest::ListenForPublishers {
            message_channel, ..
        } => message_channel,

        message => panic!("Unexpected endpoint request received: {:?}", message),
    };

    (step, init_results, message_channel)
}

async fn create_ready_step<'a>() -> (
    RtmpReceiverStep,
    Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>,
    UnboundedSender<RtmpEndpointPublisherMessage>,
) {
    let (mut step, futures, message_channel) = create_initialized_step();
    let _ = message_channel.send(RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful);

    let notification = get_pending_future_result(futures).await;
    let (mut inputs, mut outputs) = create_step_parameters();
    inputs.notifications.push(notification);

    step.execute(&mut inputs, &mut outputs);

    (step, outputs.futures, message_channel)
}

fn create_definition(port: u16, app: &str, key: &str) -> WorkflowStepDefinition {
    let mut definition = WorkflowStepDefinition {
        step_type: WorkflowStepType("rtmp_receive".to_string()),
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
