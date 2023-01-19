use crate::endpoint::{
    AudioTranscodeParams, FfmpegEndpointNotification, FfmpegEndpointRequest, FfmpegParams,
    H264Preset, TargetParams, VideoTranscodeParams,
};
use crate::workflow_steps::ffmpeg_transcode::{
    FfmpegTranscoderStepGenerator, AUDIO_CODEC_NAME, BITRATE_NAME, H264_PRESET_NAME, SIZE_NAME,
    VIDEO_CODEC_NAME,
};
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use mmids_core::codecs::{AUDIO_CODEC_AAC_RAW, VIDEO_CODEC_H264_AVC};
use mmids_core::net::ConnectionId;
use mmids_core::workflows::definitions::{WorkflowStepDefinition, WorkflowStepType};
use mmids_core::workflows::metadata::common_metadata::{
    get_is_keyframe_metadata_key, get_pts_offset_metadata_key,
};
use mmids_core::workflows::metadata::{
    MediaPayloadMetadataCollection, MetadataKey, MetadataKeyMap, MetadataValue,
};
use mmids_core::workflows::steps::test_utils::StepTestContext;
use mmids_core::workflows::steps::StepStatus;
use mmids_core::workflows::{MediaNotification, MediaNotificationContent, MediaType};
use mmids_core::{test_utils, StreamId};
use mmids_rtmp::rtmp_server::{
    RtmpEndpointMediaData, RtmpEndpointMediaMessage, RtmpEndpointPublisherMessage,
    RtmpEndpointRequest, RtmpEndpointWatcherNotification, StreamKeyRegistration,
};
use rml_rtmp::sessions::StreamMetadata;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

struct TestContext {
    step_context: StepTestContext,
    rtmp_endpoint: UnboundedReceiver<RtmpEndpointRequest>,
    ffmpeg_endpoint: UnboundedReceiver<FfmpegEndpointRequest>,
    is_keyframe_metadata_key: MetadataKey,
    pts_offset_metadata_key: MetadataKey,
}

struct DefinitionBuilder {
    vcodec: Option<String>,
    acodec: Option<String>,
    h264_preset: Option<String>,
    size: Option<String>,
    bitrate: Option<u16>,
}

impl DefinitionBuilder {
    fn new() -> Self {
        DefinitionBuilder {
            vcodec: None,
            acodec: None,
            h264_preset: None,
            size: None,
            bitrate: None,
        }
    }

    fn vcodec(mut self, vcodec: &str) -> Self {
        self.vcodec = Some(vcodec.to_string());
        self
    }

    fn acodec(mut self, acodec: &str) -> Self {
        self.acodec = Some(acodec.to_string());
        self
    }

    fn h264_preset(mut self, preset: &str) -> Self {
        self.h264_preset = Some(preset.to_string());
        self
    }

    fn size(mut self, size: &str) -> Self {
        self.size = Some(size.to_string());
        self
    }

    fn bitrate(mut self, bitrate: u16) -> Self {
        self.bitrate = Some(bitrate);
        self
    }

    fn build(self) -> WorkflowStepDefinition {
        let mut definition = WorkflowStepDefinition {
            step_type: WorkflowStepType("ffmpeg_transocde".to_string()),
            parameters: HashMap::new(),
        };

        if let Some(vcodec) = self.vcodec {
            definition
                .parameters
                .insert(VIDEO_CODEC_NAME.to_string(), Some(vcodec));
        } else {
            definition
                .parameters
                .insert(VIDEO_CODEC_NAME.to_string(), Some("copy".to_string()));
        }

        if let Some(acodec) = self.acodec {
            definition
                .parameters
                .insert(AUDIO_CODEC_NAME.to_string(), Some(acodec));
        } else {
            definition
                .parameters
                .insert(AUDIO_CODEC_NAME.to_string(), Some("copy".to_string()));
        }

        if let Some(preset) = self.h264_preset {
            definition
                .parameters
                .insert(H264_PRESET_NAME.to_string(), Some(preset));
        }

        if let Some(size) = self.size {
            definition
                .parameters
                .insert(SIZE_NAME.to_string(), Some(size));
        }

        if let Some(bitrate) = self.bitrate {
            definition
                .parameters
                .insert(BITRATE_NAME.to_string(), Some(bitrate.to_string()));
        }

        definition
    }
}

impl TestContext {
    fn new(definition: WorkflowStepDefinition) -> Result<Self> {
        let (rtmp_sender, rtmp_receiver) = unbounded_channel();
        let (ffmpeg_sender, ffmpeg_receiver) = unbounded_channel();

        let mut metadata_map = MetadataKeyMap::new();
        let is_keyframe_metadata_key = get_is_keyframe_metadata_key(&mut metadata_map);
        let pts_offset_metadata_key = get_pts_offset_metadata_key(&mut metadata_map);

        let generator = FfmpegTranscoderStepGenerator {
            ffmpeg_endpoint: ffmpeg_sender,
            rtmp_server_endpoint: rtmp_sender,
            is_keyframe_metadata_key,
            pts_offset_metadata_key,
        };

        let step_context = StepTestContext::new(Box::new(generator), definition)?;

        Ok(TestContext {
            step_context,
            rtmp_endpoint: rtmp_receiver,
            ffmpeg_endpoint: ffmpeg_receiver,
            is_keyframe_metadata_key,
            pts_offset_metadata_key,
        })
    }

    async fn accept_watch_registration(
        &mut self,
    ) -> (
        UnboundedSender<RtmpEndpointWatcherNotification>,
        UnboundedReceiver<RtmpEndpointMediaMessage>,
    ) {
        let request = test_utils::expect_mpsc_response(&mut self.rtmp_endpoint).await;
        let channels = match request {
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

        self.step_context.execute_pending_futures().await;

        channels
    }

    async fn accept_publish_registration(
        &mut self,
    ) -> UnboundedSender<RtmpEndpointPublisherMessage> {
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

        self.step_context.execute_pending_futures().await;

        channel
    }

    async fn process_ffmpeg_event(
        &mut self,
    ) -> (
        UnboundedSender<FfmpegEndpointNotification>,
        FfmpegParams,
        Uuid,
    ) {
        let request = test_utils::expect_mpsc_response(&mut self.ffmpeg_endpoint).await;
        match request {
            FfmpegEndpointRequest::StartFfmpeg {
                notification_channel,
                params,
                id,
            } => (notification_channel, params, id),

            request => panic!("Unexpected request: {:?}", request),
        }
    }
}

#[tokio::test]
async fn step_starts_in_active_state() {
    let definition = DefinitionBuilder::new().build();
    let context = TestContext::new(definition).unwrap();

    let status = context.step_context.status;
    assert_eq!(status, StepStatus::Active, "Unexpected step status");
}

#[tokio::test]
async fn step_fails_to_build_when_invalid_vcodec_specified() {
    let definition = DefinitionBuilder::new().vcodec("abcdef").build();

    match TestContext::new(definition) {
        Err(_) => (),
        Ok(_) => panic!("Expected failure"),
    }
}

#[tokio::test]
async fn step_fails_to_build_when_no_vcodec_specified() {
    let mut definition = DefinitionBuilder::new().build();
    definition.parameters.remove(VIDEO_CODEC_NAME);

    match TestContext::new(definition) {
        Err(_) => (),
        Ok(_) => panic!("Expected failure"),
    }
}

#[test]
fn step_fails_to_build_when_invalid_acodec_specified() {
    let definition = DefinitionBuilder::new().acodec("abcdef").build();

    match TestContext::new(definition) {
        Err(_) => (),
        Ok(_) => panic!("Expected failure"),
    }
}

#[test]
fn step_fails_to_build_when_no_acodec_specified() {
    let mut definition = DefinitionBuilder::new().build();
    definition.parameters.remove(AUDIO_CODEC_NAME);

    match TestContext::new(definition) {
        Err(_) => (),
        Ok(_) => panic!("Expected failure"),
    }
}

#[test]
fn step_fails_to_build_when_h264_specified_and_no_preset_specified() {
    let mut definition = DefinitionBuilder::new().vcodec("abcdef").build();
    definition.parameters.remove(H264_PRESET_NAME);

    match TestContext::new(definition) {
        Err(_) => (),
        Ok(_) => panic!("Expected failure"),
    }
}

#[test]
fn step_fails_to_build_when_h264_specified_and_invalid_preset() {
    let definition = DefinitionBuilder::new()
        .vcodec("h264")
        .h264_preset("abc")
        .build();

    match TestContext::new(definition) {
        Err(_) => (),
        Ok(_) => panic!("Expected failure"),
    }
}

#[test]
fn step_fails_to_build_when_invalid_size_specified() {
    let definition = DefinitionBuilder::new().size("abc").build();

    match TestContext::new(definition) {
        Err(_) => (),
        Ok(_) => panic!("Expected failure"),
    }
}

#[tokio::test]
async fn rtmp_watch_registration_raised_on_new_stream() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let request = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
    match request {
        RtmpEndpointRequest::ListenForWatchers {
            rtmp_stream_key, ..
        } => {
            assert_eq!(
                rtmp_stream_key,
                StreamKeyRegistration::Exact(Arc::new("abc".to_string())),
                "Unexpected stream key"
            );
        }

        request => panic!("Unexpected request received: {:?}", request),
    }
}

#[tokio::test]
async fn rtmp_publish_registration_raised_after_watch_accepted() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let _watch_channels = context.accept_watch_registration().await;

    let request = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
    match request {
        RtmpEndpointRequest::ListenForPublishers {
            rtmp_stream_key, ..
        } => {
            assert_eq!(
                rtmp_stream_key,
                StreamKeyRegistration::Exact(Arc::new("abc".to_string())),
                "Unexpected stream key"
            );
        }

        request => panic!("Unexpected request received: {:?}", request),
    }
}

#[tokio::test]
async fn ffmpeg_request_raised_after_publish_accepted() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let _watch_channels = context.accept_watch_registration().await;
    let _publish_channel = context.accept_publish_registration().await;

    let request = test_utils::expect_mpsc_response(&mut context.ffmpeg_endpoint).await;
    match request {
        FfmpegEndpointRequest::StartFfmpeg { .. } => (),
        request => panic!("Unexpected request: {:?}", request),
    }
}

#[tokio::test]
async fn h264_with_preset_passed_to_ffmpeg() {
    let definition = DefinitionBuilder::new()
        .vcodec("h264")
        .h264_preset("ultrafast")
        .build();

    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let _watch_channels = context.accept_watch_registration().await;
    let _publish_channel = context.accept_publish_registration().await;
    let (_channel, params, _id) = context.process_ffmpeg_event().await;

    match params.video_transcode {
        VideoTranscodeParams::H264 {
            preset: H264Preset::UltraFast,
        } => (),
        params => panic!("Unexpected video params: {:?}", params),
    }
}

#[tokio::test]
async fn video_copy_passed_to_ffmpeg() {
    let definition = DefinitionBuilder::new().vcodec("copy").build();

    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let _watch_channels = context.accept_watch_registration().await;
    let _publish_channel = context.accept_publish_registration().await;
    let (_channel, params, _id) = context.process_ffmpeg_event().await;

    match params.video_transcode {
        VideoTranscodeParams::Copy => (),
        params => panic!("Unexpected video params: {:?}", params),
    }
}

#[tokio::test]
async fn aac_acodec_passed_to_ffmpeg() {
    let definition = DefinitionBuilder::new().acodec("aac").build();

    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let _watch_channels = context.accept_watch_registration().await;
    let _publish_channel = context.accept_publish_registration().await;
    let (_channel, params, _id) = context.process_ffmpeg_event().await;

    match params.audio_transcode {
        AudioTranscodeParams::Aac => (),
        params => panic!("Unexpected video params: {:?}", params),
    }
}

#[tokio::test]
async fn copy_acodec_passed_to_ffmpeg() {
    let definition = DefinitionBuilder::new().acodec("copy").build();

    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let _watch_channels = context.accept_watch_registration().await;
    let _publish_channel = context.accept_publish_registration().await;
    let (_channel, params, _id) = context.process_ffmpeg_event().await;

    match params.audio_transcode {
        AudioTranscodeParams::Copy => (),
        params => panic!("Unexpected video params: {:?}", params),
    }
}

#[tokio::test]
async fn size_passed_to_ffmpeg() {
    let definition = DefinitionBuilder::new()
        .vcodec("h264")
        .h264_preset("ultrafast")
        .size("1920x1080")
        .build();

    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let _watch_channels = context.accept_watch_registration().await;
    let _publish_channel = context.accept_publish_registration().await;
    let (_channel, params, _id) = context.process_ffmpeg_event().await;

    let scale = params.scale.expect("Expected scale parameters");
    assert_eq!(scale.width, 1920, "Unexpected width");
    assert_eq!(scale.height, 1080, "Unexpected height");
}

#[tokio::test]
async fn bitrate_passed_to_ffmpeg() {
    let definition = DefinitionBuilder::new()
        .vcodec("h264")
        .h264_preset("ultrafast")
        .bitrate(1233)
        .build();

    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let _watch_channels = context.accept_watch_registration().await;
    let _publish_channel = context.accept_publish_registration().await;
    let (_channel, params, _id) = context.process_ffmpeg_event().await;

    let bitrate = params.bitrate_in_kbps.expect("Expected bitrate value");
    assert_eq!(bitrate, 1233, "Unexpected bitrate");
}

#[tokio::test]
async fn ffmpeg_always_told_to_read_in_real_time() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let _watch_channels = context.accept_watch_registration().await;
    let _publish_channel = context.accept_publish_registration().await;
    let (_channel, params, _id) = context.process_ffmpeg_event().await;

    assert!(
        params.read_in_real_time,
        "Expected read in real time to be true"
    );
}

#[tokio::test]
async fn ffmpeg_instructed_to_read_from_rtmp() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let _watch_channels = context.accept_watch_registration().await;
    let _publish_channel = context.accept_publish_registration().await;
    let (_channel, params, _id) = context.process_ffmpeg_event().await;

    match params.target {
        TargetParams::Rtmp { url } => {
            assert!(
                url.starts_with("rtmp://localhost/"),
                "Unexpected start of url: {}",
                url
            );
            assert!(url.ends_with("/abc"), "Unexpected end of url: {}", url);
        }

        target => panic!("Unexpected target: {:?}", target),
    }
}

#[tokio::test]
async fn if_ffmpeg_process_stops_unexpectedly_it_starts_again_with_same_id_and_params() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let _watch_channels = context.accept_watch_registration().await;
    let _publish_channel = context.accept_publish_registration().await;
    let (ffmpeg_channel, params, id) = context.process_ffmpeg_event().await;

    ffmpeg_channel
        .send(FfmpegEndpointNotification::FfmpegStopped)
        .expect("Failed to send ffmpeg stopped command");

    context.step_context.execute_pending_futures().await;

    let (_channel, new_params, new_id) = context.process_ffmpeg_event().await;

    assert_eq!(new_params, params, "Parameters were not equal");
    assert_eq!(new_id, id, "Ids were not equal");
}

#[tokio::test]
async fn stream_started_notification_passed_through_immediately() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context
        .step_context
        .assert_media_passed_through(MediaNotification {
            stream_id: StreamId(Arc::new("abc".to_string())),
            content: MediaNotificationContent::NewIncomingStream {
                stream_name: Arc::new("abc".to_string()),
            },
        });
}

#[tokio::test]
async fn disconnection_notification_passed_through_immediately() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context
        .step_context
        .assert_media_passed_through(MediaNotification {
            stream_id: StreamId(Arc::new("abc".to_string())),
            content: MediaNotificationContent::StreamDisconnected,
        });
}
#[tokio::test]
async fn metadata_notification_passed_as_input_does_not_get_passed_as_output() {
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

#[tokio::test]
async fn video_notification_passed_as_input_does_not_get_passed_as_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context
        .step_context
        .assert_media_not_passed_through(MediaNotification {
            stream_id: StreamId(Arc::new("test".to_string())),
            content: MediaNotificationContent::MediaPayload {
                media_type: MediaType::Video,
                payload_type: VIDEO_CODEC_H264_AVC.clone(),
                timestamp: Duration::from_millis(0),
                is_required_for_decoding: true,
                metadata: MediaPayloadMetadataCollection::new(iter::empty(), &mut BytesMut::new()),
                data: Bytes::from(vec![1, 2]),
            },
        });
}

#[tokio::test]
async fn audio_notification_passed_as_input_does_not_get_passed_as_output() {
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
async fn video_packet_sent_to_watcher_media_channel() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let (_notification, mut media_channel) = context.accept_watch_registration().await;
    let _publish_channel = context.accept_publish_registration().await;
    let _ffmpeg_results = context.process_ffmpeg_event().await;

    let media = MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::MediaPayload {
            media_type: MediaType::Video,
            payload_type: VIDEO_CODEC_H264_AVC.clone(),
            timestamp: Duration::from_millis(0),
            is_required_for_decoding: false,
            metadata: MediaPayloadMetadataCollection::new(iter::empty(), &mut BytesMut::new()),
            data: Bytes::from_static(&[1, 2]),
        },
    };

    context.step_context.execute_with_media(media.clone());

    let response = test_utils::expect_mpsc_response(&mut media_channel).await;
    assert_eq!(response.stream_key.as_str(), "abc", "Unexpected stream key");

    let expected_endpoint_media_data = RtmpEndpointMediaData::from_media_notification_content(
        media.content,
        context.is_keyframe_metadata_key,
        context.pts_offset_metadata_key,
    )
    .unwrap();

    assert_eq!(
        response.data, expected_endpoint_media_data,
        "Unexpected media sent"
    );
}

#[tokio::test]
async fn audio_packet_sent_to_watcher_media_channel() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let (_notification, mut media_channel) = context.accept_watch_registration().await;
    let _publish_channel = context.accept_publish_registration().await;
    let _ffmpeg_results = context.process_ffmpeg_event().await;

    let media = MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::MediaPayload {
            data: Bytes::from(vec![1, 2]),
            timestamp: Duration::from_millis(5),
            is_required_for_decoding: true,
            media_type: MediaType::Audio,
            payload_type: AUDIO_CODEC_AAC_RAW.clone(),
            metadata: MediaPayloadMetadataCollection::new(iter::empty(), &mut BytesMut::new()),
        },
    };

    context.step_context.execute_with_media(media.clone());

    let response = test_utils::expect_mpsc_response(&mut media_channel).await;
    assert_eq!(response.stream_key.as_str(), "abc", "Unexpected stream key");

    let expected_endpoint_media_data = RtmpEndpointMediaData::from_media_notification_content(
        media.content,
        context.is_keyframe_metadata_key,
        context.pts_offset_metadata_key,
    )
    .unwrap();

    assert_eq!(
        response.data, expected_endpoint_media_data,
        "Unexpected media sent"
    );
}

#[tokio::test]
async fn metadata_packet_sent_to_watcher_media_channel() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let (_notification, mut media_channel) = context.accept_watch_registration().await;
    let _publish_channel = context.accept_publish_registration().await;
    let _ffmpeg_results = context.process_ffmpeg_event().await;

    let media = MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::Metadata {
            data: HashMap::new(),
        },
    };

    context.step_context.execute_with_media(media.clone());
    context.step_context.execute_pending_futures().await;

    let response = test_utils::expect_mpsc_response(&mut media_channel).await;
    assert_eq!(response.stream_key.as_str(), "abc", "Unexpected stream key");

    let expected_endpoint_media_data = RtmpEndpointMediaData::from_media_notification_content(
        media.content,
        context.is_keyframe_metadata_key,
        context.pts_offset_metadata_key,
    )
    .unwrap();

    assert_eq!(
        response.data, expected_endpoint_media_data,
        "Unexpected media sent"
    );
}

#[tokio::test]
async fn video_packet_with_other_stream_id_not_sent_to_watcher_media_channel() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let (_notification, mut media_channel) = context.accept_watch_registration().await;
    let _publish_channel = context.accept_publish_registration().await;
    let _ffmpeg_results = context.process_ffmpeg_event().await;

    let media = MediaNotification {
        stream_id: StreamId(Arc::new("test".to_string())),
        content: MediaNotificationContent::MediaPayload {
            media_type: MediaType::Video,
            payload_type: VIDEO_CODEC_H264_AVC.clone(),
            data: Bytes::from(vec![1, 2]),
            timestamp: Duration::new(0, 0),
            is_required_for_decoding: true,
            metadata: MediaPayloadMetadataCollection::new(iter::empty(), &mut BytesMut::new()),
        },
    };

    context.step_context.execute_with_media(media.clone());

    test_utils::expect_mpsc_timeout(&mut media_channel).await;
}

#[tokio::test]
async fn video_packet_from_publisher_passed_as_media_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let _watch_channels = context.accept_watch_registration().await;
    let publish_channel = context.accept_publish_registration().await;
    let _ffmpeg_results = context.process_ffmpeg_event().await;

    publish_channel
        .send(RtmpEndpointPublisherMessage::NewVideoData {
            publisher: ConnectionId(Arc::new("connection".to_string())),
            data: Bytes::from(vec![1, 2, 3]),
            timestamp: RtmpTimestamp::new(5),
            is_keyframe: true,
            is_sequence_header: true,
            composition_time_offset: 123,
        })
        .expect("Failed to send video message");

    context.step_context.execute_pending_futures().await;

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(
        media.stream_id.0.as_str(),
        "abc",
        "Expected media to have original stream id"
    );

    match &media.content {
        MediaNotificationContent::MediaPayload {
            media_type,
            payload_type,
            data,
            timestamp,
            is_required_for_decoding,
            metadata,
        } => {
            let is_keyframe = metadata
                .iter()
                .filter(|m| m.key() == context.is_keyframe_metadata_key)
                .filter_map(|m| match m.value() {
                    MetadataValue::Bool(val) => Some(val),
                    _ => None,
                })
                .next()
                .unwrap_or_default();

            let pts_offset = metadata
                .iter()
                .filter(|m| m.key() == context.pts_offset_metadata_key)
                .filter_map(|m| match m.value() {
                    MetadataValue::I32(val) => Some(val),
                    _ => None,
                })
                .next()
                .unwrap_or_default();

            assert_eq!(*media_type, MediaType::Video);
            assert_eq!(
                *payload_type, *VIDEO_CODEC_H264_AVC,
                "Unexpected payload type"
            );
            assert_eq!(data, &vec![1, 2, 3], "Unexpected bytes");
            assert_eq!(timestamp, &Duration::from_millis(5), "Unexpected dts");
            assert!(
                is_required_for_decoding,
                "Expected is_required_for_decoding to be true"
            );
            assert!(is_keyframe, "Expected is_keyframe to be true");
            assert_eq!(pts_offset, 123, "Unexpected pts offset");
        }

        _ => panic!("Unexpected media content: {:?}", media.content),
    }
}

#[tokio::test]
async fn audio_packet_from_publisher_passed_as_media_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let _watch_channels = context.accept_watch_registration().await;
    let publish_channel = context.accept_publish_registration().await;
    let _ffmpeg_results = context.process_ffmpeg_event().await;

    publish_channel
        .send(RtmpEndpointPublisherMessage::NewAudioData {
            publisher: ConnectionId(Arc::new("connection".to_string())),
            data: Bytes::from(vec![1, 2, 3]),
            timestamp: RtmpTimestamp::new(5),
            is_sequence_header: true,
        })
        .expect("Failed to send video message");

    context.step_context.execute_pending_futures().await;

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(
        media.stream_id.0.as_str(),
        "abc",
        "Expected media to have original stream id"
    );

    let expected_content = MediaNotificationContent::MediaPayload {
        is_required_for_decoding: true,
        timestamp: Duration::from_millis(5),
        data: Bytes::from_static(&[1, 2, 3]),
        media_type: MediaType::Audio,
        payload_type: AUDIO_CODEC_AAC_RAW.clone(),
        metadata: MediaPayloadMetadataCollection::new(iter::empty(), &mut BytesMut::new()),
    };

    assert_eq!(media.content, expected_content, "Unexpected media content");
}

#[tokio::test]
async fn metadata_packet_from_publisher_passed_as_media_output() {
    let definition = DefinitionBuilder::new().build();
    let mut context = TestContext::new(definition).unwrap();

    context.step_context.execute_with_media(MediaNotification {
        stream_id: StreamId(Arc::new("abc".to_string())),
        content: MediaNotificationContent::NewIncomingStream {
            stream_name: Arc::new("def".to_string()),
        },
    });

    let _watch_channels = context.accept_watch_registration().await;
    let publish_channel = context.accept_publish_registration().await;
    let _ffmpeg_results = context.process_ffmpeg_event().await;

    publish_channel
        .send(RtmpEndpointPublisherMessage::StreamMetadataChanged {
            publisher: ConnectionId(Arc::new("connection".to_string())),
            metadata: StreamMetadata::new(),
        })
        .expect("Failed to send video message");

    context.step_context.execute_pending_futures().await;

    assert_eq!(
        context.step_context.media_outputs.len(),
        1,
        "Unexpected number of media outputs"
    );

    let media = &context.step_context.media_outputs[0];
    assert_eq!(
        media.stream_id.0.as_str(),
        "abc",
        "Expected media to have original stream id"
    );

    match &media.content {
        MediaNotificationContent::Metadata { data: _ } => (),
        _ => panic!("Unexpected media content: {:?}", media.content),
    }
}
