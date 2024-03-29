use super::external_stream_handler::{ExternalStreamHandler, StreamHandlerFutureWrapper};
use crate::rtmp_server::{
    IpRestriction, RegistrationType, RtmpEndpointMediaData, RtmpEndpointMediaMessage,
    RtmpEndpointRequest, RtmpEndpointWatcherNotification, StreamKeyRegistration,
};
use crate::workflow_steps::external_stream_handler::{
    ExternalStreamHandlerGenerator, ResolvedFutureStatus,
};
use mmids_core::workflows::metadata::MetadataKey;
use mmids_core::workflows::steps::futures_channel::WorkflowStepFuturesChannel;
use mmids_core::workflows::steps::{StepFutureResult, StepOutputs, StepStatus};
use mmids_core::workflows::{MediaNotification, MediaNotificationContent};
use mmids_core::StreamId;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tracing::{error, info, warn};

/// Represents logic for a basic workflow step that exposes streams to an RTMP endpoint
/// so that an external system can read the video stream.  This exposes a read-only interface for
/// media, which means the external system is not expected to push media back into the same workflow
/// as the same identifiable stream.  An example of this is providing media for ffmpeg to generate
/// HLS feeds for.
///
/// Since this is a read-only interface all media passed into it will flow as-is to the next
/// workflow step.
pub struct ExternalStreamReader {
    pub status: StepStatus,
    rtmp_server_endpoint: UnboundedSender<RtmpEndpointRequest>,
    watcher_app_name: Arc<String>,
    active_streams: HashMap<StreamId, ActiveStream>,
    stream_handler_generator: Box<dyn ExternalStreamHandlerGenerator + Sync + Send>,
    is_keyframe_metadata_key: MetadataKey,
    pts_offset_metadata_key: MetadataKey,
}

#[derive(Debug)]
enum WatchRegistrationStatus {
    Inactive,
    Pending {
        media_channel: UnboundedSender<RtmpEndpointMediaMessage>,
    },
    Active {
        media_channel: UnboundedSender<RtmpEndpointMediaMessage>,
    },
}

struct ActiveStream {
    id: StreamId,
    stream_name: Arc<String>,
    pending_media: VecDeque<MediaNotificationContent>,
    rtmp_output_status: WatchRegistrationStatus,
    external_stream_handler: Box<dyn ExternalStreamHandler + Sync + Send>,
}

enum FutureResult {
    RtmpEndpointGone,
    WatchChannelGone(StreamId),
    WatchNotificationReceived(StreamId, RtmpEndpointWatcherNotification),
}

impl StepFutureResult for FutureResult {}

impl ExternalStreamReader {
    pub fn new(
        watcher_rtmp_app_name: Arc<String>,
        rtmp_server: UnboundedSender<RtmpEndpointRequest>,
        external_handler_generator: Box<dyn ExternalStreamHandlerGenerator + Sync + Send>,
        is_keyframe_metadata_key: MetadataKey,
        pts_offset_metadata_key: MetadataKey,
        futures_channel: &WorkflowStepFuturesChannel,
    ) -> Self {
        let step = ExternalStreamReader {
            status: StepStatus::Active,
            watcher_app_name: watcher_rtmp_app_name,
            rtmp_server_endpoint: rtmp_server.clone(),
            active_streams: HashMap::new(),
            stream_handler_generator: external_handler_generator,
            is_keyframe_metadata_key,
            pts_offset_metadata_key,
        };

        futures_channel.send_on_generic_future_completion(async move {
            rtmp_server.closed().await;
            FutureResult::RtmpEndpointGone
        });

        step
    }

    pub fn handle_resolved_future(
        &mut self,
        notification: Box<dyn StepFutureResult>,
        futures_channel: &WorkflowStepFuturesChannel,
    ) {
        let notification = match notification.downcast::<StreamHandlerFutureWrapper>() {
            Err(e) => e,
            Ok(wrapper) => {
                let result = if let Some(stream) = self.active_streams.get_mut(&wrapper.stream_id) {
                    stream
                        .external_stream_handler
                        .handle_resolved_future(wrapper.future)
                } else {
                    ResolvedFutureStatus::Success
                };

                match result {
                    ResolvedFutureStatus::Success => {
                        self.prepare_stream(wrapper.stream_id, futures_channel)
                    }
                    ResolvedFutureStatus::StreamShouldBeStopped => {
                        self.stop_stream(&wrapper.stream_id);
                    }
                }

                return;
            }
        };

        let notification = match notification.downcast::<FutureResult>() {
            Ok(x) => *x,
            Err(_) => return,
        };

        match notification {
            FutureResult::RtmpEndpointGone => {
                error!("RTMP endpoint is gone!");
                self.status = StepStatus::Error {
                    message: "RTMP endpoint is gone".to_string(),
                };
                self.stop_all_streams();
            }

            FutureResult::WatchChannelGone(stream_id) => {
                if self.stop_stream(&stream_id) {
                    error!(stream_id = ?stream_id, "Rtmp watch channel disappeared for stream id {:?}", stream_id);
                }
            }

            FutureResult::WatchNotificationReceived(stream_id, notification) => {
                if !self.active_streams.contains_key(&stream_id) {
                    // late notification after stopping a stream
                    return;
                }

                self.handle_rtmp_watch_notification(stream_id, notification, futures_channel);
            }
        }
    }

    pub fn handle_media(
        &mut self,
        media: MediaNotification,
        outputs: &mut StepOutputs,
        futures_channel: &WorkflowStepFuturesChannel,
    ) {
        match &media.content {
            MediaNotificationContent::NewIncomingStream { stream_name } => {
                if let Some(stream) = self.active_streams.get(&media.stream_id) {
                    if &stream.stream_name != stream_name {
                        warn!(
                            stream_id = ?media.stream_id,
                            new_stream_name = %stream_name,
                            active_stream_name = %stream.stream_name,
                            "Unexpected new incoming stream notification received on \
                        stream id {:?} and stream name '{}', but we already have this stream id active \
                        for stream name '{}'.  Ignoring this notification",
                            media.stream_id, stream_name, stream.stream_name);
                    } else {
                        // Since the stream id / name combination is already set, this is a duplicate
                        // notification.  This is probably a bug somewhere but it's not harmful
                        // to ignore
                    }

                    return;
                }

                let stream = ActiveStream {
                    id: media.stream_id.clone(),
                    stream_name: stream_name.clone(),
                    pending_media: VecDeque::new(),
                    rtmp_output_status: WatchRegistrationStatus::Inactive,
                    external_stream_handler: self
                        .stream_handler_generator
                        .generate(media.stream_id.clone()),
                };

                self.active_streams.insert(media.stream_id.clone(), stream);
                self.prepare_stream(media.stream_id.clone(), futures_channel);
            }

            MediaNotificationContent::StreamDisconnected => {
                if self.stop_stream(&media.stream_id) {
                    info!(
                        stream_id = ?media.stream_id,
                        "Stopping stream id {:?} due to stream disconnection notification",
                        media.stream_id
                    );
                }
            }

            _ => {
                if let Some(stream) = self.active_streams.get_mut(&media.stream_id) {
                    if let WatchRegistrationStatus::Active { media_channel } =
                        &stream.rtmp_output_status
                    {
                        let media = media.clone();

                        if let Ok(media_data) =
                            RtmpEndpointMediaData::from_media_notification_content(
                                media.content,
                                self.is_keyframe_metadata_key,
                                self.pts_offset_metadata_key,
                            )
                        {
                            let _ = media_channel.send(RtmpEndpointMediaMessage {
                                stream_key: stream.id.0.clone(),
                                data: media_data,
                            });
                        }
                    } else {
                        stream.pending_media.push_back(media.content.clone());
                    }
                }
            }
        }

        outputs.media.push(media);
    }

    pub fn prepare_stream(
        &mut self,
        stream_id: StreamId,
        futures_channel: &WorkflowStepFuturesChannel,
    ) {
        if let Some(stream) = self.active_streams.get_mut(&stream_id) {
            let (output_is_active, output_media_channel) = match &stream.rtmp_output_status {
                WatchRegistrationStatus::Inactive => {
                    let (media_sender, media_receiver) = unbounded_channel();
                    let (watch_sender, watch_receiver) = unbounded_channel();
                    let _ =
                        self.rtmp_server_endpoint
                            .send(RtmpEndpointRequest::ListenForWatchers {
                                notification_channel: watch_sender,
                                rtmp_app: self.watcher_app_name.clone(),
                                rtmp_stream_key: StreamKeyRegistration::Exact(stream.id.0.clone()),
                                port: 1935,
                                media_channel: media_receiver,
                                ip_restrictions: IpRestriction::None,
                                use_tls: false,
                                requires_registrant_approval: false,
                            });

                    let stream_id = stream.id.clone();
                    let closed_stream_id = stream_id.clone();
                    futures_channel.send_on_generic_unbounded_recv(
                        watch_receiver,
                        move |event| {
                            FutureResult::WatchNotificationReceived(stream_id.clone(), event)
                        },
                        move || FutureResult::WatchChannelGone(closed_stream_id),
                    );

                    stream.rtmp_output_status = WatchRegistrationStatus::Pending {
                        media_channel: media_sender,
                    };

                    (false, None)
                }

                WatchRegistrationStatus::Pending { media_channel: _ } => (false, None),
                WatchRegistrationStatus::Active { media_channel } => (true, Some(media_channel)),
            };

            if output_is_active {
                // If the output is active, we need to send any pending media out.  Most likely this
                // will contain sequence headers, and thus we need to get them up to the rtmp endpoint
                // so clients don't miss them
                if let Some(media_channel) = output_media_channel {
                    for media in stream.pending_media.drain(..) {
                        if let Ok(media_data) =
                            RtmpEndpointMediaData::from_media_notification_content(
                                media,
                                self.is_keyframe_metadata_key,
                                self.pts_offset_metadata_key,
                            )
                        {
                            let _ = media_channel.send(RtmpEndpointMediaMessage {
                                stream_key: stream.id.0.clone(),
                                data: media_data,
                            });
                        }
                    }
                }

                stream
                    .external_stream_handler
                    .prepare_stream(&stream.stream_name, futures_channel);
            }
        }
    }

    pub fn stop_all_streams(&mut self) {
        let ids: Vec<StreamId> = self.active_streams.keys().cloned().collect();
        for id in ids {
            self.stop_stream(&id);
        }
    }

    fn stop_stream(&mut self, stream_id: &StreamId) -> bool {
        if let Some(mut stream) = self.active_streams.remove(stream_id) {
            stream.external_stream_handler.stop_stream();

            let _ = self
                .rtmp_server_endpoint
                .send(RtmpEndpointRequest::RemoveRegistration {
                    registration_type: RegistrationType::Watcher,
                    port: 1935,
                    rtmp_app: self.watcher_app_name.clone(),
                    rtmp_stream_key: StreamKeyRegistration::Exact(stream.id.0),
                });

            return true;
        }

        false
    }

    fn handle_rtmp_watch_notification(
        &mut self,
        stream_id: StreamId,
        notification: RtmpEndpointWatcherNotification,
        futures_channel: &WorkflowStepFuturesChannel,
    ) {
        if let Some(stream) = self.active_streams.get_mut(&stream_id) {
            match notification {
                RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful => {
                    let new_status = match &stream.rtmp_output_status {
                        WatchRegistrationStatus::Pending { media_channel } => {
                            info!(
                                stream_id = ?stream.id,
                                "Watch registration successful for stream id {:?}",
                                stream.id
                            );
                            Some(WatchRegistrationStatus::Active {
                                media_channel: media_channel.clone(),
                            })
                        }

                        status => {
                            error!(
                                stream_id = ?stream.id,
                                "Received watch registration successful notification for stream id \
                            {:?}, but this stream's watch status is {:?}", stream.id, status
                            );

                            None
                        }
                    };

                    if let Some(new_status) = new_status {
                        stream.rtmp_output_status = new_status;
                    }
                }

                RtmpEndpointWatcherNotification::WatcherRegistrationFailed => {
                    warn!(
                        stream_id = ?stream.id,
                        "Received watch registration failed for stream id {:?}",
                        stream.id
                    );
                    stream.rtmp_output_status = WatchRegistrationStatus::Inactive;
                }

                RtmpEndpointWatcherNotification::StreamKeyBecameActive { .. } => (),
                RtmpEndpointWatcherNotification::StreamKeyBecameInactive { .. } => (),

                RtmpEndpointWatcherNotification::WatcherRequiringApproval { .. } => {
                    error!("Received request for approval but requests should be auto-approved");
                    self.status = StepStatus::Error {
                        message:
                            "Received request for approval but requests should be auto-approved"
                                .to_string(),
                    };
                }
            }
        }

        self.prepare_stream(stream_id, futures_channel);
    }
}

impl Drop for ExternalStreamReader {
    fn drop(&mut self) {
        self.stop_all_streams();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rtmp_server::RtmpEndpointMediaData;
    use crate::utils::hash_map_to_stream_metadata;
    use crate::workflow_steps::external_stream_handler::StreamHandlerFutureResult;
    use bytes::{Bytes, BytesMut};
    use mmids_core::codecs::{AUDIO_CODEC_AAC_RAW, VIDEO_CODEC_H264_AVC};
    use mmids_core::workflows::definitions::WorkflowStepId;
    use mmids_core::workflows::metadata::common_metadata::{
        get_is_keyframe_metadata_key, get_pts_offset_metadata_key,
    };
    use mmids_core::workflows::metadata::{
        MediaPayloadMetadataCollection, MetadataEntry, MetadataKeyMap, MetadataValue,
    };
    use mmids_core::workflows::steps::futures_channel::{
        FuturesChannelInnerResult, FuturesChannelResult,
    };
    use mmids_core::workflows::MediaType;
    use mmids_core::{test_utils, VideoTimestamp};
    use rml_rtmp::time::RtmpTimestamp;
    use std::iter;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio::time::timeout;

    struct TestContext {
        external_stream_reader: ExternalStreamReader,
        rtmp_endpoint: UnboundedReceiver<RtmpEndpointRequest>,
        prepare_stream_receiver: UnboundedReceiver<String>,
        stop_stream_receiver: UnboundedReceiver<()>,
        futures_channel_receiver: UnboundedReceiver<FuturesChannelResult>,
        futures_channel: WorkflowStepFuturesChannel,
    }

    struct Handler {
        prepare_stream_sender: UnboundedSender<String>,
        stop_stream_sender: UnboundedSender<()>,
    }

    impl ExternalStreamHandler for Handler {
        fn prepare_stream(
            &mut self,
            stream_name: &str,
            _futures_channel: &WorkflowStepFuturesChannel,
        ) {
            let _ = self.prepare_stream_sender.send(stream_name.to_string());
        }

        fn stop_stream(&mut self) {
            let _ = self.stop_stream_sender.send(());
        }

        fn handle_resolved_future(
            &mut self,
            _future: Box<dyn StreamHandlerFutureResult>,
        ) -> ResolvedFutureStatus {
            ResolvedFutureStatus::Success
        }
    }

    struct Generator {
        prepare_stream_sender: UnboundedSender<String>,
        stop_stream_sender: UnboundedSender<()>,
    }

    impl ExternalStreamHandlerGenerator for Generator {
        fn generate(&self, _stream_id: StreamId) -> Box<dyn ExternalStreamHandler + Sync + Send> {
            Box::new(Handler {
                prepare_stream_sender: self.prepare_stream_sender.clone(),
                stop_stream_sender: self.stop_stream_sender.clone(),
            })
        }
    }

    impl TestContext {
        fn new() -> Self {
            let (rtmp_sender, rtmp_receiver) = unbounded_channel();
            let (prepare_sender, prepare_receiver) = unbounded_channel();
            let (stop_sender, stop_receiver) = unbounded_channel();
            let generator = Box::new(Generator {
                prepare_stream_sender: prepare_sender,
                stop_stream_sender: stop_sender,
            });

            let mut metadata_map = MetadataKeyMap::new();
            let is_keyframe_metadata_key = get_is_keyframe_metadata_key(&mut metadata_map);
            let pts_offset_metadata_key = get_pts_offset_metadata_key(&mut metadata_map);

            let (futures_sender, futures_receiver) = unbounded_channel();
            let futures_channel =
                WorkflowStepFuturesChannel::new(WorkflowStepId(123), futures_sender);

            let reader = ExternalStreamReader::new(
                Arc::new("app".to_string()),
                rtmp_sender,
                generator,
                is_keyframe_metadata_key,
                pts_offset_metadata_key,
                &futures_channel,
            );

            TestContext {
                rtmp_endpoint: rtmp_receiver,
                external_stream_reader: reader,
                prepare_stream_receiver: prepare_receiver,
                stop_stream_receiver: stop_receiver,
                futures_channel_receiver: futures_receiver,
                futures_channel,
            }
        }

        async fn accept_stream(&mut self) -> UnboundedReceiver<RtmpEndpointMediaMessage> {
            let mut outputs = StepOutputs::new();

            let media = MediaNotification {
                stream_id: StreamId(Arc::new("abc".to_string())),
                content: MediaNotificationContent::NewIncomingStream {
                    stream_name: Arc::new("def".to_string()),
                },
            };

            self.external_stream_reader
                .handle_media(media, &mut outputs, &self.futures_channel);

            let response = test_utils::expect_mpsc_response(&mut self.rtmp_endpoint).await;
            let (notification_channel, media_channel) = match response {
                RtmpEndpointRequest::ListenForWatchers {
                    notification_channel,
                    media_channel,
                    ..
                } => (notification_channel, media_channel),

                response => panic!("Unexpected request: {:?}", response),
            };

            notification_channel
                .send(RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful)
                .expect("Failed to send registration success response");

            match self.expect_future_resolved().await {
                FuturesChannelInnerResult::Generic(result) => {
                    self.external_stream_reader
                        .handle_resolved_future(result, &self.futures_channel);
                }

                FuturesChannelInnerResult::Media(_) => {
                    panic!("Expected a generic step future result but instead got media packet");
                }
            }

            media_channel
        }

        /// Gets the first future that was resolved on the workflow step futures channel. If no future
        /// is resolved, then a panic will ensue.
        pub async fn expect_future_resolved(&mut self) -> FuturesChannelInnerResult {
            let future = self.futures_channel_receiver.recv();
            match timeout(Duration::from_millis(10), future).await {
                Ok(Some(response)) => response.result,
                _ => panic!("No future resolved within timeout period"),
            }
        }
    }

    #[tokio::test]
    async fn watch_request_on_stream_connected_message() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        let media = MediaNotification {
            stream_id: StreamId(Arc::new("abc".to_string())),
            content: MediaNotificationContent::NewIncomingStream {
                stream_name: Arc::new("def".to_string()),
            },
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs, &context.futures_channel);

        let response = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
        match response {
            RtmpEndpointRequest::ListenForWatchers {
                port,
                rtmp_app,
                rtmp_stream_key: _,
                requires_registrant_approval,
                media_channel: _,
                use_tls,
                ip_restrictions,
                notification_channel: _,
            } => {
                assert_eq!(port, 1935, "Unexpected port");
                assert_eq!(rtmp_app.as_str(), "app", "Unexpected rtmp application");
                assert!(!use_tls, "Expected use tls to be disabled");
                assert!(
                    !requires_registrant_approval,
                    "Expected not to require registrant approval"
                );
                assert_eq!(
                    ip_restrictions,
                    IpRestriction::None,
                    "Expected no ip restrictions"
                );
            }

            response => panic!("Expected ListenForWatchers, instead got {:?}", response),
        }
    }

    #[tokio::test]
    async fn stream_connected_message_passed_immediately_as_output() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        let media = MediaNotification {
            stream_id: StreamId(Arc::new("abc".to_string())),
            content: MediaNotificationContent::NewIncomingStream {
                stream_name: Arc::new("def".to_string()),
            },
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs, &context.futures_channel);

        assert_eq!(outputs.media.len(), 1, "Expected single media output");
        assert_eq!(
            outputs.media[0].stream_id.0.as_str(),
            "abc",
            "Unexpected stream id"
        );
        match &outputs.media[0].content {
            MediaNotificationContent::NewIncomingStream { stream_name } => {
                assert_eq!(stream_name.as_str(), "def", "Unexpected stream name");
            }

            content => panic!("Expected NewIncomingStream, got {:?}", content),
        }
    }

    #[tokio::test]
    async fn stream_disconnected_message_passed_immediately_as_output() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        let media = MediaNotification {
            stream_id: StreamId(Arc::new("abc".to_string())),
            content: MediaNotificationContent::StreamDisconnected,
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs, &context.futures_channel);

        assert_eq!(outputs.media.len(), 1, "Expected single media output");
        assert_eq!(
            outputs.media[0].stream_id.0.as_str(),
            "abc",
            "Unexpected stream id"
        );
        match &outputs.media[0].content {
            MediaNotificationContent::StreamDisconnected => (),
            content => panic!("Expected NewIncomingStream, got {:?}", content),
        }
    }

    #[tokio::test]
    async fn metadata_message_passed_immediately_as_output() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        let mut metadata = HashMap::new();
        metadata.insert("width".to_string(), "1920".to_string());

        let media = MediaNotification {
            stream_id: StreamId(Arc::new("abc".to_string())),
            content: MediaNotificationContent::Metadata {
                data: metadata.clone(),
            },
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs, &context.futures_channel);

        assert_eq!(outputs.media.len(), 1, "Expected single media output");
        assert_eq!(
            outputs.media[0].stream_id.0.as_str(),
            "abc",
            "Unexpected stream id"
        );
        match &outputs.media[0].content {
            MediaNotificationContent::Metadata { data } => {
                assert_eq!(data, &metadata, "Unexpected metadata in output");
            }

            content => panic!("Expected NewIncomingStream, got {:?}", content),
        }
    }

    #[tokio::test]
    async fn video_message_passed_immediately_as_output() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        let video_timestamp =
            VideoTimestamp::from_durations(Duration::from_millis(5), Duration::from_millis(15));

        let mut buffer = BytesMut::new();
        let mut metadata_key_map = MetadataKeyMap::new();
        let is_keyframe_metadata_key = get_is_keyframe_metadata_key(&mut metadata_key_map);
        let pts_offset_metadata_key = get_pts_offset_metadata_key(&mut metadata_key_map);
        let is_keyframe_metadata = MetadataEntry::new(
            is_keyframe_metadata_key,
            MetadataValue::Bool(true),
            &mut buffer,
        )
        .unwrap();

        let pts_offset_metadata = MetadataEntry::new(
            pts_offset_metadata_key,
            MetadataValue::I32(video_timestamp.pts_offset()),
            &mut buffer,
        )
        .unwrap();

        let metadata = MediaPayloadMetadataCollection::new(
            [is_keyframe_metadata, pts_offset_metadata].into_iter(),
            &mut buffer,
        );

        let media_content = MediaNotificationContent::MediaPayload {
            media_type: MediaType::Video,
            payload_type: VIDEO_CODEC_H264_AVC.clone(),
            timestamp: video_timestamp.dts(),
            is_required_for_decoding: true,
            metadata,
            data: Bytes::from(vec![1, 2, 3]),
        };

        let media = MediaNotification {
            stream_id: StreamId(Arc::new("abc".to_string())),
            content: media_content.clone(),
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs, &context.futures_channel);

        assert_eq!(outputs.media.len(), 1, "Expected single media output");
        assert_eq!(
            outputs.media[0].stream_id.0.as_str(),
            "abc",
            "Unexpected stream id"
        );

        assert_eq!(
            outputs.media[0].content, media_content,
            "Unexpected media content"
        );
    }

    #[tokio::test]
    async fn audio_message_passed_immediately_as_output() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        let media = MediaNotification {
            stream_id: StreamId(Arc::new("abc".to_string())),
            content: MediaNotificationContent::MediaPayload {
                data: Bytes::from(vec![1, 2, 3]),
                timestamp: Duration::from_millis(5),
                is_required_for_decoding: true,
                media_type: MediaType::Audio,
                payload_type: AUDIO_CODEC_AAC_RAW.clone(),
                metadata: MediaPayloadMetadataCollection::new(iter::empty(), &mut BytesMut::new()),
            },
        };

        context.external_stream_reader.handle_media(
            media.clone(),
            &mut outputs,
            &context.futures_channel,
        );

        assert_eq!(outputs.media.len(), 1, "Expected single media output");
        assert_eq!(
            outputs.media[0].stream_id.0.as_str(),
            "abc",
            "Unexpected stream id"
        );

        assert_eq!(
            outputs.media[0].content, media.content,
            "Unexpected media content"
        );
    }

    #[tokio::test]
    async fn successful_watch_registration_calls_prepare_stream() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        let media = MediaNotification {
            stream_id: StreamId(Arc::new("abc".to_string())),
            content: MediaNotificationContent::NewIncomingStream {
                stream_name: Arc::new("def".to_string()),
            },
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs, &context.futures_channel);

        let response = test_utils::expect_mpsc_response(&mut context.rtmp_endpoint).await;
        let channel = match response {
            RtmpEndpointRequest::ListenForWatchers {
                notification_channel,
                ..
            } => notification_channel,
            response => panic!("Unexpected request: {:?}", response),
        };

        channel
            .send(RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful)
            .expect("Failed to send registration success response");

        match context.expect_future_resolved().await {
            FuturesChannelInnerResult::Generic(result) => {
                context
                    .external_stream_reader
                    .handle_resolved_future(result, &context.futures_channel);
            }

            FuturesChannelInnerResult::Media(_) => {
                panic!("Expected a generic step future result but instead got media packet");
            }
        }

        let stream_name =
            test_utils::expect_mpsc_response(&mut context.prepare_stream_receiver).await;

        assert_eq!(&stream_name, "def", "Unexpected stream name prepared");
    }

    #[tokio::test]
    async fn stream_disconnection_calls_stop_stream() {
        let mut context = TestContext::new();
        let _ = context.accept_stream().await;

        let mut outputs = StepOutputs::new();
        let media = MediaNotification {
            stream_id: StreamId(Arc::new("abc".to_string())),
            content: MediaNotificationContent::StreamDisconnected,
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs, &context.futures_channel);

        test_utils::expect_mpsc_response(&mut context.stop_stream_receiver).await;
    }

    #[tokio::test]
    async fn stop_stream_not_called_if_no_incoming_stream_notification_came_in() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        let media = MediaNotification {
            stream_id: StreamId(Arc::new("abc".to_string())),
            content: MediaNotificationContent::StreamDisconnected,
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs, &context.futures_channel);

        test_utils::expect_mpsc_timeout(&mut context.stop_stream_receiver).await;
    }

    #[tokio::test]
    async fn metadata_message_passed_to_watchers() {
        let mut context = TestContext::new();
        let mut media_receiver = context.accept_stream().await;

        let mut raw_metadata = HashMap::new();
        raw_metadata.insert("width".to_string(), "1920".to_string());

        let expected_metadata = hash_map_to_stream_metadata(&raw_metadata);

        let media = MediaNotification {
            stream_id: StreamId(Arc::new("abc".to_string())),
            content: MediaNotificationContent::Metadata { data: raw_metadata },
        };

        let mut outputs = StepOutputs::new();
        context
            .external_stream_reader
            .handle_media(media, &mut outputs, &context.futures_channel);

        let media = test_utils::expect_mpsc_response(&mut media_receiver).await;
        assert_eq!(
            media.stream_key.as_str(),
            "abc",
            "Unexpected stream key for media"
        );

        match &media.data {
            RtmpEndpointMediaData::NewStreamMetaData { metadata } => {
                assert_eq!(metadata, &expected_metadata, "Unexpected metadata content");
            }

            data => panic!("Unexpected media data: {:?}", data),
        }
    }

    #[tokio::test]
    async fn video_message_passed_to_watchers() {
        let mut context = TestContext::new();
        let mut media_receiver = context.accept_stream().await;

        let video_timestamp =
            VideoTimestamp::from_durations(Duration::from_millis(5), Duration::from_millis(15));

        let mut buffer = BytesMut::new();
        let mut metadata_key_map = MetadataKeyMap::new();
        let is_keyframe_metadata_key = get_is_keyframe_metadata_key(&mut metadata_key_map);
        let pts_offset_metadata_key = get_pts_offset_metadata_key(&mut metadata_key_map);
        let is_keyframe_metadata = MetadataEntry::new(
            is_keyframe_metadata_key,
            MetadataValue::Bool(true),
            &mut buffer,
        )
        .unwrap();

        let pts_offset_metadata = MetadataEntry::new(
            pts_offset_metadata_key,
            MetadataValue::I32(video_timestamp.pts_offset()),
            &mut buffer,
        )
        .unwrap();

        let metadata = MediaPayloadMetadataCollection::new(
            [is_keyframe_metadata, pts_offset_metadata].into_iter(),
            &mut buffer,
        );

        let media = MediaNotification {
            stream_id: StreamId(Arc::new("abc".to_string())),
            content: MediaNotificationContent::MediaPayload {
                media_type: MediaType::Video,
                payload_type: VIDEO_CODEC_H264_AVC.clone(),
                timestamp: video_timestamp.dts(),
                is_required_for_decoding: true,
                data: Bytes::from(vec![1, 2, 3, 4]),
                metadata,
            },
        };

        let mut outputs = StepOutputs::new();
        context
            .external_stream_reader
            .handle_media(media, &mut outputs, &context.futures_channel);

        let media = test_utils::expect_mpsc_response(&mut media_receiver).await;
        assert_eq!(
            media.stream_key.as_str(),
            "abc",
            "Unexpected stream key for media"
        );

        match &media.data {
            RtmpEndpointMediaData::NewVideoData {
                data,
                timestamp,
                is_sequence_header,
                is_keyframe,
                composition_time_offset,
            } => {
                assert_eq!(data, &vec![1, 2, 3, 4], "Unexpected bytes");
                assert_eq!(
                    timestamp,
                    &RtmpTimestamp::new(video_timestamp.dts().as_millis() as u32),
                    "Unexpected timestamp"
                );
                assert!(is_sequence_header, "Expected sequence header to be true");
                assert!(is_keyframe, "Expected key frame to be true");
                assert_eq!(
                    composition_time_offset,
                    &video_timestamp.pts_offset(),
                    "Unexpected composition time offset"
                );
            }

            data => panic!("Unexpected media data: {:?}", data),
        }
    }

    #[tokio::test]
    async fn audio_message_passed_to_watchers() {
        let mut context = TestContext::new();
        let mut media_receiver = context.accept_stream().await;

        let media = MediaNotification {
            stream_id: StreamId(Arc::new("abc".to_string())),
            content: MediaNotificationContent::MediaPayload {
                data: Bytes::from(vec![1, 2, 3, 4]),
                timestamp: Duration::from_millis(5),
                is_required_for_decoding: true,
                media_type: MediaType::Audio,
                payload_type: AUDIO_CODEC_AAC_RAW.clone(),
                metadata: MediaPayloadMetadataCollection::new(iter::empty(), &mut BytesMut::new()),
            },
        };

        let mut outputs = StepOutputs::new();
        context
            .external_stream_reader
            .handle_media(media, &mut outputs, &context.futures_channel);

        let media = test_utils::expect_mpsc_response(&mut media_receiver).await;
        assert_eq!(
            media.stream_key.as_str(),
            "abc",
            "Unexpected stream key for media"
        );

        match &media.data {
            RtmpEndpointMediaData::NewAudioData {
                data,
                timestamp,
                is_sequence_header,
            } => {
                assert_eq!(data, &vec![1, 2, 3, 4], "Unexpected bytes");
                assert_eq!(timestamp, &RtmpTimestamp::new(5), "Unexpected timestamp");
                assert!(is_sequence_header, "Expected sequence header to be true");
            }

            data => panic!("Unexpected media data: {:?}", data),
        }
    }
}
