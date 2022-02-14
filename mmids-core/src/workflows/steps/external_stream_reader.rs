use super::external_stream_handler::{ExternalStreamHandler, StreamHandlerFutureWrapper};
use crate::endpoints::rtmp_server::{
    IpRestriction, RegistrationType, RtmpEndpointMediaMessage, RtmpEndpointRequest,
    RtmpEndpointWatcherNotification, StreamKeyRegistration,
};
use crate::workflows::steps::external_stream_handler::{
    ExternalStreamHandlerGenerator, ResolvedFutureStatus,
};
use crate::workflows::steps::{FutureList, StepFutureResult, StepOutputs, StepStatus};
use crate::workflows::{MediaNotification, MediaNotificationContent};
use crate::StreamId;
use futures::FutureExt;
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
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
    watcher_app_name: String,
    active_streams: HashMap<StreamId, ActiveStream>,
    stream_handler_generator: Box<dyn ExternalStreamHandlerGenerator + Sync + Send>,
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
    stream_name: String,
    pending_media: VecDeque<MediaNotificationContent>,
    rtmp_output_status: WatchRegistrationStatus,
    external_stream_handler: Box<dyn ExternalStreamHandler + Sync + Send>,
}

enum FutureResult {
    RtmpEndpointGone,
    RtmpWatchChannelGone(StreamId),
    RtmpWatchNotificationReceived(
        StreamId,
        RtmpEndpointWatcherNotification,
        UnboundedReceiver<RtmpEndpointWatcherNotification>,
    ),
}

impl StepFutureResult for FutureResult {}

impl ExternalStreamReader {
    pub fn new(
        watcher_rtmp_app_name: String,
        rtmp_server: UnboundedSender<RtmpEndpointRequest>,
        external_handler_generator: Box<dyn ExternalStreamHandlerGenerator + Sync + Send>,
    ) -> (Self, FutureList) {
        let step = ExternalStreamReader {
            status: StepStatus::Active,
            watcher_app_name: watcher_rtmp_app_name,
            rtmp_server_endpoint: rtmp_server.clone(),
            active_streams: HashMap::new(),
            stream_handler_generator: external_handler_generator,
        };

        let futures = vec![notify_when_rtmp_endpoint_is_gone(rtmp_server).boxed()];

        (step, futures)
    }

    pub fn handle_resolved_future(
        &mut self,
        notification: Box<dyn StepFutureResult>,
        outputs: &mut StepOutputs,
    ) {
        let notification = match notification.downcast::<StreamHandlerFutureWrapper>() {
            Err(e) => e,
            Ok(wrapper) => {
                let result = if let Some(stream) = self.active_streams.get_mut(&wrapper.stream_id) {
                    stream
                        .external_stream_handler
                        .handle_resolved_future(wrapper.future, outputs)
                } else {
                    ResolvedFutureStatus::Success
                };

                match result {
                    ResolvedFutureStatus::Success => {
                        self.prepare_stream(wrapper.stream_id, outputs)
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

            FutureResult::RtmpWatchChannelGone(stream_id) => {
                if self.stop_stream(&stream_id) {
                    error!(stream_id = ?stream_id, "Rtmp watch channel disappeared for stream id {:?}", stream_id);
                }
            }

            FutureResult::RtmpWatchNotificationReceived(stream_id, notification, receiver) => {
                if !self.active_streams.contains_key(&stream_id) {
                    // late notification after stopping a stream
                    return;
                }

                outputs
                    .futures
                    .push(wait_for_watch_notification(stream_id.clone(), receiver).boxed());

                self.handle_rtmp_watch_notification(stream_id, notification, outputs);
            }
        }
    }

    pub fn handle_media(&mut self, media: MediaNotification, outputs: &mut StepOutputs) {
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
                self.prepare_stream(media.stream_id.clone(), outputs);
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
                        if let Some(media_data) = media.content.to_rtmp_media_data() {
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

    pub fn prepare_stream(&mut self, stream_id: StreamId, outputs: &mut StepOutputs) {
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

                    outputs.futures.push(
                        wait_for_watch_notification(stream.id.clone(), watch_receiver).boxed(),
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
                        if let Some(media_data) = media.to_rtmp_media_data() {
                            let _ = media_channel.send(RtmpEndpointMediaMessage {
                                stream_key: stream.id.0.clone(),
                                data: media_data,
                            });
                        }
                    }
                }

                stream
                    .external_stream_handler
                    .prepare_stream(&stream.stream_name, outputs);
            }
        }
    }

    pub fn stop_all_streams(&mut self) {
        let ids: Vec<StreamId> = self.active_streams.keys().map(|x| x.clone()).collect();
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
                    rtmp_stream_key: StreamKeyRegistration::Exact(stream.id.0.clone()),
                });

            return true;
        }

        return false;
    }

    fn handle_rtmp_watch_notification(
        &mut self,
        stream_id: StreamId,
        notification: RtmpEndpointWatcherNotification,
        outputs: &mut StepOutputs,
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

        self.prepare_stream(stream_id, outputs);
    }
}

async fn notify_when_rtmp_endpoint_is_gone(
    endpoint: UnboundedSender<RtmpEndpointRequest>,
) -> Box<dyn StepFutureResult> {
    endpoint.closed().await;

    Box::new(FutureResult::RtmpEndpointGone)
}

async fn wait_for_watch_notification(
    stream_id: StreamId,
    mut receiver: UnboundedReceiver<RtmpEndpointWatcherNotification>,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(msg) => FutureResult::RtmpWatchNotificationReceived(stream_id, msg, receiver),
        None => FutureResult::RtmpWatchChannelGone(stream_id),
    };

    Box::new(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::{AudioCodec, VideoCodec};
    use crate::endpoints::rtmp_server::RtmpEndpointMediaData;
    use crate::utils::hash_map_to_stream_metadata;
    use crate::workflows::steps::StreamHandlerFutureResult;
    use crate::{test_utils, VideoTimestamp};
    use bytes::Bytes;
    use futures::future::BoxFuture;
    use futures::stream::FuturesUnordered;
    use rml_rtmp::time::RtmpTimestamp;
    use std::time::Duration;

    struct TestContext {
        external_stream_reader: ExternalStreamReader,
        rtmp_endpoint: UnboundedReceiver<RtmpEndpointRequest>,
        futures: FuturesUnordered<BoxFuture<'static, Box<dyn StepFutureResult>>>,
        prepare_stream_receiver: UnboundedReceiver<String>,
        stop_stream_receiver: UnboundedReceiver<()>,
    }

    struct Handler {
        prepare_stream_sender: UnboundedSender<String>,
        stop_stream_sender: UnboundedSender<()>,
    }

    impl ExternalStreamHandler for Handler {
        fn prepare_stream(&mut self, stream_name: &str, _outputs: &mut StepOutputs) {
            let _ = self.prepare_stream_sender.send(stream_name.to_string());
        }

        fn stop_stream(&mut self) {
            let _ = self.stop_stream_sender.send(());
        }

        fn handle_resolved_future(
            &mut self,
            _future: Box<dyn StreamHandlerFutureResult>,
            _outputs: &mut StepOutputs,
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

            let (reader, future_list) =
                ExternalStreamReader::new("app".to_string(), rtmp_sender, generator);
            let mut futures = FuturesUnordered::new();
            futures.extend(future_list);

            TestContext {
                rtmp_endpoint: rtmp_receiver,
                external_stream_reader: reader,
                futures,
                prepare_stream_receiver: prepare_receiver,
                stop_stream_receiver: stop_receiver,
            }
        }

        async fn accept_stream(&mut self) -> UnboundedReceiver<RtmpEndpointMediaMessage> {
            let mut outputs = StepOutputs::new();

            let media = MediaNotification {
                stream_id: StreamId("abc".to_string()),
                content: MediaNotificationContent::NewIncomingStream {
                    stream_name: "def".to_string(),
                },
            };

            self.external_stream_reader
                .handle_media(media, &mut outputs);
            self.futures.extend(outputs.futures.drain(..));

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

            let result = test_utils::expect_future_resolved(&mut self.futures).await;
            self.external_stream_reader
                .handle_resolved_future(result, &mut outputs);

            media_channel
        }
    }

    #[tokio::test]
    async fn watch_request_on_stream_connected_message() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        let media = MediaNotification {
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::NewIncomingStream {
                stream_name: "def".to_string(),
            },
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs);
        context.futures.extend(outputs.futures);

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
                assert_eq!(&rtmp_app, "app", "Unexpected rtmp application");
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
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::NewIncomingStream {
                stream_name: "def".to_string(),
            },
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs);

        assert_eq!(outputs.media.len(), 1, "Expected single media output");
        assert_eq!(&outputs.media[0].stream_id.0, "abc", "Unexpected stream id");
        match &outputs.media[0].content {
            MediaNotificationContent::NewIncomingStream { stream_name } => {
                assert_eq!(stream_name, "def", "Unexpected stream name");
            }

            content => panic!("Expected NewIncomingStream, got {:?}", content),
        }
    }

    #[tokio::test]
    async fn stream_disconnected_message_passed_immediately_as_output() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        let media = MediaNotification {
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::StreamDisconnected,
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs);

        assert_eq!(outputs.media.len(), 1, "Expected single media output");
        assert_eq!(&outputs.media[0].stream_id.0, "abc", "Unexpected stream id");
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
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::Metadata {
                data: metadata.clone(),
            },
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs);

        assert_eq!(outputs.media.len(), 1, "Expected single media output");
        assert_eq!(&outputs.media[0].stream_id.0, "abc", "Unexpected stream id");
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

        let media = MediaNotification {
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::Video {
                data: Bytes::from(vec![1, 2, 3]),
                codec: VideoCodec::H264,
                timestamp: video_timestamp.clone(),
                is_keyframe: true,
                is_sequence_header: true,
            },
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs);

        assert_eq!(outputs.media.len(), 1, "Expected single media output");
        assert_eq!(&outputs.media[0].stream_id.0, "abc", "Unexpected stream id");
        match &outputs.media[0].content {
            MediaNotificationContent::Video {
                data,
                codec,
                is_sequence_header,
                is_keyframe,
                timestamp,
            } => {
                assert_eq!(data, &vec![1, 2, 3], "Unexpected bytes");
                assert_eq!(codec, &VideoCodec::H264, "Unexpected codec");
                assert!(is_sequence_header, "Expected sequence header");
                assert!(is_keyframe, "Expected key frame");
                assert_eq!(timestamp, &video_timestamp, "Unexpected video timestamp");
            }

            content => panic!("Expected NewIncomingStream, got {:?}", content),
        }
    }

    #[tokio::test]
    async fn audio_message_passed_immediately_as_output() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        let media = MediaNotification {
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::Audio {
                data: Bytes::from(vec![1, 2, 3]),
                codec: AudioCodec::Aac,
                timestamp: Duration::from_millis(5),
                is_sequence_header: true,
            },
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs);

        assert_eq!(outputs.media.len(), 1, "Expected single media output");
        assert_eq!(&outputs.media[0].stream_id.0, "abc", "Unexpected stream id");
        match &outputs.media[0].content {
            MediaNotificationContent::Audio {
                data,
                codec,
                timestamp,
                is_sequence_header,
            } => {
                assert_eq!(data, &vec![1, 2, 3], "Unexpected bytes");
                assert_eq!(codec, &AudioCodec::Aac, "Unexpected codec");
                assert_eq!(timestamp, &Duration::from_millis(5), "Unexpected timestamp");
                assert!(is_sequence_header, "Expected sequence header");
            }

            content => panic!("Expected NewIncomingStream, got {:?}", content),
        }
    }

    #[tokio::test]
    async fn successful_watch_registration_calls_prepare_stream() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        let media = MediaNotification {
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::NewIncomingStream {
                stream_name: "def".to_string(),
            },
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs);
        context.futures.extend(outputs.futures.drain(..));

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

        let result = test_utils::expect_future_resolved(&mut context.futures).await;
        context
            .external_stream_reader
            .handle_resolved_future(result, &mut outputs);
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
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::StreamDisconnected,
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs);
        context.futures.extend(outputs.futures.drain(..));

        let _ = test_utils::expect_mpsc_response(&mut context.stop_stream_receiver).await;
    }

    #[tokio::test]
    async fn stop_stream_not_called_if_no_incoming_stream_notification_came_in() {
        let mut context = TestContext::new();
        let mut outputs = StepOutputs::new();

        let media = MediaNotification {
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::StreamDisconnected,
        };

        context
            .external_stream_reader
            .handle_media(media, &mut outputs);
        context.futures.extend(outputs.futures.drain(..));

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
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::Metadata { data: raw_metadata },
        };

        let mut outputs = StepOutputs::new();
        context
            .external_stream_reader
            .handle_media(media, &mut outputs);

        let media = test_utils::expect_mpsc_response(&mut media_receiver).await;
        assert_eq!(&media.stream_key, "abc", "Unexpected stream key for media");

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

        let media = MediaNotification {
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::Video {
                data: Bytes::from(vec![1, 2, 3, 4]),
                is_keyframe: true,
                is_sequence_header: true,
                codec: VideoCodec::H264,
                timestamp: video_timestamp.clone(),
            },
        };

        let mut outputs = StepOutputs::new();
        context
            .external_stream_reader
            .handle_media(media, &mut outputs);

        let media = test_utils::expect_mpsc_response(&mut media_receiver).await;
        assert_eq!(&media.stream_key, "abc", "Unexpected stream key for media");

        match &media.data {
            RtmpEndpointMediaData::NewVideoData {
                data,
                timestamp,
                codec,
                is_sequence_header,
                is_keyframe,
                composition_time_offset,
            } => {
                assert_eq!(data, &vec![1, 2, 3, 4], "Unexpected bytes");
                assert_eq!(
                    timestamp,
                    &RtmpTimestamp::new(video_timestamp.dts.as_millis() as u32),
                    "Unexpected timestamp"
                );
                assert!(is_sequence_header, "Expected sequence header to be true");
                assert!(is_keyframe, "Expected key frame to be true");
                assert_eq!(codec, &VideoCodec::H264, "Expected h264 codec");
                assert_eq!(
                    composition_time_offset, &video_timestamp.pts_offset,
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
            stream_id: StreamId("abc".to_string()),
            content: MediaNotificationContent::Audio {
                data: Bytes::from(vec![1, 2, 3, 4]),
                is_sequence_header: true,
                codec: AudioCodec::Aac,
                timestamp: Duration::from_millis(5),
            },
        };

        let mut outputs = StepOutputs::new();
        context
            .external_stream_reader
            .handle_media(media, &mut outputs);

        let media = test_utils::expect_mpsc_response(&mut media_receiver).await;
        assert_eq!(&media.stream_key, "abc", "Unexpected stream key for media");

        match &media.data {
            RtmpEndpointMediaData::NewAudioData {
                data,
                timestamp,
                codec,
                is_sequence_header,
            } => {
                assert_eq!(data, &vec![1, 2, 3, 4], "Unexpected bytes");
                assert_eq!(timestamp, &RtmpTimestamp::new(5), "Unexpected timestamp");
                assert!(is_sequence_header, "Expected sequence header to be true");
                assert_eq!(codec, &AudioCodec::Aac, "Expected h264 codec");
            }

            data => panic!("Unexpected media data: {:?}", data),
        }
    }
}
