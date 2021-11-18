use super::external_stream_handler::{ExternalStreamHandler, StreamHandlerFutureWrapper};
use crate::endpoints::rtmp_server::{
    IpRestriction, RtmpEndpointMediaMessage, RtmpEndpointRequest, RtmpEndpointWatcherNotification,
    StreamKeyRegistration,
};
use crate::workflows::steps::external_stream_handler::{
    ExternalStreamHandlerGenerator, ResolvedFutureStatus,
};
use crate::workflows::steps::{FutureList, StepFutureResult, StepOutputs, StepStatus};
use crate::workflows::{MediaNotification, MediaNotificationContent};
use crate::StreamId;
use futures::FutureExt;
use log::{error, info, warn};
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

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
    id: String,
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
        id: String,
        watcher_rtmp_app_name: String,
        rtmp_server: UnboundedSender<RtmpEndpointRequest>,
        external_handler_generator: Box<dyn ExternalStreamHandlerGenerator + Sync + Send>,
    ) -> (Self, FutureList<'static>) {
        let step = ExternalStreamReader {
            status: StepStatus::Active,
            id,
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
                error!("Step {}: RTMP endpoint is gone!", self.id);
                self.status = StepStatus::Error;
                self.stop_all_streams();
            }

            FutureResult::RtmpWatchChannelGone(stream_id) => {
                if self.stop_stream(&stream_id) {
                    error!(
                        "Step {}: Rtmp watch channel disappeared for stream id {:?}",
                        self.id, stream_id
                    );
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
                        warn!("Step {}: Unexpected new incoming stream notification received on \
                        stream id {:?} and stream name '{}', but we already have this stream id active \
                        for stream name '{}'.  Ignoring this notification",
                            self.id, media.stream_id, stream_name, stream.stream_name);
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
                        "Step {}: Stopping stream id {:?} due to stream disconnection notification",
                        self.id, media.stream_id
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
            let output_is_active = match &stream.rtmp_output_status {
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
                            });

                    outputs.futures.push(
                        wait_for_watch_notification(stream.id.clone(), watch_receiver).boxed(),
                    );
                    stream.rtmp_output_status = WatchRegistrationStatus::Pending {
                        media_channel: media_sender,
                    };

                    false
                }

                WatchRegistrationStatus::Pending { media_channel: _ } => false,
                WatchRegistrationStatus::Active { media_channel: _ } => true,
            };

            if output_is_active {
                stream.external_stream_handler.prepare_stream(
                    &stream_id,
                    &stream.stream_name,
                    outputs,
                );
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
                                "Step {}: Watch registration successful for stream id {:?}",
                                self.id, stream.id
                            );
                            Some(WatchRegistrationStatus::Active {
                                media_channel: media_channel.clone(),
                            })
                        }

                        status => {
                            error!("Step {}: Received watch registration successful notification for stream id \
                            {:?}, but this stream's watch status is {:?}", self.id, stream.id, status);

                            None
                        }
                    };

                    if let Some(new_status) = new_status {
                        stream.rtmp_output_status = new_status;
                    }
                }

                RtmpEndpointWatcherNotification::WatcherRegistrationFailed => {
                    warn!(
                        "Step {}: Received watch registration failed for stream id {:?}",
                        self.id, stream.id
                    );
                    stream.rtmp_output_status = WatchRegistrationStatus::Inactive;
                }

                RtmpEndpointWatcherNotification::StreamKeyBecameActive { stream_key: _ } => (),
                RtmpEndpointWatcherNotification::StreamKeyBecameInactive { stream_key: _ } => (),
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