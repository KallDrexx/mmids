use crate::encoders::{AudioEncoder, VideoEncoder};
use futures::StreamExt;
use gstreamer::bus::BusStream;
use gstreamer::prelude::*;
use gstreamer::{MessageView, Pipeline, State};
use mmids_core::actor_utils::notify_on_unbounded_recv;
use mmids_core::workflows::metadata::{MetadataKey, MetadataValue};
use mmids_core::workflows::{MediaNotificationContent, MediaType};
use mmids_core::VideoTimestamp;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info, instrument};
use uuid::Uuid;

pub enum TranscodeManagerRequest {
    StopTranscode,
}

pub struct TranscoderParams {
    pub process_id: Uuid,
    pub video_encoder: Box<dyn VideoEncoder + Send>,
    pub audio_encoder: Box<dyn AudioEncoder + Send>,
    pub inbound_media: UnboundedReceiver<MediaNotificationContent>,
    pub outbound_media: UnboundedSender<MediaNotificationContent>,
    pub pipeline: Pipeline,
}

enum TranscoderFutureResult {
    EndpointGone,
    InboundMediaSendersGone,
    OutboundMediaReceiverGone,
    RequestReceived(TranscodeManagerRequest),
    MediaReceived(MediaNotificationContent),
    GstBusClosed,
    GstEosReceived,
    GstErrorReceived(GstError),
}

struct GstError {
    source_name: String,
    error_description: String,
    debug_info: Option<String>,
}

pub fn start_transcode_manager(
    parameters: TranscoderParams,
    pts_offset_metadata_key: MetadataKey,
) -> UnboundedSender<TranscodeManagerRequest> {
    let (sender, receiver) = unbounded_channel();
    let (actor_sender, actor_receiver) = unbounded_channel();
    let actor = TranscodeManager::new(parameters, receiver, pts_offset_metadata_key, actor_sender);

    tokio::spawn(actor.run(actor_receiver));

    sender
}

struct TranscodeManager {
    internal_sender: UnboundedSender<TranscoderFutureResult>,
    termination_requested: bool,
    id: Uuid,
    video_encoder: Box<dyn VideoEncoder + Send>,
    audio_encoder: Box<dyn AudioEncoder + Send>,
    pipeline: Pipeline,
    pts_offset_metadata_key: MetadataKey,
}

impl TranscodeManager {
    fn new(
        parameters: TranscoderParams,
        receiver: UnboundedReceiver<TranscodeManagerRequest>,
        pts_offset_metadata_key: MetadataKey,
        actor_sender: UnboundedSender<TranscoderFutureResult>,
    ) -> TranscodeManager {
        notify_on_unbounded_recv(
            receiver,
            actor_sender.clone(),
            TranscoderFutureResult::RequestReceived,
            || TranscoderFutureResult::EndpointGone,
        );

        notify_on_outbound_media_closed(parameters.outbound_media, actor_sender.clone());
        notify_on_inbound_media(parameters.inbound_media, actor_sender.clone());

        TranscodeManager {
            internal_sender: actor_sender,
            termination_requested: false,
            id: parameters.process_id,
            video_encoder: parameters.video_encoder,
            audio_encoder: parameters.audio_encoder,
            pipeline: parameters.pipeline,
            pts_offset_metadata_key,
        }
    }

    #[instrument(
        name = "Transcode Manager Execution",
        skip_all,
        fields(transcoding_process_id = %self.id),
    )]
    async fn run(mut self, mut actor_receiver: UnboundedReceiver<TranscoderFutureResult>) {
        info!("Starting transcoding process");

        match self.pipeline.set_state(State::Playing) {
            Ok(_) => (),
            Err(error) => {
                error!("Failed to set gstreamer pipeline to playing: {}", error);
                return;
            }
        }

        let bus = match self.pipeline.bus() {
            Some(bus) => bus,
            None => {
                error!("Failed to get pipeline bus.  Shouldn't happen!");
                return;
            }
        };

        notify_bus_message(bus.stream(), self.internal_sender.clone());

        while let Some(result) = actor_receiver.recv().await {
            match result {
                TranscoderFutureResult::EndpointGone => {
                    info!("Endpoint gone");
                    break;
                }

                TranscoderFutureResult::InboundMediaSendersGone => {
                    info!("No more media senders");
                    break;
                }

                TranscoderFutureResult::OutboundMediaReceiverGone => {
                    info!("Outbound media receiver gone");
                    break;
                }

                TranscoderFutureResult::MediaReceived(media) => {
                    self.handle_media(media);
                }

                TranscoderFutureResult::RequestReceived(request) => {
                    self.handle_request(request);
                }

                TranscoderFutureResult::GstBusClosed => {
                    info!("Gstreamer bus closed");
                    break;
                }

                TranscoderFutureResult::GstEosReceived => {
                    info!("Gstreamer pipeline sent end of stream signal");
                    break;
                }

                TranscoderFutureResult::GstErrorReceived(error) => {
                    error!(
                        gst_src = %error.source_name,
                        gst_error = %error.error_description,
                        "GStreamer threw an error from element '{}': {} (debug: {})",
                        error.source_name, error.error_description,
                        error.debug_info.as_ref().unwrap_or(&("".to_string())),
                    );

                    break;
                }
            }

            if self.termination_requested {
                info!("Termination requested");
                let _ = self.pipeline.set_state(State::Null);

                break;
            }
        }

        info!("Stopping transcoding process");
    }

    fn handle_media(&mut self, media: MediaNotificationContent) {
        if let MediaNotificationContent::MediaPayload {
            timestamp,
            payload_type,
            media_type,
            data,
            metadata,
            is_required_for_decoding,
        } = media
        {
            match media_type {
                MediaType::Audio => {
                    let result = self.audio_encoder.push_data(
                        payload_type,
                        data,
                        timestamp,
                        is_required_for_decoding,
                    );

                    if let Err(error) = result {
                        error!("Failed to push media to audio encoder: {}", error);
                        self.termination_requested = true;
                    }
                }

                MediaType::Video => {
                    let pts_offset = metadata
                        .iter()
                        .filter(|m| m.key() == self.pts_offset_metadata_key)
                        .filter_map(|m| match m.value() {
                            MetadataValue::I32(num) => Some(num),
                            _ => None,
                        })
                        .next()
                        .unwrap_or_default();

                    let pts_duration =
                        Duration::from_millis(timestamp.as_millis() as u64 + pts_offset as u64);
                    let video_timestamp = VideoTimestamp::from_durations(timestamp, pts_duration);

                    let result = self.video_encoder.push_data(
                        payload_type,
                        data,
                        video_timestamp,
                        is_required_for_decoding,
                    );

                    if let Err(error) = result {
                        error!("Failed to push media to video encoder: {}", error);
                        self.termination_requested = true;
                    }
                }

                MediaType::Other => (), // ignore non audio/video types
            }
        }
    }

    fn handle_request(&mut self, request: TranscodeManagerRequest) {
        match request {
            TranscodeManagerRequest::StopTranscode => {
                self.termination_requested = true;
            }
        }
    }
}

fn notify_on_outbound_media_closed(
    sender: UnboundedSender<MediaNotificationContent>,
    actor_sender: UnboundedSender<TranscoderFutureResult>,
) {
    tokio::spawn(async move {
        tokio::select! {
            _ = sender.closed() => {
                let _ = actor_sender.send(TranscoderFutureResult::OutboundMediaReceiverGone);
            }

            _ = actor_sender.closed() => { }
        }
    });
}

fn notify_on_inbound_media(
    mut receiver: UnboundedReceiver<MediaNotificationContent>,
    actor_sender: UnboundedSender<TranscoderFutureResult>,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = receiver.recv() => {
                    match result {
                        Some(media) => {
                            let _ = actor_sender.send(TranscoderFutureResult::MediaReceived(media));
                        }

                        None => {
                            let _ = actor_sender.send(TranscoderFutureResult::InboundMediaSendersGone);
                            break;
                        }
                    }
                }

                _ = actor_sender.closed() => {
                    break;
                }
            }
        }
    });
}

fn notify_bus_message(mut bus: BusStream, actor_sender: UnboundedSender<TranscoderFutureResult>) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = bus.next() => {
                    match result {
                        Some(message) => {
                            match message.view() {
                                MessageView::Eos(..) => {
                                    let _ = actor_sender.send(TranscoderFutureResult::GstEosReceived);
                                },

                                MessageView::Error(error) => {
                                    let result = TranscoderFutureResult::GstErrorReceived(GstError {
                                        source_name: error
                                            .src()
                                            .map(|s| s.path_string().to_string())
                                            .unwrap_or_else(|| "<none>".to_string()),

                                        error_description: error.error().to_string(),
                                        debug_info: error.debug(),
                                    });

                                    let _ = actor_sender.send(result);
                                }

                                _ => (),
                            }
                        }

                        None => {
                            let _ = actor_sender.send(TranscoderFutureResult::GstBusClosed);
                            break;
                        }
                    }
                }

                _ = actor_sender.closed() => {
                    break;
                }
            }
        }
    });
}
