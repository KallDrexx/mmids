use crate::encoders::{AudioEncoder, VideoEncoder};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use gstreamer::bus::BusStream;
use gstreamer::prelude::*;
use gstreamer::{MessageView, Pipeline, State};
use mmids_core::workflows::MediaNotificationContent;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info, instrument};
use uuid::Uuid;

pub enum TranscodeManagerRequest {
    StopTranscode,
}

pub struct TranscoderParams {
    pub process_id: Uuid,
    pub video_encoder: Box<dyn VideoEncoder>,
    pub audio_encoder: Box<dyn AudioEncoder>,
    pub inbound_media: UnboundedReceiver<MediaNotificationContent>,
    pub outbound_media: UnboundedSender<MediaNotificationContent>,
    pub pipeline: Pipeline,
}

enum TranscoderFutureResult {
    EndpointGone,
    InboundMediaSendersGone,
    OutboundMediaReceiverGone,
    RequestReceived(
        TranscodeManagerRequest,
        UnboundedReceiver<TranscodeManagerRequest>,
    ),
    MediaReceived(
        MediaNotificationContent,
        UnboundedReceiver<MediaNotificationContent>,
    ),
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
) -> UnboundedSender<TranscodeManagerRequest> {
    let (sender, receiver) = unbounded_channel();
    let actor = TranscodeManager::new(parameters, receiver);
    tokio::spawn(actor.run());

    sender
}

struct TranscodeManager {
    termination_requested: bool,
    id: Uuid,
    futures: FuturesUnordered<BoxFuture<'static, TranscoderFutureResult>>,
    video_encoder: Box<dyn VideoEncoder>,
    audio_encoder: Box<dyn AudioEncoder>,
    pipeline: Pipeline,
}

unsafe impl Send for TranscodeManager {}
unsafe impl Sync for TranscodeManager {}

impl TranscodeManager {
    fn new(
        parameters: TranscoderParams,
        receiver: UnboundedReceiver<TranscodeManagerRequest>,
    ) -> TranscodeManager {
        let futures = FuturesUnordered::new();
        futures.push(wait_for_request(receiver).boxed());
        futures.push(notify_on_outbound_media_closed(parameters.outbound_media).boxed());
        futures.push(notify_on_inbound_media(parameters.inbound_media).boxed());

        TranscodeManager {
            termination_requested: false,
            id: parameters.process_id,
            futures,
            video_encoder: parameters.video_encoder,
            audio_encoder: parameters.audio_encoder,
            pipeline: parameters.pipeline,
        }
    }

    #[instrument(name = "Transcode Manager Execution", skip(self), fields(transcoding_process_id = %self.id))]
    async fn run(mut self) {
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

        self.futures.push(notify_bus_message(bus.stream()).boxed());

        while let Some(result) = self.futures.next().await {
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

                TranscoderFutureResult::MediaReceived(media, receiver) => {
                    self.futures.push(notify_on_inbound_media(receiver).boxed());

                    self.handle_media(media);
                }

                TranscoderFutureResult::RequestReceived(request, receiver) => {
                    self.futures.push(wait_for_request(receiver).boxed());

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
        match media {
            MediaNotificationContent::Video {
                timestamp,
                codec,
                data,
                is_sequence_header,
                is_keyframe: _,
            } => {
                let result =
                    self.video_encoder
                        .push_data(codec, data, timestamp, is_sequence_header);

                if let Err(error) = result {
                    error!("Failed to push media to video encoder: {}", error);
                    self.termination_requested = true;
                }
            }

            MediaNotificationContent::Audio {
                timestamp,
                codec,
                data,
                is_sequence_header,
            } => {
                let result =
                    self.audio_encoder
                        .push_data(codec, data, timestamp, is_sequence_header);

                if let Err(error) = result {
                    error!("Failed to push media to audio encoder: {}", error);
                    self.termination_requested = true;
                }
            }

            // Don't care about other content types for now
            _ => (),
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

async fn wait_for_request(
    mut receiver: UnboundedReceiver<TranscodeManagerRequest>,
) -> TranscoderFutureResult {
    match receiver.recv().await {
        Some(request) => TranscoderFutureResult::RequestReceived(request, receiver),
        None => TranscoderFutureResult::EndpointGone,
    }
}

async fn notify_on_outbound_media_closed(
    sender: UnboundedSender<MediaNotificationContent>,
) -> TranscoderFutureResult {
    sender.closed().await;

    TranscoderFutureResult::OutboundMediaReceiverGone
}

async fn notify_on_inbound_media(
    mut receiver: UnboundedReceiver<MediaNotificationContent>,
) -> TranscoderFutureResult {
    match receiver.recv().await {
        Some(media) => TranscoderFutureResult::MediaReceived(media, receiver),
        None => TranscoderFutureResult::InboundMediaSendersGone,
    }
}

async fn notify_bus_message(mut bus: BusStream) -> TranscoderFutureResult {
    while let Some(message) = bus.next().await {
        match message.view() {
            MessageView::Eos(..) => return TranscoderFutureResult::GstEosReceived,

            MessageView::Error(error) => {
                return TranscoderFutureResult::GstErrorReceived(GstError {
                    source_name: error
                        .src()
                        .map(|s| s.path_string().to_string())
                        .unwrap_or("<none>".to_string()),

                    error_description: error.error().to_string(),
                    debug_info: error.debug(),
                })
            }

            _ => (),
        }
    }

    TranscoderFutureResult::GstBusClosed
}
