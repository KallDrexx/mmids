mod transcoding_manager;

use std::collections::HashMap;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, FutureExt};
use gstreamer::glib;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{info, instrument, warn};
use uuid::Uuid;
use mmids_core::workflows::MediaNotificationContent;
use crate::endpoints::gst_transcoder::transcoding_manager::TranscodeManagerRequest;

pub enum GstTranscoderRequest {
    StartTranscoding {
        id: Uuid,
        input_media: UnboundedReceiver<MediaNotificationContent>,
        video_encoder_name: String,
        audio_encoder_name: String,
        audio_parameters: HashMap<String, Option<String>>,
        video_parameters: HashMap<String, Option<String>>,
        notification_channel: UnboundedSender<GstTranscoderNotification>,
    },

    StopTranscoding {
        id: Uuid,
    },
}

pub enum GstTranscoderNotification {
    TranscodingStarted {
        output_media: UnboundedSender<MediaNotificationContent>,
    },

    TranscodingStopped(GstTranscoderStoppedCause),
}

pub enum EncoderType { Video, Audio }

pub enum GstTranscoderStoppedCause {
    InvalidEncoderName {
        encoder_type: EncoderType,
        name: String,
    },

    EncoderCreationFailure {
        encoder_type: EncoderType,
        details: String,
    },

    PipelineReachedEndOfStream,
    PipelineError(String),
    IdAlreadyActive(Uuid),
    StopRequested,
}

#[derive(thiserror::Error, Debug)]
pub enum EndpointStartError {
    #[error("Gstreamer failed to initialize")]
    GstreamerError(#[from] glib::Error),
}

pub fn start_gst_transcoder() -> Result<UnboundedSender<GstTranscoderRequest>, EndpointStartError> {
    let (sender, receiver) = unbounded_channel();
    let actor = EndpointActor::new(receiver)?;
    tokio::spawn(actor.run());

    Ok(sender)
}

enum EndpointFuturesResult {
    AllConsumersGone,
    RequestReceived(GstTranscoderRequest, UnboundedReceiver<GstTranscoderRequest>),
}

struct ActiveTranscode {
    sender: UnboundedSender<TranscodeManagerRequest>,
    notification_channel: UnboundedSender<GstTranscoderNotification>,
}

struct EndpointActor {
    futures: FuturesUnordered<BoxFuture<'static, EndpointFuturesResult>>,
    active_transcodes: HashMap<Uuid, ActiveTranscode>,
}

impl EndpointActor {
    fn new(
        receiver: UnboundedReceiver<GstTranscoderRequest>,
    ) -> Result<EndpointActor, EndpointStartError> {
        gstreamer::init()?;

        let futures = FuturesUnordered::new();
        futures.push(endpoint_futures::wait_for_request(receiver).boxed());

        Ok(EndpointActor {
            futures,
            active_transcodes: HashMap::new(),
        })
    }

    #[instrument(name = "GstTranscodeEndpoint Execution", skip(self))]
    async fn run(mut self) {
        info!("Starting endpoint");

        while let Some(future) = self.futures.next().await {
            match future {
                EndpointFuturesResult::AllConsumersGone => {
                    info!("All consumers gone");
                    break;
                }

                EndpointFuturesResult::RequestReceived(request, receiver) => {
                    self.futures
                        .push(endpoint_futures::wait_for_request(receiver).boxed());

                    self.handle_request(request);
                }
            }
        }

        info!("Closing endpoint");
    }

    fn handle_request(&mut self, request: GstTranscoderRequest) {
        match request {
            GstTranscoderRequest::StartTranscoding {
                id,
                notification_channel,
                input_media,
                video_encoder_name,
                video_parameters,
                audio_encoder_name,
                audio_parameters,
            } => {
                if self.active_transcodes.contains_key(&id) {
                    warn!("Transcoding requested with id {}, but that id is already active", id);
                    let _ = notification_channel.send(
                        GstTranscoderNotification::TranscodingStopped(
                            GstTranscoderStoppedCause::IdAlreadyActive(id)
                        ));

                    return;
                }

                // TODO: Create gst pipeline, create encoders, start manager

            }

            GstTranscoderRequest::StopTranscoding {id} => {
                info!("Requested transcoding process id {} stopped", id);
                if let Some(transcode) = self.active_transcodes.remove(&id) {
                    let _ = transcode.notification_channel.send(
                        GstTranscoderNotification::TranscodingStopped(
                            GstTranscoderStoppedCause::StopRequested
                        )
                    );

                    let _ = transcode.sender.send(TranscodeManagerRequest::StopTranscode);
                }
            }
        }
    }
}

mod endpoint_futures {
    use tokio::sync::mpsc::UnboundedReceiver;
    use crate::endpoints::gst_transcoder::{EndpointFuturesResult, GstTranscoderRequest};

    pub(super) async fn wait_for_request(
        mut receiver: UnboundedReceiver<GstTranscoderRequest>,
    ) -> EndpointFuturesResult {
        match receiver.recv().await {
            Some(request) => EndpointFuturesResult::RequestReceived(request, receiver),
            None => EndpointFuturesResult::AllConsumersGone,
        }
    }
}
