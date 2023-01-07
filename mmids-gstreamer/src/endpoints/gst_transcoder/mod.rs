mod transcoding_manager;

use crate::encoders::EncoderFactory;
use crate::endpoints::gst_transcoder::endpoint_futures::notify_manager_gone;
use crate::endpoints::gst_transcoder::transcoding_manager::{
    start_transcode_manager, TranscodeManagerRequest, TranscoderParams,
};
use crate::GSTREAMER_INIT_RESULT;
use gstreamer::{glib, Pipeline};
use mmids_core::actor_utils::notify_on_unbounded_recv;
use mmids_core::workflows::metadata::MetadataKey;
use mmids_core::workflows::MediaNotificationContent;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

/// Requests that can be made to the gstreamer transcoding endpoint
pub enum GstTranscoderRequest {
    /// Makes a request for the endpoint to start transcoding
    StartTranscoding {
        /// A unique identifier that is associated with this transcoding request.  Used for logging
        /// and to associate stop transcoding requests.
        id: Uuid,

        /// The channel in which audio and video data will come in for the transcoding process
        input_media: UnboundedReceiver<MediaNotificationContent>,

        /// The name of the video encoder to use for transcoding.  Must match a valid name
        /// registered with the encoder factory
        video_encoder_name: String,

        /// The name of hte audio encoder to use for transcoding.  Must match a valid name
        /// registered with the encoder factory
        audio_encoder_name: String,

        /// Parameters to pass to the audio encoder
        audio_parameters: HashMap<String, Option<String>>,

        /// Parameters to pass to the video encoder
        video_parameters: HashMap<String, Option<String>>,

        /// Channel to send responses and notifications to
        notification_channel: UnboundedSender<GstTranscoderNotification>,
    },

    /// Makes a request for the endpoint to stop transcoding
    StopTranscoding {
        /// The identifier of the transcoding process to stop.
        id: Uuid,
    },
}

/// Notifications the transcoding endpoint can raise
pub enum GstTranscoderNotification {
    /// Notification that transcoding has started
    TranscodingStarted {
        /// Channel in which resulting audio and video data will be sent to
        output_media: UnboundedReceiver<MediaNotificationContent>,
    },

    /// Notification that transcoding stopped
    TranscodingStopped(GstTranscoderStoppedCause),
}

#[derive(Debug, PartialEq, Eq)]
pub enum EncoderType {
    Video,
    Audio,
}

/// Reasons transcoding have stopped
#[derive(Debug, PartialEq, Eq)]
pub enum GstTranscoderStoppedCause {
    /// No encoder generator has been registered with the encoder factory with the specified name
    InvalidEncoderName {
        encoder_type: EncoderType,
        name: String,
    },

    /// An error occurred when the encoder was attempted to be created, either due to an error
    /// with gstreamer or with invalid parameters
    EncoderCreationFailure {
        /// What type of encoder that failed
        encoder_type: EncoderType,

        /// Error description of why a failure occurred.
        details: String,
    },

    /// Transcoding was requested to be started with an id that is already active
    IdAlreadyActive(Uuid),

    /// Transcoding stopped because a request was made for it to stop.
    StopRequested,

    /// The transcoding process was unexpectedly terminated without an explicit error being raised.
    /// Will probably need to look in logs to get more info on why.  This should be rare.
    UnexpectedlyTerminated,
}

/// Errors that can occur when attempting to start the endpoint
#[derive(thiserror::Error, Debug)]
pub enum EndpointStartError {
    #[error("Gstreamer failed to initialize")]
    GstreamerError(#[from] &'static glib::Error),
}

struct StartTranscodeParams {
    id: Uuid,
    notification_channel: UnboundedSender<GstTranscoderNotification>,
    input_media: UnboundedReceiver<MediaNotificationContent>,
    video_encoder_name: String,
    video_parameters: HashMap<String, Option<String>>,
    audio_encoder_name: String,
    audio_parameters: HashMap<String, Option<String>>,
}

/// Starts the gstreamer transcode process, and returns a channel in which communication with the
/// endpoint can be made.
pub fn start_gst_transcoder(
    encoder_factory: Arc<EncoderFactory>,
    pts_offset_metadata_key: MetadataKey,
) -> Result<UnboundedSender<GstTranscoderRequest>, EndpointStartError> {
    let (sender, receiver) = unbounded_channel();
    let (actor_sender, actor_receiver) = unbounded_channel();
    let actor = EndpointActor::new(
        receiver,
        encoder_factory,
        pts_offset_metadata_key,
        actor_sender,
    )?;

    tokio::spawn(actor.run(actor_receiver));

    Ok(sender)
}

enum EndpointFuturesResult {
    AllConsumersGone,
    RequestReceived(GstTranscoderRequest),
    TranscodeManagerGone(Uuid),
}

struct ActiveTranscode {
    sender: UnboundedSender<TranscodeManagerRequest>,
    notification_channel: UnboundedSender<GstTranscoderNotification>,
}

struct EndpointActor {
    internal_sender: UnboundedSender<EndpointFuturesResult>,
    active_transcodes: HashMap<Uuid, ActiveTranscode>,
    encoder_factory: Arc<EncoderFactory>,
    pts_offset_metadata_key: MetadataKey,
}

impl EndpointActor {
    fn new(
        receiver: UnboundedReceiver<GstTranscoderRequest>,
        encoder_factory: Arc<EncoderFactory>,
        pts_offset_metadata_key: MetadataKey,
        actor_sender: UnboundedSender<EndpointFuturesResult>,
    ) -> Result<EndpointActor, EndpointStartError> {
        (*GSTREAMER_INIT_RESULT).as_ref()?;

        notify_on_unbounded_recv(
            receiver,
            actor_sender.clone(),
            EndpointFuturesResult::RequestReceived,
            || EndpointFuturesResult::AllConsumersGone,
        );

        Ok(EndpointActor {
            internal_sender: actor_sender,
            active_transcodes: HashMap::new(),
            encoder_factory,
            pts_offset_metadata_key,
        })
    }

    #[instrument(name = "GstTranscodeEndpoint Execution", skip_all)]
    async fn run(mut self, mut actor_receiver: UnboundedReceiver<EndpointFuturesResult>) {
        info!("Starting endpoint");

        while let Some(future) = actor_receiver.recv().await {
            match future {
                EndpointFuturesResult::AllConsumersGone => {
                    info!("All consumers gone");
                    break;
                }

                EndpointFuturesResult::RequestReceived(request) => {
                    self.handle_request(request);
                }

                EndpointFuturesResult::TranscodeManagerGone(id) => {
                    if let Some(details) = self.active_transcodes.remove(&id) {
                        info!("Transcode process {} stopped", id);

                        let _ = details.notification_channel.send(
                            GstTranscoderNotification::TranscodingStopped(
                                GstTranscoderStoppedCause::UnexpectedlyTerminated,
                            ),
                        );
                    }
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
                self.handle_start_transcode_request(StartTranscodeParams {
                    id,
                    notification_channel,
                    input_media,
                    video_encoder_name,
                    video_parameters,
                    audio_encoder_name,
                    audio_parameters,
                });
            }

            GstTranscoderRequest::StopTranscoding { id } => {
                info!("Requested transcoding process id {} stopped", id);
                if let Some(transcode) = self.active_transcodes.remove(&id) {
                    let _ = transcode.notification_channel.send(
                        GstTranscoderNotification::TranscodingStopped(
                            GstTranscoderStoppedCause::StopRequested,
                        ),
                    );

                    let _ = transcode
                        .sender
                        .send(TranscodeManagerRequest::StopTranscode);
                }
            }
        }
    }

    fn handle_start_transcode_request(&mut self, params: StartTranscodeParams) {
        if self.active_transcodes.contains_key(&params.id) {
            warn!(
                "Transcoding requested with id {}, but that id is already active",
                params.id
            );
            let _ =
                params
                    .notification_channel
                    .send(GstTranscoderNotification::TranscodingStopped(
                        GstTranscoderStoppedCause::IdAlreadyActive(params.id),
                    ));

            return;
        }

        let (outbound_media_sender, outbound_media_receiver) = unbounded_channel();

        let pipeline_name = format!("transcode_pipeline_{}", params.id);
        let pipeline = Pipeline::new(Some(pipeline_name.as_str()));

        let video_encoder = self.encoder_factory.get_video_encoder(
            params.video_encoder_name.clone(),
            &pipeline,
            &params.video_parameters,
            outbound_media_sender.clone(),
        );

        let video_encoder = match video_encoder {
            Ok(encoder) => encoder,
            Err(error) => {
                error!(
                    "Failed to create the {} video encoder: {:?}",
                    params.video_encoder_name, error,
                );

                let _ = params.notification_channel.send(
                    GstTranscoderNotification::TranscodingStopped(
                        GstTranscoderStoppedCause::EncoderCreationFailure {
                            encoder_type: EncoderType::Video,
                            details: format!("{:?}", error),
                        },
                    ),
                );

                return;
            }
        };

        let audio_encoder = self.encoder_factory.get_audio_encoder(
            params.audio_encoder_name.clone(),
            &pipeline,
            &params.audio_parameters,
            outbound_media_sender.clone(),
        );

        let audio_encoder = match audio_encoder {
            Ok(encoder) => encoder,
            Err(error) => {
                error!(
                    "Failed to create the {} audio encoder: {:?}",
                    params.audio_encoder_name, error,
                );

                let _ = params.notification_channel.send(
                    GstTranscoderNotification::TranscodingStopped(
                        GstTranscoderStoppedCause::EncoderCreationFailure {
                            encoder_type: EncoderType::Audio,
                            details: format!("{:?}", error),
                        },
                    ),
                );

                return;
            }
        };

        let parameters = TranscoderParams {
            pipeline,
            video_encoder,
            audio_encoder,
            inbound_media: params.input_media,
            outbound_media: outbound_media_sender,
            process_id: params.id,
        };

        let manager = start_transcode_manager(parameters, self.pts_offset_metadata_key);

        let _ = params
            .notification_channel
            .send(GstTranscoderNotification::TranscodingStarted {
                output_media: outbound_media_receiver,
            });

        notify_manager_gone(params.id, manager.clone(), self.internal_sender.clone());

        self.active_transcodes.insert(
            params.id,
            ActiveTranscode {
                sender: manager,
                notification_channel: params.notification_channel,
            },
        );
    }
}

mod endpoint_futures {
    use crate::endpoints::gst_transcoder::transcoding_manager::TranscodeManagerRequest;
    use crate::endpoints::gst_transcoder::EndpointFuturesResult;
    use tokio::sync::mpsc::UnboundedSender;
    use uuid::Uuid;

    pub(super) fn notify_manager_gone(
        id: Uuid,
        sender: UnboundedSender<TranscodeManagerRequest>,
        actor_sender: UnboundedSender<EndpointFuturesResult>,
    ) {
        tokio::spawn(async move {
            tokio::select! {
                _ = sender.closed() => {
                    let _ = actor_sender.send(EndpointFuturesResult::TranscodeManagerGone(id));
                }

                _ = actor_sender.closed() => { }
            }
        });
    }
}
