//! A workflow step that that utilizes the ffmpeg executable to transcode media streams.  When a
//! new stream comes into the step, it will coordinate with the RTMP server endpoint to provision
//! a special app/stream key combination to push a video stream out and another app/stream key
//! combination to receive the transcoded video stream back.
//!
//! It will then request the ffmpeg endpoint to pull video from the output rtmp location, how
//! ffmpeg should transcode the video, and to send the resulting video back.  The transcoded media
//! is then passed to onto the next step.
//!
//! Media notifications that this step receives are passed to the RTMP endpoint but are not
//! passed along to the next step.  When the step receives transcoded media it will then pass those
//! to the next step.

use crate::endpoints::ffmpeg::{
    AudioTranscodeParams, FfmpegEndpointNotification, FfmpegEndpointRequest, FfmpegParams,
    H264Preset, TargetParams, VideoScale, VideoTranscodeParams,
};
use crate::endpoints::rtmp_server::{
    IpRestriction, RegistrationType, RtmpEndpointMediaMessage, RtmpEndpointPublisherMessage,
    RtmpEndpointRequest, RtmpEndpointWatcherNotification, StreamKeyRegistration,
};
use crate::utils::stream_metadata_to_hash_map;
use crate::workflows::definitions::WorkflowStepDefinition;
use crate::workflows::steps::factory::StepGenerator;
use crate::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use crate::workflows::{MediaNotification, MediaNotificationContent};
use crate::StreamId;
use futures::FutureExt;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info, warn};
use uuid::Uuid;

const VIDEO_CODEC_NAME: &'static str = "vcodec";
const AUDIO_CODEC_NAME: &'static str = "acodec";
const H264_PRESET_NAME: &'static str = "h264_preset";
const SIZE_NAME: &'static str = "size";
const BITRATE_NAME: &'static str = "kbps";

/// Generates new ffmpeg transcoding step instances based on specified step definitions.
pub struct FfmpegTranscoderStepGenerator {
    rtmp_server_endpoint: UnboundedSender<RtmpEndpointRequest>,
    ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
}

struct FfmpegTranscoder {
    definition: WorkflowStepDefinition,
    ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    rtmp_server_endpoint: UnboundedSender<RtmpEndpointRequest>,
    video_codec_params: VideoTranscodeParams,
    audio_codec_params: AudioTranscodeParams,
    video_scale_params: Option<VideoScale>,
    bitrate: Option<u16>,
    active_streams: HashMap<StreamId, ActiveStream>,
    status: StepStatus,
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

#[derive(Debug)]
enum PublishRegistrationStatus {
    Inactive,
    Pending,
    Active,
}

#[derive(Debug)]
enum FfmpegStatus {
    Inactive,
    Pending,
    Active,
}

struct ActiveStream {
    id: StreamId,
    stream_name: String,
    pending_media: VecDeque<MediaNotificationContent>,
    rtmp_output_status: WatchRegistrationStatus,
    rtmp_input_status: PublishRegistrationStatus,
    ffmpeg_status: FfmpegStatus,
    ffmpeg_id: Uuid,
}

enum FutureResult {
    RtmpEndpointGone,
    FfmpegEndpointGone,
    RtmpWatchNotificationReceived(
        StreamId,
        RtmpEndpointWatcherNotification,
        UnboundedReceiver<RtmpEndpointWatcherNotification>,
    ),
    RtmpWatchChannelGone(StreamId),
    RtmpPublishNotificationReceived(
        StreamId,
        RtmpEndpointPublisherMessage,
        UnboundedReceiver<RtmpEndpointPublisherMessage>,
    ),
    RtmpPublishChannelGone(StreamId),
    FfmpegNotificationReceived(
        StreamId,
        FfmpegEndpointNotification,
        UnboundedReceiver<FfmpegEndpointNotification>,
    ),
    FfmpegChannelGone(StreamId),
}

impl StepFutureResult for FutureResult {}

#[derive(Error, Debug)]
enum StepStartupError {
    #[error("Invalid video codec specified ({0}).  {} is a required field and valid values are: 'copy' and 'h264'", VIDEO_CODEC_NAME)]
    InvalidVideoCodecSpecified(String),

    #[error("Invalid audio codec specified ({0}).  {} is a required field and valid values are: 'copy' and 'aac'", AUDIO_CODEC_NAME)]
    InvalidAudioCodecSpecified(String),

    #[error("Invalid h264 preset specified ({0}).  {} is the name of any h264 profile (e.g. veryfast, medium, etc...)", H264_PRESET_NAME)]
    InvalidH264PresetSpecified(String),

    #[error(
        "Invalid video size specified ({0}).  {} must be in the format of '<width>x<height>'",
        SIZE_NAME
    )]
    InvalidVideoSizeSpecified(String),

    #[error("Invalid bitrate specified ({0}).  {} must be a number", BITRATE_NAME)]
    InvalidBitrateSpecified(String),
}

impl FfmpegTranscoderStepGenerator {
    pub fn new(
        rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
        ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    ) -> Self {
        FfmpegTranscoderStepGenerator {
            rtmp_server_endpoint: rtmp_endpoint,
            ffmpeg_endpoint,
        }
    }
}

impl StepGenerator for FfmpegTranscoderStepGenerator {
    fn generate(&self, definition: WorkflowStepDefinition) -> StepCreationResult {
        let vcodec = match definition.parameters.get(VIDEO_CODEC_NAME) {
            Some(Some(value)) => match value.to_lowercase().trim() {
                "copy" => VideoTranscodeParams::Copy,
                "h264" => match definition.parameters.get(H264_PRESET_NAME) {
                    Some(Some(value)) => match value.to_lowercase().trim() {
                        "ultrafast" => VideoTranscodeParams::H264 {
                            preset: H264Preset::UltraFast,
                        },
                        "superfast" => VideoTranscodeParams::H264 {
                            preset: H264Preset::SuperFast,
                        },
                        "veryfast" => VideoTranscodeParams::H264 {
                            preset: H264Preset::VeryFast,
                        },
                        "faster" => VideoTranscodeParams::H264 {
                            preset: H264Preset::Faster,
                        },
                        "fast" => VideoTranscodeParams::H264 {
                            preset: H264Preset::Fast,
                        },
                        "medium" => VideoTranscodeParams::H264 {
                            preset: H264Preset::Medium,
                        },
                        "slow" => VideoTranscodeParams::H264 {
                            preset: H264Preset::Slow,
                        },
                        "slower" => VideoTranscodeParams::H264 {
                            preset: H264Preset::Slower,
                        },
                        "veryslow" => VideoTranscodeParams::H264 {
                            preset: H264Preset::VerySlow,
                        },
                        x => {
                            return Err(Box::new(StepStartupError::InvalidH264PresetSpecified(
                                x.to_string(),
                            )))
                        }
                    },
                    _ => VideoTranscodeParams::H264 {
                        preset: H264Preset::VeryFast,
                    },
                },
                x => {
                    return Err(Box::new(StepStartupError::InvalidVideoCodecSpecified(
                        x.to_string(),
                    )))
                }
            },

            _ => {
                return Err(Box::new(StepStartupError::InvalidVideoCodecSpecified(
                    "".to_string(),
                )))
            }
        };

        let acodec = match definition.parameters.get(AUDIO_CODEC_NAME) {
            Some(Some(value)) => match value.to_lowercase().trim() {
                "copy" => AudioTranscodeParams::Copy,
                "aac" => AudioTranscodeParams::Aac,
                x => {
                    return Err(Box::new(StepStartupError::InvalidAudioCodecSpecified(
                        x.to_string(),
                    )))
                }
            },

            _ => {
                return Err(Box::new(StepStartupError::InvalidAudioCodecSpecified(
                    "".to_string(),
                )))
            }
        };

        let size = match definition.parameters.get(SIZE_NAME) {
            Some(Some(value)) => {
                let mut dimensions = Vec::new();
                for part in value.split('x') {
                    match part.parse::<u16>() {
                        Ok(num) => dimensions.push(num),
                        Err(_) => {
                            return Err(Box::new(StepStartupError::InvalidVideoSizeSpecified(
                                value.clone(),
                            )))
                        }
                    }
                }

                if dimensions.len() != 2 {
                    return Err(Box::new(StepStartupError::InvalidVideoSizeSpecified(
                        value.clone(),
                    )));
                }

                Some(VideoScale {
                    width: dimensions[0],
                    height: dimensions[1],
                })
            }

            _ => None,
        };

        let bitrate = match definition.parameters.get(BITRATE_NAME) {
            Some(Some(value)) => {
                if let Ok(num) = value.parse() {
                    Some(num)
                } else {
                    return Err(Box::new(StepStartupError::InvalidBitrateSpecified(
                        value.clone(),
                    )));
                }
            }

            _ => None,
        };

        let step = FfmpegTranscoder {
            definition: definition.clone(),
            active_streams: HashMap::new(),
            audio_codec_params: acodec,
            rtmp_server_endpoint: self.rtmp_server_endpoint.clone(),
            ffmpeg_endpoint: self.ffmpeg_endpoint.clone(),
            video_scale_params: size,
            video_codec_params: vcodec,
            bitrate,
            status: StepStatus::Active,
        };

        let futures = vec![
            notify_when_ffmpeg_endpoint_is_gone(self.ffmpeg_endpoint.clone()).boxed(),
            notify_when_rtmp_endpoint_is_gone(self.rtmp_server_endpoint.clone()).boxed(),
        ];

        Ok((Box::new(step), futures))
    }
}

impl FfmpegTranscoder {
    fn get_source_rtmp_app(&self) -> String {
        format!("ffmpeg-transcoder-original-{}", self.definition.get_id())
    }

    fn get_result_rtmp_app(&self) -> String {
        format!("ffmpeg-transcoder-result-{}", self.definition.get_id())
    }

    fn handle_resolved_future(
        &mut self,
        notification: Box<dyn StepFutureResult>,
        outputs: &mut StepOutputs,
    ) {
        if self.status == StepStatus::Error {
            return;
        }

        let notification = match notification.downcast::<FutureResult>() {
            Ok(x) => *x,
            Err(_) => return,
        };

        match notification {
            FutureResult::FfmpegEndpointGone => {
                error!("Ffmpeg endpoint is gone!");
                self.status = StepStatus::Error;

                let ids: Vec<StreamId> = self.active_streams.keys().map(|x| x.clone()).collect();
                for id in ids {
                    self.stop_stream(&id);
                }
            }

            FutureResult::RtmpEndpointGone => {
                error!("RTMP endpoint is gone!");
                self.status = StepStatus::Error;

                let ids: Vec<StreamId> = self.active_streams.keys().map(|x| x.clone()).collect();
                for id in ids {
                    self.stop_stream(&id);
                }
            }

            FutureResult::RtmpWatchChannelGone(stream_id) => {
                if self.stop_stream(&stream_id) {
                    error!(stream_id = ?stream_id, "Rtmp watch channel disappeared for stream id {:?}", stream_id);
                }
            }

            FutureResult::RtmpPublishChannelGone(stream_id) => {
                if self.stop_stream(&stream_id) {
                    error!(
                        stream_id = ?stream_id,
                        "Rtmp publish channel dissappeared for stream id {:?}", stream_id
                    );
                }
            }

            FutureResult::FfmpegChannelGone(stream_id) => {
                if self.stop_stream(&stream_id) {
                    error!(
                        stream_id = ?stream_id,
                        "Ffmpeg channel disappeared for stream id {:?}", stream_id
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

            FutureResult::RtmpPublishNotificationReceived(stream_id, notification, receiver) => {
                if !self.active_streams.contains_key(&stream_id) {
                    // late notification after stopping a stream
                    return;
                }

                outputs
                    .futures
                    .push(wait_for_publish_notification(stream_id.clone(), receiver).boxed());
                self.handle_rtmp_publish_notification(stream_id, notification, outputs);
            }

            FutureResult::FfmpegNotificationReceived(stream_id, notification, receiver) => {
                if !self.active_streams.contains_key(&stream_id) {
                    // late notification after stopping a stream
                    return;
                }

                outputs
                    .futures
                    .push(wait_for_ffmpeg_notification(stream_id.clone(), receiver).boxed());
                self.handle_ffmpeg_notification(stream_id, notification, outputs);
            }
        }
    }

    fn handle_media(&mut self, media: MediaNotification, outputs: &mut StepOutputs) {
        if self.status == StepStatus::Error {
            return;
        }

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
                    rtmp_input_status: PublishRegistrationStatus::Inactive,
                    ffmpeg_status: FfmpegStatus::Inactive,
                    ffmpeg_id: Uuid::new_v4(),
                };

                self.active_streams.insert(media.stream_id.clone(), stream);
                self.prepare_stream(media.stream_id.clone(), outputs);

                outputs.media.push(media.clone());
            }

            MediaNotificationContent::StreamDisconnected => {
                if self.stop_stream(&media.stream_id) {
                    info!(
                        stream_id = ?media.stream_id,
                        "Stopping stream id {:?} due to stream disconnection notification", media.stream_id
                    );
                }

                outputs.media.push(media.clone());
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
    }

    fn prepare_stream(&mut self, stream_id: StreamId, outputs: &mut StepOutputs) {
        let source_rtmp_app = self.get_source_rtmp_app();
        let result_rtmp_app = self.get_result_rtmp_app();

        if let Some(stream) = self.active_streams.get_mut(&stream_id) {
            let (output_is_active, output_media_channel) = match &stream.rtmp_output_status {
                WatchRegistrationStatus::Inactive => {
                    let (media_sender, media_receiver) = unbounded_channel();
                    let (watch_sender, watch_receiver) = unbounded_channel();
                    let _ =
                        self.rtmp_server_endpoint
                            .send(RtmpEndpointRequest::ListenForWatchers {
                                notification_channel: watch_sender,
                                rtmp_app: source_rtmp_app.clone(),
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
            }

            let input_is_active = match &stream.rtmp_input_status {
                PublishRegistrationStatus::Inactive => {
                    let (sender, receiver) = unbounded_channel();
                    let _ =
                        self.rtmp_server_endpoint
                            .send(RtmpEndpointRequest::ListenForPublishers {
                                port: 1935,
                                rtmp_app: result_rtmp_app.clone(),
                                rtmp_stream_key: StreamKeyRegistration::Exact(stream.id.0.clone()),
                                stream_id: Some(stream.id.clone()),
                                message_channel: sender,
                                ip_restrictions: IpRestriction::None,
                                use_tls: false,
                                requires_registrant_approval: false,
                            });

                    outputs
                        .futures
                        .push(wait_for_publish_notification(stream.id.clone(), receiver).boxed());
                    stream.rtmp_input_status = PublishRegistrationStatus::Pending;

                    false
                }

                PublishRegistrationStatus::Pending => false,
                PublishRegistrationStatus::Active => true,
            };

            match &stream.ffmpeg_status {
                FfmpegStatus::Inactive => {
                    // Not worth starting ffmpeg until both input and outputs registrations are complete
                    if input_is_active && output_is_active {
                        let parameters = FfmpegParams {
                            read_in_real_time: true,
                            bitrate_in_kbps: self.bitrate,
                            input: format!("rtmp://localhost/{}/{}", source_rtmp_app, stream.id.0),
                            video_transcode: self.video_codec_params.clone(),
                            audio_transcode: self.audio_codec_params.clone(),
                            scale: self.video_scale_params.clone(),
                            target: TargetParams::Rtmp {
                                url: format!(
                                    "rtmp://localhost/{}/{}",
                                    result_rtmp_app, stream.id.0
                                ),
                            },
                        };

                        let (sender, receiver) = unbounded_channel();
                        let _ = self
                            .ffmpeg_endpoint
                            .send(FfmpegEndpointRequest::StartFfmpeg {
                                id: stream.ffmpeg_id.clone(),
                                params: parameters,
                                notification_channel: sender,
                            });

                        outputs.futures.push(
                            wait_for_ffmpeg_notification(stream.id.clone(), receiver).boxed(),
                        );
                        stream.ffmpeg_status = FfmpegStatus::Pending;
                    }
                }

                _ => (),
            }
        }
    }

    fn stop_stream(&mut self, stream_id: &StreamId) -> bool {
        if let Some(stream) = self.active_streams.remove(stream_id) {
            match &stream.ffmpeg_status {
                FfmpegStatus::Pending => {
                    let _ = self
                        .ffmpeg_endpoint
                        .send(FfmpegEndpointRequest::StopFfmpeg {
                            id: stream.ffmpeg_id.clone(),
                        });
                }

                FfmpegStatus::Active => {
                    let _ = self
                        .ffmpeg_endpoint
                        .send(FfmpegEndpointRequest::StopFfmpeg {
                            id: stream.ffmpeg_id.clone(),
                        });
                }

                FfmpegStatus::Inactive => (),
            }

            let _ = self
                .rtmp_server_endpoint
                .send(RtmpEndpointRequest::RemoveRegistration {
                    registration_type: RegistrationType::Watcher,
                    port: 1935,
                    rtmp_app: self.get_source_rtmp_app(),
                    rtmp_stream_key: StreamKeyRegistration::Exact(stream.id.0.clone()),
                });

            let _ = self
                .rtmp_server_endpoint
                .send(RtmpEndpointRequest::RemoveRegistration {
                    registration_type: RegistrationType::Publisher,
                    port: 1935,
                    rtmp_app: self.get_result_rtmp_app(),
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
                                "Watch registration successful for stream id {:?}", stream.id
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
                        "Received watch registration failed for stream id {:?}", stream.id
                    );
                    stream.rtmp_output_status = WatchRegistrationStatus::Inactive;
                }

                RtmpEndpointWatcherNotification::StreamKeyBecameActive { stream_key: _ } => (),
                RtmpEndpointWatcherNotification::StreamKeyBecameInactive { stream_key: _ } => (),

                RtmpEndpointWatcherNotification::WatcherRequiringApproval { .. } => {
                    error!("Watcher requires approval but all watchers should be auto-approved");
                    self.status = StepStatus::Error;
                }
            }
        }

        self.prepare_stream(stream_id, outputs);
    }

    fn handle_rtmp_publish_notification(
        &mut self,
        stream_id: StreamId,
        notification: RtmpEndpointPublisherMessage,
        outputs: &mut StepOutputs,
    ) {
        let mut prepare_stream = false;
        if let Some(stream) = self.active_streams.get_mut(&stream_id) {
            match notification {
                RtmpEndpointPublisherMessage::PublisherRegistrationFailed => {
                    warn!(
                        stream_id = ?stream_id,
                        "Rtmp publish registration failed for stream {:?}", stream_id
                    );
                    stream.rtmp_input_status = PublishRegistrationStatus::Inactive;
                    prepare_stream = true;
                }

                RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => {
                    info!(
                        stream_id = ?stream_id,
                        "Rtmp publish registration successful for stream {:?}", stream_id
                    );
                    stream.rtmp_input_status = PublishRegistrationStatus::Active;
                    prepare_stream = true;
                }

                RtmpEndpointPublisherMessage::NewPublisherConnected {
                    stream_id: _,
                    stream_key: _,
                    connection_id: _,
                } => (),
                RtmpEndpointPublisherMessage::PublishingStopped { connection_id: _ } => (),

                RtmpEndpointPublisherMessage::StreamMetadataChanged {
                    publisher: _,
                    metadata,
                } => {
                    let metadata = stream_metadata_to_hash_map(metadata);
                    outputs.media.push(MediaNotification {
                        stream_id: stream_id.clone(),
                        content: MediaNotificationContent::Metadata { data: metadata },
                    });
                }

                RtmpEndpointPublisherMessage::NewVideoData {
                    publisher: _,
                    codec,
                    data,
                    is_sequence_header,
                    is_keyframe,
                    timestamp,
                } => outputs.media.push(MediaNotification {
                    stream_id: stream_id.clone(),
                    content: MediaNotificationContent::Video {
                        codec,
                        timestamp: Duration::from_millis(timestamp.value as u64),
                        is_keyframe,
                        is_sequence_header,
                        data,
                    },
                }),

                RtmpEndpointPublisherMessage::NewAudioData {
                    publisher: _,
                    codec,
                    data,
                    is_sequence_header,
                    timestamp,
                } => outputs.media.push(MediaNotification {
                    stream_id: stream_id.clone(),
                    content: MediaNotificationContent::Audio {
                        codec,
                        timestamp: Duration::from_millis(timestamp.value as u64),
                        is_sequence_header,
                        data,
                    },
                }),

                RtmpEndpointPublisherMessage::PublisherRequiringApproval { .. } => {
                    error!("Publisher approval requested but publishers should be auto-approved");
                    self.status = StepStatus::Error;
                }
            }
        }

        if prepare_stream {
            self.prepare_stream(stream_id, outputs);
        }
    }

    fn handle_ffmpeg_notification(
        &mut self,
        stream_id: StreamId,
        notification: FfmpegEndpointNotification,
        outputs: &mut StepOutputs,
    ) {
        if let Some(stream) = self.active_streams.get_mut(&stream_id) {
            match notification {
                FfmpegEndpointNotification::FfmpegStarted => {
                    let new_status = match &stream.ffmpeg_status {
                        FfmpegStatus::Pending => {
                            info!(
                                stream_id = ?stream.id,
                                ffmpeg_id = ?stream.ffmpeg_id,
                                "Received notification that ffmpeg became active for stream id \
                                    {:?} with ffmpeg id {}", stream.id, stream.ffmpeg_id
                            );

                            Some(FfmpegStatus::Active)
                        }

                        status => {
                            error!(
                                stream_id = ?stream.id,
                                "Received notification that ffmpeg became active for stream id \
                                    {:?}, but this stream was in the {:?} status instead of pending", stream.id, status
                            );

                            None
                        }
                    };

                    if let Some(new_status) = new_status {
                        stream.ffmpeg_status = new_status;
                    }
                }

                FfmpegEndpointNotification::FfmpegStopped => {
                    info!(
                        stream_id = ?stream.id,
                        "Got ffmpeg stopped notification for stream {:?}", stream.id
                    );
                    stream.ffmpeg_status = FfmpegStatus::Inactive;
                }

                FfmpegEndpointNotification::FfmpegFailedToStart { cause } => {
                    warn!(
                        stream_id = ?stream.id,
                        "Ffmpeg failed to start for stream {:?}: {:?}", stream.id, cause
                    );
                    stream.ffmpeg_status = FfmpegStatus::Inactive;
                }
            }
        }

        self.prepare_stream(stream_id, outputs);
    }
}

impl WorkflowStep for FfmpegTranscoder {
    fn get_status(&self) -> &StepStatus {
        &self.status
    }

    fn get_definition(&self) -> &WorkflowStepDefinition {
        &self.definition
    }

    fn execute(&mut self, inputs: &mut StepInputs, outputs: &mut StepOutputs) {
        if self.status == StepStatus::Error {
            return;
        }

        for notification in inputs.notifications.drain(..) {
            self.handle_resolved_future(notification, outputs);
        }

        for media in inputs.media.drain(..) {
            self.handle_media(media, outputs);
        }
    }

    fn shutdown(&mut self) {
        let stream_ids = self.active_streams.drain().map(|x| x.0).collect::<Vec<_>>();
        for stream_id in stream_ids {
            self.stop_stream(&stream_id);
        }

        self.status = StepStatus::Shutdown;
    }
}

async fn notify_when_ffmpeg_endpoint_is_gone(
    endpoint: UnboundedSender<FfmpegEndpointRequest>,
) -> Box<dyn StepFutureResult> {
    endpoint.closed().await;

    Box::new(FutureResult::FfmpegEndpointGone)
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

async fn wait_for_publish_notification(
    stream_id: StreamId,
    mut receiver: UnboundedReceiver<RtmpEndpointPublisherMessage>,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(msg) => FutureResult::RtmpPublishNotificationReceived(stream_id, msg, receiver),
        None => FutureResult::RtmpPublishChannelGone(stream_id),
    };

    Box::new(result)
}

async fn wait_for_ffmpeg_notification(
    stream_id: StreamId,
    mut receiver: UnboundedReceiver<FfmpegEndpointNotification>,
) -> Box<dyn StepFutureResult> {
    let result = match receiver.recv().await {
        Some(msg) => FutureResult::FfmpegNotificationReceived(stream_id, msg, receiver),
        None => FutureResult::FfmpegChannelGone(stream_id),
    };

    Box::new(result)
}
