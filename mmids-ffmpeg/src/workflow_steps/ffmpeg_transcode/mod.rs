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

#[cfg(test)]
mod tests;

use crate::endpoint::{
    AudioTranscodeParams, FfmpegEndpointNotification, FfmpegEndpointRequest, FfmpegParams,
    H264Preset, TargetParams, VideoScale, VideoTranscodeParams,
};
use bytes::BytesMut;
use mmids_core::codecs::{AUDIO_CODEC_AAC_RAW, VIDEO_CODEC_H264_AVC};
use mmids_core::workflows::definitions::WorkflowStepDefinition;
use mmids_core::workflows::metadata::{
    MediaPayloadMetadataCollection, MetadataEntry, MetadataKey, MetadataValue,
};
use mmids_core::workflows::steps::factory::StepGenerator;
use mmids_core::workflows::steps::futures_channel::WorkflowStepFuturesChannel;
use mmids_core::workflows::steps::{
    StepCreationResult, StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use mmids_core::workflows::{MediaNotification, MediaNotificationContent, MediaType};
use mmids_core::StreamId;
use mmids_rtmp::rtmp_server::{
    IpRestriction, RegistrationType, RtmpEndpointMediaData, RtmpEndpointMediaMessage,
    RtmpEndpointPublisherMessage, RtmpEndpointRequest, RtmpEndpointWatcherNotification,
    StreamKeyRegistration,
};
use mmids_rtmp::utils::stream_metadata_to_hash_map;
use std::collections::{HashMap, VecDeque};
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tracing::{error, info, warn};
use uuid::Uuid;

const VIDEO_CODEC_NAME: &str = "vcodec";
const AUDIO_CODEC_NAME: &str = "acodec";
const H264_PRESET_NAME: &str = "h264_preset";
const SIZE_NAME: &str = "size";
const BITRATE_NAME: &str = "kbps";

/// Generates new ffmpeg transcoding step instances based on specified step definitions.
pub struct FfmpegTranscoderStepGenerator {
    rtmp_server_endpoint: UnboundedSender<RtmpEndpointRequest>,
    ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
    is_keyframe_metadata_key: MetadataKey,
    pts_offset_metadata_key: MetadataKey,
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
    metadata_buffer: BytesMut,
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
    stream_name: Arc<String>,
    pending_media: VecDeque<MediaNotificationContent>,
    rtmp_output_status: WatchRegistrationStatus,
    rtmp_input_status: PublishRegistrationStatus,
    ffmpeg_status: FfmpegStatus,
    ffmpeg_id: Uuid,
}

enum FutureResult {
    RtmpEndpointGone,
    FfmpegEndpointGone,
    RtmpWatchNotificationReceived(StreamId, RtmpEndpointWatcherNotification),
    RtmpWatchChannelGone(StreamId),
    RtmpPublishNotificationReceived(StreamId, RtmpEndpointPublisherMessage),
    RtmpPublishChannelGone(StreamId),
    FfmpegNotificationReceived(StreamId, FfmpegEndpointNotification),
    FfmpegChannelGone(StreamId),
}

impl StepFutureResult for FutureResult {}

#[derive(Error, Debug)]
enum StepStartupError {
    #[error("Invalid video codec specified ({0}).  {} is a required field and valid values are: 'copy' and 'h264'", VIDEO_CODEC_NAME)]
    InvalidVideoCodec(String),

    #[error("Invalid audio codec specified ({0}).  {} is a required field and valid values are: 'copy' and 'aac'", AUDIO_CODEC_NAME)]
    InvalidAudioCodec(String),

    #[error("Invalid h264 preset specified ({0}).  {} is the name of any h264 profile (e.g. veryfast, medium, etc...)", H264_PRESET_NAME)]
    InvalidH264Preset(String),

    #[error(
        "Invalid video size specified ({0}).  {} must be in the format of '<width>x<height>'",
        SIZE_NAME
    )]
    InvalidVideoSize(String),

    #[error("Invalid bitrate specified ({0}).  {} must be a number", BITRATE_NAME)]
    InvalidBitrate(String),
}

impl FfmpegTranscoderStepGenerator {
    pub fn new(
        rtmp_endpoint: UnboundedSender<RtmpEndpointRequest>,
        ffmpeg_endpoint: UnboundedSender<FfmpegEndpointRequest>,
        is_keyframe_metadata_key: MetadataKey,
        pts_offset_metadata_key: MetadataKey,
    ) -> Self {
        FfmpegTranscoderStepGenerator {
            rtmp_server_endpoint: rtmp_endpoint,
            ffmpeg_endpoint,
            is_keyframe_metadata_key,
            pts_offset_metadata_key,
        }
    }
}

impl StepGenerator for FfmpegTranscoderStepGenerator {
    fn generate(
        &self,
        definition: WorkflowStepDefinition,
        futures_channel: WorkflowStepFuturesChannel,
    ) -> StepCreationResult {
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
                            return Err(Box::new(StepStartupError::InvalidH264Preset(
                                x.to_string(),
                            )))
                        }
                    },
                    _ => VideoTranscodeParams::H264 {
                        preset: H264Preset::VeryFast,
                    },
                },
                x => return Err(Box::new(StepStartupError::InvalidVideoCodec(x.to_string()))),
            },

            _ => {
                return Err(Box::new(StepStartupError::InvalidVideoCodec(
                    "".to_string(),
                )))
            }
        };

        let acodec = match definition.parameters.get(AUDIO_CODEC_NAME) {
            Some(Some(value)) => match value.to_lowercase().trim() {
                "copy" => AudioTranscodeParams::Copy,
                "aac" => AudioTranscodeParams::Aac,
                x => return Err(Box::new(StepStartupError::InvalidAudioCodec(x.to_string()))),
            },

            _ => {
                return Err(Box::new(StepStartupError::InvalidAudioCodec(
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
                            return Err(Box::new(StepStartupError::InvalidVideoSize(value.clone())))
                        }
                    }
                }

                if dimensions.len() != 2 {
                    return Err(Box::new(StepStartupError::InvalidVideoSize(value.clone())));
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
                    return Err(Box::new(StepStartupError::InvalidBitrate(value.clone())));
                }
            }

            _ => None,
        };

        let step = FfmpegTranscoder {
            definition,
            active_streams: HashMap::new(),
            audio_codec_params: acodec,
            rtmp_server_endpoint: self.rtmp_server_endpoint.clone(),
            ffmpeg_endpoint: self.ffmpeg_endpoint.clone(),
            video_scale_params: size,
            video_codec_params: vcodec,
            bitrate,
            status: StepStatus::Active,
            metadata_buffer: BytesMut::new(),
            is_keyframe_metadata_key: self.is_keyframe_metadata_key,
            pts_offset_metadata_key: self.pts_offset_metadata_key,
        };

        let ffmpeg_endpoint = self.ffmpeg_endpoint.clone();
        futures_channel.send_on_generic_future_completion(async move {
            ffmpeg_endpoint.closed().await;
            FutureResult::FfmpegEndpointGone
        });

        let rtmp_endpoint = self.rtmp_server_endpoint.clone();
        futures_channel.send_on_generic_future_completion(async move {
            rtmp_endpoint.closed().await;
            FutureResult::RtmpEndpointGone
        });

        let status = step.status.clone();
        Ok((Box::new(step), status))
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
        futures_channel: &WorkflowStepFuturesChannel,
    ) {
        let notification = match notification.downcast::<FutureResult>() {
            Ok(x) => *x,
            Err(_) => return,
        };

        match notification {
            FutureResult::FfmpegEndpointGone => {
                error!("Ffmpeg endpoint is gone!");
                self.status = StepStatus::Error {
                    message: "Ffmpeg endpoint is gone".to_string(),
                };

                let ids: Vec<StreamId> = self.active_streams.keys().cloned().collect();
                for id in ids {
                    self.stop_stream(&id);
                }
            }

            FutureResult::RtmpEndpointGone => {
                error!("RTMP endpoint is gone!");
                self.status = StepStatus::Error {
                    message: "Rtmp endpoint is gone".to_string(),
                };

                let ids: Vec<StreamId> = self.active_streams.keys().cloned().collect();
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

            FutureResult::RtmpWatchNotificationReceived(stream_id, notification) => {
                if !self.active_streams.contains_key(&stream_id) {
                    // late notification after stopping a stream
                    return;
                }

                self.handle_rtmp_watch_notification(stream_id, notification, futures_channel);
            }

            FutureResult::RtmpPublishNotificationReceived(stream_id, notification) => {
                if !self.active_streams.contains_key(&stream_id) {
                    // late notification after stopping a stream
                    return;
                }

                self.handle_rtmp_publish_notification(
                    stream_id,
                    notification,
                    outputs,
                    futures_channel,
                );
            }

            FutureResult::FfmpegNotificationReceived(stream_id, notification) => {
                if !self.active_streams.contains_key(&stream_id) {
                    // late notification after stopping a stream
                    return;
                }

                self.handle_ffmpeg_notification(stream_id, notification, futures_channel);
            }
        }
    }

    fn handle_media(
        &mut self,
        media: MediaNotification,
        outputs: &mut StepOutputs,
        futures_channel: &WorkflowStepFuturesChannel,
    ) {
        match &media.content {
            MediaNotificationContent::NewIncomingStream { stream_name } => {
                if let Some(stream) = self.active_streams.get(&media.stream_id) {
                    if stream.stream_name != *stream_name {
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
                self.prepare_stream(media.stream_id.clone(), futures_channel);

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
    }

    fn prepare_stream(
        &mut self,
        stream_id: StreamId,
        futures_channel: &WorkflowStepFuturesChannel,
    ) {
        let source_rtmp_app = Arc::new(self.get_source_rtmp_app());
        let result_rtmp_app = Arc::new(self.get_result_rtmp_app());

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

                    let recv_stream_id = stream.id.clone();
                    let closed_stream_id = stream.id.clone();
                    futures_channel.send_on_generic_unbounded_recv(
                        watch_receiver,
                        move |message| {
                            FutureResult::RtmpWatchNotificationReceived(
                                recv_stream_id.clone(),
                                message,
                            )
                        },
                        move || FutureResult::RtmpWatchChannelGone(closed_stream_id),
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

                    let recv_stream_id = stream.id.clone();
                    let closed_stream_id = stream.id.clone();
                    futures_channel.send_on_generic_unbounded_recv(
                        receiver,
                        move |message| {
                            FutureResult::RtmpPublishNotificationReceived(
                                recv_stream_id.clone(),
                                message,
                            )
                        },
                        move || FutureResult::RtmpPublishChannelGone(closed_stream_id),
                    );

                    stream.rtmp_input_status = PublishRegistrationStatus::Pending;

                    false
                }

                PublishRegistrationStatus::Pending => false,
                PublishRegistrationStatus::Active => true,
            };

            if let FfmpegStatus::Inactive = &stream.ffmpeg_status {
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
                            url: format!("rtmp://localhost/{}/{}", result_rtmp_app, stream.id.0),
                        },
                    };

                    let (sender, receiver) = unbounded_channel();
                    let _ = self
                        .ffmpeg_endpoint
                        .send(FfmpegEndpointRequest::StartFfmpeg {
                            id: stream.ffmpeg_id,
                            params: parameters,
                            notification_channel: sender,
                        });

                    let recv_stream_id = stream.id.clone();
                    let closed_stream_id = stream.id.clone();
                    futures_channel.send_on_generic_unbounded_recv(
                        receiver,
                        move |message| {
                            FutureResult::FfmpegNotificationReceived(
                                recv_stream_id.clone(),
                                message,
                            )
                        },
                        || FutureResult::FfmpegChannelGone(closed_stream_id),
                    );

                    stream.ffmpeg_status = FfmpegStatus::Pending;
                }
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
                            id: stream.ffmpeg_id,
                        });
                }

                FfmpegStatus::Active => {
                    let _ = self
                        .ffmpeg_endpoint
                        .send(FfmpegEndpointRequest::StopFfmpeg {
                            id: stream.ffmpeg_id,
                        });
                }

                FfmpegStatus::Inactive => (),
            }

            let _ = self
                .rtmp_server_endpoint
                .send(RtmpEndpointRequest::RemoveRegistration {
                    registration_type: RegistrationType::Watcher,
                    port: 1935,
                    rtmp_app: Arc::new(self.get_source_rtmp_app()),
                    rtmp_stream_key: StreamKeyRegistration::Exact(stream.id.0.clone()),
                });

            let _ = self
                .rtmp_server_endpoint
                .send(RtmpEndpointRequest::RemoveRegistration {
                    registration_type: RegistrationType::Publisher,
                    port: 1935,
                    rtmp_app: Arc::new(self.get_result_rtmp_app()),
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

                RtmpEndpointWatcherNotification::StreamKeyBecameActive {
                    stream_key: _,
                    reactor_update_channel: _,
                } => (),

                RtmpEndpointWatcherNotification::StreamKeyBecameInactive { stream_key: _ } => (),

                RtmpEndpointWatcherNotification::WatcherRequiringApproval { .. } => {
                    error!("Watcher requires approval but all watchers should be auto-approved");
                    self.status = StepStatus::Error {
                        message:
                            "Watcher requires approval but all watchers should be auto-approved"
                                .to_string(),
                    };
                }
            }
        }

        self.prepare_stream(stream_id, futures_channel);
    }

    fn handle_rtmp_publish_notification(
        &mut self,
        stream_id: StreamId,
        notification: RtmpEndpointPublisherMessage,
        outputs: &mut StepOutputs,
        futures_channel: &WorkflowStepFuturesChannel,
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
                    reactor_update_channel: _,
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
                    data,
                    is_sequence_header,
                    is_keyframe,
                    timestamp,
                    composition_time_offset,
                } => {
                    let is_keyframe_metadata = MetadataEntry::new(
                        self.is_keyframe_metadata_key,
                        MetadataValue::Bool(is_keyframe),
                        &mut self.metadata_buffer,
                    )
                    .unwrap(); // Only fails from type mismatch

                    let pts_offset_metadata = MetadataEntry::new(
                        self.pts_offset_metadata_key,
                        MetadataValue::I32(composition_time_offset),
                        &mut self.metadata_buffer,
                    )
                    .unwrap(); // Only fails from type mismatch

                    let metadata = MediaPayloadMetadataCollection::new(
                        [is_keyframe_metadata, pts_offset_metadata].into_iter(),
                        &mut self.metadata_buffer,
                    );

                    outputs.media.push(MediaNotification {
                        stream_id: stream_id.clone(),
                        content: MediaNotificationContent::MediaPayload {
                            media_type: MediaType::Video,
                            payload_type: VIDEO_CODEC_H264_AVC.clone(),
                            timestamp: Duration::from_millis(timestamp.value as u64),
                            is_required_for_decoding: is_sequence_header,
                            data,
                            metadata,
                        },
                    })
                }

                RtmpEndpointPublisherMessage::NewAudioData {
                    publisher: _,
                    data,
                    is_sequence_header,
                    timestamp,
                } => outputs.media.push(MediaNotification {
                    stream_id: stream_id.clone(),
                    content: MediaNotificationContent::MediaPayload {
                        timestamp: Duration::from_millis(timestamp.value as u64),
                        is_required_for_decoding: is_sequence_header,
                        data,
                        media_type: MediaType::Audio,
                        payload_type: AUDIO_CODEC_AAC_RAW.clone(),
                        metadata: MediaPayloadMetadataCollection::new(
                            iter::empty(),
                            &mut self.metadata_buffer,
                        ),
                    },
                }),

                RtmpEndpointPublisherMessage::PublisherRequiringApproval { .. } => {
                    error!("Publisher approval requested but publishers should be auto-approved");
                    self.status = StepStatus::Error {
                        message:
                            "Publisher approval requested but publishers should be auto-approved"
                                .to_string(),
                    };
                }
            }
        }

        if prepare_stream {
            self.prepare_stream(stream_id, futures_channel);
        }
    }

    fn handle_ffmpeg_notification(
        &mut self,
        stream_id: StreamId,
        notification: FfmpegEndpointNotification,
        futures_channel: &WorkflowStepFuturesChannel,
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

        self.prepare_stream(stream_id, futures_channel);
    }
}

impl WorkflowStep for FfmpegTranscoder {
    fn execute(
        &mut self,
        inputs: &mut StepInputs,
        outputs: &mut StepOutputs,
        futures_channel: WorkflowStepFuturesChannel,
    ) -> StepStatus {
        for notification in inputs.notifications.drain(..) {
            self.handle_resolved_future(notification, outputs, &futures_channel);
        }

        for media in inputs.media.drain(..) {
            self.handle_media(media, outputs, &futures_channel);
        }

        self.status.clone()
    }
}

impl Drop for FfmpegTranscoder {
    fn drop(&mut self) {
        let stream_ids = self.active_streams.drain().map(|x| x.0).collect::<Vec<_>>();
        for stream_id in stream_ids {
            self.stop_stream(&stream_id);
        }
    }
}
