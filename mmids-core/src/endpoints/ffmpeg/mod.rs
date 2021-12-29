//! Endpoint used to manage a local ffmpeg executable.  Workflow steps can request FFMPEG be run
//! with specific parameters, and the endpoint will run it.  If the ffmpeg process stops before
//! being requested to stop, then the endpoint will ensure it gets re-run.

use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time::sleep;
use tracing::{error, info, instrument};
use uuid::Uuid;

/// Requests of ffmpeg operations
#[derive(Debug)]
pub enum FfmpegEndpointRequest {
    /// Request that ffmpeg should be started with the specified parameters
    StartFfmpeg {
        /// A unique identifier to use for this ffmpeg operation.  Any further requests that should
        /// affect this ffmpeg operation should use this same identifier
        id: Uuid,

        /// The channel that the endpoint will send notifications on to notify the requester of
        /// changes in the ffmpeg operation.
        notification_channel: UnboundedSender<FfmpegEndpointNotification>,

        /// What parameters ffmpeg should be run with
        params: FfmpegParams,
    },

    /// Requests that the specified ffmpeg operation should be stopped
    StopFfmpeg {
        /// The identifier of the existing ffmpeg operation
        id: Uuid,
    },
}

/// Notifications of what's happening with an ffmpeg operation
#[derive(Debug)]
pub enum FfmpegEndpointNotification {
    FfmpegStarted,
    FfmpegStopped,
    FfmpegFailedToStart { cause: FfmpegFailureCause },
}

/// Reasons that ffmpeg may fail to start
#[derive(Debug)]
pub enum FfmpegFailureCause {
    /// The log file for ffmpeg's standard output could not be created
    LogFileCouldNotBeCreated(String, std::io::Error),

    /// ffmpeg was requested to be started with an identifier that matches an ffmpeg operation
    /// that's already being run.
    DuplicateId(Uuid),

    /// The ffmpeg process failed to start due to an issue with the executable itself
    FfmpegFailedToStart,
}

/// Error that occurs when starting the ffmpeg endpoint
#[derive(Error, Debug)]
pub enum FfmpegEndpointStartError {
    #[error("The ffmpeg executable '{0}' was not found")]
    FfmpegExecutableNotFound(String),

    #[error("Failed to create log directory")]
    LogDirectoryCreationFailure,

    #[error("The log directory '{0}' is an invalid path")]
    LogDirectoryInvalidPath(String),
}

/// H264 presets
#[derive(Clone, Debug)]
pub enum H264Preset {
    UltraFast,
    SuperFast,
    VeryFast,
    Faster,
    Fast,
    Medium,
    Slow,
    Slower,
    VerySlow,
}

/// Video transcode instructions
#[derive(Clone, Debug)]
pub enum VideoTranscodeParams {
    Copy,
    H264 { preset: H264Preset },
}

/// Audio transcode instructions
#[derive(Clone, Debug)]
pub enum AudioTranscodeParams {
    Copy,
    Aac,
}

/// Where should ffmpeg send the media
#[derive(Clone, Debug)]
pub enum TargetParams {
    /// Send the media stream to an RTMP server
    Rtmp { url: String },

    /// Save the media stream as an HLS playlist
    Hls {
        /// The directory the playlist should be saved to.
        path: String,

        /// How long (in seconds) should each segment be
        segment_length: u16,

        /// The maximum number of segments that should be in the playlist.  If none is specified
        /// than ffmpeg's default will be used
        max_entries: Option<u16>,
    },
}

/// The dimensions video should be scaled to
#[derive(Clone, Debug)]
pub struct VideoScale {
    pub width: u16,
    pub height: u16,
}

/// Parameters to pass to the ffmpeg process
#[derive(Clone, Debug)]
pub struct FfmpegParams {
    pub read_in_real_time: bool,
    pub input: String,
    pub video_transcode: VideoTranscodeParams,
    pub scale: Option<VideoScale>,
    pub audio_transcode: AudioTranscodeParams,
    pub bitrate_in_kbps: Option<u16>,
    pub target: TargetParams,
}

/// Starts a new ffmpeg endpoint, and returns the channel in which the newly created endpoint
/// can be communicated with
pub fn start_ffmpeg_endpoint(
    ffmpeg_exe_path: String,
    log_root: String,
) -> Result<UnboundedSender<FfmpegEndpointRequest>, FfmpegEndpointStartError> {
    let actor = Actor::new(ffmpeg_exe_path, log_root)?;
    let (sender, receiver) = unbounded_channel();

    tokio::spawn(actor.run(receiver));

    Ok(sender)
}

enum FutureResult {
    AllConsumersGone,
    NotificationChannelGone(Uuid),
    RequestReceived(
        FfmpegEndpointRequest,
        UnboundedReceiver<FfmpegEndpointRequest>,
    ),
    CheckProcess(Uuid),
}

struct FfmpegProcess {
    handle: Child,
    notification_channel: UnboundedSender<FfmpegEndpointNotification>,
}

struct Actor {
    ffmpeg_exe_path: String,
    log_path: PathBuf,
    futures: FuturesUnordered<BoxFuture<'static, FutureResult>>,
    processes: HashMap<Uuid, FfmpegProcess>,
}

impl Actor {
    fn new(ffmpeg_exe_path: String, log_root: String) -> Result<Self, FfmpegEndpointStartError> {
        let path = Path::new(ffmpeg_exe_path.as_str());
        if !path.is_file() {
            return Err(FfmpegEndpointStartError::FfmpegExecutableNotFound(
                ffmpeg_exe_path,
            ));
        }

        let mut path = PathBuf::from(log_root.as_str());
        if path.is_file() {
            // We expected the path to be a new or existing directory, not a file
            return Err(FfmpegEndpointStartError::LogDirectoryInvalidPath(log_root));
        }

        path.push("ffmpeg_stdout");
        if !path.exists() {
            if let Err(error) = std::fs::create_dir_all(&path) {
                error!(
                    "Could not create log directory '{}': {:?}",
                    path.display().to_string(),
                    error
                );
                return Err(FfmpegEndpointStartError::LogDirectoryCreationFailure);
            }
        }

        Ok(Actor {
            ffmpeg_exe_path,
            log_path: path,
            processes: HashMap::new(),
            futures: FuturesUnordered::new(),
        })
    }

    async fn run(mut self, receiver: UnboundedReceiver<FfmpegEndpointRequest>) {
        self.futures.push(wait_for_request(receiver).boxed());

        info!("Ffmpeg endpoint started");
        info!("Ffmpeg path: {}", self.ffmpeg_exe_path);
        while let Some(result) = self.futures.next().await {
            match result {
                FutureResult::AllConsumersGone => {
                    info!("All consumers gone");
                    break;
                }

                FutureResult::NotificationChannelGone(id) => {
                    self.handle_notification_channel_gone(id);
                }

                FutureResult::CheckProcess(id) => {
                    self.check_status(id);
                }

                FutureResult::RequestReceived(request, receiver) => {
                    self.futures.push(wait_for_request(receiver).boxed());
                    self.handle_request(request).await;
                }
            }
        }

        info!("Ffmpeg endpoint closing");

        for (id, process) in self.processes.drain() {
            stop_process(id, process);
        }
    }

    #[instrument(skip(self, id), fields(ffmpeg_id = ?id))]
    fn check_status(&mut self, id: Uuid) {
        let mut has_exited = false;
        if let Some(process) = self.processes.get_mut(&id) {
            has_exited = match process.handle.try_wait() {
                Ok(None) => false, // still running
                Ok(Some(status)) => {
                    info!("Ffmpeg process {} exited with status {}", id, status);
                    true
                }

                Err(e) => {
                    info!(
                        "Error attempting to get status for ffmpeg process {}: {}",
                        id, e
                    );
                    let _ = process.handle.kill();
                    true
                }
            };

            if !has_exited {
                self.futures.push(wait_for_next_check(id).boxed());
            }
        }

        if has_exited {
            let process = self.processes.remove(&id).unwrap();
            let _ = process
                .notification_channel
                .send(FfmpegEndpointNotification::FfmpegStopped);
        }
    }

    fn handle_notification_channel_gone(&mut self, id: Uuid) {
        info!(id = ?id, "Consumer for ffmpeg process {} is gone", id);
        if let Some(process) = self.processes.remove(&id) {
            stop_process(id, process);
        }
    }

    async fn handle_request(&mut self, request: FfmpegEndpointRequest) {
        match request {
            FfmpegEndpointRequest::StopFfmpeg { id } => {
                if let Some(process) = self.processes.remove(&id) {
                    stop_process(id, process);
                }
            }

            FfmpegEndpointRequest::StartFfmpeg {
                id,
                params,
                notification_channel,
            } => {
                if self.processes.contains_key(&id) {
                    let _ = notification_channel.send(
                        FfmpegEndpointNotification::FfmpegFailedToStart {
                            cause: FfmpegFailureCause::DuplicateId(id),
                        },
                    );

                    return;
                }

                let log_file_name = format!("{}.log", id.to_string());
                let log_path = self.log_path.as_path().join(log_file_name.as_str());
                let log_file_result = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(log_path)
                    .await;

                let mut log_file = match log_file_result {
                    Ok(x) => x,
                    Err(e) => {
                        error!("Failed to create ffmpeg log file '{}'", log_file_name);
                        let _ = notification_channel.send(
                            FfmpegEndpointNotification::FfmpegFailedToStart {
                                cause: FfmpegFailureCause::LogFileCouldNotBeCreated(
                                    log_file_name.to_string(),
                                    e,
                                ),
                            },
                        );

                        return;
                    }
                };

                // Add a separator so we have a clear boundary when appending to an existing log file.
                // We will append if we re-use the same ffmpeg id multiple times.  This is usually done
                // to keep the logs from a restarting ffmpeg instance together.
                let _ = log_file
                    .write(b"\n\n------------------New Execution----------------\n\n")
                    .await;

                let handle = match self.start_ffmpeg(&id, &params, log_file) {
                    Ok(x) => x,
                    Err(e) => {
                        error!("Failed to start ffmpeg: {}", e);
                        let _ = notification_channel.send(
                            FfmpegEndpointNotification::FfmpegFailedToStart {
                                cause: FfmpegFailureCause::FfmpegFailedToStart,
                            },
                        );

                        return;
                    }
                };

                self.futures.push(wait_for_next_check(id.clone()).boxed());
                let _ = notification_channel.send(FfmpegEndpointNotification::FfmpegStarted);
                self.processes.insert(
                    id,
                    FfmpegProcess {
                        handle,
                        notification_channel: notification_channel.clone(),
                    },
                );

                self.futures.push(
                    wait_for_notification_channel_gone(id.clone(), notification_channel).boxed(),
                );
            }
        }
    }

    fn start_ffmpeg(
        &self,
        id: &Uuid,
        params: &FfmpegParams,
        mut log_file: File,
    ) -> Result<Child, std::io::Error> {
        let mut args = Vec::new();
        if params.read_in_real_time {
            args.push("-re".to_string());
        }

        args.push("-i".to_string());
        args.push(params.input.clone());

        args.push("-vcodec".to_string());
        match &params.video_transcode {
            VideoTranscodeParams::Copy => args.push("copy".to_string()),
            VideoTranscodeParams::H264 { preset } => {
                args.push("libx264".to_string());
                args.push("-preset".to_string());

                match preset {
                    H264Preset::UltraFast => args.push("ultrafast".to_string()),
                    H264Preset::SuperFast => args.push("superfast".to_string()),
                    H264Preset::VeryFast => args.push("veryfast".to_string()),
                    H264Preset::Faster => args.push("faster".to_string()),
                    H264Preset::Fast => args.push("fast".to_string()),
                    H264Preset::Medium => args.push("medium".to_string()),
                    H264Preset::Slow => args.push("slow".to_string()),
                    H264Preset::Slower => args.push("slower".to_string()),
                    H264Preset::VerySlow => args.push("veryslow".to_string()),
                }
            }
        }

        if let Some(bitrate) = &params.bitrate_in_kbps {
            let rate = format!("{}K", bitrate);
            args.push("-b:v".to_string());
            args.push(rate.clone());

            args.push("-minrate".to_string());
            args.push(rate.clone());

            args.push("-maxrate".to_string());
            args.push(rate.clone());
        }

        if let Some(scale) = &params.scale {
            args.push("-vf".to_string());
            args.push(format!("scale={}:{}", scale.width, scale.height));
        }

        args.push("-acodec".to_string());
        match &params.audio_transcode {
            AudioTranscodeParams::Copy => args.push("copy".to_string()),
            AudioTranscodeParams::Aac => args.push("aac".to_string()),
        }

        args.push("-f".to_string());
        match &params.target {
            TargetParams::Rtmp { url } => {
                args.push("flv".to_string());
                args.push(url.to_string());
            }

            TargetParams::Hls {
                path,
                max_entries,
                segment_length,
            } => {
                args.push("hls".to_string());

                args.push("-hls_time".to_string());
                args.push(segment_length.to_string());

                if let Some(entries) = max_entries {
                    args.push("-hls_list_size".to_string());
                    args.push(entries.to_string());
                }

                args.push(path.clone());
            }
        }

        args.push("-y".to_string()); // always overwrite
        args.push("-nostats".to_string());

        info!(
            ffmpeg_id = ?id,
            "Starting ffmpeg for id {} with the following arguments: {:?}",
            id, args
        );

        let mut command = Command::new(&self.ffmpeg_exe_path)
            .args(args)
            .stderr(Stdio::piped()) // ffmpeg seems to write output to stderr
            .spawn()?;

        if let Some(stderr) = command.stderr.take() {
            if let Ok(mut stdout) = tokio::process::ChildStderr::from_std(stderr) {
                tokio::spawn(async move {
                    let _ = tokio::io::copy(&mut stdout, &mut log_file).await;
                });
            }
        }

        Ok(command)
    }
}

fn stop_process(id: Uuid, mut process: FfmpegProcess) {
    info!(id = ?id, "Killing ffmpeg process {}", id);
    let _ = process.handle.kill();

    let _ = process
        .notification_channel
        .send(FfmpegEndpointNotification::FfmpegStopped);
}

async fn wait_for_request(mut receiver: UnboundedReceiver<FfmpegEndpointRequest>) -> FutureResult {
    match receiver.recv().await {
        Some(x) => FutureResult::RequestReceived(x, receiver),
        None => FutureResult::AllConsumersGone,
    }
}

async fn wait_for_next_check(id: Uuid) -> FutureResult {
    sleep(Duration::from_secs(5)).await;

    FutureResult::CheckProcess(id)
}

async fn wait_for_notification_channel_gone(
    id: Uuid,
    channel: UnboundedSender<FfmpegEndpointNotification>,
) -> FutureResult {
    channel.closed().await;

    FutureResult::NotificationChannelGone(id)
}
