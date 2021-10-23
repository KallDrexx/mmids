use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use log::{info, error};
use std::collections::HashMap;
use std::path::Path;
use std::process::{Child, Command};
use std::time::Duration;
use futures::{FutureExt, StreamExt};
use thiserror::Error;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::sleep;
use uuid::Uuid;

pub enum FfmpegEndpointRequest {
    StartFfmpeg {
        id: Uuid,
        notification_channel: UnboundedSender<FfmpegEndpointNotification>,
        params: FfmpegParams,
    },

    StopFfmpeg {
        id: Uuid,
    },
}

pub enum FfmpegEndpointNotification {
    FfmpegStarted,
    FfmpegStopped,
    FfmpegFailedToStart { cause: FfmpegFailureCause },
}

#[derive(Debug)]
pub enum FfmpegFailureCause {
    LogFileCouldNotBeCreated(String),
    DuplicateId(Uuid),
    FfmpegFailedToStart,
}

#[derive(Error, Debug)]
pub enum FfmpegEndpointStartError {
    #[error("The ffmpeg executable '{0}' was not found")]
    FfmpegExecutableNotFound(String),

    #[error("Failed to create log directory")]
    LogDirectoryFailure(#[from] std::io::Error),
}

#[derive(Clone, Debug)]
pub enum H264Preset { UltraFast, SuperFast, VeryFast, Faster, Fast, Medium, Slow, Slower, VerySlow }

#[derive(Clone, Debug)]
pub enum VideoTranscodeParams { Copy, H264 { preset: H264Preset } }

#[derive(Clone, Debug)]
pub enum AudioTranscodeParams { Copy, Aac }

#[derive(Clone, Debug)]
pub enum TargetParams {
    Rtmp { url: String },
    Hls {
        path: String,
        segment_length: u16,
        max_entries: Option<u16>,
    },
}

#[derive(Clone, Debug)]
pub struct VideoScale {
    pub width: u16,
    pub height: u16,
}

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

pub fn start_ffmpeg_endpoint(ffmpeg_exe_path: String) -> Result<UnboundedSender<FfmpegEndpointRequest>, FfmpegEndpointStartError> {
    let actor = Actor::new(ffmpeg_exe_path)?;
    let (sender, receiver) = unbounded_channel();

    tokio::spawn(actor.run(receiver));

    Ok(sender)
}

enum FutureResult {
    AllConsumersGone,
    NotificationChannelGone(Uuid),
    RequestReceived(FfmpegEndpointRequest, UnboundedReceiver<FfmpegEndpointRequest>),
    CheckProcess(Uuid),
}

struct FfmpegProcess {
    handle: Child,
    notification_channel: UnboundedSender<FfmpegEndpointNotification>,
}

struct Actor<'a> {
    ffmpeg_exe_path: String,
    futures: FuturesUnordered<BoxFuture<'a, FutureResult>>,
    processes: HashMap<Uuid, FfmpegProcess>,
}

impl<'a> Actor<'a> {
    fn new(ffmpeg_exe_path: String) -> Result<Self, FfmpegEndpointStartError> {
        let path = Path::new(ffmpeg_exe_path.as_str());
        if !path.exists() || !path.is_file() {
            return Err(FfmpegEndpointStartError::FfmpegExecutableNotFound(ffmpeg_exe_path));
        }

        Ok(Actor {
            ffmpeg_exe_path,
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
                    self.handle_request(request);
                }
            }
        }

        info!("Ffmpeg endpoint closing");

        for (id, process) in self.processes.drain() {
            stop_process(id, process);
        }
    }

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
                    info!("Error attempting to get status for ffmpeg process {}: {}", id, e);
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
            let _ = process.notification_channel.send(FfmpegEndpointNotification::FfmpegStopped);
        }
    }

    fn handle_notification_channel_gone(&mut self, id: Uuid) {
        info!("Consumer for ffmpeg process {} is gone", id);
        if let Some(process) = self.processes.remove(&id) {
            stop_process(id, process);
        }
    }

    fn handle_request(&mut self, request: FfmpegEndpointRequest) {
        match request {
            FfmpegEndpointRequest::StopFfmpeg {id} => {
                if let Some(process) = self.processes.remove(&id) {
                    stop_process(id, process);
                }
            }

            FfmpegEndpointRequest::StartFfmpeg {id, params, notification_channel} => {
                if self.processes.contains_key(&id) {
                    let _ = notification_channel.send(FfmpegEndpointNotification::FfmpegFailedToStart {
                        cause: FfmpegFailureCause::DuplicateId(id),
                    });

                    return;
                }

                let handle = match self.start_ffmpeg(&id, &params) {
                    Ok(x) => x,
                    Err(e) => {
                        error!("Failed to start ffmpeg: {}", e);
                        let _ = notification_channel.send(FfmpegEndpointNotification::FfmpegFailedToStart {
                            cause: FfmpegFailureCause::FfmpegFailedToStart,
                        });

                        return;
                    }
                };

                self.futures.push(wait_for_next_check(id.clone()).boxed());
                let _ = notification_channel.send(FfmpegEndpointNotification::FfmpegStarted);
                self.processes.insert(id, FfmpegProcess {
                    handle,
                    notification_channel: notification_channel.clone(),
                });

                self.futures.push(wait_for_notification_channel_gone(id.clone(), notification_channel).boxed());
            }
        }
    }

    fn start_ffmpeg(&self, id: &Uuid, params: &FfmpegParams) -> Result<Child, std::io::Error> {
        let mut args = Vec::new();
        if params.read_in_real_time {
            args.push("-re".to_string());
        }

        args.push("-i".to_string());
        args.push(params.input.clone());

        args.push("-vcodec".to_string());
        match &params.video_transcode {
            VideoTranscodeParams::Copy => args.push("copy".to_string()),
            VideoTranscodeParams::H264 {preset} => {
                args.push("libx264".to_string());
                args.push("-preset".to_string());

                match preset {
                    H264Preset::UltraFast => args.push("ultrafast".to_string()),
                    H264Preset::SuperFast => args.push("superfast".to_string()),
                    H264Preset::VeryFast => args.push("veryfast".to_string()),
                    H264Preset::Faster  => args.push("faster".to_string()),
                    H264Preset::Fast => args.push("fast".to_string()),
                    H264Preset::Medium => args.push("medium".to_string()),
                    H264Preset::Slow => args.push("slow".to_string()),
                    H264Preset::Slower => args.push("slower".to_string()),
                    H264Preset::VerySlow => args.push("veryslow".to_string()),
                }
            }
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
            TargetParams::Rtmp {url} => {
                args.push("flv".to_string());
                args.push(url.to_string());
            },

            TargetParams::Hls {path, max_entries, segment_length} => {
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

        info!("Starting ffmpeg for id {} with the following arguments: {:?}", id, args);

        Command::new(&self.ffmpeg_exe_path)
            .args(args)
            .spawn()
    }
}

fn stop_process(id: Uuid, mut process: FfmpegProcess) {
    info!("Killing ffmpeg process {}", id);
    let _ = process.handle.kill();

    let _ = process.notification_channel.send(FfmpegEndpointNotification::FfmpegStopped);
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

async fn wait_for_notification_channel_gone(id: Uuid, channel: UnboundedSender<FfmpegEndpointNotification>) -> FutureResult {
    channel.closed().await;

    FutureResult::NotificationChannelGone(id)
}