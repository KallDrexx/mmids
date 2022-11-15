use log::info;
use mmids_ffmpeg::endpoint::{
    start_ffmpeg_endpoint, AudioTranscodeParams, FfmpegEndpointNotification, FfmpegEndpointRequest,
    FfmpegParams, H264Preset, TargetParams, VideoScale, VideoTranscodeParams,
};
use tokio::sync::mpsc::unbounded_channel;
use uuid::Uuid;

#[tokio::main()]
pub async fn main() {
    env_logger::init();

    let endpoint = match start_ffmpeg_endpoint(
        "c:\\users\\me\\tools\\ffmpeg\\bin\\ffmpeg.exe".to_string(),
        "c:\\temp".to_string(),
    ) {
        Ok(x) => x,
        Err(e) => panic!("Error starting ffmpeg: {:?}", e),
    };

    info!("Ffmpeg runner started");

    let (notification_sender, mut notification_receiver) = unbounded_channel();
    let _ = endpoint.send(FfmpegEndpointRequest::StartFfmpeg {
        id: Uuid::new_v4(),
        params: hls_test(),
        notification_channel: notification_sender,
    });

    match notification_receiver.recv().await {
        None => panic!("ffmpeg endpoint is dead"),
        Some(FfmpegEndpointNotification::FfmpegStopped) => panic!("Premature ffmpeg stop received"),
        Some(FfmpegEndpointNotification::FfmpegFailedToStart { cause }) => {
            panic!("Ffmpeg failed to start: {:?}", cause);
        }

        Some(FfmpegEndpointNotification::FfmpegStarted) => {
            info!("Ffmpeg started as expected")
        }
    }

    // wait for it to stop
    match notification_receiver.recv().await {
        None => panic!("ffmpeg endpoint died"),
        Some(FfmpegEndpointNotification::FfmpegStarted) => {
            panic!("Unexpected started notification received")
        }
        Some(FfmpegEndpointNotification::FfmpegFailedToStart { cause: _ }) => {
            panic!("Unexpected start failure received")
        }
        Some(FfmpegEndpointNotification::FfmpegStopped) => {
            info!("Received expected stopped notification");
        }
    }
}

fn hls_test() -> FfmpegParams {
    FfmpegParams {
        read_in_real_time: false,
        input: "C:\\users\\me\\Documents\\bbb.flv".to_string(),
        video_transcode: VideoTranscodeParams::H264 {
            preset: H264Preset::UltraFast,
        },
        audio_transcode: AudioTranscodeParams::Aac,
        scale: Some(VideoScale {
            width: 640,
            height: 480,
        }),
        bitrate_in_kbps: Some(3000),
        target: TargetParams::Hls {
            path: "c:\\temp\\test\\hlstest.m3u8".to_string(),
            max_entries: None,
            segment_length: 2,
        },
    }
}
