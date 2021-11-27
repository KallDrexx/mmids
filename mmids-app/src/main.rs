use mmids_core::config::{parse as parse_config_file, MmidsConfig};
use mmids_core::endpoints::ffmpeg::start_ffmpeg_endpoint;
use mmids_core::endpoints::rtmp_server::start_rtmp_server_endpoint;
use mmids_core::net::tcp::{start_socket_manager, TlsOptions};
use mmids_core::workflows::definitions::WorkflowStepType;
use mmids_core::workflows::manager::{start_workflow_manager, WorkflowManagerRequest};
use mmids_core::workflows::steps::factory::WorkflowStepFactory;
use mmids_core::workflows::steps::ffmpeg_hls::FfmpegHlsStepGenerator;
use mmids_core::workflows::steps::ffmpeg_pull::FfmpegPullStepGenerator;
use mmids_core::workflows::steps::ffmpeg_rtmp_push::FfmpegRtmpPushStepGenerator;
use mmids_core::workflows::steps::ffmpeg_transcode::FfmpegTranscoderStepGenerator;
use mmids_core::workflows::steps::rtmp_receive::RtmpReceiverStepGenerator;
use mmids_core::workflows::steps::rtmp_watch::RtmpWatchStepGenerator;
use native_tls::Identity;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::sleep;
use tracing::info;

const RTMP_RECEIVE: &str = "rtmp_receive";
const RTMP_WATCH: &str = "rtmp_watch";
const FFMPEG_TRANSCODE: &str = "ffmpeg_transcode";
const FFMPEG_HLS: &str = "ffmpeg_hls";
const FFMPEG_PUSH: &str = "ffmpeg_push";
const FFMPEG_PULL: &str = "ffmpeg_pull";

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let config = read_config();
    let _manager = init(config).await;

    loop {
        sleep(Duration::from_millis(100)).await;
    }
}

fn read_config() -> MmidsConfig {
    let contents = std::fs::read_to_string("mmids.config").expect("Failed to read 'mmids.config'");

    return parse_config_file(contents.as_str()).expect("Failed to parse config file");
}

async fn init(config: MmidsConfig) -> UnboundedSender<WorkflowManagerRequest> {
    info!("Starting all endpoints");

    let cert_path = match config.settings.get("tls_cert_path") {
        Some(x) => x.clone(),
        None => None,
    };

    let cert_password = match config.settings.get("tls_cert_password") {
        Some(x) => x.clone(),
        None => None,
    };

    let tls_options = if let Some(path) = cert_path {
        if let Some(password) = cert_password {
            Some(load_tls_options(path, password).await)
        } else {
            panic!("TLS certificate specified without a password");
        }
    } else {
        None
    };

    let socket_manager = start_socket_manager(tls_options);
    let rtmp_endpoint = start_rtmp_server_endpoint(socket_manager);

    let ffmpeg_path = config
        .settings
        .get("ffmpeg_path")
        .expect("No ffmpeg_path setting found")
        .as_ref()
        .expect("no ffmpeg path specified");

    let log_dir = config
        .settings
        .get("log_path")
        .expect("No log_path setting found")
        .as_ref()
        .expect("No log_path value specified");

    let mut log_path = PathBuf::from(log_dir);
    if log_path.is_relative() {
        log_path = std::env::current_dir().expect("Failed to get current directory");
        log_path.push(log_dir);
    }

    let log_dir = log_path.to_str().unwrap().to_string();
    info!("Writing logs to '{}'", log_dir);
    let ffmpeg_endpoint = start_ffmpeg_endpoint(ffmpeg_path.to_string(), log_dir)
        .expect("Failed to start ffmpeg endpoint");

    info!("Starting workflow step factory, and adding known step types to it");
    let mut step_factory = WorkflowStepFactory::new();
    step_factory
        .register(
            WorkflowStepType(RTMP_RECEIVE.to_string()),
            Box::new(RtmpReceiverStepGenerator::new(rtmp_endpoint.clone())),
        )
        .expect("Failed to register rtmp_receive step");

    step_factory
        .register(
            WorkflowStepType(RTMP_WATCH.to_string()),
            Box::new(RtmpWatchStepGenerator::new(rtmp_endpoint.clone())),
        )
        .expect("Failed to register rtmp_watch step");

    step_factory
        .register(
            WorkflowStepType(FFMPEG_TRANSCODE.to_string()),
            Box::new(FfmpegTranscoderStepGenerator::new(
                rtmp_endpoint.clone(),
                ffmpeg_endpoint.clone(),
            )),
        )
        .expect("Failed to register ffmpeg_transcode step");

    step_factory
        .register(
            WorkflowStepType(FFMPEG_HLS.to_string()),
            Box::new(FfmpegHlsStepGenerator::new(
                rtmp_endpoint.clone(),
                ffmpeg_endpoint.clone(),
            )),
        )
        .expect("Failed to register ffmpeg_hls step");

    step_factory
        .register(
            WorkflowStepType(FFMPEG_PUSH.to_string()),
            Box::new(FfmpegRtmpPushStepGenerator::new(
                rtmp_endpoint.clone(),
                ffmpeg_endpoint.clone(),
            )),
        )
        .expect("Failed to register ffmpeg_push step");

    step_factory
        .register(
            WorkflowStepType(FFMPEG_PULL.to_string()),
            Box::new(FfmpegPullStepGenerator::new(
                rtmp_endpoint.clone(),
                ffmpeg_endpoint.clone(),
            )),
        )
        .expect("Failed to register ffmpeg_push step");

    let step_factory = Arc::new(step_factory);

    info!("Starting workflow manager");
    let manager = start_workflow_manager(step_factory);
    for (_, workflow) in config.workflows {
        let _ = manager.send(WorkflowManagerRequest::UpsertWorkflow {
            definition: workflow,
        });
    }

    manager
}

async fn load_tls_options(cert_path: String, password: String) -> TlsOptions {
    let mut file = match File::open(&cert_path).await {
        Ok(file) => file,
        Err(e) => panic!("Error reading pfx at '{}': {:?}", cert_path, e),
    };

    let mut file_content = Vec::new();
    match file.read_to_end(&mut file_content).await {
        Ok(_) => (),
        Err(e) => panic!("Failed to open file {}: {:?}", cert_path, e),
    }

    let identity = match Identity::from_pkcs12(&file_content, password.as_str()) {
        Ok(identity) => identity,
        Err(e) => panic!("Failed reading cert from '{}': {:?}", cert_path, e),
    };

    TlsOptions {
        certificate: identity,
    }
}
