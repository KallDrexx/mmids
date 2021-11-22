use mmids_core::config::{parse as parse_config_file, MmidsConfig};
use mmids_core::endpoints::ffmpeg::start_ffmpeg_endpoint;
use mmids_core::endpoints::rtmp_server::start_rtmp_server_endpoint;
use mmids_core::net::tcp::{start_socket_manager, TlsOptions};
use mmids_core::workflows::definitions::WorkflowStepType;
use mmids_core::workflows::steps::factory::{start_step_factory, FactoryRequest};
use mmids_core::workflows::steps::ffmpeg_hls::FfmpegHlsStep;
use mmids_core::workflows::steps::ffmpeg_pull::FfmpegPullStep;
use mmids_core::workflows::steps::ffmpeg_rtmp_push::FfmpegRtmpPushStep;
use mmids_core::workflows::steps::ffmpeg_transcode::FfmpegTranscoder;
use mmids_core::workflows::steps::rtmp_receive::RtmpReceiverStep;
use mmids_core::workflows::steps::rtmp_watch::RtmpWatchStep;
use mmids_core::workflows::{start_workflow, WorkflowRequest};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
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
    let _workflows = init(config).await;

    loop {
        sleep(Duration::from_millis(100)).await;
    }
}

fn read_config() -> MmidsConfig {
    let contents = std::fs::read_to_string("mmids.config").expect("Failed to read 'mmids.config'");

    return parse_config_file(contents.as_str()).expect("Failed to parse config file");
}

async fn init(config: MmidsConfig) -> Vec<UnboundedSender<WorkflowRequest>> {
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
            Some(TlsOptions {
                pfx_file_location: path,
                cert_password: password,
            })
        } else {
            None
        }
    } else {
        None
    };

    let socket_manager = start_socket_manager(tls_options);

    let rtmp_endpoint = start_rtmp_server_endpoint(socket_manager);
    let step_factory = start_step_factory();

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
    let (factory_response_sender, mut factory_response_receiver) = unbounded_channel();
    let _ = step_factory.send(FactoryRequest::RegisterFunction {
        step_type: WorkflowStepType(RTMP_RECEIVE.to_string()),
        creation_fn: RtmpReceiverStep::create_factory_fn(rtmp_endpoint.clone()),
        response_channel: factory_response_sender.clone(),
    });

    factory_response_receiver
        .recv()
        .await
        .unwrap()
        .expect("Failed to register rtmp_receive step");

    let _ = step_factory.send(FactoryRequest::RegisterFunction {
        step_type: WorkflowStepType(RTMP_WATCH.to_string()),
        creation_fn: RtmpWatchStep::create_factory_fn(rtmp_endpoint.clone()),
        response_channel: factory_response_sender.clone(),
    });

    factory_response_receiver
        .recv()
        .await
        .unwrap()
        .expect("Failed to register rtmp_watch step");

    let _ = step_factory.send(FactoryRequest::RegisterFunction {
        step_type: WorkflowStepType(FFMPEG_TRANSCODE.to_string()),
        creation_fn: FfmpegTranscoder::create_factory_fn(
            ffmpeg_endpoint.clone(),
            rtmp_endpoint.clone(),
        ),
        response_channel: factory_response_sender.clone(),
    });

    factory_response_receiver
        .recv()
        .await
        .unwrap()
        .expect("Failed to register ffmpeg_transcode step");

    let _ = step_factory.send(FactoryRequest::RegisterFunction {
        step_type: WorkflowStepType(FFMPEG_HLS.to_string()),
        creation_fn: FfmpegHlsStep::create_factory_fn(
            ffmpeg_endpoint.clone(),
            rtmp_endpoint.clone(),
        ),
        response_channel: factory_response_sender.clone(),
    });

    factory_response_receiver
        .recv()
        .await
        .unwrap()
        .expect("Failed to register ffmpeg_hls step");

    let _ = step_factory.send(FactoryRequest::RegisterFunction {
        step_type: WorkflowStepType(FFMPEG_PUSH.to_string()),
        creation_fn: FfmpegRtmpPushStep::create_factory_fn(
            ffmpeg_endpoint.clone(),
            rtmp_endpoint.clone(),
        ),
        response_channel: factory_response_sender.clone(),
    });

    factory_response_receiver
        .recv()
        .await
        .unwrap()
        .expect("Failed to register ffmpeg_push step");

    let _ = step_factory.send(FactoryRequest::RegisterFunction {
        step_type: WorkflowStepType(FFMPEG_PULL.to_string()),
        creation_fn: FfmpegPullStep::create_factory_fn(
            rtmp_endpoint.clone(),
            ffmpeg_endpoint.clone(),
        ),
        response_channel: factory_response_sender.clone(),
    });

    factory_response_receiver
        .recv()
        .await
        .unwrap()
        .expect("Failed to register ffmpeg_pull step");

    info!("Starting {} workflows", config.workflows.len());
    let mut workflows = Vec::new();
    for (name, workflow) in &config.workflows {
        info!("Starting '{}' workflow", name);
        workflows.push(start_workflow(workflow.clone(), step_factory.clone()));
    }

    workflows
}
