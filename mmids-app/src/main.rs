use log::info;
use mmids_core::config::{MmidsConfig, parse as parse_config_file};
use mmids_core::endpoints::ffmpeg::start_ffmpeg_endpoint;
use mmids_core::endpoints::rtmp_server::start_rtmp_server_endpoint;
use mmids_core::net::tcp::start_socket_manager;
use mmids_core::workflows::definitions::WorkflowStepType;
use mmids_core::workflows::steps::factory::{start_step_factory, FactoryRequest};
use mmids_core::workflows::steps::ffmpeg_hls::FfmpegHlsStep;
use mmids_core::workflows::steps::ffmpeg_transcode::FfmpegTranscoder;
use mmids_core::workflows::steps::rtmp_receive::RtmpReceiverStep;
use mmids_core::workflows::steps::rtmp_watch::RtmpWatchStep;
use mmids_core::workflows::{start_workflow, WorkflowRequest};
use std::fs;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::time::sleep;

const RTMP_RECEIVE: &str = "rtmp_receive";
const RTMP_WATCH: &str = "rtmp_watch";
const FFMPEG_TRANSCODE: &str = "ffmpeg_transcode";
const FFMPEG_HLS: &str = "ffmpeg_hls";

#[tokio::main]
pub async fn main() {
    env_logger::init();

    let config = read_config();
    let _workflows = init(config).await.expect("Initialization failed");

    loop {
        sleep(Duration::from_millis(100)).await;
    }
}

fn read_config() -> MmidsConfig {
    let contents = fs::read_to_string("mmids.config")
        .expect("Failed to read 'mmids.config'");

    return parse_config_file(contents.as_str())
        .expect("Failed to parse config file");
}

async fn init(config: MmidsConfig) -> Result<Vec<UnboundedSender<WorkflowRequest>>, Box<dyn std::error::Error + Send + Sync>>
{
    info!("Starting all endpoints");
    let socket_manager = start_socket_manager();
    let rtmp_endpoint = start_rtmp_server_endpoint(socket_manager);
    let step_factory = start_step_factory();

    let ffmpeg_path = config.settings.get("ffmpeg_path")
        .expect("No ffmpeg_path setting found").as_ref()
        .expect("no ffmpeg path specified");

    let ffmpeg_endpoint = start_ffmpeg_endpoint(ffmpeg_path.to_string())?;

    info!("Starting workflow step factory, and adding known step types to it");
    let (factory_response_sender, mut factory_response_receiver) = unbounded_channel();
    let _ = step_factory.send(FactoryRequest::RegisterFunction {
        step_type: WorkflowStepType(RTMP_RECEIVE.to_string()),
        creation_fn: RtmpReceiverStep::create_factory_fn(rtmp_endpoint.clone()),
        response_channel: factory_response_sender.clone(),
    });

    factory_response_receiver.recv().await.unwrap()?;

    let _ = step_factory.send(FactoryRequest::RegisterFunction {
        step_type: WorkflowStepType(RTMP_WATCH.to_string()),
        creation_fn: RtmpWatchStep::create_factory_fn(rtmp_endpoint.clone()),
        response_channel: factory_response_sender.clone(),
    });

    factory_response_receiver.recv().await.unwrap()?;

    let _ = step_factory.send(FactoryRequest::RegisterFunction {
        step_type: WorkflowStepType(FFMPEG_TRANSCODE.to_string()),
        creation_fn: FfmpegTranscoder::create_factory_fn(
            ffmpeg_endpoint.clone(),
            rtmp_endpoint.clone(),
        ),
        response_channel: factory_response_sender.clone(),
    });

    factory_response_receiver.recv().await.unwrap()?;

    let _ = step_factory.send(FactoryRequest::RegisterFunction {
        step_type: WorkflowStepType(FFMPEG_HLS.to_string()),
        creation_fn: FfmpegHlsStep::create_factory_fn(
            ffmpeg_endpoint.clone(),
            rtmp_endpoint.clone(),
        ),
        response_channel: factory_response_sender.clone(),
    });

    factory_response_receiver.recv().await.unwrap()?;

    info!("Starting {} workflows", config.workflows.len());
    let mut workflows = Vec::new();
    for (name, workflow) in &config.workflows {
        info!("Starting '{}' workflow", name);
        workflows.push(start_workflow(workflow.clone(), step_factory.clone()));
    }

    Ok(workflows)
}

