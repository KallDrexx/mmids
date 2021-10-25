use log::info;
use mmids_core::endpoints::ffmpeg::start_ffmpeg_endpoint;
use mmids_core::endpoints::rtmp_server::start_rtmp_server_endpoint;
use mmids_core::net::tcp::start_socket_manager;
use mmids_core::workflows::definitions::{
    WorkflowDefinition, WorkflowStepDefinition, WorkflowStepType,
};
use mmids_core::workflows::steps::factory::{start_step_factory, FactoryRequest};
use mmids_core::workflows::steps::ffmpeg_hls::FfmpegHlsStep;
use mmids_core::workflows::steps::ffmpeg_transcode::FfmpegTranscoder;
use mmids_core::workflows::steps::rtmp_receive::RtmpReceiverStep;
use mmids_core::workflows::steps::rtmp_watch::RtmpWatchStep;
use mmids_core::workflows::{start_workflow, WorkflowRequest};
use std::collections::HashMap;
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

    info!("Test");
    let _workflow_request_sender = match init().await {
        Ok(x) => x,
        Err(error) => panic!("Initialization failed: {:?}", error),
    };

    loop {
        sleep(Duration::from_millis(100)).await;
    }
}

async fn init() -> Result<UnboundedSender<WorkflowRequest>, Box<dyn std::error::Error + Send + Sync>>
{
    let socket_manager = start_socket_manager();
    let rtmp_endpoint = start_rtmp_server_endpoint(socket_manager);
    let step_factory = start_step_factory();

    let ffmpeg_path = "c:\\users\\me\\tools\\ffmpeg\\bin\\ffmpeg.exe";
    let ffmpeg_endpoint = start_ffmpeg_endpoint(ffmpeg_path.to_string())?;

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

    let workflow_definition = define_workflow();
    let workflow = start_workflow(workflow_definition, step_factory);

    Ok(workflow)
}

fn define_workflow() -> WorkflowDefinition {
    let mut receive_step = WorkflowStepDefinition {
        step_type: WorkflowStepType(RTMP_RECEIVE.to_string()),
        parameters: HashMap::new(),
    };

    receive_step
        .parameters
        .insert("rtmp_app".to_string(), "receive".to_string());
    receive_step
        .parameters
        .insert("stream_key".to_string(), "*".to_string());

    let mut hls_step = WorkflowStepDefinition {
        step_type: WorkflowStepType(FFMPEG_HLS.to_string()),
        parameters: HashMap::new(),
    };

    hls_step
        .parameters
        .insert("path".to_string(), "c:\\temp\\hls".to_string());
    hls_step
        .parameters
        .insert("duration".to_string(), "2".to_string());
    hls_step
        .parameters
        .insert("count".to_string(), "5".to_string());

    let mut preview_step = WorkflowStepDefinition {
        step_type: WorkflowStepType(RTMP_WATCH.to_string()),
        parameters: HashMap::new(),
    };

    preview_step
        .parameters
        .insert("rtmp_app".to_string(), "preview".to_string());
    preview_step
        .parameters
        .insert("stream_key".to_string(), "*".to_string());

    let mut transcode_step = WorkflowStepDefinition {
        step_type: WorkflowStepType(FFMPEG_TRANSCODE.to_string()),
        parameters: HashMap::new(),
    };

    transcode_step
        .parameters
        .insert("vcodec".to_string(), "h264".to_string());
    transcode_step
        .parameters
        .insert("acodec".to_string(), "aac".to_string());
    transcode_step
        .parameters
        .insert("h264_preset".to_string(), "ultrafast".to_string());
    transcode_step
        .parameters
        .insert("size".to_string(), "640x480".to_string());
    transcode_step
        .parameters
        .insert("kbps".to_string(), "1000".to_string());

    let mut watch_step = WorkflowStepDefinition {
        step_type: WorkflowStepType(RTMP_WATCH.to_string()),
        parameters: HashMap::new(),
    };

    watch_step
        .parameters
        .insert("rtmp_app".to_string(), "live".to_string());
    watch_step
        .parameters
        .insert("stream_key".to_string(), "*".to_string());

    WorkflowDefinition {
        name: "Test".to_string(),
        steps: vec![
            receive_step,
            hls_step,
            preview_step,
            transcode_step,
            watch_step,
        ],
    }
}
