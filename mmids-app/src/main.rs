use std::collections::HashMap;
use std::time::Duration;
use log::info;
use mmids_core::net::tcp::start_socket_manager;
use mmids_core::endpoints::rtmp_server::start_rtmp_server_endpoint;
use mmids_core::workflows::{start_workflow, WorkflowRequest};
use mmids_core::workflows::definitions::{WorkflowStepDefinition, WorkflowDefinition, WorkflowStepType};
use mmids_core::workflows::steps::factory::{FactoryRequest, start_step_factory};
use mmids_core::workflows::steps::rtmp_receive::RtmpReceiverStep;
use mmids_core::workflows::steps::rtmp_watch::RtmpWatchStep;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::time::sleep;

const RTMP_RECEIVE: &str = "rtmp_receive";
const RTMP_WATCH: &str = "rtmp_watch";

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

async fn init() -> Result<UnboundedSender<WorkflowRequest>, Box<dyn std::error::Error + Send + Sync>> {
    let socket_manager = start_socket_manager();
    let rtmp_endpoint = start_rtmp_server_endpoint(socket_manager);
    let step_factory = start_step_factory();

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
        response_channel: factory_response_sender,
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

    receive_step.parameters.insert("rtmp_app".to_string(), "receive".to_string());
    receive_step.parameters.insert("stream_key".to_string(), "*".to_string());

    let mut watch_step = WorkflowStepDefinition {
        step_type: WorkflowStepType(RTMP_WATCH.to_string()),
        parameters: HashMap::new(),
    };

    watch_step.parameters.insert("rtmp_app".to_string(), "watch".to_string());
    watch_step.parameters.insert("stream_key".to_string(), "*".to_string());

    WorkflowDefinition {
        name: "Test".to_string(),
        steps: vec![receive_step, watch_step],
    }
}
