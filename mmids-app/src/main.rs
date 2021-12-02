mod http_handlers;

use hyper::Method;
use mmids_core::config::{parse as parse_config_file, MmidsConfig};
use mmids_core::endpoints::ffmpeg::{start_ffmpeg_endpoint, FfmpegEndpointRequest};
use mmids_core::endpoints::rtmp_server::{start_rtmp_server_endpoint, RtmpEndpointRequest};
use mmids_core::http_api::routing::{PathPart, Route, RoutingTable};
use mmids_core::http_api::HttpApiShutdownSignal;
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
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Sender;
use tracing::{info, warn};

const RTMP_RECEIVE: &str = "rtmp_receive";
const RTMP_WATCH: &str = "rtmp_watch";
const FFMPEG_TRANSCODE: &str = "ffmpeg_transcode";
const FFMPEG_HLS: &str = "ffmpeg_hls";
const FFMPEG_PUSH: &str = "ffmpeg_push";
const FFMPEG_PULL: &str = "ffmpeg_pull";

struct Endpoints {
    rtmp: UnboundedSender<RtmpEndpointRequest>,
    ffmpeg: UnboundedSender<FfmpegEndpointRequest>,
}

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let config = read_config();
    let log_dir = get_log_directory(&config);
    let tls_options = load_tls_options(&config).await;
    let endpoints = start_endpoints(&config, tls_options, log_dir);
    let step_factory = register_steps(endpoints);
    let manager = start_workflows(&config, step_factory);
    let http_api_shutdown = start_http_api(&config, manager);

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install ctrl+c signal handler");

    if let Some(sender) = http_api_shutdown {
        let _ = sender.send(HttpApiShutdownSignal {});
    }
}

fn read_config() -> MmidsConfig {
    let contents = std::fs::read_to_string("mmids.config").expect("Failed to read 'mmids.config'");

    return parse_config_file(contents.as_str()).expect("Failed to parse config file");
}

fn get_log_directory(config: &MmidsConfig) -> String {
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
    log_dir
}

fn register_steps(endpoints: Endpoints) -> Arc<WorkflowStepFactory> {
    info!("Starting workflow step factory, and adding known step types to it");
    let mut step_factory = WorkflowStepFactory::new();
    step_factory
        .register(
            WorkflowStepType(RTMP_RECEIVE.to_string()),
            Box::new(RtmpReceiverStepGenerator::new(endpoints.rtmp.clone())),
        )
        .expect("Failed to register rtmp_receive step");

    step_factory
        .register(
            WorkflowStepType(RTMP_WATCH.to_string()),
            Box::new(RtmpWatchStepGenerator::new(endpoints.rtmp.clone())),
        )
        .expect("Failed to register rtmp_watch step");

    step_factory
        .register(
            WorkflowStepType(FFMPEG_TRANSCODE.to_string()),
            Box::new(FfmpegTranscoderStepGenerator::new(
                endpoints.rtmp.clone(),
                endpoints.ffmpeg.clone(),
            )),
        )
        .expect("Failed to register ffmpeg_transcode step");

    step_factory
        .register(
            WorkflowStepType(FFMPEG_HLS.to_string()),
            Box::new(FfmpegHlsStepGenerator::new(
                endpoints.rtmp.clone(),
                endpoints.ffmpeg.clone(),
            )),
        )
        .expect("Failed to register ffmpeg_hls step");

    step_factory
        .register(
            WorkflowStepType(FFMPEG_PUSH.to_string()),
            Box::new(FfmpegRtmpPushStepGenerator::new(
                endpoints.rtmp.clone(),
                endpoints.ffmpeg.clone(),
            )),
        )
        .expect("Failed to register ffmpeg_push step");

    step_factory
        .register(
            WorkflowStepType(FFMPEG_PULL.to_string()),
            Box::new(FfmpegPullStepGenerator::new(
                endpoints.rtmp.clone(),
                endpoints.ffmpeg.clone(),
            )),
        )
        .expect("Failed to register ffmpeg_push step");

    Arc::new(step_factory)
}

async fn load_tls_options(config: &MmidsConfig) -> Option<TlsOptions> {
    info!("Loading TLS options");
    let cert_path = match config.settings.get("tls_cert_path") {
        Some(Some(x)) => x.clone(),
        _ => {
            warn!("No certificate file specified. TLS not available");
            return None;
        }
    };

    let cert_password = match config.settings.get("tls_cert_password") {
        Some(Some(x)) => x.clone(),
        _ => {
            panic!("Certificate file specified but no password given");
        }
    };

    let mut file = match File::open(&cert_path).await {
        Ok(file) => file,
        Err(e) => panic!("Error reading pfx at '{}': {:?}", cert_path, e),
    };

    let mut file_content = Vec::new();
    match file.read_to_end(&mut file_content).await {
        Ok(_) => (),
        Err(e) => panic!("Failed to open file {}: {:?}", cert_path, e),
    }

    let identity = match Identity::from_pkcs12(&file_content, cert_password.as_str()) {
        Ok(identity) => identity,
        Err(e) => panic!("Failed reading cert from '{}': {:?}", cert_path, e),
    };

    Some(TlsOptions {
        certificate: identity,
    })
}

fn start_endpoints(
    config: &MmidsConfig,
    tls_options: Option<TlsOptions>,
    log_dir: String,
) -> Endpoints {
    info!("Starting all endpoints");

    let socket_manager = start_socket_manager(tls_options);
    let rtmp_endpoint = start_rtmp_server_endpoint(socket_manager);

    let ffmpeg_path = config
        .settings
        .get("ffmpeg_path")
        .expect("No ffmpeg_path setting found")
        .as_ref()
        .expect("no ffmpeg path specified");

    let ffmpeg_endpoint = start_ffmpeg_endpoint(ffmpeg_path.to_string(), log_dir)
        .expect("Failed to start ffmpeg endpoint");

    Endpoints {
        rtmp: rtmp_endpoint,
        ffmpeg: ffmpeg_endpoint,
    }
}

fn start_workflows(
    config: &MmidsConfig,
    step_factory: Arc<WorkflowStepFactory>,
) -> UnboundedSender<WorkflowManagerRequest> {
    info!("Starting workflow manager");
    let manager = start_workflow_manager(step_factory);
    for (_, workflow) in &config.workflows {
        let _ = manager.send(WorkflowManagerRequest::UpsertWorkflow {
            definition: workflow.clone(),
        });
    }

    manager
}

fn start_http_api(
    config: &MmidsConfig,
    manager: UnboundedSender<WorkflowManagerRequest>,
) -> Option<Sender<HttpApiShutdownSignal>> {
    let port = match config.settings.get("http_api_port") {
        Some(Some(value)) => match value.parse::<u16>() {
            Ok(port) => port,
            Err(_) => {
                panic!("http_api_port value of '{}' is not a valid number", value);
            }
        },

        _ => {
            warn!("No `http_api_port` setting specified. HTTP api disabled");
            return None;
        }
    };

    let mut routes = RoutingTable::new();
    routes
        .register(Route {
            method: Method::GET,
            path: vec![PathPart::Exact {
                value: "workflows".to_string(),
            }],
            handler: Box::new(
                mmids_core::http_api::handlers::list_workflow_details::ListWorkflowsHandler,
            ),
        })
        .expect("Failed to register list workflows route");

    routes
        .register(Route {
            method: Method::GET,
            path: vec![
                PathPart::Exact {
                    value: "workflows".to_string(),
                },
                PathPart::Parameter {
                    name: "workflow".to_string(),
                },
            ],
            handler: Box::new(
                mmids_core::http_api::handlers::get_workflow_details::GetWorkflowDetailsHandler,
            ),
        })
        .expect("Failed to register get workflow details route");

    routes
        .register(Route {
            method: Method::GET,
            path: vec![PathPart::Exact {
                value: "version".to_string(),
            }],
            handler: Box::new(http_handlers::VersionHandler),
        })
        .expect("Failed to register version route");

    let addr = ([127, 0, 0, 1], port).into();
    Some(mmids_core::http_api::start_http_api(addr, routes, manager))
}
