mod http_handlers;

use hyper::Method;
use mmids_core::config::{parse as parse_config_file, MmidsConfig};
use mmids_core::endpoints::ffmpeg::{start_ffmpeg_endpoint, FfmpegEndpointRequest};
use mmids_core::endpoints::rtmp_server::{start_rtmp_server_endpoint, RtmpEndpointRequest};
use mmids_core::event_hub::{start_event_hub, PublishEventRequest, SubscriptionRequest};
use mmids_core::http_api::handlers;
use mmids_core::http_api::routing::{PathPart, Route, RoutingTable};
use mmids_core::http_api::HttpApiShutdownSignal;
use mmids_core::net::tcp::{start_socket_manager, TlsOptions};
use mmids_core::reactors::executors::simple_http_executor::SimpleHttpExecutorGenerator;
use mmids_core::reactors::executors::ReactorExecutorFactory;
use mmids_core::reactors::manager::{
    start_reactor_manager, CreateReactorResult, ReactorManagerRequest,
};
use mmids_core::workflows::definitions::WorkflowStepType;
use mmids_core::workflows::manager::{
    start_workflow_manager, WorkflowManagerRequest, WorkflowManagerRequestOperation,
};
use mmids_core::workflows::steps::factory::WorkflowStepFactory;
use mmids_core::workflows::steps::ffmpeg_hls::FfmpegHlsStepGenerator;
use mmids_core::workflows::steps::ffmpeg_pull::FfmpegPullStepGenerator;
use mmids_core::workflows::steps::ffmpeg_rtmp_push::FfmpegRtmpPushStepGenerator;
use mmids_core::workflows::steps::ffmpeg_transcode::FfmpegTranscoderStepGenerator;
use mmids_core::workflows::steps::rtmp_receive::RtmpReceiverStepGenerator;
use mmids_core::workflows::steps::rtmp_watch::RtmpWatchStepGenerator;
use mmids_core::workflows::steps::workflow_forwarder::WorkflowForwarderStepGenerator;
use mmids_gstreamer::encoders::{
    AudioCopyEncoderGenerator, AudioDropEncoderGenerator, AvencAacEncoderGenerator, EncoderFactory,
    VideoCopyEncoderGenerator, VideoDropEncoderGenerator, X264EncoderGenerator,
};
use mmids_gstreamer::endpoints::gst_transcoder::{start_gst_transcoder, GstTranscoderRequest};
use mmids_gstreamer::steps::basic_transcoder::BasicTranscodeStepGenerator;
use native_tls::Identity;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::{channel, Sender};
use tracing::{info, warn, Level};
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::{fmt, layer::SubscriberExt};

const RTMP_RECEIVE: &str = "rtmp_receive";
const RTMP_WATCH: &str = "rtmp_watch";
const FORWARD_STEP: &str = "forward_to_workflow";
const BASIC_TRANSCODE_STEP: &str = "basic_transcode";

// ffmpeg steps will be depreciated at some point
const FFMPEG_TRANSCODE: &str = "ffmpeg_transcode";
const FFMPEG_HLS: &str = "ffmpeg_hls";
const FFMPEG_PUSH: &str = "ffmpeg_push";
const FFMPEG_PULL: &str = "ffmpeg_pull";

struct Endpoints {
    rtmp: UnboundedSender<RtmpEndpointRequest>,
    ffmpeg: UnboundedSender<FfmpegEndpointRequest>,
    gst_transcoder: UnboundedSender<GstTranscoderRequest>,
}

#[tokio::main]
pub async fn main() {
    // Start logging
    let log_dir = get_log_directory();
    let mut app_log_path = PathBuf::from(log_dir.clone());
    app_log_path.push("application");

    let log_level = match env::var("mmids_log") {
        Ok(level) => match level.to_lowercase().as_str() {
            "error" => Level::ERROR,
            "warn" => Level::WARN,
            "info" => Level::INFO,
            "debug" => Level::DEBUG,
            "trace" => Level::TRACE,
            _ => Level::INFO,
        },

        Err(_) => Level::INFO,
    };

    let appender = tracing_appender::rolling::hourly(app_log_path.clone(), "application.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(appender);
    let stdout_writer = std::io::stdout.with_max_level(log_level);
    let json_writer = non_blocking.with_max_level(log_level);

    let subscriber = tracing_subscriber::registry()
        .with(fmt::Layer::new().with_writer(stdout_writer).pretty())
        .with(fmt::Layer::new().with_writer(json_writer).json());

    tracing::subscriber::set_global_default(subscriber).expect("Unable to set a global collector");

    info!("mmmids {} started", env!("CARGO_PKG_VERSION"));
    info!("Logging to {}", app_log_path.display().to_string());

    let config = read_config();
    let tls_options = load_tls_options(&config).await;
    let endpoints = start_endpoints(&config, tls_options, log_dir);
    let (pub_sender, sub_sender) = start_event_hub();
    let reactor_manager = start_reactor(&config, sub_sender.clone()).await;
    let step_factory = register_steps(endpoints, sub_sender, reactor_manager);
    let manager = start_workflows(&config, step_factory, pub_sender);
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

fn get_log_directory() -> String {
    let log_dir = "logs";
    let mut log_path = PathBuf::from(log_dir);
    if log_path.is_relative() {
        log_path = std::env::current_dir().expect("Failed to get current directory");
        log_path.push(log_dir);
    }

    let log_dir = log_path.to_str().unwrap().to_string();

    log_dir
}

fn register_steps(
    endpoints: Endpoints,
    subscription_sender: UnboundedSender<SubscriptionRequest>,
    reactor_manager: UnboundedSender<ReactorManagerRequest>,
) -> Arc<WorkflowStepFactory> {
    info!("Starting workflow step factory, and adding known step types to it");
    let mut step_factory = WorkflowStepFactory::new();
    step_factory
        .register(
            WorkflowStepType(RTMP_RECEIVE.to_string()),
            Box::new(RtmpReceiverStepGenerator::new(
                endpoints.rtmp.clone(),
                reactor_manager.clone(),
            )),
        )
        .expect("Failed to register rtmp_receive step");

    step_factory
        .register(
            WorkflowStepType(RTMP_WATCH.to_string()),
            Box::new(RtmpWatchStepGenerator::new(
                endpoints.rtmp.clone(),
                reactor_manager.clone(),
            )),
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

    step_factory
        .register(
            WorkflowStepType(FORWARD_STEP.to_string()),
            Box::new(WorkflowForwarderStepGenerator::new(
                subscription_sender,
                reactor_manager,
            )),
        )
        .expect("Failed to register forward_to_workflow step");

    step_factory
        .register(
            WorkflowStepType(BASIC_TRANSCODE_STEP.to_string()),
            Box::new(BasicTranscodeStepGenerator::new(endpoints.gst_transcoder)),
        )
        .expect("Failed to register the basic transcoder step");

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

    let mut encoder_factory = EncoderFactory::new();
    encoder_factory
        .register_video_encoder("drop", Box::new(VideoDropEncoderGenerator {}))
        .expect("Failed to add video drop encoder");

    encoder_factory
        .register_video_encoder("copy", Box::new(VideoCopyEncoderGenerator {}))
        .expect("Failed to add video copy encoder");

    encoder_factory
        .register_video_encoder("x264", Box::new(X264EncoderGenerator {}))
        .expect("Failed to add the x264 encoder");

    encoder_factory
        .register_audio_encoder("drop", Box::new(AudioDropEncoderGenerator {}))
        .expect("Failed to add the audio drop encoder");

    encoder_factory
        .register_audio_encoder("copy", Box::new(AudioCopyEncoderGenerator {}))
        .expect("Failed to add the audio copy encoder");

    encoder_factory
        .register_audio_encoder("avenc_aac", Box::new(AvencAacEncoderGenerator {}))
        .expect("Failed to add the avenc_aac encoder");

    let gst_transcoder =
        start_gst_transcoder(Arc::new(encoder_factory)).expect("Failed to start gst transcoder");

    Endpoints {
        rtmp: rtmp_endpoint,
        ffmpeg: ffmpeg_endpoint,
        gst_transcoder,
    }
}

fn start_workflows(
    config: &MmidsConfig,
    step_factory: Arc<WorkflowStepFactory>,
    event_hub_publisher: UnboundedSender<PublishEventRequest>,
) -> UnboundedSender<WorkflowManagerRequest> {
    info!("Starting workflow manager");
    let manager = start_workflow_manager(step_factory, event_hub_publisher);
    for (_, workflow) in &config.workflows {
        let _ = manager.send(WorkflowManagerRequest {
            request_id: "mmids-app-startup".to_string(),
            operation: WorkflowManagerRequestOperation::UpsertWorkflow {
                definition: workflow.clone(),
            },
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
            handler: Box::new(handlers::list_workflows::ListWorkflowsHandler::new(
                manager.clone(),
            )),
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
                handlers::get_workflow_details::GetWorkflowDetailsHandler::new(manager.clone()),
            ),
        })
        .expect("Failed to register get workflow details route");

    routes
        .register(Route {
            method: Method::DELETE,
            path: vec![
                PathPart::Exact {
                    value: "workflows".to_string(),
                },
                PathPart::Parameter {
                    name: "workflow".to_string(),
                },
            ],
            handler: Box::new(handlers::stop_workflow::StopWorkflowHandler::new(
                manager.clone(),
            )),
        })
        .expect("Failed to register stop workflow route");

    routes
        .register(Route {
            method: Method::PUT,
            path: vec![PathPart::Exact {
                value: "workflows".to_string(),
            }],
            handler: Box::new(handlers::start_workflow::StartWorkflowHandler::new(
                manager.clone(),
            )),
        })
        .expect("Failed to register start workflow route");

    routes
        .register(Route {
            method: Method::GET,
            path: Vec::new(),
            handler: Box::new(http_handlers::VersionHandler),
        })
        .expect("Failed to register version route");

    let addr = ([127, 0, 0, 1], port).into();
    Some(mmids_core::http_api::start_http_api(addr, routes))
}

async fn start_reactor(
    config: &MmidsConfig,
    event_hub_subscriber: UnboundedSender<SubscriptionRequest>,
) -> UnboundedSender<ReactorManagerRequest> {
    let mut factory = ReactorExecutorFactory::new();
    factory
        .register(
            "simple_http".to_string(),
            Box::new(SimpleHttpExecutorGenerator {}),
        )
        .expect("Failed to add simple_http reactor executor");

    let reactor_manager = start_reactor_manager(factory, event_hub_subscriber.clone());
    for (name, definition) in &config.reactors {
        let (sender, receiver) = channel();
        let _ = reactor_manager.send(ReactorManagerRequest::CreateReactor {
            definition: definition.clone(),
            response_channel: sender,
        });

        match receiver.await {
            Ok(CreateReactorResult::Success) => (),
            Ok(error) => panic!("Failed to start reactor {}: {:?}", name, error),
            Err(_) => panic!("Reactor manager closed unexpectedly"),
        }
    }

    reactor_manager
}
