mod list_workflows;

use crate::workflows::manager::WorkflowManagerRequest;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use std::net::SocketAddr;
use std::time::Instant;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::{channel, Receiver, Sender};
use tracing::{error, info, instrument};
use uuid::Uuid;

pub struct HttpApiShutdownSignal {}

pub fn start_http_api(
    bind_address: SocketAddr,
    manager: UnboundedSender<WorkflowManagerRequest>,
) -> Sender<HttpApiShutdownSignal> {
    let service = make_service_fn(move |socket: &AddrStream| {
        let remote_address = socket.remote_addr();
        let manager_clone = manager.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |request: Request<Body>| {
                let request_id = Uuid::new_v4();
                execute_request(request, remote_address, manager_clone.clone(), request_id)
            }))
        }
    });

    let (sender, receiver) = channel();
    let server = Server::bind(&bind_address)
        .serve(service)
        .with_graceful_shutdown(graceful_shutdown(receiver));

    info!("Starting HTTP api on {}", bind_address);
    tokio::spawn(async { server.await });

    sender
}

async fn graceful_shutdown(shutdown_signal: Receiver<HttpApiShutdownSignal>) {
    let _ = shutdown_signal.await;
}

#[instrument(
    skip(request, client_address, manager),
    fields(http_method = %request.method(), http_uri = %request.uri(), client_ip = %client_address.ip())
)]
async fn execute_request(
    request: Request<Body>,
    client_address: SocketAddr,
    manager: UnboundedSender<WorkflowManagerRequest>,
    request_id: Uuid,
) -> Result<Response<Body>, hyper::Error> {
    info!(
        "Incoming HTTP request for {} {} from {}",
        request.method(),
        request.uri(),
        client_address.ip()
    );

    let started_at = Instant::now();

    match execute_handler(&request, manager).await {
        Ok(response) => {
            let elapsed = started_at.elapsed();
            info!(
                duration = %elapsed.as_millis(),
                "Request returning status code {} in {} ms", response.status(), elapsed.as_millis()
            );

            Ok(response)
        }

        Err(error) => {
            let elapsed = started_at.elapsed();
            error!(
                duration = %elapsed.as_millis(),
                "Request thrown error: {:?}", error
            );

            Err(error)
        }
    }
}

async fn execute_handler(
    request: &Request<Body>,
    manager: UnboundedSender<WorkflowManagerRequest>,
) -> Result<Response<Body>, hyper::Error> {
    if request.method() == Method::GET {
        if request.uri().path() == "/workflows" {
            return Ok(list_workflows::execute(manager).await);
        }
    }

    let mut response = Response::new(Body::from("Invalid URL"));
    *response.status_mut() = StatusCode::NOT_FOUND;

    Ok(response)
}
