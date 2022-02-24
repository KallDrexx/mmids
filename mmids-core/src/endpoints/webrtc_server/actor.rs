use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{info, instrument, warn};
use crate::endpoints::webrtc_server::actor::FutureResult::AllConsumersGone;
use crate::endpoints::webrtc_server::WebrtcServerRequest;

pub fn start_webrtc_server() -> UnboundedSender<WebrtcServerRequest> {
    let (sender, receiver) = unbounded_channel();
    let actor = Actor::new(receiver);
    tokio::spawn(actor.run());

    sender
}

enum FutureResult {
    AllConsumersGone,
    RequestReceived(WebrtcServerRequest, UnboundedReceiver<WebrtcServerRequest>),
}

struct Actor {
    futures: FuturesUnordered<BoxFuture<'static,  FutureResult>>,
}

impl Actor {
    fn new(receiver: UnboundedReceiver<WebrtcServerRequest>) -> Actor {
        let futures = FuturesUnordered::new();
        futures.push(notify_on_request_received(receiver).boxed());

        Actor {
            futures,
        }
    }

    #[instrument(name = "WebRTC Server Execution", skip(self))]
    async fn run(mut self) {
        info!("Starting WebRTC server");

        while let Some(result) = self.futures.next().await {
            match result {
                FutureResult::AllConsumersGone => {
                    warn!("All consumers gone");

                    break;
                }

                FutureResult::RequestReceived(request, receiver) => {
                    self.futures
                        .push(notify_on_request_received(receiver).boxed());

                    self.handle_request(request);
                }
            }
        }

        info!("WebRTC server stoppingb");
    }

    fn handle_request(&mut self, request: WebrtcServerRequest) {

    }
}

async fn notify_on_request_received(
    mut receiver: UnboundedReceiver<WebrtcServerRequest>,
) -> FutureResult {
    match receiver.recv().await {
        Some(request) => FutureResult::RequestReceived(request, receiver),
        None => AllConsumersGone,
    }
}