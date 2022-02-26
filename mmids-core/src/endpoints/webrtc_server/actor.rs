use std::collections::HashMap;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{info, instrument, warn};
use crate::codecs::{AudioCodec, VideoCodec};
use crate::endpoints::webrtc_server::actor::FutureResult::AllConsumersGone;
use crate::endpoints::webrtc_server::{StreamNameRegistration, WebrtcServerPublisherRegistrantNotification, WebrtcServerRequest};

pub fn start_webrtc_server() -> UnboundedSender<WebrtcServerRequest> {
    let (sender, receiver) = unbounded_channel();
    let actor = Actor::new(receiver);
    tokio::spawn(actor.run());

    sender
}

struct ApplicationDetails {
    publisher_registrants: HashMap<StreamNameRegistration, PublisherRegistrant>,
}

struct PublisherRegistrant {
    notification_channel: UnboundedSender<WebrtcServerPublisherRegistrantNotification>,
    requires_registrant_approval: bool,
    video_codec: VideoCodec,
    audio_codec: AudioCodec,
}

enum FutureResult {
    AllConsumersGone,
    RequestReceived(WebrtcServerRequest, UnboundedReceiver<WebrtcServerRequest>),
    PublisherRegistrantGone {
        app_name: String,
        stream_name: StreamNameRegistration,
    }
}

struct Actor {
    futures: FuturesUnordered<BoxFuture<'static,  FutureResult>>,
    applications: HashMap<String, ApplicationDetails>,
}

impl Actor {
    fn new(receiver: UnboundedReceiver<WebrtcServerRequest>) -> Actor {
        let futures = FuturesUnordered::new();
        futures.push(notify_on_request_received(receiver).boxed());

        Actor {
            futures,
            applications: HashMap::new(),
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

                FutureResult::PublisherRegistrantGone {app_name, stream_name} => {
                    self.remove_publisher_registrant(app_name, stream_name);
                }
            }
        }

        info!("WebRTC server stoppingb");
    }

    fn handle_request(&mut self, request: WebrtcServerRequest) {
        match request {
            WebrtcServerRequest::ListenForPublishers {
                stream_name,
                notification_channel,
                application_name,
                requires_registrant_approval,
                audio_codec,
                video_codec,
            } => {
                self.handle_listen_for_publishers_request(
                    application_name,
                    stream_name,
                    notification_channel,
                    requires_registrant_approval,
                    audio_codec,
                    video_codec,
                );
            }

            todo => todo!()
        }
    }

    #[instrument(skip(self, notification_channel))]
    fn handle_listen_for_publishers_request(
        &mut self,
        application_name: String,
        stream_name: StreamNameRegistration,
        notification_channel: UnboundedSender<WebrtcServerPublisherRegistrantNotification>,
        requires_registrant_approval: bool,
        audio_codec: AudioCodec,
        video_codec: VideoCodec,
    ) {
        let application = self.applications
            .entry(application_name.clone())
            .or_insert_with(|| ApplicationDetails {
                publisher_registrants: HashMap::new(),
            });

        if application.publisher_registrants.contains_key(&StreamNameRegistration::Any) {
            warn!("WebRTC publish registration failed as another system has requested publisher \
                    registration for all stream names for application {}", application_name);

            let _ = notification_channel
                .send(WebrtcServerPublisherRegistrantNotification::RegistrationFailed {});

            return;
        }

        if let StreamNameRegistration::Exact(exact_name) = &stream_name {
            if application.publisher_registrants
                .contains_key(&StreamNameRegistration::Exact(exact_name.clone())) {
                warn!("WebRTC publish registration failed as another system has requested publisher \
                        registration for stream name '{}' on application '{}'",
                    exact_name, application_name);

                let _ = notification_channel
                    .send(WebrtcServerPublisherRegistrantNotification::RegistrationFailed {});

                return;
            }
        } else {
            if application.publisher_registrants.len() > 0 {
                warn!("WebRTC publish registration failed as another system has requested publisher \
                        registration at least one other stream on application '{}', which conflicts \
                        with this request for all streams", application_name);

                let _ = notification_channel
                    .send(WebrtcServerPublisherRegistrantNotification::RegistrationFailed {});

                return;
            }
        }

        application.publisher_registrants.insert(stream_name.clone(), PublisherRegistrant {
            notification_channel: notification_channel.clone(),
            requires_registrant_approval,
            video_codec,
            audio_codec,
        });

        self.futures.push(
            notify_on_pub_registrant_channel_closed(
                application_name,
                stream_name,
                notification_channel)
                .boxed()
            );
    }

    fn remove_publisher_registrant(
        &mut self,
        application_name: String,
        stream_name: StreamNameRegistration,
    ) {
        let application = match self.applications.get_mut(&application_name) {
            Some(app) => app,
            None => return,
        };

        application.publisher_registrants.remove(&stream_name);
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

async fn notify_on_pub_registrant_channel_closed(
    application_name: String,
    stream_name: StreamNameRegistration,
    sender: UnboundedSender<WebrtcServerPublisherRegistrantNotification>,
) -> FutureResult {
    sender.closed().await;

    FutureResult::PublisherRegistrantGone {
        app_name: application_name,
        stream_name,
    }
}