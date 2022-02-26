use crate::codecs::{AudioCodec, VideoCodec};
use crate::endpoints::webrtc_server::actor::FutureResult::AllConsumersGone;
use crate::endpoints::webrtc_server::{RequestType, StreamNameRegistration, ValidationResponse, WebrtcServerPublisherRegistrantNotification, WebrtcServerRequest, WebrtcServerWatcherRegistrantNotification, WebrtcStreamPublisherNotification, WebrtcStreamWatcherNotification};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::channel;
use tracing::{info, instrument, warn};
use uuid::Uuid;
use webrtc::util::Conn;
use crate::net::ConnectionId;

pub fn start_webrtc_server() -> UnboundedSender<WebrtcServerRequest> {
    let (sender, receiver) = unbounded_channel();
    let actor = Actor::new(receiver);
    tokio::spawn(actor.run());

    sender
}

struct ApplicationDetails {
    publisher_registrants: HashMap<StreamNameRegistration, PublisherRegistrant>,
    watcher_registrants: HashMap<StreamNameRegistration, WatcherRegistrant>,
}

struct PublisherRegistrant {
    notification_channel: UnboundedSender<WebrtcServerPublisherRegistrantNotification>,
    requires_registrant_approval: bool,
    video_codec: VideoCodec,
    audio_codec: AudioCodec,
}

struct WatcherRegistrant {
    notification_channel: UnboundedSender<WebrtcServerWatcherRegistrantNotification>,
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
    },

    WatcherRegistrantGone {
        app_name: String,
        stream_name: StreamNameRegistration,
    },

    ValidationChannelClosed {
        application_name: String,
        stream_name: String,
        operation_type: RequestType,
        connection_id: ConnectionId,
    },

    PublishValidationResponse {
        application_name: String,
        stream_name: String,
        connection_id: ConnectionId,
        response: ValidationResponse,
        notification_channel: UnboundedSender<WebrtcStreamPublisherNotification>,
    },

    WatcherValidationResponse {
        application_name: String,
        stream_name: String,
        connection_id: ConnectionId,
        response: ValidationResponse,
        notification_channel: UnboundedSender<WebrtcStreamWatcherNotification>,
    },
}

struct Actor {
    futures: FuturesUnordered<BoxFuture<'static, FutureResult>>,
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

                FutureResult::PublisherRegistrantGone {
                    app_name,
                    stream_name,
                } => {
                    self.remove_publisher_registrant(app_name, stream_name);
                }

                FutureResult::WatcherRegistrantGone {
                    app_name,
                    stream_name,
                } => {
                    self.remove_watcher_registrant(app_name, stream_name);
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

            WebrtcServerRequest::ListenForWatchers {
                stream_name,
                application_name,
                notification_channel,
                requires_registrant_approval,
                audio_codec,
                video_codec,
            } => {
                self.handle_listen_for_watcher_request(
                    application_name,
                    stream_name,
                    notification_channel,
                    requires_registrant_approval,
                    video_codec,
                    audio_codec,
                );
            }

            WebrtcServerRequest::RemoveRegistration {
                stream_name,
                application_name,
                registration_type,
            } => match registration_type {
                RequestType::Publisher => {
                    self.remove_publisher_registrant(application_name, stream_name)
                }

                RequestType::Watcher => {
                    self.remove_watcher_registrant(application_name, stream_name)
                }
            },

            WebrtcServerRequest::StreamPublishRequested {
                application_name,
                stream_name,
                notification_channel,
                offer_sdp,
            } => {

            }

            todo => todo!(),
        }
    }

    #[instrument(skip(self, notification_channel))]
    fn handle_stream_publish_requested(
        &mut self,
        application_name: String,
        stream_name: String,
        notification_channel: UnboundedSender<WebrtcStreamPublisherNotification>,
        offer_sdp: String,
    ) {
        let connection_id = ConnectionId(Uuid::new_v4().to_string());
        let application = match self.applications.get_mut(&application_name) {
            Some(app) => app,
            None => {
                info!(
                    connection_id = ?connection_id,
                    "Client requested publishing stream on application '{}' with stream name \
                    '{}', but no system has registered to receive publishers for this application",
                    application_name, stream_name
                );

                let _ = notification_channel
                    .send(WebrtcStreamPublisherNotification::PublishRequestRejected);

                return;
            }
        };

        let registrant = match application.publisher_registrants.get(&StreamNameRegistration::Any) {
            Some(registrant) => registrant,
            None => match application.publisher_registrants.get(&StreamNameRegistration::Exact(stream_name.clone())) {
                Some(registrant) => registrant,
                None => {
                    info!(
                        connection_id = ?connection_id,
                        "Client requested publishing stream on application '{}' with stream name \
                        '{}', but no system has registered to receive publishers on this stream name for \
                        this application", application_name, stream_name
                    );

                    let _ = notification_channel
                        .send(WebrtcStreamPublisherNotification::PublishRequestRejected);

                    return;
                }
            }
        };

        if registrant.requires_registrant_approval {
            info!(
                connection_id = ?connection_id,
                "Client requested publishing stream on application '{}' with stream name '{}', but \
                publishing requires approval", application_name, stream_name
            );

            let (sender, receiver) = channel();
            let _ = registrant.notification_channel
                .send(WebrtcServerPublisherRegistrantNotification::PublisherRequiringApproval {
                    stream_name: stream_name.clone(),
                    connection_id,
                    response_channel: sender,
                });


        }
    }

    #[instrument(skip(self, notification_channel))]
    fn handle_listen_for_watcher_request(
        &mut self,
        application_name: String,
        stream_name: StreamNameRegistration,
        notification_channel: UnboundedSender<WebrtcServerWatcherRegistrantNotification>,
        requires_registrant_approval: bool,
        video_codec: VideoCodec,
        audio_codec: AudioCodec,
    ) {
        let application = self
            .applications
            .entry(application_name.clone())
            .or_insert_with(|| ApplicationDetails {
                publisher_registrants: HashMap::new(),
                watcher_registrants: HashMap::new(),
            });

        if application
            .watcher_registrants
            .contains_key(&StreamNameRegistration::Any)
        {
            warn!(
                "WebRTC watcher registration failed as another system has requested watcher \
                    registration for all stream names for application '{}'",
                application_name
            );

            let _ = notification_channel
                .send(WebrtcServerWatcherRegistrantNotification::RegistrationFailed);

            return;
        }

        if let StreamNameRegistration::Exact(exact_name) = &stream_name {
            if application
                .watcher_registrants
                .contains_key(&StreamNameRegistration::Exact(exact_name.clone()))
            {
                warn!(
                    "WebRTC watcher registration failed as another system has requested watcher \
                        registration for the stream name '{}' for application '{}'",
                    exact_name, application_name
                );

                let _ = notification_channel
                    .send(WebrtcServerWatcherRegistrantNotification::RegistrationFailed);

                return;
            }
        } else if !application.watcher_registrants.is_empty() {
            // Requester requested ::Any, but at least one stream name was already registered
            warn!("WebRTC watcher registration for all stream names on application '{}' failed as \
                    at least one other stream name for this application has already been registered",
                    application_name);

            let _ = notification_channel
                .send(WebrtcServerWatcherRegistrantNotification::RegistrationFailed);

            return;
        }

        // registration requirements are successful
        application.watcher_registrants.insert(
            stream_name.clone(),
            WatcherRegistrant {
                notification_channel: notification_channel.clone(),
                video_codec,
                audio_codec,
                requires_registrant_approval,
            },
        );

        self.futures.push(
            notify_on_watcher_registrant_channel_closed(
                application_name,
                stream_name,
                notification_channel.clone(),
            )
            .boxed(),
        );

        let _ = notification_channel
            .send(WebrtcServerWatcherRegistrantNotification::RegistrationSuccessful);
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
        let application = self
            .applications
            .entry(application_name.clone())
            .or_insert_with(|| ApplicationDetails {
                publisher_registrants: HashMap::new(),
                watcher_registrants: HashMap::new(),
            });

        if application
            .publisher_registrants
            .contains_key(&StreamNameRegistration::Any)
        {
            warn!(
                "WebRTC publish registration failed as another system has requested publisher \
                    registration for all stream names for application {}",
                application_name
            );

            let _ = notification_channel
                .send(WebrtcServerPublisherRegistrantNotification::RegistrationFailed {});

            return;
        }

        if let StreamNameRegistration::Exact(exact_name) = &stream_name {
            if application
                .publisher_registrants
                .contains_key(&StreamNameRegistration::Exact(exact_name.clone()))
            {
                warn!(
                    "WebRTC publish registration failed as another system has requested publisher \
                        registration for stream name '{}' on application '{}'",
                    exact_name, application_name
                );

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

        application.publisher_registrants.insert(
            stream_name.clone(),
            PublisherRegistrant {
                notification_channel: notification_channel.clone(),
                requires_registrant_approval,
                video_codec,
                audio_codec,
            },
        );

        self.futures.push(
            notify_on_pub_registrant_channel_closed(
                application_name,
                stream_name,
                notification_channel.clone(),
            )
            .boxed(),
        );

        let _ = notification_channel
            .send(WebrtcServerPublisherRegistrantNotification::RegistrationSuccessful);
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

    fn remove_watcher_registrant(
        &mut self,
        application_name: String,
        stream_name: StreamNameRegistration,
    ) {
        let application = match self.applications.get_mut(&application_name) {
            Some(app) => app,
            None => return,
        };

        application.watcher_registrants.remove(&stream_name);
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

async fn notify_on_watcher_registrant_channel_closed(
    application_name: String,
    stream_name: StreamNameRegistration,
    sender: UnboundedSender<WebrtcServerWatcherRegistrantNotification>,
) -> FutureResult {
    sender.closed().await;

    FutureResult::WatcherRegistrantGone {
        app_name: application_name,
        stream_name,
    }
}
