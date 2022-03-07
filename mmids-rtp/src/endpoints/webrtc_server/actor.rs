use crate::endpoints::webrtc_server::publisher_connection_handler::{
    start_publisher_connection, PublisherConnectionHandlerParams, PublisherConnectionHandlerRequest,
};
use crate::endpoints::webrtc_server::watcher_connection_handler::{
    start_watcher_connection, WatcherConnectionHandlerParams, WatcherConnectionHandlerRequest,
};
use crate::endpoints::webrtc_server::{RequestType, StreamNameRegistration, ValidationResponse, WebrtcServerPublisherRegistrantNotification, WebrtcServerRequest, WebrtcServerWatcherRegistrantNotification, WebrtcStreamPublisherNotification, WebrtcStreamWatcherNotification};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use mmids_core::codecs::{AudioCodec, VideoCodec};
use mmids_core::net::ConnectionId;
use mmids_core::reactors::ReactorWorkflowUpdate;
use mmids_core::workflows::{MediaNotification, MediaNotificationContent};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::{channel, Receiver};
use tracing::{error, info, instrument, warn};
use uuid::Uuid;
use mmids_core::StreamId;
use crate::endpoints::webrtc_server::actor::ConnectionState::WatcherActive;

pub fn start_webrtc_server() -> UnboundedSender<WebrtcServerRequest> {
    let (sender, receiver) = unbounded_channel();
    let actor = Actor::new(receiver);
    tokio::spawn(actor.run());

    sender
}

struct ApplicationDetails {
    publisher_registrants: HashMap<StreamNameRegistration, PublisherRegistrant>,
    watcher_registrants: HashMap<StreamNameRegistration, WatcherRegistrant>,
    published_streams: HashMap<String, PublishedStreamDetails>,
    watched_streams: HashMap<String, WatchedStreamDetails>,
    stream_id_to_name_map: HashMap<StreamId, String>,
}

struct PublishedStreamDetails {
    publisher: ConnectionId,
}

struct WatchedStreamDetails {
    watchers: HashSet<ConnectionId>,
    video_sequence_header: Option<MediaNotificationContent>,
    audio_sequence_header: Option<MediaNotificationContent>,
}

struct PublisherRegistrant {
    notification_channel: UnboundedSender<WebrtcServerPublisherRegistrantNotification>,
    requires_registrant_approval: bool,
    video_codec: Option<VideoCodec>,
    audio_codec: Option<AudioCodec>,
}

struct WatcherRegistrant {
    notification_channel: UnboundedSender<WebrtcServerWatcherRegistrantNotification>,
    requires_registrant_approval: bool,
    video_codec: Option<VideoCodec>,
    audio_codec: Option<AudioCodec>,
}

#[derive(Debug)]
enum ConnectionState {
    PublisherPendingValidation {
        application_name: String,
        stream_name: String,
        offer_sdp: String,
        publisher_channel: UnboundedSender<WebrtcStreamPublisherNotification>,
    },

    PublisherActive {
        application_name: String,
        stream_name: String,
        _connection_handler: UnboundedSender<PublisherConnectionHandlerRequest>,
    },

    WatcherPendingValidation {
        application_name: String,
        stream_name: String,
        offer_sdp: String,
        watcher_channel: UnboundedSender<WebrtcStreamWatcherNotification>,
    },

    WatcherActive {
        application_name: String,
        stream_name: String,
        connection_handler: UnboundedSender<WatcherConnectionHandlerRequest>,
    },
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

    PublishValidationChannelClosed {
        connection_id: ConnectionId,
    },

    PublishValidationResponse {
        connection_id: ConnectionId,
        response: ValidationResponse,
    },

    PublishConnectionHandlerGone(ConnectionId),

    WatcherValidationChannelClosed {
        connection_id: ConnectionId,
    },

    WatcherValidationResponse {
        connection_id: ConnectionId,
        response: ValidationResponse,
    },

    WatcherConnectionHandlerGone(ConnectionId),

    MediaReceived {
        application_name: String,
        registered_stream_name: StreamNameRegistration,
        media: MediaNotification,
        receiver: UnboundedReceiver<MediaNotification>,
    },

    MediaChannelClosed {
        application_name: String,
        registered_stream_name: StreamNameRegistration,
    },
}

struct Actor {
    futures: FuturesUnordered<BoxFuture<'static, FutureResult>>,
    applications: HashMap<String, ApplicationDetails>,
    connections: HashMap<ConnectionId, ConnectionState>,
}

impl Actor {
    fn new(receiver: UnboundedReceiver<WebrtcServerRequest>) -> Actor {
        let futures = FuturesUnordered::new();
        futures.push(notify_on_request_received(receiver).boxed());

        Actor {
            futures,
            applications: HashMap::new(),
            connections: HashMap::new(),
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

                FutureResult::PublishValidationChannelClosed { connection_id } => {
                    self.handle_pub_validation_channel_closed(connection_id);
                }

                FutureResult::PublishValidationResponse {
                    connection_id,
                    response,
                } => {
                    self.handle_pub_validation_response(connection_id, response);
                }

                FutureResult::PublishConnectionHandlerGone(connection_id) => {
                    info!(connection_id = ?connection_id, "Publisher connection gone");
                    self.remove_connection(connection_id);
                }

                FutureResult::WatcherConnectionHandlerGone(connection_id) => {
                    info!(connection_id = ?connection_id, "Watcher connection gone");
                    self.remove_connection(connection_id);
                }

                FutureResult::WatcherValidationResponse {
                    connection_id,
                    response,
                } => {
                    self.handle_watch_validation_response(connection_id, response);
                }

                FutureResult::WatcherValidationChannelClosed { connection_id } => {
                    self.handle_watch_validation_channel_closed(connection_id);
                }

                FutureResult::MediaReceived {
                    application_name,
                    registered_stream_name,
                    media,
                    receiver,
                } => {
                    self.futures.push(notify_on_media_received(
                        application_name.clone(),
                        registered_stream_name.clone(),
                        receiver
                    ).boxed());

                    self.handle_media(application_name, registered_stream_name, media);
                }

                FutureResult::MediaChannelClosed {application_name, registered_stream_name} => {
                    info!(
                        application_name = %application_name,
                        registered_stream_name = ?registered_stream_name,
                        "Media channel for registrant closed",
                    );

                    self.remove_watcher_registrant(application_name, registered_stream_name);
                }
            }
        }

        info!("WebRTC server stopping");
    }

    fn handle_watch_validation_channel_closed(&mut self, connection_id: ConnectionId) {
        let connection = match self.connections.remove(&connection_id) {
            Some(connection) => connection,
            None => return,
        };

        match connection {
            ConnectionState::WatcherPendingValidation {
                application_name,
                stream_name,
                watcher_channel,
                ..
            } => {
                warn!(
                    application_name = %application_name,
                    stream_name = %stream_name,
                    connection_id = ?connection_id,
                    "Stream watch request auto-rejected, as the validation channel was closed"
                );

                let _ = watcher_channel.send(WebrtcStreamWatcherNotification::WatchRequestRejected);
            }

            state => {
                error!(
                    connection_id = ?connection_id,
                    "Received stream watch validation channel closed message on a connection in an \
                    unexpected state of {:?}.  Connection has been removed.", state
                );
            }
        }
    }

    fn handle_pub_validation_channel_closed(&mut self, connection_id: ConnectionId) {
        let connection = match self.connections.remove(&connection_id) {
            Some(connection) => connection,
            None => return,
        };

        match connection {
            ConnectionState::PublisherPendingValidation {
                application_name,
                stream_name,
                offer_sdp: _,
                publisher_channel: notification_channel,
            } => {
                warn!(
                    application_name = %application_name,
                    stream_name = %stream_name,
                    connection_id = ?connection_id,
                    "Stream publish request on application '{}' and stream '{}' auto-rejected \
                    as the validation channel was closed", application_name, stream_name
                );

                let _ = notification_channel
                    .send(WebrtcStreamPublisherNotification::PublishRequestRejected);
            }

            state => {
                error!(
                    connection_id = ?connection_id,
                    "Received stream publish validation channel closed message on a connection in an \
                    unexpected state of {:?}.  Connection has been removed.",
                    state,
                )
            }
        }
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
                media_channel,
            } => {
                self.handle_listen_for_watcher_request(
                    application_name,
                    stream_name,
                    notification_channel,
                    requires_registrant_approval,
                    video_codec,
                    audio_codec,
                    media_channel,
                );
            }

            WebrtcServerRequest::StreamPublishRequested {
                application_name,
                stream_name,
                notification_channel,
                offer_sdp,
            } => {
                self.handle_stream_publish_requested(
                    application_name,
                    stream_name,
                    notification_channel,
                    offer_sdp,
                );
            }

            WebrtcServerRequest::StreamWatchRequested {
                application_name,
                stream_name,
                notification_channel,
                offer_sdp,
            } => {
                self.handle_stream_watch_requested(
                    application_name,
                    stream_name,
                    notification_channel,
                    offer_sdp,
                );
            }

            WebrtcServerRequest::RemoveRegistration {
                application_name,
                stream_name,
                registration_type,
            } => {
                info!(
                    application_name = %application_name,
                    stream_name = ?stream_name,
                    "Request made to remove {:?} registrant", registration_type
                );

                match registration_type {
                    RequestType::Watcher => {
                        self.remove_watcher_registrant(application_name, stream_name)
                    },

                    RequestType::Publisher => {
                        self.remove_publisher_registrant(application_name, stream_name)
                    },
                }
            }
        }
    }

    #[instrument(skip(self, notification_channel, offer_sdp))]
    fn handle_stream_watch_requested(
        &mut self,
        application_name: String,
        stream_name: String,
        notification_channel: UnboundedSender<WebrtcStreamWatcherNotification>,
        offer_sdp: String,
    ) {
        let connection_id = ConnectionId(Uuid::new_v4().to_string());
        let application = match self.applications.get_mut(&application_name) {
            Some(app) => app,
            None => {
                info!(
                    connection_id = ?connection_id,
                    "Client requested watching stream on application '{}' with stream name \
                    '{}', but no system has registered to receive watchers for this application",
                    application_name, stream_name
                );

                let _ = notification_channel
                    .send(WebrtcStreamWatcherNotification::WatchRequestRejected);

                return;
            }
        };

        let registrant = match application
            .watcher_registrants
            .get(&StreamNameRegistration::Any)
        {
            Some(registrant) => registrant,
            None => match application
                .watcher_registrants
                .get(&StreamNameRegistration::Exact(stream_name.clone()))
            {
                Some(registrant) => registrant,
                None => {
                    info!(
                        connection_id = ?connection_id,
                        "Client requested watching stream on application '{}' with stream name \
                        '{}', but no system has registered to receive watchers on this stream name for \
                        this application", application_name, stream_name
                    );

                    let _ = notification_channel
                        .send(WebrtcStreamWatcherNotification::WatchRequestRejected);

                    return;
                }
            },
        };

        if registrant.requires_registrant_approval {
            info!(
                connection_id = ?connection_id,
                "Client requested to watch stream but watching requires approval"
            );

            let (sender, receiver) = channel();
            let _ = registrant.notification_channel.send(
                WebrtcServerWatcherRegistrantNotification::WatcherRequiringApproval {
                    stream_name: stream_name.clone(),
                    connection_id: connection_id.clone(),
                    response_channel: sender,
                },
            );

            self.connections.insert(connection_id.clone(), ConnectionState::WatcherPendingValidation {
                application_name,
                stream_name,
                offer_sdp,
                watcher_channel: notification_channel,
            });

            self.futures
                .push(notify_on_watch_validation(connection_id, receiver).boxed());

            return;
        }

        // No registration required
        self.setup_watcher_connection(
            connection_id,
            application_name,
            stream_name,
            offer_sdp,
            notification_channel,
            None,
        );
    }

    #[instrument(skip(self, notification_channel, offer_sdp))]
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

        let registrant = match application
            .publisher_registrants
            .get(&StreamNameRegistration::Any)
        {
            Some(registrant) => registrant,
            None => match application
                .publisher_registrants
                .get(&StreamNameRegistration::Exact(stream_name.clone()))
            {
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
            },
        };

        if registrant.requires_registrant_approval {
            info!(
                connection_id = ?connection_id,
                "Client requested publishing stream on application '{}' with stream name '{}', but \
                publishing requires approval", application_name, stream_name
            );

            let (sender, receiver) = channel();
            let _ = registrant.notification_channel.send(
                WebrtcServerPublisherRegistrantNotification::PublisherRequiringApproval {
                    stream_name: stream_name.clone(),
                    connection_id: connection_id.clone(),
                    response_channel: sender,
                },
            );

            self.connections.insert(connection_id.clone(), ConnectionState::PublisherPendingValidation {
                application_name,
                stream_name,
                publisher_channel: notification_channel,
                offer_sdp,
            });

            self.futures
                .push(notify_on_pub_validation(connection_id, receiver).boxed());

            return;
        }

        // No registration required
        self.setup_publisher_connection(
            connection_id,
            application_name,
            stream_name,
            offer_sdp,
            notification_channel,
            None,
        );
    }

    #[instrument(skip(self, notification_channel))]
    fn handle_listen_for_watcher_request(
        &mut self,
        application_name: String,
        stream_name: StreamNameRegistration,
        notification_channel: UnboundedSender<WebrtcServerWatcherRegistrantNotification>,
        requires_registrant_approval: bool,
        video_codec: Option<VideoCodec>,
        audio_codec: Option<AudioCodec>,
        media_channel: UnboundedReceiver<MediaNotification>,
    ) {
        let application = self
            .applications
            .entry(application_name.clone())
            .or_insert_with(|| ApplicationDetails {
                publisher_registrants: HashMap::new(),
                watcher_registrants: HashMap::new(),
                published_streams: HashMap::new(),
                watched_streams: HashMap::new(),
                stream_id_to_name_map: HashMap::new(),
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
                application_name.clone(),
                stream_name.clone(),
                notification_channel.clone(),
            )
            .boxed(),
        );

        self.futures.push(
            notify_on_media_received(
                application_name,
                stream_name,
                media_channel,
            ).boxed(),
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
        audio_codec: Option<AudioCodec>,
        video_codec: Option<VideoCodec>,
    ) {
        let application = self
            .applications
            .entry(application_name.clone())
            .or_insert_with(|| ApplicationDetails {
                publisher_registrants: HashMap::new(),
                watcher_registrants: HashMap::new(),
                published_streams: HashMap::new(),
                watched_streams: HashMap::new(),
                stream_id_to_name_map: HashMap::new(),
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

    #[instrument(skip(self))]
    fn handle_watch_validation_response(
        &mut self,
        connection_id: ConnectionId,
        response: ValidationResponse,
    ) {
        let connection_state = match self.connections.remove(&connection_id) {
            Some(state) => state,
            None => return,
        };

        let(app_name, stream_name, offer_sdp, notification_channel) = match connection_state {
            ConnectionState::WatcherPendingValidation {
                application_name,
                stream_name,
                offer_sdp,
                watcher_channel,
            } => {
                (application_name, stream_name, offer_sdp, watcher_channel)
            }

            state => {
                error!(
                    "Connection received a watcher validation response but was in an unexpected \
                    connection state of {:?}.  Ignoring...", state
                );

                self.connections.insert(connection_id, state);

                return;
            }
        };

        match response {
            ValidationResponse::Reject => {
                info!(
                    application_name = %app_name,
                    stream_name = %stream_name,
                    "Watch request for application '{}' and stream '{}' was rejected",
                    app_name, stream_name,
                );

                let _ = notification_channel
                    .send(WebrtcStreamWatcherNotification::WatchRequestRejected);
            }

            ValidationResponse::Approve {reactor_update_channel} => {
                info!(
                    application_name = %app_name,
                    stream_name = %stream_name,
                    "Watch request on application '{}' and stream '{}' was accepted",
                    app_name, stream_name,
                );

                self.setup_watcher_connection(
                    connection_id,
                    app_name,
                    stream_name,
                    offer_sdp,
                    notification_channel,
                    Some(reactor_update_channel),
                );
            }
        }
    }

    #[instrument(skip(self))]
    fn handle_pub_validation_response(
        &mut self,
        connection_id: ConnectionId,
        response: ValidationResponse,
    ) {
        let connection_state = match self.connections.remove(&connection_id) {
            Some(state) => state,
            None => return, // connection probably closed before validation came back
        };

        let (app_name, stream_name, offer_sdp, notification_channel) = match connection_state {
            ConnectionState::PublisherPendingValidation {
                application_name,
                stream_name,
                offer_sdp,
                publisher_channel: notification_channel,
            } => (
                application_name,
                stream_name,
                offer_sdp,
                notification_channel,
            ),

            state => {
                error!(
                    connection_id = ?connection_id,
                    "Connection received a publisher validation response but was in an unexpected \
                    connection state of {:?}.  Ignoring...", state
                );

                self.connections.insert(connection_id, state);

                return;
            }
        };

        match response {
            ValidationResponse::Reject => {
                info!(
                    connection_id = ?connection_id,
                    application_name = %app_name,
                    stream_name = %stream_name,
                    "Publish request on application '{}' stream '{}' was rejected",
                    app_name, stream_name
                );

                let _ = notification_channel
                    .send(WebrtcStreamPublisherNotification::PublishRequestRejected);
            }

            ValidationResponse::Approve {
                reactor_update_channel,
            } => {
                info!(
                    connection_id = ?connection_id,
                    application_name = %app_name,
                    stream_name = %stream_name,
                    "Publish request on application '{}' stream '{}' was accepted",
                    app_name, stream_name
                );

                self.setup_publisher_connection(
                    connection_id,
                    app_name,
                    stream_name,
                    offer_sdp,
                    notification_channel,
                    Some(reactor_update_channel),
                );
            }
        }
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

        if let Some(_) = application.publisher_registrants.remove(&stream_name) {
            info!(
                application_name = %application_name,
                stream_name = ?stream_name,
                "Publishing registrant removed. Clearing relevant published streams"
            );

            match stream_name {
                StreamNameRegistration::Any => application.published_streams.clear(),
                StreamNameRegistration::Exact(name) => {application.published_streams.remove(&name);}
            }
        }
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

        if let Some(_) = application.watcher_registrants.remove(&stream_name) {
            info!(
                application_name = %application_name,
                stream_name = ?stream_name,
                "Watcher registrant removed. Clearing relevant published streams"
            );

            match stream_name {
                StreamNameRegistration::Any => application.watched_streams.clear(),
                StreamNameRegistration::Exact(name) => {application.watched_streams.remove(&name);},
            }
        }
    }

    #[instrument(skip(self, notification_channel, offer_sdp))]
    fn setup_publisher_connection(
        &mut self,
        connection_id: ConnectionId,
        application_name: String,
        stream_name: String,
        offer_sdp: String,
        notification_channel: UnboundedSender<WebrtcStreamPublisherNotification>,
        reactor_update_channel: Option<UnboundedReceiver<ReactorWorkflowUpdate>>,
    ) {
        // We need to validate these again in case the registrants has gone away after validation
        let application = match self.applications.get_mut(application_name.as_str()) {
            Some(app) => app,
            None => {
                info!(
                    "Client requested publishing stream but no system has registered to receive \
                    publishers for this application",
                );

                let _ = notification_channel
                    .send(WebrtcStreamPublisherNotification::PublishRequestRejected);

                return;
            }
        };

        let registrant = match application
            .publisher_registrants
            .get_mut(&StreamNameRegistration::Any)
        {
            Some(registrant) => registrant,
            None => match application
                .publisher_registrants
                .get_mut(&StreamNameRegistration::Exact(stream_name.clone()))
            {
                Some(registrant) => registrant,
                None => {
                    info!(
                        "Client requested publishing stream but no system has registered to receive \
                        publishers on this stream name for this application"
                    );

                    let _ = notification_channel
                        .send(WebrtcStreamPublisherNotification::PublishRequestRejected);

                    return;
                }
            },
        };

        if application.published_streams.contains_key(&stream_name) {
            info!(
                "Client requested publishing but another publisher is already active for this stream"
            );

            let _ = notification_channel
                .send(WebrtcStreamPublisherNotification::PublishRequestRejected);

            return;
        }

        let parameters = PublisherConnectionHandlerParams {
            connection_id: connection_id.clone(),
            video_codec: registrant.video_codec,
            audio_codec: registrant.audio_codec,
            offer_sdp,
            publisher_notification_channel: notification_channel.clone(),
            registrant_notification_channel: registrant.notification_channel.clone(),
            stream_name: stream_name.clone(),
            reactor_update_channel,
        };

        let connection_handler = start_publisher_connection(parameters);
        self.futures.push(
            notify_on_pub_connection_handler_gone(
                connection_id.clone(),
                connection_handler.clone(),
            )
            .boxed(),
        );

        self.connections.insert(
            connection_id.clone(),
            ConnectionState::PublisherActive {
                application_name,
                stream_name: stream_name.clone(),
                _connection_handler: connection_handler,
            },
        );

        application.published_streams.insert(
            stream_name,
            PublishedStreamDetails {
                publisher: connection_id,
            },
        );
    }

    fn setup_watcher_connection(
        &mut self,
        connection_id: ConnectionId,
        application_name: String,
        stream_name: String,
        offer_sdp: String,
        notification_channel: UnboundedSender<WebrtcStreamWatcherNotification>,
        reactor_update_channel: Option<UnboundedReceiver<ReactorWorkflowUpdate>>,
    ) {
        // We need to validate these again in case the registrants has gone away after validation
        let application = match self.applications.get_mut(application_name.as_str()) {
            Some(app) => app,
            None => {
                info!(
                    "Client requested watching stream but no system has registered to receive \
                    watchers for this application",
                );

                let _ = notification_channel
                    .send(WebrtcStreamWatcherNotification::WatchRequestRejected);

                return;
            }
        };

        let registrant = match application
            .watcher_registrants
            .get_mut(&StreamNameRegistration::Any)
        {
            Some(registrant) => registrant,
            None => match application
                .watcher_registrants
                .get_mut(&StreamNameRegistration::Exact(stream_name.clone()))
            {
                Some(registrant) => registrant,
                None => {
                    info!(
                        "Client requested watching stream but no system has registered to receive \
                        watchers on this stream name for this application"
                    );

                    let _ = notification_channel
                        .send(WebrtcStreamWatcherNotification::WatchRequestRejected);

                    return;
                }
            },
        };

        let parameters = WatcherConnectionHandlerParams {
            stream_name: stream_name.clone(),
            connection_id: connection_id.clone(),
            audio_codec: registrant.audio_codec,
            video_codec: registrant.video_codec,
            offer_sdp,
            watcher_channel: notification_channel,
        };

        let connection_handler = start_watcher_connection(parameters);

        self.futures.push(
            notify_on_watch_connection_handler_gone(
                connection_id.clone(),
                connection_handler.clone(),
            )
            .boxed(),
        );

        if let Some(details) = application.watched_streams.get(&stream_name) {
            if details.watchers.is_empty() {
                let _ = registrant.notification_channel.send(
                    WebrtcServerWatcherRegistrantNotification::StreamNameBecameActive {
                        stream_name: stream_name.clone(),
                        reactor_update_channel,
                    },
                );
            }
        }

        let entry = application
            .watched_streams
            .entry(stream_name.clone())
            .or_insert_with(|| WatchedStreamDetails {
                watchers: HashSet::new(),
                video_sequence_header: None,
                audio_sequence_header: None,
            });

        entry.watchers.insert(connection_id.clone());

        self.connections.insert(connection_id, WatcherActive {
            application_name,
            stream_name,
            connection_handler,
        });
    }

    #[instrument(skip(self))]
    fn remove_connection(&mut self, connection_id: ConnectionId) {
        match self.connections.remove(&connection_id) {
            Some(ConnectionState::PublisherActive {
                application_name,
                stream_name,
                ..
            }) => {
                if let Some(application) = self.applications.get_mut(&application_name) {
                    if let Some(details) = application.published_streams.get_mut(&stream_name) {
                        if details.publisher == connection_id {
                            info!(
                                application_name = %application_name,
                                stream_name = %stream_name,
                                "Publisher of {}/{} being removed", application_name, stream_name,
                            );

                            application.published_streams.remove(&stream_name);

                            let channel = if let Some(registrant) = application
                                .publisher_registrants
                                .get(&StreamNameRegistration::Any) {
                                Some(&registrant.notification_channel)
                            } else if let Some(registrant) = application
                                .publisher_registrants
                                .get(&StreamNameRegistration::Exact(stream_name.clone())) {
                                Some(&registrant.notification_channel)
                            } else {
                                None
                            };

                            if let Some(channel) = channel {
                                let _ = channel.send(
                                    WebrtcServerPublisherRegistrantNotification::PublisherDisconnected {
                                        stream_name,
                                        connection_id,
                                    }
                                );
                            }
                        }
                    }
                }
            }

            Some(ConnectionState::WatcherActive {
                application_name,
                stream_name,
                ..
            }) => {
                if let Some(application) = self.applications.get_mut(&application_name) {
                    if let Some(details) = application.watched_streams.get_mut(&stream_name) {
                        info!(
                            application_name = %application_name,
                            stream_name = %stream_name,
                            "Watcher of {}/{} is being removed", application_name, stream_name,
                        );

                        details.watchers.remove(&connection_id);

                        if details.watchers.is_empty() {
                            let channel = if let Some(registrant) = application
                                .watcher_registrants
                                .get(&StreamNameRegistration::Any)
                            {
                                Some(&registrant.notification_channel)
                            } else if let Some(registrant) = application
                                .watcher_registrants
                                .get(&StreamNameRegistration::Exact(stream_name.clone()))
                            {
                                Some(&registrant.notification_channel)
                            } else {
                                None
                            };

                            if let Some(channel) = channel {
                                let _ = channel.send(
                                    WebrtcServerWatcherRegistrantNotification::StreamNameBecameInactive {
                                        stream_name,
                                    }
                                );
                            }
                        }
                    }
                }
            }

            Some(ConnectionState::PublisherPendingValidation { .. }) => {
                info!("Publisher pending validation was removed");
            },
            Some(ConnectionState::WatcherPendingValidation { .. }) => {
                info!("Watcher pending validation was removed");
            }

            None => (),
        }
    }

    fn handle_media(
        &mut self,
        application_name: String,
        registered_stream_name: StreamNameRegistration,
        media: MediaNotification,
    ) {
        let application = match self.applications.get_mut(&application_name) {
            Some(app) => app,
            None => return, // application is no longer valid
        };

        if !application.watcher_registrants.contains_key(&registered_stream_name) {
            return; // No one listening to registrants
        }

        match &media.content {
            MediaNotificationContent::NewIncomingStream {stream_name} => {
                if let Some(old_name) = application.stream_id_to_name_map.get(&media.stream_id) {
                    warn!(
                        stream_id = ?media.stream_id,
                        "Duplicate NewIncomingStream notification for stream id.  Old stream name was \
                        {} and new stream id is {}.  Keeping old one mapped",
                        old_name, stream_name
                    );
                } else {
                    application.stream_id_to_name_map.insert(media.stream_id.clone(), stream_name.clone());
                }
            }

            MediaNotificationContent::StreamDisconnected => {
                if let Some(stream_name) = application.stream_id_to_name_map.remove(&media.stream_id) {
                    if let Some(watchers) = application.watched_streams.get(&stream_name) {
                        if watchers.watchers.is_empty() {
                            // Since we have no watchers, no reason to keep this in memory
                            application.watched_streams.remove(&stream_name);
                        }
                    }
                } else {
                    warn!(
                        stream_id = ?media.stream_id,
                        "Untracked stream disconnected",
                    );
                }
            }

            MediaNotificationContent::Video {is_sequence_header, ..} => {
                if let Some(stream_name) = application.stream_id_to_name_map.get(&media.stream_id) {
                    if let Some(details) = application.watched_streams.get_mut(stream_name) {
                        if *is_sequence_header {
                            details.video_sequence_header = Some(media.content.clone());
                        }

                        for connection_id in &details.watchers {
                            if let Some(connection) = self.connections.get(&connection_id) {
                                match connection {
                                    ConnectionState::WatcherActive {connection_handler, ..} => {
                                        let _ = connection_handler
                                            .send(WatcherConnectionHandlerRequest::SendMedia(
                                                    media.content.clone(),
                                            ));
                                    }

                                    state => error!(
                                        connection_id = ?connection_id,
                                        "Attempted to pass media to watcher connection, but watcher \
                                        was in state {:?} when WatcherActive was expected", state
                                    ),
                                }
                            }
                        }
                    }
                }
            }

            MediaNotificationContent::Audio {is_sequence_header, ..} => {
                if let Some(stream_name) = application.stream_id_to_name_map.get(&media.stream_id) {
                    if let Some(details) = application.watched_streams.get_mut(stream_name) {
                        if *is_sequence_header {
                            details.audio_sequence_header = Some(media.content.clone());
                        }

                        for connection_id in &details.watchers {
                            if let Some(connection) = self.connections.get(&connection_id) {
                                match connection {
                                    ConnectionState::WatcherActive {connection_handler, ..} => {
                                        let _ = connection_handler
                                            .send(WatcherConnectionHandlerRequest::SendMedia(
                                                media.content.clone(),
                                            ));
                                    }

                                    state => error!(
                                        connection_id = ?connection_id,
                                        "Attempted to pass media to watcher connection, but watcher \
                                        was in state {:?} when WatcherActive was expected", state
                                    ),
                                }
                            }
                        }
                    }
                }
            }

            MediaNotificationContent::Metadata {..} => (),
        }
    }
}

async fn notify_on_request_received(
    mut receiver: UnboundedReceiver<WebrtcServerRequest>,
) -> FutureResult {
    match receiver.recv().await {
        Some(request) => FutureResult::RequestReceived(request, receiver),
        None => FutureResult::AllConsumersGone,
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

async fn notify_on_pub_validation(
    connection_id: ConnectionId,
    receiver: Receiver<ValidationResponse>,
) -> FutureResult {
    match receiver.await {
        Ok(response) => FutureResult::PublishValidationResponse {
            connection_id,
            response,
        },

        Err(_) => FutureResult::PublishValidationChannelClosed { connection_id },
    }
}

async fn notify_on_pub_connection_handler_gone(
    connection_id: ConnectionId,
    sender: UnboundedSender<PublisherConnectionHandlerRequest>,
) -> FutureResult {
    sender.closed().await;

    FutureResult::PublishConnectionHandlerGone(connection_id)
}

async fn notify_on_watch_connection_handler_gone(
    connection_id: ConnectionId,
    sender: UnboundedSender<WatcherConnectionHandlerRequest>,
) -> FutureResult {
    sender.closed().await;

    FutureResult::WatcherConnectionHandlerGone(connection_id)
}

async fn notify_on_watch_validation(
    connection_id: ConnectionId,
    receiver: Receiver<ValidationResponse>,
) -> FutureResult {
    match receiver.await {
        Ok(response) => FutureResult::WatcherValidationResponse {
            response,
            connection_id,
        },

        Err(_) => FutureResult::WatcherValidationChannelClosed { connection_id },
    }
}

async fn notify_on_media_received(
    application_name: String,
    registered_stream_name: StreamNameRegistration,
    mut receiver: UnboundedReceiver<MediaNotification>,
) -> FutureResult {
    match receiver.recv().await {
        Some(media) => FutureResult::MediaReceived {
            application_name,
            registered_stream_name,
            media,
            receiver,
        },

        None => FutureResult::MediaChannelClosed {application_name, registered_stream_name}
    }
}
