use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tracing_subscriber::{fmt, layer::SubscriberExt};
use tracing::{debug, error, info};
use tracing::log::warn;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use mmids_core::codecs::{VideoCodec};
use mmids_core::net::ConnectionId;
use mmids_core::workflows::{MediaNotification, MediaNotificationContent};
use mmids_rtp::endpoints::webrtc_server::publisher_connection_handler::{PublisherConnectionHandlerParams, start_publisher_connection};
use mmids_rtp::endpoints::webrtc_server::{start_webrtc_server, StreamNameRegistration, WebrtcServerPublisherRegistrantNotification, WebrtcServerRequest, WebrtcServerWatcherRegistrantNotification, WebrtcStreamPublisherNotification, WebrtcStreamWatcherNotification};
use mmids_rtp::endpoints::webrtc_server::watcher_connection_handler::{start_watcher_connection, WatcherConnectionHandlerParams, WatcherConnectionHandlerRequest};

const APP_NAME: &str = "app";
const STREAM_NAME: &str = "stream";

#[tokio::main()]
pub async fn main() {
    let subscriber = tracing_subscriber::registry()
        .with(fmt::Layer::new().with_writer(std::io::stdout).pretty());

    tracing::subscriber::set_global_default(subscriber).expect("Unable to set a global collector");

    info!("WebRTC validator starting");

    info!("Starting endpoint");
    let endpoint = start_webrtc_server();

    info!("Register for listeners and watchers");
    let pub_registration = register_for_publishers(&endpoint).await;
    let (watch_registration, media_sender) = register_for_watchers(&endpoint).await;

    let _ = connect_watcher(&endpoint).await;
    let _ = connect_publisher(&endpoint).await;

    info!("Watcher and publisher accepted");
    loop {
        tokio::select! {
            notification = pub_registration.recv() => {
                if let Some(notification) = notification {
                    handle_pub_notification(notification);
                } else {
                    panic!("Publisher registrant disappeared");
                }
            }

            notification = watch_registration.recv() => {
                if let Some(notification) = notification {
                    handle_watch_notification(notification);
                } else {
                    panic!("Watcher registrant disappeared");
                }
            }
        }
    }
}

fn handle_pub_notification(
    notification: WebrtcServerPublisherRegistrantNotification,
    media_sender: UnboundedSender<MediaNotification>,
) {
    match notification {
        WebrtcServerPublisherRegistrantNotification::NewPublisherConnected {
            stream_id,
            stream_name,
            connection_id: _,
            mut media_channel,
            reactor_update_channel: _,
        } => {
            info!("New publisher connection on stream {}", stream_name);

            let media_sender = media_sender.clone();
            tokio::spawn(async move {
                info!("Media sender for stream {:?} started", stream_name);
                let _ = media_sender.send(MediaNotification {
                    stream_id: stream_id.clone(),
                    content: MediaNotificationContent::NewIncomingStream {
                        stream_name: stream_name.clone(),
                    },
                });

                while let Some(media) = media_channel.recv().await {
                    let _ = media_sender.send(MediaNotification {
                        stream_id: stream_id.clone(),
                        content: media,
                    });
                }

                info!("Media sender for stream {:?} ended", stream_name);
            });
        }

        WebrtcServerPublisherRegistrantNotification::PublisherDisconnected {connection_id, ..} => {
            let _ = media_sender.send(MediaNotification {
                stream_id: stream_id.clone(),
                content: MediaNotificationContent::StreamDisconnected,
            });
        }
    }
}

fn handle_watch_notification(
    notification: WebrtcServerWatcherRegistrantNotification,
) {

}

async fn connect_watcher(
    endpoint: &UnboundedSender<WebrtcServerRequest>,
) -> UnboundedReceiver<WebrtcStreamWatcherNotification> {
    println!("Enter watcher base64 SDP:");
    let offer_sdp = read_sdp_from_stdin().await;

    let (sender, mut receiver) = unbounded_channel();
    let _ = endpoint.send(WebrtcServerRequest::StreamWatchRequested {
        application_name: APP_NAME.to_string(),
        stream_name: STREAM_NAME.to_string(),
        notification_channel: sender,
        offer_sdp,
    });

    match receiver.recv().await {
        Some(WebrtcStreamWatcherNotification::WatchRequestAccepted {answer_sdp}) => {
            debug!("Watcher accepted, answer SDP: {}", answer_sdp);
        }

        Some(notification) => {
            panic!("Unexpected watcher notification received: {:?}", notification);
        }

        None => {
            panic!("WebRTC server closed");
        }
    }

    receiver
}

async fn connect_publisher(
    endpoint: &UnboundedSender<WebrtcServerRequest>,
) -> UnboundedReceiver<WebrtcStreamPublisherNotification> {
    println!("Enter publisher base64 SDP: ");
    let offer_sdp = read_sdp_from_stdin().await;

    let (sender, mut receiver) = unbounded_channel();
    let _ = endpoint.send(WebrtcServerRequest::StreamPublishRequested {
        application_name: APP_NAME.to_string(),
        stream_name: STREAM_NAME.to_string(),
        offer_sdp,
        notification_channel: sender,
    });

    match receiver.recv().await {
        Some(WebrtcStreamPublisherNotification::PublishRequestAccepted {answer_sdp}) => {
            debug!("Publisher accepted, answer SDP: {}", answer_sdp);
        }

        Some(notification) => {
            panic!("Unexpcted wathernotification received: {:?}", notification);
        }

        None => {
            panic!("WebRTC server closed");
        }
    }

    receiver
}

async fn register_for_publishers(
    endpoint: &UnboundedSender<WebrtcServerRequest>,
) -> UnboundedReceiver<WebrtcServerPublisherRegistrantNotification> {
    let (sender, mut receiver) = unbounded_channel();
    let _ = endpoint.send(WebrtcServerRequest::ListenForPublishers {
        application_name: APP_NAME.to_string(),
        stream_name: StreamNameRegistration::Any,
        audio_codec: None,
        video_codec: Some(VideoCodec::H264),
        requires_registrant_approval: false,
        notification_channel: sender,
    });

    match receiver.recv().await {
        Some(WebrtcServerPublisherRegistrantNotification::RegistrationSuccessful) => receiver,
        Some(notification) => {
            panic!("Received unexpected publisher registration notification: {:?}", notification);
        }

        None => {
            panic!("WebRTC server unexpectedly closed");
        }
    }
}

async fn register_for_watchers(
    endpoint: &UnboundedSender<WebrtcServerRequest>,
) -> (UnboundedReceiver<WebrtcServerWatcherRegistrantNotification>, UnboundedSender<MediaNotification>) {
    let (sender, mut receiver) = unbounded_channel();
    let (media_sender, media_receiver) = unbounded_channel();
    let _ = endpoint.send(WebrtcServerRequest::ListenForWatchers {
        application_name: APP_NAME.to_string(),
        stream_name: StreamNameRegistration::Any,
        audio_codec: None,
        video_codec: Some(VideoCodec::H264),
        requires_registrant_approval: false,
        notification_channel: sender,
        media_channel: media_receiver,
    });

    match receiver.recv().await {
        Some(WebrtcServerWatcherRegistrantNotification::RegistrationSuccessful) => (receiver, media_sender),
        Some(notification) => {
            panic!("Unexpected watch registration notification: {:?}", notification);
        }

        None => {
            panic!("WebRTC server unexpectedly closed");
        }
    }
}

async fn read_sdp_from_stdin() -> String {
    let stdin = tokio::io::stdin();
    let reader = BufReader::new(stdin);
    let line = reader.lines().next_line().await.unwrap().unwrap();
    let bytes = base64::decode(line).unwrap();
    let json = String::from_utf8(bytes).unwrap();
    let offer = serde_json::from_str::<RTCSessionDescription>(&json).unwrap();

    offer.sdp
}

fn handle_registrant_notification(
    notification: WebrtcServerPublisherRegistrantNotification,
    watcher_connection_handler: UnboundedSender<WatcherConnectionHandlerRequest>,
) {
    match notification {
        WebrtcServerPublisherRegistrantNotification::RegistrationFailed {} => {
            info!("Registrant registration reported as failed");
        }

        WebrtcServerPublisherRegistrantNotification::RegistrationSuccessful {} => {
            info!("Registrant registration reported as successful");
        }

        WebrtcServerPublisherRegistrantNotification::NewPublisherConnected {
            connection_id,
            stream_name,
            mut media_channel,
            ..
        } => {
            info!("New publisher connected with connection {:?} and stream name {}", connection_id, stream_name);

            tokio::spawn(async move {
                let mut video_received = false;
                let mut audio_received = false;
                while let Some(content) = media_channel.recv().await {
                    match content {
                        MediaNotificationContent::Video {..} => {
                            if !video_received {
                                info!("Video Received");
                                video_received = true;
                            }

                            let _ = watcher_connection_handler
                                .send(WatcherConnectionHandlerRequest::SendMedia(content));
                        },
                        MediaNotificationContent::Audio {..} => {
                            if !audio_received {
                                info!("Audio Received");
                                audio_received = true;
                            }
                        },
                        _ => (),
                    }
                }

                info!("Media channel closed");
            });
        }

        WebrtcServerPublisherRegistrantNotification::PublisherRequiringApproval {..} => {
            info!("Publisher requires approval");
        }

        WebrtcServerPublisherRegistrantNotification::PublisherDisconnected {..} => {
            info!("Publisher disconnected");
        }
    }
}
