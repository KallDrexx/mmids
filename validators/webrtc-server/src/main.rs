use mmids_core::codecs::VideoCodec;
use mmids_core::net::ConnectionId;
use mmids_core::workflows::{MediaNotification, MediaNotificationContent};
use mmids_core::StreamId;
use mmids_rtp::endpoints::webrtc_server::{
    start_webrtc_server, StreamNameRegistration, WebrtcServerPublisherRegistrantNotification,
    WebrtcServerRequest, WebrtcServerWatcherRegistrantNotification,
    WebrtcStreamPublisherNotification, WebrtcStreamWatcherNotification,
};
use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{debug, error, info};
use tracing_subscriber::{fmt, layer::SubscriberExt};
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

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
    let mut pub_registration = register_for_publishers(&endpoint).await;
    let (mut watch_registration, media_sender) = register_for_watchers(&endpoint).await;

    let _ = connect_watcher(&endpoint).await;
    let _ = connect_publisher(&endpoint).await;

    info!("Watcher and publisher accepted");
    let mut stream_id_map = HashMap::new();
    loop {
        tokio::select! {
            notification = pub_registration.recv() => {
                if let Some(notification) = notification {
                    handle_pub_notification(notification, media_sender.clone(), &mut stream_id_map);
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
    stream_id_map: &mut HashMap<ConnectionId, StreamId>,
) {
    match notification {
        WebrtcServerPublisherRegistrantNotification::NewPublisherConnected {
            stream_id,
            stream_name,
            connection_id,
            mut media_channel,
            reactor_update_channel: _,
        } => {
            info!("New publisher connection on stream {}", stream_name);

            stream_id_map.insert(connection_id, stream_id.clone());
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

        WebrtcServerPublisherRegistrantNotification::PublisherDisconnected {
            connection_id,
            ..
        } => {
            if let Some(stream_id) = stream_id_map.remove(&connection_id) {
                let _ = media_sender.send(MediaNotification {
                    stream_id,
                    content: MediaNotificationContent::StreamDisconnected,
                });
            }
        }

        WebrtcServerPublisherRegistrantNotification::PublisherRequiringApproval { .. } => {
            error!("Unexpected PublisherRequiringApproval event");
        }

        WebrtcServerPublisherRegistrantNotification::RegistrationFailed { .. } => {
            error!("Unexpected RegistrationFailed event");
        }

        WebrtcServerPublisherRegistrantNotification::RegistrationSuccessful => {
            error!("Unexpected Registration Successful event");
        }
    }
}

fn handle_watch_notification(_notification: WebrtcServerWatcherRegistrantNotification) {}

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
        Some(WebrtcStreamWatcherNotification::WatchRequestAccepted { answer_sdp }) => {
            let answer_sdp = sdp_to_base64_json(answer_sdp);
            debug!("Watcher accepted, answer SDP: {}", answer_sdp);
        }

        Some(notification) => {
            panic!(
                "Unexpected watcher notification received: {:?}",
                notification
            );
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
        Some(WebrtcStreamPublisherNotification::PublishRequestAccepted { answer_sdp }) => {
            let answer_sdp = sdp_to_base64_json(answer_sdp);
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
            panic!(
                "Received unexpected publisher registration notification: {:?}",
                notification
            );
        }

        None => {
            panic!("WebRTC server unexpectedly closed");
        }
    }
}

async fn register_for_watchers(
    endpoint: &UnboundedSender<WebrtcServerRequest>,
) -> (
    UnboundedReceiver<WebrtcServerWatcherRegistrantNotification>,
    UnboundedSender<MediaNotification>,
) {
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
        Some(WebrtcServerWatcherRegistrantNotification::RegistrationSuccessful) => {
            (receiver, media_sender)
        }
        Some(notification) => {
            panic!(
                "Unexpected watch registration notification: {:?}",
                notification
            );
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

fn sdp_to_base64_json(answer_sdp: String) -> String {
    let sdp = answer_sdp.replace("\r", "").replace("\n", "\\n");
    let json = format!("{{\"type\": \"answer\", \"sdp\": \"{}\"}}", sdp);
    base64::encode(json)
}
