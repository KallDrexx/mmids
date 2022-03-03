use futures::StreamExt;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::sync::mpsc::unbounded_channel;
use tracing_subscriber::{fmt, layer::SubscriberExt};
use tracing::{info};
use tracing::log::warn;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use mmids_core::codecs::{AudioCodec, VideoCodec};
use mmids_core::endpoints::webrtc_server::publisher_connection_handler::{PublisherConnectionHandlerParams, start_publisher_connection};
use mmids_core::endpoints::webrtc_server::{WebrtcServerPublisherRegistrantNotification, WebrtcStreamPublisherNotification};
use mmids_core::net::ConnectionId;
use mmids_core::workflows::MediaNotificationContent;

#[tokio::main()]
pub async fn main() {
    let subscriber = tracing_subscriber::registry()
        .with(fmt::Layer::new().with_writer(std::io::stdout).pretty());

    tracing::subscriber::set_global_default(subscriber).expect("Unable to set a global collector");

    info!("WebRTC validator starting");

    println!("Enter base64 sdp: ");
    let stdin = tokio::io::stdin();
    let reader = BufReader::new(stdin);
    let line = reader.lines().next_line().await.unwrap().unwrap();
    let bytes = base64::decode(line).unwrap();
    let json = String::from_utf8(bytes).unwrap();
    let offer = serde_json::from_str::<RTCSessionDescription>(&json).unwrap();
    let sdp = offer.sdp;

    let (registrant_sender, mut registrant_receiver) = unbounded_channel();
    let (publisher_sender, mut publisher_receiver) = unbounded_channel();

    let publish_params = PublisherConnectionHandlerParams {
        stream_name: "abc".to_string(),
        connection_id: ConnectionId("publisher".to_string()),
        reactor_update_channel: None,
        offer_sdp: sdp,
        video_codec: Some(VideoCodec::H264),
        audio_codec: None,
        registrant_notification_channel: registrant_sender,
        publisher_notification_channel: publisher_sender,
    };

    let connection_handler = start_publisher_connection(publish_params);

    loop {
        tokio::select! {
            _ = connection_handler.closed() => {
                info!("Connection handler closed");
                break;
            }

            registrant_msg = registrant_receiver.recv() => {
                if let Some(registrant_msg) = registrant_msg {
                    handle_registrant_notification(registrant_msg);
                } else {
                    warn!("Registrant sender gone");
                    break;
                }
            }

            publisher_msg = publisher_receiver.recv() => {
                if let Some(publisher_msg) = publisher_msg {
                    handle_publisher_notification(publisher_msg);
                } else {
                    warn!("Publisher sender gone");
                    break;
                }
            }
        }
    }
}

fn handle_registrant_notification(notification: WebrtcServerPublisherRegistrantNotification) {
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
                while let Some(content) = media_channel.recv().await {
                    match content {
                        MediaNotificationContent::Video {..} => info!("Video Received"),
                        MediaNotificationContent::Audio {..} => info!("Audio Received"),
                        _ => (),
                    }
                }

                info!("Media channel closed");
            });
        }

        WebrtcServerPublisherRegistrantNotification::PublisherRequiringApproval {..} => {
            info!("Publisher requires approval");
        }
    }
}

fn handle_publisher_notification(notification: WebrtcStreamPublisherNotification) {
    match notification {
        WebrtcStreamPublisherNotification::PublishRequestRejected => {
            info!("Publish request rejected");
        }

        WebrtcStreamPublisherNotification::PublishRequestAccepted {answer_sdp} => {
            let sdp = answer_sdp.replace("\r", "").replace("\n", "\\n");
            let json = format!("{{\"type\": \"answer\", \"sdp\": \"{}\"}}", sdp);
            let encoded_json = base64::encode(json);

            info!("Publish request accepted with (base64'ed) answer sdp of: {}", encoded_json);
        }
    }
}