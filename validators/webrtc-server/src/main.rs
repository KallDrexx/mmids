use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tracing_subscriber::{fmt, layer::SubscriberExt};
use tracing::{debug, error, info};
use tracing::log::warn;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use mmids_core::codecs::{VideoCodec};
use mmids_core::net::ConnectionId;
use mmids_core::workflows::MediaNotificationContent;
use mmids_rtp::endpoints::webrtc_server::publisher_connection_handler::{PublisherConnectionHandlerParams, start_publisher_connection};
use mmids_rtp::endpoints::webrtc_server::{WebrtcServerPublisherRegistrantNotification, WebrtcStreamPublisherNotification, WebrtcStreamWatcherNotification};
use mmids_rtp::endpoints::webrtc_server::watcher_connection_handler::{start_watcher_connection, WatcherConnectionHandlerParams, WatcherConnectionHandlerRequest};

#[tokio::main()]
pub async fn main() {
    let subscriber = tracing_subscriber::registry()
        .with(fmt::Layer::new().with_writer(std::io::stdout).pretty());

    tracing::subscriber::set_global_default(subscriber).expect("Unable to set a global collector");

    info!("WebRTC validator starting");

    println!("Enter watcher base64 sdp: ");
    let watcher_offer_sdp = read_sdp_from_stdin().await;

    let (registrant_sender, mut registrant_receiver) = unbounded_channel();
    let (publisher_sender, mut publisher_receiver) = unbounded_channel();
    let (watcher_sender, mut watcher_receiver) = unbounded_channel();
    let watch_params = WatcherConnectionHandlerParams {
        stream_name: "abc".to_string(),
        connection_id: ConnectionId("watcher".to_string()),
        offer_sdp: watcher_offer_sdp,
        video_codec: Some(VideoCodec::H264),
        audio_codec: None,
        watcher_channel: watcher_sender,
    };

    let watch_connection_handler = start_watcher_connection(watch_params);
    loop {
        let notification = match watcher_receiver.recv().await {
            Some(WebrtcStreamWatcherNotification::WatchRequestRejected) => {
                panic!("Watch registration rejected");
            }

            Some(WebrtcStreamWatcherNotification::WatchRequestAccepted {answer_sdp}) => {
                let sdp = answer_sdp.replace("\r", "").replace("\n", "\\n");
                let json = format!("{{\"type\": \"answer\", \"sdp\": \"{}\"}}", sdp);
                let encoded_json = base64::encode(json);

                debug!("watch request accepted with (base64'ed) answer sdp of: {}", encoded_json);

                break;
            }

            None => {
                panic!("Watch notification sender gone");
            }
        };
    }

    println!("Enter publisher base64 sdp: ");
    let publisher_offer_sdp = read_sdp_from_stdin().await;

    let publish_params = PublisherConnectionHandlerParams {
        stream_name: "abc".to_string(),
        connection_id: ConnectionId("publisher".to_string()),
        reactor_update_channel: None,
        offer_sdp: publisher_offer_sdp,
        video_codec: Some(VideoCodec::H264),
        audio_codec: None,
        registrant_notification_channel: registrant_sender,
        publisher_notification_channel: publisher_sender,
    };


    let pub_connection_handler = start_publisher_connection(publish_params);

    loop {
        tokio::select! {
            _ = pub_connection_handler.closed() => {
                info!("Publisher connection handler closed");
                break;
            }

            registrant_msg = registrant_receiver.recv() => {
                if let Some(registrant_msg) = registrant_msg {
                    handle_registrant_notification(registrant_msg, watch_connection_handler.clone());
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

            debug!("Publish request accepted with (base64'ed) answer sdp of: {}", encoded_json);
        }
    }
}

fn handle_watcher_notification(notification: WebrtcStreamWatcherNotification) {
    match notification {
        WebrtcStreamWatcherNotification::WatchRequestRejected => {
            info!("Publish request rejected");
        }

        WebrtcStreamWatcherNotification::WatchRequestAccepted {answer_sdp} => {
        }
    }
}