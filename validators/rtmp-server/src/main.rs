use log::{error, info, warn};
use mmids_core::net::tcp::start_socket_manager;

use mmids_core::endpoints::rtmp_server::{
    start_rtmp_server_endpoint, RtmpEndpointMediaData, RtmpEndpointMediaMessage,
    RtmpEndpointPublisherMessage, RtmpEndpointRequest, RtmpEndpointWatcherNotification,
    StreamKeyRegistration,
};

use std::collections::HashMap;
use tokio::sync::mpsc::unbounded_channel;

#[tokio::main()]
pub async fn main() {
    env_logger::init();

    info!("Starting rtmp server validator");

    let socket_manager_sender = start_socket_manager();
    let rtmp_server_sender = start_rtmp_server_endpoint(socket_manager_sender);
    let (rtmp_response_sender, mut publish_notification_receiver) = unbounded_channel();
    let _ = rtmp_server_sender.send(RtmpEndpointRequest::ListenForPublishers {
        port: 1935,
        rtmp_app: "live".to_string(),
        rtmp_stream_key: StreamKeyRegistration::Any,
        message_channel: rtmp_response_sender,
        stream_id: None,
    });

    info!("Requesting to listen for publish requests on port 1935 and app 'live'");

    match publish_notification_receiver.recv().await {
        Some(RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful) => (),
        Some(x) => {
            error!("Unexpected initial message: {:?}", x);
            return;
        }

        None => {
            error!("response receiver closed before any responses came");
            return;
        }
    }

    info!("Publish listen request successful");

    let (notification_sender, mut watch_notification_receiver) = unbounded_channel();
    let (media_sender, media_receiver) = unbounded_channel();

    let _ = rtmp_server_sender.send(RtmpEndpointRequest::ListenForWatchers {
        port: 1935,
        rtmp_app: "live".to_string(),
        rtmp_stream_key: StreamKeyRegistration::Any,
        media_channel: media_receiver,
        notification_channel: notification_sender,
    });

    info!("Requesting to listening for play requests on port 1935 and app 'live'");
    match watch_notification_receiver.recv().await {
        Some(RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful) => (),
        Some(x) => {
            error!("Unexpected initial watch message: {:?}", x);
            return;
        }

        None => {
            error!("Watch response receiver closed before any responses came");
            return;
        }
    }

    info!("Playback listen request successful");

    let mut publisher_stream_key_map = HashMap::new();
    let mut announce_video_data = true;
    let mut announce_audio_data = true;
    loop {
        tokio::select! {
            message = watch_notification_receiver.recv() => {
                if message.is_none() {
                    break;
                }

                match message.unwrap() {
                    RtmpEndpointWatcherNotification::StreamKeyBecameActive {stream_key} => {
                        info!("Stream key '{}' now has at least one watcher", stream_key);
                    }

                    RtmpEndpointWatcherNotification::StreamKeyBecameInactive {stream_key} => {
                        info!("Stream key '{}' no longer has any watchers", stream_key);
                    }

                    event => {
                        info!("Unexpected watcher notification: {:?}", event);
                    }
                }
            }

            message = publish_notification_receiver.recv() => {
                if message.is_none() {
                    break;
                }

                match message.unwrap() {
                    RtmpEndpointPublisherMessage::NewPublisherConnected {connection_id, stream_key, stream_id} => {
                        info!("Connection {} connected as publisher for stream_key {} and stream id {:?}", connection_id, stream_key, stream_id);
                        publisher_stream_key_map.insert(connection_id, stream_key);
                    }

                    RtmpEndpointPublisherMessage::PublishingStopped {connection_id} => {
                        info!("Connection {} stopped publishing", connection_id);
                    }

                    RtmpEndpointPublisherMessage::StreamMetadataChanged {publisher, metadata} => {
                        info!("Connection {} sent new stream metadata: {:?}", publisher, metadata);

                        let stream_key = match publisher_stream_key_map.get(&publisher) {
                            Some(x) => x,
                            None => {
                                error!("Received stream metadata from unknown publisher with connection id {}", publisher);
                                continue;
                            }
                        };

                        let _ = media_sender.send(RtmpEndpointMediaMessage {
                            stream_key: (*stream_key).clone(),
                            data: RtmpEndpointMediaData::NewStreamMetaData {
                                metadata,
                            },
                        });
                    }

                    RtmpEndpointPublisherMessage::NewVideoData {
                        publisher,
                        data,
                        timestamp,
                        is_keyframe,
                        is_sequence_header,
                        codec,
                    } => {
                        if announce_video_data {
                            info!("Connection {} sent video data", publisher);
                            announce_video_data = false;
                        }

                        let stream_key = match publisher_stream_key_map.get(&publisher) {
                            Some(x) => x,
                            None => {
                                error!("Received video from unknown publisher with connection id {}", publisher);
                                continue;
                            }
                        };

                        let _ = media_sender.send(RtmpEndpointMediaMessage {
                            stream_key: (*stream_key).clone(),
                            data: RtmpEndpointMediaData::NewVideoData {
                                data,
                                timestamp,
                                codec,
                                is_sequence_header,
                                is_keyframe,
                            },
                        });
                    }

                    RtmpEndpointPublisherMessage::NewAudioData {
                        publisher,
                        data,
                        is_sequence_header,
                        timestamp,
                        codec,
                    } => {
                        if announce_audio_data {
                            info!("Connection {} sent audio data", publisher);
                            announce_audio_data = false;
                        }

                        let stream_key = match publisher_stream_key_map.get(&publisher) {
                            Some(x) => x,
                            None => {
                                error!("Received audio from unknown publisher with connection id {}", publisher);
                                continue;
                            }
                        };

                        let _ = media_sender.send(RtmpEndpointMediaMessage {
                            stream_key: (*stream_key).clone(),
                            data: RtmpEndpointMediaData::NewAudioData {
                                data,
                                is_sequence_header,
                                timestamp,
                                codec,
                            },
                        });
                    }

                    message => {
                        warn!("Unknown message received from publisher: {:?}", message);
                    }
                }

            }
        }
    }

    info!("Terminating");
}
