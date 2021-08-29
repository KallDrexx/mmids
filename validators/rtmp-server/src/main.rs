use log::{info, error, warn};
use mmids_core::net::ConnectionId;
use mmids_core::net::tcp::{
    start_socket_manager,
    OutboundPacket,
    TcpSocketRequest,
    TcpSocketResponse,
};

use mmids_core::endpoints::rtmp_server::{
    start_rtmp_server_endpoint,
    RtmpEndpointRequest,
    StreamKeyRegistration,
    RtmpEndpointPublisherMessage,
};

use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

#[tokio::main()]
pub async fn main() {
    env_logger::init();

    info!("Starting rtmp server validator");

    let socket_manager_sender = start_socket_manager();
    let rtmp_server_sender = start_rtmp_server_endpoint(socket_manager_sender);
    let (rtmp_response_sender, mut rtmp_response_receiver) = unbounded_channel();
    let _ = rtmp_server_sender.send(RtmpEndpointRequest::ListenForPublishers {
        port: 1935,
        rtmp_app: "live".to_string(),
        rtmp_stream_key: StreamKeyRegistration::Any,
        message_channel: rtmp_response_sender,
        stream_id: None,
    });

    info!("Listening for publish requests on port 1935 and app 'live'");

    match rtmp_response_receiver.recv().await {
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

    let mut announce_video_data = true;
    let mut announce_audio_data = true;
    while let Some(message) = rtmp_response_receiver.recv().await {
        match message {
            RtmpEndpointPublisherMessage::NewPublisherConnected {connection_id, stream_key, stream_id} => {
                info!("Connection {} connected as publisher for stream_key {} and stream id {:?}", connection_id, stream_key, stream_id);
            }

            RtmpEndpointPublisherMessage::PublishingStopped {connection_id} => {
                info!("Connection {} stopped publishing", connection_id);
            }

            RtmpEndpointPublisherMessage::StreamMetadataChanged {publisher, metadata} => {
                info!("Connection {} sent new stream metadata: {:?}", publisher, metadata);
            }

            RtmpEndpointPublisherMessage::NewVideoData {publisher, data: _, timestamp: _} => {
                if announce_video_data {
                    info!("Connection {} sent video data", publisher);
                    announce_video_data = false;
                }
            }

            RtmpEndpointPublisherMessage::NewAudioData {publisher, data: _, timestamp: _} => {
                if announce_audio_data {
                    info!("Connection {} sent audio data", publisher);
                    announce_audio_data = false;
                }
            }

            message => {
                warn!("Unknown message received from publisher: {:?}", message);
            }
        }
    }

    info!("Terminating");
}
