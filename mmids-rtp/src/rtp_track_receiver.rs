use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::watch;
use tracing::{info, error, instrument};
use webrtc::track::track_remote::TrackRemote;
use mmids_core::net::ConnectionId;
use mmids_core::workflows::MediaNotificationContent;
use crate::media_senders::RtpToMediaContentSender;

#[instrument(skip(track, cancellation_token, media_sender))]
pub async fn receive_rtp_track_media(
    track: Arc<TrackRemote>,
    connection_id: ConnectionId,
    mut cancellation_token: watch::Receiver<bool>,
    mut media_sender: Box<dyn RtpToMediaContentSender + Send>,
) {
    info!("Starting rtp track reader");
    loop {
        tokio::select! {
            result = track.read_rtp() => {
                match result {
                    Ok((rtp_packet, _)) => {
                        match media_sender.send_rtp_data(&rtp_packet) {
                            Ok(()) => (),
                            Err(error) => {
                                error!("Failed to process rtp packet: {:?}", error);
                                break;
                            },
                        }
                    }

                    Err(error) => {
                        error!("Error reading rtp packet: {:?}", error);
                        break;
                    }
                }
            }

            value_changed = cancellation_token.changed() => {
                match value_changed {
                    Ok(()) => match *cancellation_token.borrow_and_update() {
                        false => (),
                        true => {
                            info!("Cancellation request received");
                            break;
                        }
                    }

                    Err(_) => {
                        info!("Cancellation token sender gone");
                        break;
                    }
                }
            }
        }
    }

    info!("Stopping rtp track reader");
}
