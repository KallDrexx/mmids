use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::unbounded_channel;
use tracing_subscriber::{fmt, layer::SubscriberExt};
use tracing::{info};
use tracing::log::warn;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use mmids_core::codecs::{VideoCodec};
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

}
