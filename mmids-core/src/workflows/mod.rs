pub mod definitions;
pub mod steps;
mod runner;

pub use runner::{start as start_workflow, WorkflowRequest};

use crate::codecs::{AudioCodec, VideoCodec};
use crate::StreamId;
use bytes::Bytes;
use std::collections::HashMap;
use std::time::Duration;

/// Notification about media coming across a specific stream
pub struct MediaNotification {
    /// The identifier for the stream that this notification pertains to
    pub stream_id: StreamId,

    /// The content of the notification message
    pub content: MediaNotificationContent,
}

/// The detailed information contained within a media notification
#[derive(Debug)]
pub enum MediaNotificationContent {
    /// Announces that this stream has now connected, and steps that receive this notification
    /// should prepare for media data to start coming through
    NewIncomingStream {
        /// The name for the stream that's being published
        stream_name: String,
    },

    /// Announces that this stream's source has disconnected and will no longer be sending any
    /// new notifications down.  Steps that receive this message can use this to clean up any
    /// information they are tracking about this stream, as no new media will arrive without
    /// a new `NewIncomingStream` announcement.
    StreamDisconnected,

    /// Video content
    Video {
        codec: VideoCodec,
        is_sequence_header: bool,
        is_keyframe: bool,
        data: Bytes,
        timestamp: Duration,
    },

    /// Audio content
    Audio {
        codec: AudioCodec,
        is_sequence_header: bool,
        data: Bytes,
        timestamp: Duration,
    },

    /// New stream metadata
    Metadata { data: HashMap<String, String> },
}
