//! A workflow represents a single media pipeline.  Each workflow contains one or more steps that
//! can either receive video, transform video, or send video to other sources.  Media data
//! transitions from one step to the next in a linear fashion based on the order in which they
//! were defined.

pub mod definitions;
pub mod manager;
mod runner;
pub mod steps;

pub use runner::{start_workflow, WorkflowRequest, WorkflowRequestOperation, WorkflowStatus};

use crate::codecs::{AudioCodec, VideoCodec};
use crate::endpoints::rtmp_server::RtmpEndpointMediaData;
use crate::utils::hash_map_to_stream_metadata;
use crate::{StreamId, VideoTimestamp};
use bytes::Bytes;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::HashMap;
use std::time::Duration;

pub use runner::{WorkflowState, WorkflowStepState};

/// Notification about media coming across a specific stream
#[derive(Clone, Debug, PartialEq)]
pub struct MediaNotification {
    /// The identifier for the stream that this notification pertains to
    pub stream_id: StreamId,

    /// The content of the notification message
    pub content: MediaNotificationContent,
}

/// The detailed information contained within a media notification
#[derive(Clone, Debug, PartialEq)]
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
        timestamp: VideoTimestamp,
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

impl MediaNotificationContent {
    /// Creates an RTMP representation of the media data from the specified media content
    pub fn to_rtmp_media_data(&self) -> Option<RtmpEndpointMediaData> {
        match self {
            MediaNotificationContent::StreamDisconnected => return None,
            MediaNotificationContent::NewIncomingStream { stream_name: _ } => return None,
            MediaNotificationContent::Metadata { data } => {
                Some(RtmpEndpointMediaData::NewStreamMetaData {
                    metadata: hash_map_to_stream_metadata(&data),
                })
            }

            MediaNotificationContent::Video {
                codec,
                is_keyframe,
                is_sequence_header,
                data,
                timestamp,
            } => Some(RtmpEndpointMediaData::NewVideoData {
                data: data.clone(),
                codec: codec.clone(),
                is_keyframe: *is_keyframe,
                is_sequence_header: *is_sequence_header,
                timestamp: RtmpTimestamp::new(timestamp.dts.as_millis() as u32),
                composition_time_offset: timestamp.pts_offset,
            }),

            MediaNotificationContent::Audio {
                codec,
                is_sequence_header,
                timestamp,
                data,
            } => Some(RtmpEndpointMediaData::NewAudioData {
                data: data.clone(),
                codec: codec.clone(),
                timestamp: RtmpTimestamp::new(timestamp.as_millis() as u32),
                is_sequence_header: *is_sequence_header,
            }),
        }
    }
}
