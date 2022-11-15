//! This crate contains all the building blocks and foundational systems that a mmids application
//! requires. It also contains standard workflow steps that are likely to be used in most
//! mmids applications.

extern crate pest;
#[macro_use]
extern crate pest_derive;

use std::num::Wrapping;
use std::time::Duration;
use tracing::error;

pub mod codecs;
pub mod config;
pub mod event_hub;
pub mod http_api;
pub mod net;
pub mod reactors;
#[cfg(feature = "test-utils")]
pub mod test_utils;
pub mod workflows;

/// Unique identifier that identifies the flow of video end-to-end.  Normally when media data enters
/// the beginning of a workflow it will be given a unique stream identifier, and it will keep that
/// identifier until it leaves the last stage of the workflow.  This allows for logging to give
/// visibility of how media is processed throughout it's all lifetime.
///
/// If a workflow has a step that requires media to leave the system and then come back in for
/// further steps, than it should keep the same stream identifier.  For example, if
/// a workflow has an ffmpeg transcoding step in the workflow (e.g. to add a watermark), when
/// ffmpeg pushes the video back in it will keep the same identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamId(pub String);

/// Represents timestamps relevant to video data.  Contains the decoding time stamp (dts) and
/// presentation time stamp (dts).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VideoTimestamp {
    dts: Duration,
    pts_offset: i32,
}

impl VideoTimestamp {
    /// Creates a new video timestamp based on absolute dts and pts values.
    pub fn from_durations(dts: Duration, pts: Duration) -> Self {
        let mut pts_offset = pts.as_millis() as i64 - dts.as_millis() as i64;
        if !(-8388608..838607).contains(&pts_offset) {
            error!("PTS ({pts:?}) and DTS ({dts:?}) differ by more than a 24 bit number. Setting pts = dts");
            pts_offset = 0;
        }

        VideoTimestamp {
            dts,
            pts_offset: pts_offset as i32,
        }
    }

    /// Creates a video timestamp at zero
    pub fn from_zero() -> Self {
        VideoTimestamp {
            dts: Duration::new(0, 0),
            pts_offset: 0,
        }
    }

    /// Gets the decoding time stamp for this video packet
    pub fn dts(&self) -> Duration {
        self.dts
    }

    /// Gets the presentation time stamp for the video packet
    pub fn pts(&self) -> Duration {
        let mut dts = Wrapping(self.dts.as_millis() as u64);
        if self.pts_offset > 0 {
            dts += Wrapping(self.pts_offset as u64);
        } else {
            dts -= Wrapping((-self.pts_offset) as u64);
        }

        Duration::from_millis(dts.0)
    }

    /// Gets the offset from the decoding timestamp for the pts
    pub fn pts_offset(&self) -> i32 {
        self.pts_offset
    }
}
