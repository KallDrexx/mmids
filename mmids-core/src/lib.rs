extern crate pest;
#[macro_use]
extern crate pest_derive;

use rml_rtmp::time::RtmpTimestamp;
use std::num::Wrapping;
use std::time::Duration;
use tracing::error;

pub mod codecs;
pub mod config;
pub mod endpoints;
pub mod event_hub;
pub mod http_api;
pub mod net;
pub mod reactors;
#[cfg(test)]
mod test_utils;
mod utils;
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
#[derive(Clone, Debug, PartialEq)]
pub struct VideoTimestamp {
    dts: Duration,
    pts_offset: i32,
}

impl VideoTimestamp {
    /// Creates a new video timestamp from RTMP data.  RTMP packets contain a timestamp in the
    /// RTMP header itself and a composition time offset in the `AVCVIDEOPACKET` header.  The RTMP
    /// timestamp is the decoding timestamp (dts), while the composition time offset is added to the
    /// dts to get the presentation timestamp (pts).
    pub fn from_rtmp_data(rtmp_timestamp: RtmpTimestamp, mut composition_time_offset: i32) -> Self {
        if composition_time_offset < -8388608 || composition_time_offset > 8388607 {
            error!("Composition time offset of {composition_time_offset} is out of 24 bit range.  Leaving at zero");
            composition_time_offset = 0;
        }

        VideoTimestamp {
            dts: Duration::from_millis(rtmp_timestamp.value as u64),
            pts_offset: composition_time_offset,
        }
    }

    /// Creates a new video timestamp based on absolute dts and pts values.
    pub fn from_durations(dts: Duration, pts: Duration) -> Self {
        let mut pts_offset = pts.as_millis() as i64 - dts.as_millis() as i64;
        if pts_offset < -8388608 || pts_offset > 8388607 {
            error!("PTS ({pts:?}) and DTS ({dts:?}) differ by more than a 24 bit number. Setting pts = dts");
            pts_offset = 0;
        }

        VideoTimestamp {
            dts,
            pts_offset: pts_offset as i32,
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
            dts -= Wrapping((self.pts_offset * -1) as u64);
        }

        Duration::from_millis(dts.0)
    }

    /// Gets the offset from the decoding timestamp for the pts
    pub fn pts_offset(&self) -> i32 {
        self.pts_offset
    }
}
