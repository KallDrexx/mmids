use rml_rtmp::sessions::StreamMetadata;
use std::collections::HashMap;
use std::time::Duration;
use rml_rtmp::time::RtmpTimestamp;
use tracing::error;
use mmids_core::VideoTimestamp;

/// Creates a new video timestamp from RTMP data.  RTMP packets contain a timestamp in the
/// RTMP header itself and a composition time offset in the `AVCVIDEOPACKET` header.  The RTMP
/// timestamp is the decoding timestamp (dts), while the composition time offset is added to the
/// dts to get the presentation timestamp (pts).
pub fn video_timestamp_from_rtmp_data(rtmp_timestamp: RtmpTimestamp, mut composition_time_offset: i32) -> VideoTimestamp {
    if !(-8388608..838607).contains(&composition_time_offset) {
        error!("Composition time offset of {composition_time_offset} is out of 24 bit range.  Leaving at zero");
        composition_time_offset = 0;
    }

    VideoTimestamp::from_durations(
        Duration::from_millis(rtmp_timestamp.value as u64),
        Duration::from_millis(rtmp_timestamp.value as u64 + composition_time_offset as u64),
    )
}

/// Takes items from an RTMP stream metadata message and maps them to standardized key/value
/// entries in a hash map.
pub fn stream_metadata_to_hash_map(metadata: StreamMetadata) -> HashMap<String, String> {
    let mut map = HashMap::new();

    if let Some(codec) = metadata.video_codec {
        map.insert("videocodecid".to_string(), codec);
    }

    if let Some(x) = metadata.audio_bitrate_kbps {
        map.insert("audiodatarate".to_string(), x.to_string());
    }

    if let Some(x) = metadata.audio_channels {
        map.insert("audiochannels".to_string(), x.to_string());
    }

    if let Some(codec) = metadata.audio_codec {
        map.insert("audiocodecid".to_string(), codec);
    }

    if let Some(x) = metadata.audio_is_stereo {
        map.insert("stereo".to_string(), x.to_string());
    }

    if let Some(x) = metadata.audio_sample_rate {
        map.insert("audiosamplerate".to_string(), x.to_string());
    }

    if let Some(x) = metadata.encoder {
        map.insert("encoder".to_string(), x);
    }

    if let Some(x) = metadata.video_bitrate_kbps {
        map.insert("videodatarate".to_string(), x.to_string());
    }

    if let Some(x) = metadata.video_width {
        map.insert("width".to_string(), x.to_string());
    }

    if let Some(x) = metadata.video_height {
        map.insert("height".to_string(), x.to_string());
    }

    if let Some(x) = metadata.video_frame_rate {
        map.insert("framerate".to_string(), x.to_string());
    }

    map
}

/// Attempts to extract RTMP stream metadata values from a hash map
pub fn hash_map_to_stream_metadata(properties: &HashMap<String, String>) -> StreamMetadata {
    let mut metadata = StreamMetadata::new();
    if let Some(Ok(video_codec_id)) = properties.get("videocodecid").map(|id| id.parse()) {
        metadata.video_codec = Some(video_codec_id);
    }

    if let Some(audio_data_rate) = properties.get("audiodatarate") {
        if let Ok(num) = audio_data_rate.parse() {
            metadata.audio_bitrate_kbps = Some(num);
        }
    }

    if let Some(count) = properties.get("audiochannels") {
        if let Ok(num) = count.parse() {
            metadata.audio_channels = Some(num);
        }
    }

    if let Some(Ok(codec)) = properties.get("audiocodecid").map(|id| id.parse()) {
        metadata.audio_codec = Some(codec);
    }

    if let Some(stereo) = properties.get("stereo") {
        if let Ok(bool_val) = stereo.parse() {
            metadata.audio_is_stereo = Some(bool_val);
        }
    }

    if let Some(samples) = properties.get("audiosamplerate") {
        if let Ok(sample_rate) = samples.parse() {
            metadata.audio_sample_rate = Some(sample_rate);
        }
    }

    if let Some(encoder) = properties.get("encoder") {
        metadata.encoder = Some(encoder.clone());
    }

    if let Some(rate) = properties.get("videodatarate") {
        if let Ok(rate) = rate.parse() {
            metadata.video_bitrate_kbps = Some(rate);
        }
    }

    if let Some(width) = properties.get("width") {
        if let Ok(width) = width.parse() {
            metadata.video_width = Some(width);
        }
    }

    if let Some(height) = properties.get("height") {
        if let Ok(height) = height.parse() {
            metadata.video_height = Some(height);
        }
    }

    if let Some(rate) = properties.get("framerate") {
        if let Ok(rate) = rate.parse() {
            metadata.video_frame_rate = Some(rate);
        }
    }

    metadata
}
