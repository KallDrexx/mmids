use rml_rtmp::sessions::StreamMetadata;
use std::collections::HashMap;

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