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

pub fn hash_map_to_stream_metadata(properties: &HashMap<String, String>) -> StreamMetadata {
    let mut metadata = StreamMetadata::new();
    if let Some(video_codec_id) = properties.get("videocodecid") {
        metadata.video_codec = Some(video_codec_id.clone());
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

    if let Some(codec) = properties.get("audiocodecid") {
        metadata.audio_codec = Some(codec.clone());
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