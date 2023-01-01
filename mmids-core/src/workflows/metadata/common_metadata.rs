//! Common types of media payload metadata that may be used

use crate::workflows::metadata::{MetadataKey, MetadataKeyMap, MetadataValueType};

/// Returns the metadata key for a metadata entry describing if a media payload is a video
/// key frame.
pub fn get_is_keyframe_metadata_key(metadata_map: &mut MetadataKeyMap) -> MetadataKey {
    metadata_map.register("is_keyframe", MetadataValueType::Bool)
}

/// Returns the metadata key for a metadata entry describing the number of milliseconds the
/// pts (presentation timestamp) value is offset from the dts (decoding timestamp).
pub fn get_pts_offset_metadata_key(metadata_map: &mut MetadataKeyMap) -> MetadataKey {
    metadata_map.register("pts_offset", MetadataValueType::I32)
}
