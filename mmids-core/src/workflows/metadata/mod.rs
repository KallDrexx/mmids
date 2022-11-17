mod keys;
mod klv;

use std::sync::Arc;
use crate::workflows::metadata::keys::{MetadataKey, MetadataKeyMap};
use crate::workflows::metadata::klv::{KlvData, KlvItem};
use bytes::{Bytes, BytesMut};

#[derive(Clone)]
pub struct MediaPayloadMetadata {
    data: KlvData,
    key_map: Arc<MetadataKeyMap>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum MetadataValueType {U8, U16, U32, U64, I8, I16, I32, I64, Bytes, Bool}

#[derive(Debug)]
pub enum MetadataValue {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    Bytes(Bytes),
    Bool(bool),
}

#[derive(thiserror::Error, Debug)]
pub enum MetadataCreationError {
    #[error("Metadata entry had a value that could not be stored")]
    InvalidValue,
}


impl MediaPayloadMetadata {
    pub fn new(
        key_map: Arc<MetadataKeyMap>,
        key_value_pairs: impl Iterator<Item = (MetadataKey, MetadataValue)>,
        buffer: &mut BytesMut,
    ) -> Result<Self, MetadataCreationError> {
        let mut klv_buffer = buffer.split_off(buffer.len());
        let mut iterator_buffer = klv_buffer.split_off(klv_buffer.len());
        let iterator = key_value_pairs
            .map(|(k, v)| KlvItem::from_metadata(k, v, &mut iterator_buffer));

        let klv_data = KlvData::from_iter(&mut klv_buffer, iterator)
            .map_err(|_| MetadataCreationError::InvalidValue)?;

        Ok(MediaPayloadMetadata { data: klv_data, key_map })
    }

    pub fn iter(&self) -> impl Iterator<Item = (MetadataKey, MetadataValue)> {
        let map = self.key_map.clone();
        self.data
            .iter()
            .map(move |item| item.into_metadata(&map))
            .filter_map(|item| item.ok())
    }
}
