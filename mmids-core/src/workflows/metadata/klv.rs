use anyhow::{Result, anyhow};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::workflows::metadata::{MetadataKey, MetadataValue, MetadataValueType};
use crate::workflows::metadata::keys::MetadataKeyMap;

pub struct KlvItem {
    pub key: u16,
    pub value: Bytes,
}

#[derive(Clone)]
pub struct KlvData {
    data: Bytes,
}

/// Iterates through KLV data in order. This is a separate structure from `KlvData` to allow a
/// single instance of `KlvData` to be iterated more than once.
pub struct KlvIterator {
    data: Bytes,
}

impl KlvItem {
    pub fn from_metadata(key: MetadataKey, value: MetadataValue, buffer: &mut BytesMut) -> Self {
        assert_eq!(key.value_type, value, "Value was not compatible with key's value type");
        let mut buffer = buffer.split_off(buffer.len());
        match value {
            MetadataValue::U8(num) => buffer.put_u8(num),
            MetadataValue::U16(num) => buffer.put_u16(num),
            MetadataValue::U32(num) => buffer.put_u32(num),
            MetadataValue::U64(num) => buffer.put_u64(num),
            MetadataValue::I8(num) => buffer.put_i8(num),
            MetadataValue::I16(num) => buffer.put_i16(num),
            MetadataValue::I32(num) => buffer.put_i32(num),
            MetadataValue::I64(num) => buffer.put_i64(num),
            MetadataValue::Bool(value) => buffer.put_u8(if value { 1 } else { 0 }),
            MetadataValue::Bytes(bytes) => buffer.put(bytes),
        };

        KlvItem {
            key: key.klv_id,
            value: buffer.freeze(),
        }
    }

    pub fn into_metadata(mut self, key_map: &MetadataKeyMap) -> Option<(MetadataKey, MetadataValue)> {
        let value_type = match key_map.get_type_for_klv_id(self.key) {
            Some(key) => key,
            None => return None, // No idea how to map the value type, so this item is ignored
        };

        match value_type {
            MetadataValueType::U8 => {
                if self.
            }
        }
    }
}

impl KlvData {
    pub fn from_iter(
        buffer: &mut BytesMut,
        iterator: impl Iterator<Item=KlvItem>,
    ) -> Result<Self> {
        let mut buffer = buffer.split_off(buffer.len());
        for item in iterator {
            if item.value.len() >= u16::MAX as usize {
                return Err(anyhow!("Tlv value was too large"));
            }

            buffer.put_u16(item.key);
            buffer.put_u16(item.value.len() as u16);
            buffer.put(item.value);
        }

        Ok(KlvData {data: buffer.freeze()})
    }

    pub fn iter(&self) -> KlvIterator {
        KlvIterator {
            data: self.data.clone(),
        }
    }
}

impl Iterator for KlvIterator {
    type Item = KlvItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.is_empty() {
            return None;
        }

        let key = self.data.get_u16();
        let length = self.data.get_u16();
        let value = self.data.split_to(length as usize);

        Some(KlvItem { key, value })
    }
}
