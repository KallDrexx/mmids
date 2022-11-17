use crate::workflows::metadata::keys::MetadataKeyMap;
use crate::workflows::metadata::{MetadataKey, MetadataValue, MetadataValueType};
use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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

    pub fn into_metadata(
        mut self,
        key_map: &MetadataKeyMap,
    ) -> Result<(MetadataKey, MetadataValue)> {
        let value_type = match key_map.get_type_for_klv_id(self.key) {
            Some(key) => key,
            None => return Err(anyhow!("No value type found for key")),
        };

        let key = MetadataKey {
            klv_id: self.key,
            value_type,
        };
        match value_type {
            MetadataValueType::U8 => {
                if self.value.is_empty() {
                    Err(anyhow!("Not enough bytes for a u8"))
                } else {
                    Ok((key, MetadataValue::U8(self.value.get_u8())))
                }
            }

            MetadataValueType::U16 => {
                if self.value.len() < 2 {
                    Err(anyhow!("Not enough bytes for a u16"))
                } else {
                    Ok((key, MetadataValue::U16(self.value.get_u16())))
                }
            }

            MetadataValueType::U32 => {
                if self.value.len() < 4 {
                    Err(anyhow!("Not enough bytes for a u32"))
                } else {
                    Ok((key, MetadataValue::U32(self.value.get_u32())))
                }
            }

            MetadataValueType::U64 => {
                if self.value.len() < 8 {
                    Err(anyhow!("Not enough bytes for a u64"))
                } else {
                    Ok((key, MetadataValue::U64(self.value.get_u64())))
                }
            }

            MetadataValueType::I8 => {
                if self.value.is_empty() {
                    Err(anyhow!("Not enough bytes for a i8"))
                } else {
                    Ok((key, MetadataValue::I8(self.value.get_i8())))
                }
            }

            MetadataValueType::I16 => {
                if self.value.len() < 2 {
                    Err(anyhow!("Not enough bytes for a i16"))
                } else {
                    Ok((key, MetadataValue::I16(self.value.get_i16())))
                }
            }

            MetadataValueType::I32 => {
                if self.value.len() < 4 {
                    Err(anyhow!("Not enough bytes for a i32"))
                } else {
                    Ok((key, MetadataValue::I32(self.value.get_i32())))
                }
            }

            MetadataValueType::I64 => {
                if self.value.len() < 8 {
                    Err(anyhow!("Not enough bytes for a i64"))
                } else {
                    Ok((key, MetadataValue::I64(self.value.get_i64())))
                }
            }

            MetadataValueType::Bool => {
                if self.value.is_empty() {
                    Err(anyhow!("Not enough bytes for a bool"))
                } else {
                    let value = match self.value.get_u8() {
                        0 => false,
                        1 => true,
                        x => return Err(anyhow!("Invalied boolean value of {x}")),
                    };

                    Ok((key, MetadataValue::Bool(value)))
                }
            }

            MetadataValueType::Bytes => {
                Ok((key, MetadataValue::Bytes(self.value)))
            }
        }
    }
}

impl KlvData {
    pub fn from_iter(
        buffer: &mut BytesMut,
        iterator: impl Iterator<Item = KlvItem>,
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

        Ok(KlvData {
            data: buffer.freeze(),
        })
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
