mod keys;
mod klv;

use crate::workflows::metadata::klv::{KlvData, KlvItem};
use bytes::{BufMut, Bytes, BytesMut};
use tracing::error;

pub use keys::{MetadataKey, MetadataKeyMap};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MediaPayloadMetadata {
    data: KlvData,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum MetadataValueType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    Bytes,
    Bool,
}

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

pub struct MetadataEntry {
    key: MetadataKey,
    raw_value: Bytes,
}

#[derive(thiserror::Error, Debug)]
pub enum MetadataEntryError {
    #[error(
        "Metadata entry's value was {value:?} but the type was expected to be {expected_type:?}"
    )]
    ValueDoesNotMatchType {
        value: MetadataValue,
        expected_type: MetadataValueType,
    },

    #[error("Entry's data was too large, and must be under 65,535")]
    ValueTooLarge,
}

impl MediaPayloadMetadata {
    pub fn new(entries: impl Iterator<Item = MetadataEntry>, buffer: &mut BytesMut) -> Self {
        let mut klv_buffer = buffer.split_off(buffer.len());
        let klv_items = entries.map(|e| KlvItem {
            key: e.key.klv_id,
            value: e.raw_value,
        });

        let klv_data = KlvData::from_iter(&mut klv_buffer, klv_items).unwrap();

        MediaPayloadMetadata { data: klv_data }
    }

    pub fn iter(&self) -> impl Iterator<Item = MetadataEntry> {
        self.data.iter().map(|item| MetadataEntry {
            key: MetadataKey::from_klv_id(item.key),
            raw_value: item.value,
        })
    }
}

impl MetadataEntry {
    pub fn new(
        key: MetadataKey,
        value: MetadataValue,
        buffer: &mut BytesMut,
    ) -> Result<Self, MetadataEntryError> {
        let mut buffer = buffer.split_off(buffer.len());
        match value {
            MetadataValue::U8(num) => {
                if key.value_type != MetadataValueType::U8 {
                    return Err(MetadataEntryError::ValueDoesNotMatchType {
                        value,
                        expected_type: MetadataValueType::U8,
                    });
                }

                buffer.put_u8(num);
            }

            MetadataValue::U16(num) => {
                if key.value_type != MetadataValueType::U16 {
                    return Err(MetadataEntryError::ValueDoesNotMatchType {
                        value,
                        expected_type: MetadataValueType::U16,
                    });
                }

                buffer.put_u16(num);
            }

            MetadataValue::U32(num) => {
                if key.value_type != MetadataValueType::U32 {
                    return Err(MetadataEntryError::ValueDoesNotMatchType {
                        value,
                        expected_type: MetadataValueType::U32,
                    });
                }

                buffer.put_u32(num);
            }

            MetadataValue::U64(num) => {
                if key.value_type != MetadataValueType::U64 {
                    return Err(MetadataEntryError::ValueDoesNotMatchType {
                        value,
                        expected_type: MetadataValueType::U64,
                    });
                }

                buffer.put_u64(num);
            }

            MetadataValue::I8(num) => {
                if key.value_type != MetadataValueType::I8 {
                    return Err(MetadataEntryError::ValueDoesNotMatchType {
                        value,
                        expected_type: MetadataValueType::I8,
                    });
                }

                buffer.put_i8(num);
            }

            MetadataValue::I16(num) => {
                if key.value_type != MetadataValueType::I16 {
                    return Err(MetadataEntryError::ValueDoesNotMatchType {
                        value,
                        expected_type: MetadataValueType::I16,
                    });
                }

                buffer.put_i16(num);
            }

            MetadataValue::I32(num) => {
                if key.value_type != MetadataValueType::I32 {
                    return Err(MetadataEntryError::ValueDoesNotMatchType {
                        value,
                        expected_type: MetadataValueType::I32,
                    });
                }

                buffer.put_i32(num);
            }

            MetadataValue::I64(num) => {
                if key.value_type != MetadataValueType::I64 {
                    return Err(MetadataEntryError::ValueDoesNotMatchType {
                        value,
                        expected_type: MetadataValueType::I64,
                    });
                }

                buffer.put_i64(num);
            }

            MetadataValue::Bool(boolean) => {
                if key.value_type != MetadataValueType::Bool {
                    return Err(MetadataEntryError::ValueDoesNotMatchType {
                        value,
                        expected_type: MetadataValueType::Bool,
                    });
                }

                buffer.put_u8(boolean.into());
            }

            MetadataValue::Bytes(bytes) => {
                if key.value_type != MetadataValueType::Bytes {
                    return Err(MetadataEntryError::ValueDoesNotMatchType {
                        value: MetadataValue::Bytes(bytes),
                        expected_type: MetadataValueType::Bytes,
                    });
                }

                buffer.put(bytes);
            }
        }

        if buffer.len() >= u16::MAX as usize {
            return Err(MetadataEntryError::ValueTooLarge);
        }

        Ok(MetadataEntry {
            key,
            raw_value: buffer.freeze(),
        })
    }
}
