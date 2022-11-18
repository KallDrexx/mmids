//! This module contains functionality for storing and retrieving metadata about individual
//! media payloads.

mod keys;
mod klv;

use crate::workflows::metadata::klv::{KlvItem, KlvStore};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tracing::error;

pub use keys::{MetadataKey, MetadataKeyMap};

/// Allows storing arbitrary attributes and value pairs that can are relevant to an individual
/// media payload/packet. These are stored in a relatively efficient way to make it cheap to
/// clone and attempt to minimize per-packet heap allocations. Once a metadata collection has been
/// created it cannot be modified.
///
/// The metadata currently relies on being passed in a `BytesMut` buffer that it will use for
/// storage. This allows for the creator of media payloads to maintain an arena style memory
/// buffer that persists across media payloads, which should eventually cause each media payload
/// to no longer require its own heap allocation and efficiently re-use unreserved parts of the
/// memory buffer.
///
/// The trade off for cloning and allocation efficiency is that iterating through metadata is an
/// O(N) operation, which means if you need to look for a specific type of metadata you may have to
/// iterate through all other metadata items first. This tradeoff was deemed acceptable for now
/// with the idea that each payload would only have a small amount of metadata attached to it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MediaPayloadMetadataCollection {
    data: KlvStore,
}

/// Declares what type of data is being stored in a metadata entry
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

/// An actual value stored in a metadata entry
#[derive(Debug, PartialEq, Eq, Clone)]
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

/// An individual key/value paired stored as metadata
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MetadataEntry {
    key: MetadataKey,
    raw_value: Bytes,
}

/// Errors that can occur when creating a metadata entry
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

impl MediaPayloadMetadataCollection {
    /// Creates a new collection of metadata based on the provided entries. A buffer is passed in
    /// which can allow the creators of the collection to maintain an arena to reduce allocations
    /// for each new metadata collection that is created.
    pub fn new(entries: impl Iterator<Item = MetadataEntry>, buffer: &mut BytesMut) -> Self {
        let mut klv_buffer = buffer.split_off(buffer.len());
        let klv_items = entries.map(|e| KlvItem {
            key: e.key.klv_id,
            value: e.raw_value,
        });

        let klv_data = KlvStore::from_iter(&mut klv_buffer, klv_items).unwrap();

        MediaPayloadMetadataCollection { data: klv_data }
    }

    /// Provides a non-consuming iterator that allows reading of entries within the collection
    pub fn iter(&self) -> impl Iterator<Item = MetadataEntry> {
        self.data.iter().map(|item| MetadataEntry {
            key: MetadataKey::from_klv_id(item.key),
            raw_value: item.value,
        })
    }
}

impl MetadataEntry {
    /// Creates a new media metadata payload entry for the key and value pair
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

    /// Retrieves the key from the entry
    pub fn key(&self) -> MetadataKey {
        self.key
    }

    /// Retrieves the value from the entry
    pub fn value(&self) -> MetadataValue {
        // Clone to not advance the original buffer
        let mut buffer = self.raw_value.clone();

        // We shouldn't have to worry about validation as consumers should have only been able
        // to create an entry via a `new()` call, and therefore we are sure the raw value is
        // correct and matches.
        match self.key.value_type {
            MetadataValueType::U8 => MetadataValue::U8(buffer.get_u8()),
            MetadataValueType::U16 => MetadataValue::U16(buffer.get_u16()),
            MetadataValueType::U32 => MetadataValue::U32(buffer.get_u32()),
            MetadataValueType::U64 => MetadataValue::U64(buffer.get_u64()),
            MetadataValueType::I8 => MetadataValue::I8(buffer.get_i8()),
            MetadataValueType::I16 => MetadataValue::I16(buffer.get_i16()),
            MetadataValueType::I32 => MetadataValue::I32(buffer.get_i32()),
            MetadataValueType::I64 => MetadataValue::I64(buffer.get_i64()),
            MetadataValueType::Bytes => MetadataValue::Bytes(buffer),
            MetadataValueType::Bool => match buffer.get_u8() {
                0 => MetadataValue::Bool(false),
                1 => MetadataValue::Bool(true),
                x => panic!("Invalid boolean value of {}", x),
            },
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn can_create_and_get_value_from_u8_metadata_entry() {
        let value = MetadataValue::U8(5);
        let key = MetadataKey {
            klv_id: 15,
            value_type: MetadataValueType::U8,
        };
        let entry = MetadataEntry::new(key, value.clone(), &mut BytesMut::new()).unwrap();
        let returned_value = entry.value();

        assert_eq!(returned_value, value);
    }

    #[test]
    fn can_create_and_get_value_from_u16_metadata_entry() {
        let value = MetadataValue::U16(5);
        let key = MetadataKey {
            klv_id: 15,
            value_type: MetadataValueType::U16,
        };
        let entry = MetadataEntry::new(key, value.clone(), &mut BytesMut::new()).unwrap();
        let returned_value = entry.value();

        assert_eq!(returned_value, value);
    }

    #[test]
    fn can_create_and_get_value_from_u32_metadata_entry() {
        let value = MetadataValue::U32(5);
        let key = MetadataKey {
            klv_id: 15,
            value_type: MetadataValueType::U32,
        };
        let entry = MetadataEntry::new(key, value.clone(), &mut BytesMut::new()).unwrap();
        let returned_value = entry.value();

        assert_eq!(returned_value, value);
    }

    #[test]
    fn can_create_and_get_value_from_u64_metadata_entry() {
        let value = MetadataValue::U64(5);
        let key = MetadataKey {
            klv_id: 15,
            value_type: MetadataValueType::U64,
        };
        let entry = MetadataEntry::new(key, value.clone(), &mut BytesMut::new()).unwrap();
        let returned_value = entry.value();

        assert_eq!(returned_value, value);
    }

    #[test]
    fn can_create_and_get_value_from_i8_metadata_entry() {
        let value = MetadataValue::I8(5);
        let key = MetadataKey {
            klv_id: 15,
            value_type: MetadataValueType::I8,
        };
        let entry = MetadataEntry::new(key, value.clone(), &mut BytesMut::new()).unwrap();
        let returned_value = entry.value();

        assert_eq!(returned_value, value);
    }

    #[test]
    fn can_create_and_get_value_from_i16_metadata_entry() {
        let value = MetadataValue::I16(5);
        let key = MetadataKey {
            klv_id: 15,
            value_type: MetadataValueType::I16,
        };
        let entry = MetadataEntry::new(key, value.clone(), &mut BytesMut::new()).unwrap();
        let returned_value = entry.value();

        assert_eq!(returned_value, value);
    }

    #[test]
    fn can_create_and_get_value_from_i32_metadata_entry() {
        let value = MetadataValue::I32(5);
        let key = MetadataKey {
            klv_id: 15,
            value_type: MetadataValueType::I32,
        };
        let entry = MetadataEntry::new(key, value.clone(), &mut BytesMut::new()).unwrap();
        let returned_value = entry.value();

        assert_eq!(returned_value, value);
    }

    #[test]
    fn can_create_and_get_value_from_i64_metadata_entry() {
        let value = MetadataValue::I64(5);
        let key = MetadataKey {
            klv_id: 15,
            value_type: MetadataValueType::I64,
        };
        let entry = MetadataEntry::new(key, value.clone(), &mut BytesMut::new()).unwrap();
        let returned_value = entry.value();

        assert_eq!(returned_value, value);
    }

    #[test]
    fn can_create_and_get_value_from_bool_metadata_entry() {
        let value = MetadataValue::Bool(true);
        let key = MetadataKey {
            klv_id: 15,
            value_type: MetadataValueType::Bool,
        };
        let entry = MetadataEntry::new(key, value.clone(), &mut BytesMut::new()).unwrap();
        let returned_value = entry.value();

        assert_eq!(returned_value, value);
    }

    #[test]
    fn can_create_and_get_value_from_bytes_metadata_entry() {
        let value = MetadataValue::Bytes(Bytes::from_static(&[1, 2, 3, 4, 5]));
        let key = MetadataKey {
            klv_id: 15,
            value_type: MetadataValueType::Bytes,
        };
        let entry = MetadataEntry::new(key, value.clone(), &mut BytesMut::new()).unwrap();
        let returned_value = entry.value();

        assert_eq!(returned_value, value);
    }

    #[test]
    fn can_create_and_retrieve_media_payload_metadata() {
        let mut buffer = BytesMut::new();
        let mut map = MetadataKeyMap::default();

        let keys = [
            map.register("first", MetadataValueType::U8),
            map.register("second", MetadataValueType::Bool),
            map.register("third", MetadataValueType::Bytes),
        ];

        let values = [
            MetadataValue::U8(5),
            MetadataValue::Bool(true),
            MetadataValue::Bytes(Bytes::from_static(&[1, 2, 3, 4])),
        ];

        let entries = vec![
            MetadataEntry::new(keys[0], values[0].clone(), &mut buffer).unwrap(),
            MetadataEntry::new(keys[1], values[1].clone(), &mut buffer).unwrap(),
            MetadataEntry::new(keys[2], values[2].clone(), &mut buffer).unwrap(),
        ];

        let metadata =
            MediaPayloadMetadataCollection::new(entries.clone().into_iter(), &mut buffer);
        let mut iterator = metadata.iter();

        assert_eq!(
            iterator.next(),
            Some(entries[0].clone()),
            "Unexpected first entry"
        );
        assert_eq!(
            iterator.next(),
            Some(entries[1].clone()),
            "Unexpected second entry"
        );
        assert_eq!(
            iterator.next(),
            Some(entries[2].clone()),
            "Unexpected third entry"
        );
        assert_eq!(iterator.next(), None, "Unexpected fourth entry");
    }

    #[test]
    fn media_payload_metadata_can_be_iterated_multiple_times() {
        let mut buffer = BytesMut::new();
        let mut map = MetadataKeyMap::default();

        let keys = [
            map.register("first", MetadataValueType::U8),
            map.register("second", MetadataValueType::Bool),
        ];

        let values = [MetadataValue::U8(5), MetadataValue::Bool(true)];

        let entries = vec![
            MetadataEntry::new(keys[0], values[0].clone(), &mut buffer).unwrap(),
            MetadataEntry::new(keys[1], values[1].clone(), &mut buffer).unwrap(),
        ];

        let metadata = MediaPayloadMetadataCollection::new(entries.into_iter(), &mut buffer);

        assert_eq!(
            metadata.iter().count(),
            2,
            "Unexpected number of items in iterator"
        );
        assert_eq!(
            metadata.iter().count(),
            2,
            "Unexpected number of items in iterator"
        );
    }
}
