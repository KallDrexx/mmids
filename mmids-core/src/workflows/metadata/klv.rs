//! Key-length-value encoding of byte data. Allows storing a set of data in a single contiguous
//! `Bytes` collection, enabling the storing of different types of data in re-usable memory
//! arenas, and being cheap to clone.

use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// An individual Key-Length-Value item
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct KlvItem {
    /// Identifies the item in the KLV set. This should be unique for each logical type of
    /// data being stored, although the key can be re-used if multiple values for the same type
    /// of data exists (e.g. an array of values of the same type).
    pub key: u16,
    pub value: Bytes,
}

/// Storage of the KLV data
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct KlvStore {
    data: Bytes,
}

/// Iterates through KLV data in order. This is a separate structure from `KlvData` to allow a
/// single instance of `KlvData` to be iterated more than once.
pub struct KlvIterator {
    data: Bytes,
}

impl KlvStore {
    /// Creates a new `KlvData` structure from an iterator of items. Items are stored in the order
    /// they are returned in the iterator.
    ///
    /// This function takes in a  buffer that it should use to fill. This enables re-use of an
    /// existing buffer arena to  prevent allocations for this data if we can fit it in an existing
    /// and unused `BytesMut` storage.
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

        Ok(KlvStore {
            data: buffer.freeze(),
        })
    }

    /// Creates a new iterator that goes through the items in the KLV store. This is guaranteed
    /// to be in the same order that they were added in.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_create_and_read_items_from_klv_store() {
        let items = [
            KlvItem {
                key: 1,
                value: Bytes::from_static(&[1, 2, 3]),
            },
            KlvItem {
                key: 202,
                value: Bytes::from_static(&[4, 5]),
            },
            KlvItem {
                key: 1,
                value: Bytes::from_static(&[6]),
            },
        ];

        let store = KlvStore::from_iter(&mut BytesMut::new(), items.iter().cloned()).unwrap();
        let mut iterator = store.iter();

        assert_eq!(
            iterator.next(),
            Some(items[0].clone()),
            "Unexpected first item"
        );
        assert_eq!(
            iterator.next(),
            Some(items[1].clone()),
            "Unexpected second item"
        );
        assert_eq!(
            iterator.next(),
            Some(items[2].clone()),
            "Unexpected third item"
        );
        assert_eq!(iterator.next(), None, "Expected no other items");
    }
}
