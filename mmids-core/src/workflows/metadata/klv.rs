use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub struct KlvItem {
    pub key: u16,
    pub value: Bytes,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct KlvData {
    data: Bytes,
}

/// Iterates through KLV data in order. This is a separate structure from `KlvData` to allow a
/// single instance of `KlvData` to be iterated more than once.
pub struct KlvIterator {
    data: Bytes,
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
