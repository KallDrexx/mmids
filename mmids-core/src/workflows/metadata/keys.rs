use crate::workflows::metadata::MetadataValueType;
use std::collections::HashMap;

const VALUE_TYPE_SHIFT: u16 = 12;

#[derive(PartialEq, Eq, Clone, Copy)]
pub struct MetadataKey {
    pub(super) klv_id: u16,
    pub(super) value_type: MetadataValueType,
}

#[derive(Default)]
pub struct MetadataKeyMap {
    name_to_key_map: HashMap<MetadataNameTypePair, MetadataKey>,
    next_id: u16,
}

#[derive(PartialEq, Eq, Hash)]
struct MetadataNameTypePair {
    name: &'static str,
    value_type: MetadataValueType,
}

impl MetadataKey {
    pub(super) fn from_klv_id(klv_id: u16) -> Self {
        let value_type = value_type_from_klv_id(klv_id);

        MetadataKey { klv_id, value_type }
    }
}

impl MetadataKeyMap {
    pub fn get_key(&mut self, name: &'static str, value_type: MetadataValueType) -> MetadataKey {
        let name_type_pair = MetadataNameTypePair { name, value_type };
        match self.name_to_key_map.get(&name_type_pair) {
            Some(key) => *key,
            None => {
                let id = apply_value_type_to_klv_id(self.next_id, value_type);
                self.next_id = match self.next_id.checked_add(1) {
                    Some(num) => num,
                    None => panic!("Too many metadata key name and type pairs added to key map, only 4,095 are allowed"),
                };

                let key = MetadataKey {
                    klv_id: id,
                    value_type,
                };

                self.name_to_key_map.insert(name_type_pair, key);

                key
            }
        }
    }
}

fn apply_value_type_to_klv_id(id: u16, value_type: MetadataValueType) -> u16 {
    let mut type_id = match value_type {
        MetadataValueType::U8 => 1,
        MetadataValueType::U16 => 2,
        MetadataValueType::U32 => 3,
        MetadataValueType::U64 => 4,
        MetadataValueType::I8 => 5,
        MetadataValueType::I16 => 6,
        MetadataValueType::I32 => 7,
        MetadataValueType::I64 => 8,
        MetadataValueType::Bool => 9,
        MetadataValueType::Bytes => 10,
    };

    type_id <<= VALUE_TYPE_SHIFT;
    id & type_id
}

fn value_type_from_klv_id(klv_id: u16) -> MetadataValueType {
    let value_type_id = klv_id >> VALUE_TYPE_SHIFT;
    match value_type_id {
        1 => MetadataValueType::U8,
        2 => MetadataValueType::U16,
        3 => MetadataValueType::U32,
        4 => MetadataValueType::U64,
        5 => MetadataValueType::I8,
        6 => MetadataValueType::I16,
        7 => MetadataValueType::I32,
        8 => MetadataValueType::I64,
        9 => MetadataValueType::Bool,
        10 => MetadataValueType::Bytes,
        x => panic!("Unknown value type id of {}", x),
    }
}
