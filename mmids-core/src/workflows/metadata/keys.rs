use std::collections::HashMap;
use crate::workflows::metadata::MetadataValueType;

#[derive(PartialEq, Eq, Clone, Copy)]
pub struct MetadataKey {
    pub(super) klv_id: u16,
    pub(super) value_type: MetadataValueType,
}

#[derive(Default)]
pub struct MetadataKeyMap {
    name_to_key_map: HashMap<MetadataNameTypePair, MetadataKey>,
    klv_id_to_value_type_map: HashMap<u16, MetadataValueType>,
    next_id: u16,
}

#[derive(PartialEq, Eq, Hash)]
struct MetadataNameTypePair {
    name: &'static str,
    value_type: MetadataValueType,
}

impl MetadataKeyMap {
    pub fn register(&mut self, name: &'static str, value_type: MetadataValueType) -> MetadataKey {
        let name_type_pair = MetadataNameTypePair {name, value_type};
        match self.name_to_key_map.get(&name_type_pair) {
            Some(key) => *key,
            None => {
                let id = self.next_id;
                self.next_id = match self.next_id.checked_add(1) {
                    Some(num) => num,
                    None => panic!("Too many items added to key map, only 65,535 are allowed"),
                };

                let key = MetadataKey {
                    klv_id: id,
                    value_type,
                };

                self.name_to_key_map.insert(name_type_pair, key);
                self.klv_id_to_value_type_map.insert(key.klv_id, value_type);

                key
            }
        }
    }

    pub fn get_key(&self, name: &'static str, value_type: MetadataValueType) -> Option<MetadataKey> {
        let name_type_pair = MetadataNameTypePair {name, value_type};
        self.name_to_key_map.get(&name_type_pair).copied()
    }

    pub(super) fn get_type_for_klv_id(&self, klv_id: u16) -> Option<MetadataValueType> {
        self.klv_id_to_value_type_map.get(&klv_id).copied()
    }
}

