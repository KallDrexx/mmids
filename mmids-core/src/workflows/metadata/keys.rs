use crate::workflows::metadata::MetadataValueType;
use std::collections::HashMap;

/// How much to shift a u16 in order to store/read the
const VALUE_TYPE_SHIFT: u16 = 12;

/// Distinctly identifies a single metadata attribute that can have data stored for it.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct MetadataKey {
    pub(super) klv_id: u16,
    pub(super) value_type: MetadataValueType,
}

/// Creates distinct metadata keys for each unique combination of metadata name and value type.
/// If the same name and type pair are registered on the same `MetadataKeyMap` instance, then those
/// keys returned are guaranteed to be equal to each other.
///
/// Keys created between different instances of `MetadataKeyMap` are not guaranteed (nor likely)
/// to be consistent. Therefore, keys should only be compared with keys created by the same map
/// instance, and in most cases you probably only want one instance for the whole process.
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
    /// Creates a new `MetadataKeyMap` instance
    pub fn new() -> Self {
        Default::default()
    }

    /// Registers the supplied metadata name and value type pair with the key map. If this
    /// pair has not been registered yet then a new `MetadataKey` will be generated and returned.
    /// If the same pair has already been registered, then the pre-generated key will be returned
    pub fn register(&mut self, name: &'static str, value_type: MetadataValueType) -> MetadataKey {
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
    id | type_id
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_apply_u8_value_type_to_klv_id() {
        let original_id = 5;
        let id = apply_value_type_to_klv_id(original_id, MetadataValueType::U8);
        assert_ne!(
            id, original_id,
            "Applied id should not have been the same as the original id"
        );

        let value_type = value_type_from_klv_id(id);
        assert_eq!(value_type, MetadataValueType::U8);
    }

    #[test]
    fn can_apply_u16_value_type_to_klv_id() {
        let original_id = 5;
        let id = apply_value_type_to_klv_id(original_id, MetadataValueType::U16);
        assert_ne!(
            id, original_id,
            "Applied id should not have been the same as the original id"
        );

        let value_type = value_type_from_klv_id(id);
        assert_eq!(value_type, MetadataValueType::U16);
    }

    #[test]
    fn can_apply_u32_value_type_to_klv_id() {
        let original_id = 5;
        let id = apply_value_type_to_klv_id(original_id, MetadataValueType::U32);
        assert_ne!(
            id, original_id,
            "Applied id should not have been the same as the original id"
        );

        let value_type = value_type_from_klv_id(id);
        assert_eq!(value_type, MetadataValueType::U32);
    }

    #[test]
    fn can_apply_u64_value_type_to_klv_id() {
        let original_id = 5;
        let id = apply_value_type_to_klv_id(original_id, MetadataValueType::U64);
        assert_ne!(
            id, original_id,
            "Applied id should not have been the same as the original id"
        );

        let value_type = value_type_from_klv_id(id);
        assert_eq!(value_type, MetadataValueType::U64);
    }

    #[test]
    fn can_apply_i8_value_type_to_klv_id() {
        let original_id = 5;
        let id = apply_value_type_to_klv_id(original_id, MetadataValueType::I8);
        assert_ne!(
            id, original_id,
            "Applied id should not have been the same as the original id"
        );

        let value_type = value_type_from_klv_id(id);
        assert_eq!(value_type, MetadataValueType::I8);
    }

    #[test]
    fn can_apply_i16_value_type_to_klv_id() {
        let original_id = 5;
        let id = apply_value_type_to_klv_id(original_id, MetadataValueType::I16);
        assert_ne!(
            id, original_id,
            "Applied id should not have been the same as the original id"
        );

        let value_type = value_type_from_klv_id(id);
        assert_eq!(value_type, MetadataValueType::I16);
    }

    #[test]
    fn can_apply_i32_value_type_to_klv_id() {
        let original_id = 5;
        let id = apply_value_type_to_klv_id(original_id, MetadataValueType::I32);
        assert_ne!(
            id, original_id,
            "Applied id should not have been the same as the original id"
        );

        let value_type = value_type_from_klv_id(id);
        assert_eq!(value_type, MetadataValueType::I32);
    }

    #[test]
    fn can_apply_i64_value_type_to_klv_id() {
        let original_id = 5;
        let id = apply_value_type_to_klv_id(original_id, MetadataValueType::I64);
        assert_ne!(
            id, original_id,
            "Applied id should not have been the same as the original id"
        );

        let value_type = value_type_from_klv_id(id);
        assert_eq!(value_type, MetadataValueType::I64);
    }

    #[test]
    fn can_apply_bool_value_type_to_klv_id() {
        let original_id = 5;
        let id = apply_value_type_to_klv_id(original_id, MetadataValueType::Bool);
        assert_ne!(
            id, original_id,
            "Applied id should not have been the same as the original id"
        );

        let value_type = value_type_from_klv_id(id);
        assert_eq!(value_type, MetadataValueType::Bool);
    }

    #[test]
    fn can_apply_bytes_value_type_to_klv_id() {
        let original_id = 5;
        let id = apply_value_type_to_klv_id(original_id, MetadataValueType::Bytes);
        assert_ne!(
            id, original_id,
            "Applied id should not have been the same as the original id"
        );

        let value_type = value_type_from_klv_id(id);
        assert_eq!(value_type, MetadataValueType::Bytes);
    }

    #[test]
    fn same_name_type_pair_gets_same_key_returned() {
        let name = "test123";

        let mut map = MetadataKeyMap::default();
        let key1 = map.register(name, MetadataValueType::U32);
        let key2 = map.register(name, MetadataValueType::U32);

        assert_eq!(key1, key2);
    }

    #[test]
    fn different_name_gets_different_keys_returned() {
        let name1 = "test123";
        let name2 = "3456";

        let mut map = MetadataKeyMap::default();
        let key1 = map.register(name1, MetadataValueType::Bool);
        let key2 = map.register(name2, MetadataValueType::Bool);

        assert_ne!(key1, key2);
    }

    #[test]
    fn different_type_gets_different_keys_returned() {
        let name = "test123";

        let mut map = MetadataKeyMap::default();
        let key1 = map.register(name, MetadataValueType::Bool);
        let key2 = map.register(name, MetadataValueType::U32);

        assert_ne!(key1, key2);
    }
}
