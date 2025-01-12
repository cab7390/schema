use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

use simd_json::{BorrowedValue, StaticNode};

const MAX_OBJECT_KEYS: usize = 200;
const MAX_STRING_SET_VALUES: usize = 100;
const MAX_STRING_SET_VARIANT_LENGTH: usize = 50;
const CONSDIER_STRING_SET: bool = true;

const CONSIDER_ARRAY_ITEMS: bool = true;
const MAX_ARRAY_ITEMS: usize = 5;

bitflags::bitflags! {
    /// Each bit indicates presence of a certain "base" type.
    /// E.g. STRING | NULL means "Either(String, Null)".

    #[derive(Clone, Debug, Copy)]
    pub struct TypeMask: u32 {
        const STRING     = 0b0000_0000_0001;
        const BOOLEAN    = 0b0000_0000_0010;
        const NULL       = 0b0000_0000_0100;
        const ARRAY      = 0b0000_0000_1000;
        const OBJECT     = 0b0000_0001_0000;

        // For numbers, you can separate them or combine them
        const I64        = 0b0000_0010_0000;
        const U64        = 0b0000_0100_0000;
        const F64        = 0b0000_1000_0000;

        // Absence of any type: Optional(String, Null, etc.)
        const ABSENT     = 0b0001_0000_0000;

        // Type for Object with too many keys
        const LARGE_OBJ  = 0b0010_0000_0000;

        // Type for String Set (if you want to consider it)
        const STRING_SET = 0b0100_0000_0000;

        // You can add more bits as needed
    }
}

/// A unified schema node that can represent multiple primitive types
/// plus an optional object structure. The "Either" concept is stored
/// in `type_mask` as multiple bits set. "Optional" is just `NULL` bit set
/// alongside something else.
#[derive(Clone, Debug)]
pub struct Schema {
    /// Which base types are allowed: String, Number(I64), Number(U64), etc.
    pub type_mask: TypeMask,

    /// If `type_mask` includes "object", then `object_properties` is `Some(...)`.
    /// Otherwise `None`.
    // pub object_properties: Option<BTreeMap<String, Schema>>,
    pub object_properties: Option<HashMap<String, Schema>>,

    // If `type_mask` includes "string_set", then `string_values` is `Some(...)`.
    pub string_values: Option<HashSet<String>>,

    // / If `type_mask` includes "array" and you need deeper array validation
    // / (like "array of X"), you could store that schema here.
    pub array_items: Option<Box<Schema>>,
}

#[inline]
pub fn infer_type(value: &BorrowedValue) -> Schema {
    match value {
        BorrowedValue::Static(static_node) => match static_node {
            StaticNode::I64(_) => Schema::new(TypeMask::I64),
            StaticNode::U64(_) => Schema::new(TypeMask::U64),
            StaticNode::F64(_) => Schema::new(TypeMask::F64),
            StaticNode::Bool(_) => Schema::new(TypeMask::BOOLEAN),
            StaticNode::Null => Schema::new(TypeMask::NULL),
        },
        BorrowedValue::String(value) => {
            // if we're not considering string sets, just return a string
            if !CONSDIER_STRING_SET {
                return Schema::new(TypeMask::STRING);
            }

            // if the string is too long don't bother with a set
            if value.len() > MAX_STRING_SET_VARIANT_LENGTH {
                return Schema::new(TypeMask::STRING);
            }

            // otherwise, add it to the set
            let mut set = HashSet::new();
            set.insert(value.to_string());
            Schema {
                type_mask: TypeMask::STRING_SET,
                object_properties: None,
                string_values: Some(set),
                array_items: None,
            }
        }
        BorrowedValue::Array(arr) => {
            if !CONSIDER_ARRAY_ITEMS {
                return Schema::new(TypeMask::ARRAY);
            }

            let mut schema = Schema::new(TypeMask::ARRAY);
            let mut item_schema: Option<Schema> = None;

            // if the array is too long, don't bother with items
            // if arr.len() > MAX_ARRAY_ITEMS {
            //     return schema;
            // }

            for element in arr.iter().take(MAX_ARRAY_ITEMS) {
                let element_schema = infer_type(element);
                match &mut item_schema {
                    Some(existing) => existing.merge(element_schema),
                    None => item_schema = Some(element_schema),
                }
            }

            schema.array_items = item_schema.map(Box::new);
            schema
        }
        BorrowedValue::Object(inner) => Schema {
            type_mask: TypeMask::OBJECT,
            object_properties: Some(
                inner
                    .iter()
                    .map(|(key, value)| (key.to_string(), infer_type(value)))
                    .collect(),
            ),
            string_values: None,
            array_items: None,
        },
    }
}

impl Schema {
    pub fn new(mask: TypeMask) -> Self {
        Self {
            type_mask: mask,
            object_properties: None,
            string_values: None,
            array_items: None,
        }
    }

    pub fn merge(&mut self, other: Schema) {
        // Special case for string sets (if enabled)
        if CONSDIER_STRING_SET {
            if self.type_mask.contains(TypeMask::STRING_SET)
                && other.type_mask.contains(TypeMask::STRING)
                || self.type_mask.contains(TypeMask::STRING)
                    && other.type_mask.contains(TypeMask::STRING_SET)
            {
                self.type_mask &= !TypeMask::STRING_SET;
                self.type_mask |= TypeMask::STRING;
                self.string_values = None;
            } else if self.type_mask.contains(TypeMask::STRING_SET)
                && other.type_mask.contains(TypeMask::STRING_SET)
            {
                let mut string_values_set = std::mem::take(&mut self.string_values);
                if let Some(self_values) = &mut string_values_set {
                    if let Some(other_values) = other.string_values {
                        if self_values.len() + other_values.len() > MAX_STRING_SET_VALUES {
                            self.type_mask &= !TypeMask::STRING_SET;
                            self.type_mask |= TypeMask::STRING;
                            self.string_values = None;
                            // return;
                        } else {
                            self_values.extend(other_values);
                        }
                    }
                } else {
                    self.string_values = other.string_values;
                }
                self.string_values = string_values_set;
            } else {
                self.type_mask |= other.type_mask;
            }
        } else {
            self.type_mask |= other.type_mask;
        }

        // Special case for arrays
        if CONSIDER_ARRAY_ITEMS
            && self.type_mask.contains(TypeMask::ARRAY)
            && other.type_mask.contains(TypeMask::ARRAY)
        {
            match (&mut self.array_items, other.array_items) {
                (Some(self_items), Some(other_items)) => {
                    // Merge the two item schemas
                    self_items.merge(*other_items);
                }
                (None, Some(other_items)) => {
                    // If one side never inferred any item schema,
                    // we can just take the other's item schema.
                    self.array_items = Some(other_items);
                }
                (Some(_), None) => {
                    // If the other side had an empty array or never inferred items,
                    // do nothing. We keep our existing item schema.
                }
                (None, None) => {}
            }
        }

        if self.type_mask.contains(TypeMask::OBJECT) {
            if let Some(self_props) = &self.object_properties {
                if self_props.len() > MAX_OBJECT_KEYS {
                    self.type_mask &= !TypeMask::OBJECT; // remove object
                    self.type_mask |= TypeMask::LARGE_OBJ; // add large object
                    self.object_properties = None; // remove properties
                    return;
                }
            }
        }

        match (&mut self.object_properties, other.object_properties) {
            (Some(self_props), Some(other_props)) => {
                // we have to handle from both sides to account for absent keys in either

                let mut leftover_self_props = std::mem::take(self_props);

                // let mut new_props = HashMap::new();
                for (key, mut other_prop) in other_props {
                    match leftover_self_props.remove(&key) {
                        Some(mut self_prop) => {
                            self_prop.merge(other_prop);
                            self_props.insert(key, self_prop);
                        }
                        None => {
                            other_prop.type_mask |= TypeMask::ABSENT;
                            self_props.insert(key, other_prop);
                        }
                    }
                }

                for (key, mut self_prop) in leftover_self_props {
                    self_prop.type_mask |= TypeMask::ABSENT;
                    self_props.insert(key, self_prop);
                }
            }
            (None, Some(mut other_props)) => {
                for other_prop in other_props.values_mut() {
                    other_prop.type_mask |= TypeMask::ABSENT;
                }
                self.object_properties = Some(other_props);
            }
            (Some(self_props), None) => {
                let mut self_props = std::mem::take(self_props);
                for self_prop in self_props.values_mut() {
                    self_prop.type_mask |= TypeMask::ABSENT;
                }
                self.object_properties = Some(self_props);
            }
            _ => {}
        }
    }
}