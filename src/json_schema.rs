use std::{collections::HashMap, fmt::Display};

use serde::Serialize;

use crate::schema::{Schema, TypeMask};

#[derive(Debug)]
pub enum JsonSchemaType {
    // #[serde(rename = "object")]
    Object,

    // #[serde(rename = "array")]
    Array,

    // #[serde(rename = "string")]
    String,

    // #[serde(rename = "number")]
    Number,

    // #[serde(rename = "boolean")]
    Boolean,

    // #[serde(rename = "null")]
    Null,
}

impl Serialize for JsonSchemaType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let type_str = match self {
            JsonSchemaType::Object => "object",
            JsonSchemaType::Array => "array",
            JsonSchemaType::String => "string",
            JsonSchemaType::Number => "number",
            JsonSchemaType::Boolean => "boolean",
            JsonSchemaType::Null => "null",
        };
        serializer.serialize_str(type_str)
    }
}

#[derive(Debug)]
pub struct JsonSchema {
    // #[serde(skip_serializing_if = "Option::is_none", default)]
    pub description: Option<String>,

    // #[serde(rename = "type", skip_serializing_if = "Vec::is_empty", default)]
    pub schema_type: Vec<JsonSchemaType>,

    // #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub properties: HashMap<String, JsonSchema>,

    pub items: Option<Box<JsonSchema>>,

    // #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub required: Vec<String>,

    // #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub any_of: Vec<JsonSchemaVariant>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum JsonSchemaVariant {
    StringEnum {
        r#type: JsonSchemaType,
        r#enum: Vec<String>,
    },
}

impl Serialize for JsonSchema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(None)?;

        if let Some(description) = &self.description {
            map.serialize_entry("description", description)?;
        }

        // Serialize "type" as a single value if there's only one item, or as an array if there are multiple items
        if !self.schema_type.is_empty() {
            if self.schema_type.len() == 1 {
                map.serialize_entry("type", &self.schema_type[0])?;
            } else {
                map.serialize_entry("type", &self.schema_type)?;
            }
        }

        if !self.properties.is_empty() {
            map.serialize_entry("properties", &self.properties)?;
        }

        if let Some(items) = &self.items {
            map.serialize_entry("items", items)?;
        }

        if !self.required.is_empty() {
            map.serialize_entry("required", &self.required)?;
        }

        if !self.any_of.is_empty() {
            map.serialize_entry("anyOf", &self.any_of)?;
        }

        map.end()
    }
}

#[derive(Debug, Serialize)]
pub struct RootJsonSchema {
    #[serde(rename = "$schema")]
    pub schema: String,
    #[serde(flatten)]
    pub inner: JsonSchema,
}

impl RootJsonSchema {
    pub fn new(schema: Schema) -> Self {
        let json_schema: JsonSchema = schema.into();
        json_schema.into()
    }
}

impl From<Schema> for JsonSchema {
    fn from(schema: Schema) -> JsonSchema {
        let mut result = JsonSchema {
            description: None,
            schema_type: vec![],
            properties: HashMap::new(),
            items: None,
            required: Vec::new(),
            any_of: vec![],
        };

        // Populate the type field based on the schema's type_mask
        if schema.type_mask.contains(TypeMask::ARRAY) {
            result.schema_type.push(JsonSchemaType::Array);
            if let Some(items) = schema.array_items {
                let inner: JsonSchema = (*items).into();
                result.items = Some(Box::new(inner));
            }
        }

        if schema.type_mask.contains(TypeMask::STRING) {
            result.schema_type.push(JsonSchemaType::String);
        }

        if schema.type_mask.contains(TypeMask::I64)
            || schema.type_mask.contains(TypeMask::U64)
            || schema.type_mask.contains(TypeMask::F64)
        {
            result.schema_type.push(JsonSchemaType::Number);
        }

        if schema.type_mask.contains(TypeMask::BOOLEAN) {
            result.schema_type.push(JsonSchemaType::Boolean);
        }

        if schema.type_mask.contains(TypeMask::NULL) {
            result.schema_type.push(JsonSchemaType::Null);
        }

        if schema.type_mask.contains(TypeMask::OBJECT) {
            result.schema_type.push(JsonSchemaType::Object);
        }

        if schema.type_mask.contains(TypeMask::LARGE_OBJ) {
            result.description = Some("Large object".to_string());
        }

        // Handle object properties
        if let Some(object_properties) = schema.object_properties {
            for (key, value) in object_properties {
                // If the value is required, add it to the required list
                if !value.type_mask.contains(TypeMask::ABSENT) {
                    result.required.push(key.clone());
                }

                // Recursively convert nested properties
                result.properties.insert(key, value.into());
            }
        }

        // Handle STRING_SET for anyOf
        if schema.type_mask.contains(TypeMask::STRING_SET) {
            result.schema_type.push(JsonSchemaType::String);
            if let Some(values) = schema.string_values {
                result.any_of.push(JsonSchemaVariant::StringEnum {
                    r#type: JsonSchemaType::String,
                    r#enum: values.into_iter().collect(),
                });
            }
        }

        result
    }
}

impl Display for RootJsonSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let json = serde_json::to_string_pretty(self).map_err(|_| std::fmt::Error)?;
        write!(f, "{}", json)
    }
}

impl From<JsonSchema> for RootJsonSchema {
    fn from(val: JsonSchema) -> Self {
        RootJsonSchema {
            schema: "https://json-schema.org/draft/2020-12/schema".to_string(),
            inner: val,
        }
    }
}