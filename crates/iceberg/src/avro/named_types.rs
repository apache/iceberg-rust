// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Reading Avro files whose schema defines a named type more than once.
//!
//! The Avro specification allows one definition per full name. iceberg-rust
//! wrote manifests that define a `fixed` or `decimal` type once for every field
//! that uses it, while `apache-avro` 0.21 accepted that. `apache-avro` 0.22
//! rejects such a file's header. Rewriting each repeated definition as a
//! reference to the first one keeps those files readable, because a reference
//! decodes the same bytes as the definition it refers to.

use std::collections::HashMap;
use std::io::Cursor;

use apache_avro::Schema as AvroSchema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::schema::MapSchema;
use apache_avro::types::Value as AvroValue;
use apache_avro::writer::datum::GenericDatumWriter;
use serde_json::{Map, Value as JsonValue};

use crate::Result;
use crate::error::invalid_data;

const MAGIC: &[u8] = b"Obj\x01";
const SCHEMA_KEY: &str = "avro.schema";

/// Rewrites the header of an Avro object container file so that each named type
/// is defined once and referenced by name afterwards. Returns the rewritten
/// file and the full names of the repeated types, or `None` if a repeated
/// definition differs from the first one, which leaves the file ambiguous.
pub(crate) fn define_named_types_once(bs: &[u8]) -> Result<Option<(Vec<u8>, Vec<String>)>> {
    rewrite_schema(bs, reference_repeated_definitions)
}

/// Rewrites the schema in the header of an Avro object container file with
/// `rewrite`, leaving the data blocks as they are. Returns `None` if `rewrite`
/// does.
fn rewrite_schema<T>(
    bs: &[u8],
    rewrite: impl FnOnce(&mut JsonValue) -> Option<T>,
) -> Result<Option<(Vec<u8>, T)>> {
    let body = bs
        .strip_prefix(MAGIC)
        .ok_or_else(|| invalid_data!("Not an Avro object container file"))?;
    // The header's metadata is an Avro `map<bytes>`, followed by the sync marker.
    let metadata_schema = AvroSchema::Map(MapSchema {
        types: Box::new(AvroSchema::Bytes),
        attributes: Default::default(),
    });
    let mut cursor = Cursor::new(body);
    let AvroValue::Map(mut metadata) = GenericDatumReader::builder(&metadata_schema)
        .build()?
        .read_value(&mut cursor)?
    else {
        return Err(invalid_data!("Avro file metadata is not a map"));
    };
    let rest = usize::try_from(cursor.position())
        .ok()
        .and_then(|position| body.get(position..))
        .ok_or_else(|| invalid_data!("Avro file metadata extends past the end of the file"))?;

    let Some(AvroValue::Bytes(schema)) = metadata.get(SCHEMA_KEY) else {
        return Err(invalid_data!("Avro file metadata has no {SCHEMA_KEY}"));
    };
    let mut schema: JsonValue = serde_json::from_slice(schema)?;
    let Some(rewritten) = rewrite(&mut schema) else {
        return Ok(None);
    };
    metadata.insert(
        SCHEMA_KEY.to_string(),
        AvroValue::Bytes(serde_json::to_vec(&schema)?),
    );
    let metadata = GenericDatumWriter::builder(&metadata_schema)
        .build()?
        .write_value_to_vec(AvroValue::Map(metadata))?;

    Ok(Some(([MAGIC, &metadata, rest].concat(), rewritten)))
}

/// Rewrites the header of an Avro object container file so that each reference
/// to a named type repeats its definition, as iceberg-rust wrote manifests with
/// `apache-avro` 0.21.
#[cfg(test)]
pub(crate) fn define_named_types_repeatedly(bs: &[u8]) -> Vec<u8> {
    fn inline_references(schema: &mut JsonValue, defined: &mut HashMap<String, JsonValue>) {
        match schema {
            JsonValue::String(name) => {
                if let Some(definition) = defined.get(name.as_str()) {
                    *schema = definition.clone();
                }
            }
            JsonValue::Array(schemas) => schemas
                .iter_mut()
                .for_each(|schema| inline_references(schema, defined)),
            JsonValue::Object(object) => {
                if let (Some(JsonValue::String(name)), Some("record" | "enum" | "fixed")) = (
                    object.get("name"),
                    object.get("type").and_then(JsonValue::as_str),
                ) {
                    defined.insert(name.clone(), JsonValue::Object(object.clone()));
                }
                for key in ["type", "items", "values", "fields"] {
                    if let Some(child) = object.get_mut(key) {
                        inline_references(child, defined);
                    }
                }
            }
            _ => {}
        }
    }

    rewrite_schema(bs, |schema| {
        inline_references(schema, &mut HashMap::new());
        Some(())
    })
    .unwrap()
    .unwrap()
    .0
}

/// Replaces each repeated definition of a named type in `schema` with its full
/// name. Returns the replaced names, or `None` if a repeated definition differs
/// from the first one.
fn reference_repeated_definitions(schema: &mut JsonValue) -> Option<Vec<String>> {
    let mut defined = HashMap::new();
    let mut repeated = Vec::new();
    reference_repeated(schema, None, &mut defined, &mut repeated).then_some(repeated)
}

/// Walks `schema` in the order Avro resolves names, recording each named type's
/// definition without its name. Returns `false` on a repeated name whose
/// definition differs from the first one.
fn reference_repeated(
    schema: &mut JsonValue,
    namespace: Option<&str>,
    defined: &mut HashMap<String, Map<String, JsonValue>>,
    repeated: &mut Vec<String>,
) -> bool {
    let object = match schema {
        JsonValue::Array(variants) => {
            return variants
                .iter_mut()
                .all(|variant| reference_repeated(variant, namespace, defined, repeated));
        }
        JsonValue::Object(object) => object,
        _ => return true,
    };
    let type_name = match object.get_mut("type") {
        Some(JsonValue::String(type_name)) => type_name.as_str(),
        Some(nested) => return reference_repeated(nested, namespace, defined, repeated),
        None => return true,
    };
    match type_name {
        "record" | "error" | "enum" | "fixed" => {}
        "array" => {
            return object
                .get_mut("items")
                .is_none_or(|items| reference_repeated(items, namespace, defined, repeated));
        }
        "map" => {
            return object
                .get_mut("values")
                .is_none_or(|values| reference_repeated(values, namespace, defined, repeated));
        }
        _ => return true,
    }

    let Some(full_name) = full_name(object, namespace) else {
        // Leave a malformed schema for apache-avro to report.
        return true;
    };
    let mut definition = object.clone();
    definition.remove("name");
    definition.remove("namespace");
    if let Some(first) = defined.get(&full_name) {
        if *first != definition {
            return false;
        }
        *schema = JsonValue::String(full_name.clone());
        repeated.push(full_name);
        return true;
    }
    defined.insert(full_name.clone(), definition);

    let record_namespace = full_name.rsplit_once('.').map(|(namespace, _)| namespace);
    match object.get_mut("fields") {
        Some(JsonValue::Array(fields)) => fields.iter_mut().all(|field| {
            field.get_mut("type").is_none_or(|field_type| {
                reference_repeated(field_type, record_namespace, defined, repeated)
            })
        }),
        _ => true,
    }
}

/// The full name of a named type, following the Avro specification: a name
/// containing a dot is already full, and otherwise the type's own `namespace`
/// or the enclosing namespace qualifies it.
fn full_name(object: &Map<String, JsonValue>, enclosing_namespace: Option<&str>) -> Option<String> {
    let name = object.get("name")?.as_str()?;
    if name.contains('.') {
        return Some(name.to_string());
    }
    let namespace = match object.get("namespace") {
        Some(namespace) => namespace.as_str(),
        None => enclosing_namespace,
    };
    Some(match namespace {
        Some(namespace) if !namespace.is_empty() => format!("{namespace}.{name}"),
        _ => name.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn decimal_10_2() -> JsonValue {
        json!({"type": "fixed", "name": "decimal_10_2", "size": 5,
               "logicalType": "decimal", "precision": 10, "scale": 2})
    }

    #[test]
    fn test_reference_repeated_definitions() {
        let mut schema = json!({"type": "record", "name": "r102", "fields": [
            {"name": "a", "type": ["null", decimal_10_2()]},
            {"name": "b", "type": ["null", decimal_10_2()]},
            {"name": "c", "type": {"type": "array", "items": decimal_10_2()}},
        ]});

        let repeated = reference_repeated_definitions(&mut schema);

        assert_eq!(
            repeated,
            Some(vec!["decimal_10_2".to_string(), "decimal_10_2".to_string()])
        );
        assert_eq!(
            schema,
            json!({"type": "record", "name": "r102", "fields": [
                {"name": "a", "type": ["null", decimal_10_2()]},
                {"name": "b", "type": ["null", "decimal_10_2"]},
                {"name": "c", "type": {"type": "array", "items": "decimal_10_2"}},
            ]})
        );
    }

    #[test]
    fn test_reference_repeated_definitions_leaves_unique_names() {
        let original = json!({"type": "record", "name": "r", "fields": [
            {"name": "a", "type": decimal_10_2()},
            {"name": "b", "type": "decimal_10_2"},
            {"name": "c", "type": {"type": "fixed", "name": "fixed_4", "size": 4}},
        ]});
        let mut schema = original.clone();

        assert_eq!(reference_repeated_definitions(&mut schema), Some(vec![]));
        assert_eq!(schema, original);
    }

    #[test]
    fn test_reference_repeated_definitions_uses_full_names() {
        // The same short name in two namespaces names two different types.
        let mut schema = json!({"type": "record", "name": "r", "fields": [
            {"name": "a", "type": {"type": "fixed", "name": "f", "namespace": "x", "size": 4}},
            {"name": "b", "type": {"type": "fixed", "name": "f", "namespace": "y", "size": 8}},
            {"name": "c", "type": {"type": "fixed", "name": "x.f", "size": 4}},
            {"name": "d", "type": {"type": "record", "name": "n", "namespace": "x", "fields": [
                {"name": "e", "type": {"type": "fixed", "name": "f", "size": 4}},
            ]}},
        ]});

        let repeated = reference_repeated_definitions(&mut schema);

        assert_eq!(repeated, Some(vec!["x.f".to_string(), "x.f".to_string()]));
        assert_eq!(schema["fields"][2]["type"], json!("x.f"));
        assert_eq!(
            schema["fields"][3]["type"]["fields"][0]["type"],
            json!("x.f")
        );
    }

    #[test]
    fn test_reference_repeated_definitions_rejects_different_definitions() {
        let mut schema = json!({"type": "record", "name": "r", "fields": [
            {"name": "a", "type": {"type": "fixed", "name": "f", "size": 4}},
            {"name": "b", "type": {"type": "fixed", "name": "f", "size": 8}},
        ]});

        assert_eq!(reference_repeated_definitions(&mut schema), None);
    }
}
