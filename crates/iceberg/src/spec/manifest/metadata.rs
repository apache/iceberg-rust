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

use std::collections::HashMap;

use typed_builder::TypedBuilder;

use super::{FormatVersion, ManifestContentType, PartitionSpec, Schema};
use crate::error::{Result, invalid_data};
use crate::spec::{PartitionField, SchemaId, SchemaRef};
use crate::util::parsed_cache::ParsedCache;

/// Table schemas parsed from manifest metadata, keyed by the exact `schema`
/// JSON bytes. The manifests of a table version all embed the same schema.
static SCHEMAS: ParsedCache<Schema> = ParsedCache::new();

/// Meta data of a manifest that is stored in the key-value metadata of the Avro file
#[derive(Debug, PartialEq, Clone, Eq, TypedBuilder)]
pub struct ManifestMetadata {
    /// The table schema at the time the manifest
    /// was written
    pub schema: SchemaRef,
    /// ID of the schema used to write the manifest as a string
    pub schema_id: SchemaId,
    /// The partition spec used to write the manifest
    pub partition_spec: PartitionSpec,
    /// Table format version number of the manifest as a string
    pub format_version: FormatVersion,
    /// Type of content files tracked by the manifest: “data” or “deletes”
    pub content: ManifestContentType,
}

impl ManifestMetadata {
    /// Parse from metadata in avro file. Manifests that embed the same schema
    /// JSON share one parsed [`SchemaRef`].
    pub fn parse(meta: &HashMap<String, Vec<u8>>) -> Result<Self> {
        let schema = {
            let bs = meta.get("schema").ok_or_else(|| {
                invalid_data!("schema is required in manifest metadata but not found")
            })?;
            SCHEMAS.get_or_parse(bs, || {
                serde_json::from_slice::<Schema>(bs).map_err(|err| {
                    invalid_data!("Fail to parse schema in manifest metadata").with_source(err)
                })
            })?
        };
        let schema_id: i32 = meta
            .get("schema-id")
            .map(|bs| {
                String::from_utf8_lossy(bs).parse().map_err(|err| {
                    invalid_data!("Fail to parse schema id in manifest metadata").with_source(err)
                })
            })
            .transpose()?
            .unwrap_or(0);
        let partition_spec = {
            let fields = {
                let bs = meta.get("partition-spec").ok_or_else(|| {
                    invalid_data!("partition-spec is required in manifest metadata but not found")
                })?;
                serde_json::from_slice::<Vec<PartitionField>>(bs).map_err(|err| {
                    invalid_data!("Fail to parse partition spec in manifest metadata")
                        .with_source(err)
                })?
            };
            let spec_id = meta
                .get("partition-spec-id")
                .map(|bs| {
                    String::from_utf8_lossy(bs).parse().map_err(|err| {
                        invalid_data!("Fail to parse partition spec id in manifest metadata")
                            .with_source(err)
                    })
                })
                .transpose()?
                .unwrap_or(0);
            PartitionSpec::builder(schema.clone())
                .with_spec_id(spec_id)
                .add_unbound_fields(fields.into_iter().map(|f| f.into_unbound()))?
                .build()?
        };
        let format_version = if let Some(bs) = meta.get("format-version") {
            serde_json::from_slice::<FormatVersion>(bs).map_err(|err| {
                invalid_data!("Fail to parse format version in manifest metadata").with_source(err)
            })?
        } else {
            FormatVersion::V1
        };
        let content = if let Some(v) = meta.get("content") {
            let v = String::from_utf8_lossy(v);
            v.parse()?
        } else {
            ManifestContentType::Data
        };
        Ok(ManifestMetadata {
            schema,
            schema_id,
            partition_spec,
            format_version,
            content,
        })
    }

    /// Get the schema of table at the time manifest was written
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Get the ID of schema used to write the manifest
    pub fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// Get the partition spec used to write manifest
    pub fn partition_spec(&self) -> &PartitionSpec {
        &self.partition_spec
    }

    /// Get the table format version
    pub fn format_version(&self) -> &FormatVersion {
        &self.format_version
    }

    /// Get the type of content files tracked by manifest
    pub fn content(&self) -> &ManifestContentType {
        &self.content
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::ErrorKind;
    use crate::spec::{NestedField, PrimitiveType, Type};

    fn test_schema(name: &str) -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                name,
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap()
    }

    /// Avro metadata of a V2 unpartitioned data manifest written with `schema`.
    fn test_meta(schema: &Schema) -> HashMap<String, Vec<u8>> {
        HashMap::from([
            ("schema".to_string(), serde_json::to_vec(schema).unwrap()),
            ("schema-id".to_string(), b"1".to_vec()),
            ("partition-spec".to_string(), b"[]".to_vec()),
            ("partition-spec-id".to_string(), b"0".to_vec()),
            ("format-version".to_string(), b"2".to_vec()),
            ("content".to_string(), b"data".to_vec()),
        ])
    }

    fn expected_metadata(schema: Schema) -> ManifestMetadata {
        let schema = Arc::new(schema);
        ManifestMetadata {
            schema_id: 1,
            partition_spec: PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .build()
                .unwrap(),
            schema,
            format_version: FormatVersion::V2,
            content: ManifestContentType::Data,
        }
    }

    /// Parses `meta` twice in a row, retrying while a parallel test's miss
    /// clears the shared cache between the two parses (see
    /// [`ParsedCache::get_or_parse_twice`]).
    fn parse_twice(meta: &HashMap<String, Vec<u8>>) -> (ManifestMetadata, ManifestMetadata) {
        const ATTEMPTS: usize = 100;
        let mut pair = None;
        for _ in 0..ATTEMPTS {
            let first = ManifestMetadata::parse(meta).unwrap();
            let second = ManifestMetadata::parse(meta).unwrap();
            let hit = Arc::ptr_eq(&first.schema, &second.schema);
            pair = Some((first, second));
            if hit {
                break;
            }
        }
        pair.unwrap()
    }

    #[test]
    fn test_parse_shares_schema_between_manifests_with_same_schema_json() {
        let schema = test_schema("same");

        let (first, second) = parse_twice(&test_meta(&schema));

        assert!(Arc::ptr_eq(&first.schema, &second.schema));
        assert_eq!(first, expected_metadata(schema));
    }

    #[test]
    fn test_parse_keeps_different_schema_json_apart() {
        let schema_a = test_schema("a");
        let schema_b = test_schema("b");

        let parsed_a = ManifestMetadata::parse(&test_meta(&schema_a)).unwrap();
        let parsed_b = ManifestMetadata::parse(&test_meta(&schema_b)).unwrap();

        assert!(!Arc::ptr_eq(&parsed_a.schema, &parsed_b.schema));
        assert_eq!(parsed_a, expected_metadata(schema_a));
        assert_eq!(parsed_b, expected_metadata(schema_b));
    }

    #[test]
    fn test_parse_reports_malformed_schema_json() {
        let mut meta = test_meta(&test_schema("broken"));
        meta.insert("schema".to_string(), b"{\"type\": \"struct\"".to_vec());

        let err = ManifestMetadata::parse(&meta).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("Fail to parse schema in manifest metadata")
        );
    }
}
