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
use std::sync::Arc;

use typed_builder::TypedBuilder;

use super::{FormatVersion, ManifestContentType, PartitionSpec, Schema};
use crate::error::{Result, invalid_data};
use crate::spec::{PartitionField, SchemaId, SchemaRef};

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

/// The `partition-spec` manifest metadata value. The spec defines it as the
/// partition fields array; the full spec object is accepted defensively
/// because some writers (e.g. iceberg-cpp) emit it.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ManifestPartitionSpec {
    Fields(Vec<PartitionField>),
    Spec {
        #[serde(rename = "spec-id")]
        spec_id: Option<i32>,
        fields: Vec<PartitionField>,
    },
}

impl ManifestMetadata {
    /// Parse from metadata in avro file.
    pub fn parse(meta: &HashMap<String, Vec<u8>>) -> Result<Self> {
        let schema = Arc::new({
            let bs = meta.get("schema").ok_or_else(|| {
                invalid_data!("schema is required in manifest metadata but not found")
            })?;
            serde_json::from_slice::<Schema>(bs).map_err(|err| {
                invalid_data!("Fail to parse schema in manifest metadata").with_source(err)
            })?
        });
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
            let (fields, embedded_spec_id) = {
                let bs = meta.get("partition-spec").ok_or_else(|| {
                    invalid_data!("partition-spec is required in manifest metadata but not found")
                })?;
                match serde_json::from_slice::<ManifestPartitionSpec>(bs).map_err(|err| {
                    invalid_data!("Fail to parse partition spec in manifest metadata")
                        .with_source(err)
                })? {
                    ManifestPartitionSpec::Fields(fields) => (fields, None),
                    ManifestPartitionSpec::Spec { spec_id, fields } => (fields, spec_id),
                }
            };
            let spec_id = meta
                .get("partition-spec-id")
                .map(|bs| {
                    String::from_utf8_lossy(bs).parse().map_err(|err| {
                        invalid_data!("Fail to parse partition spec id in manifest metadata")
                            .with_source(err)
                    })
                })
                .transpose()?;
            let spec_id = match (spec_id, embedded_spec_id) {
                (Some(spec_id), Some(embedded)) if spec_id != embedded => {
                    return Err(invalid_data!(
                        "partition-spec-id {spec_id} does not match spec-id {embedded} embedded in partition-spec of manifest metadata"
                    ));
                }
                (spec_id, embedded) => spec_id.or(embedded).unwrap_or(0),
            };
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use apache_avro::Reader as AvroReader;
    use tempfile::TempDir;

    use super::*;
    use crate::ErrorKind;
    use crate::io::FileIO;
    use crate::spec::{ManifestWriterBuilder, NestedField, PrimitiveType, Transform, Type};

    fn test_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "data",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap()
    }

    fn test_meta(
        partition_spec: &str,
        partition_spec_id: Option<&str>,
    ) -> HashMap<String, Vec<u8>> {
        let mut meta = HashMap::from([
            (
                "schema".to_string(),
                serde_json::to_vec(&test_schema()).unwrap(),
            ),
            ("schema-id".to_string(), b"0".to_vec()),
            (
                "partition-spec".to_string(),
                partition_spec.as_bytes().to_vec(),
            ),
            ("format-version".to_string(), b"2".to_vec()),
            ("content".to_string(), b"data".to_vec()),
        ]);
        if let Some(spec_id) = partition_spec_id {
            meta.insert("partition-spec-id".to_string(), spec_id.as_bytes().to_vec());
        }
        meta
    }

    fn expected_spec(spec_id: i32) -> PartitionSpec {
        PartitionSpec::builder(test_schema())
            .with_spec_id(spec_id)
            .add_partition_field("data", "data_bucket", Transform::Bucket(16))
            .unwrap()
            .build()
            .unwrap()
    }

    const FIELDS: &str =
        r#"[{"source-id":2,"field-id":1000,"name":"data_bucket","transform":"bucket[16]"}]"#;

    #[test]
    fn test_parse_partition_spec_array_form() {
        // The spec form: only the partition fields array.
        let metadata = ManifestMetadata::parse(&test_meta(FIELDS, Some("1"))).unwrap();
        assert_eq!(metadata.partition_spec, expected_spec(1));
    }

    #[test]
    fn test_parse_partition_spec_object_form() {
        // Non-conformant full-spec object, as written by iceberg-cpp.
        let object = format!(r#"{{"spec-id":1,"fields":{FIELDS}}}"#);
        let metadata = ManifestMetadata::parse(&test_meta(&object, Some("1"))).unwrap();
        assert_eq!(metadata.partition_spec, expected_spec(1));

        // Without the `partition-spec-id` key the embedded spec-id is used.
        let metadata = ManifestMetadata::parse(&test_meta(&object, None)).unwrap();
        assert_eq!(metadata.partition_spec, expected_spec(1));

        // `spec-id` is optional in the object form.
        let object = format!(r#"{{"fields":{FIELDS}}}"#);
        let metadata = ManifestMetadata::parse(&test_meta(&object, Some("1"))).unwrap();
        assert_eq!(metadata.partition_spec, expected_spec(1));
    }

    #[test]
    fn test_parse_partition_spec_object_form_spec_id_mismatch() {
        let object = format!(r#"{{"spec-id":2,"fields":{FIELDS}}}"#);
        let err = ManifestMetadata::parse(&test_meta(&object, Some("1"))).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("spec-id"), "{err}");
    }

    #[test]
    fn test_parse_partition_spec_malformed() {
        let err = ManifestMetadata::parse(&test_meta(r#"[{"name":"x"}]"#, Some("1"))).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn test_writer_emits_partition_spec_as_array() {
        let schema = Arc::new(test_schema());
        let partition_spec = expected_spec(1);
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let output_file = FileIO::new_with_fs()
            .new_output(path.to_str().unwrap())
            .unwrap();
        ManifestWriterBuilder::new(output_file, Some(1), schema, partition_spec.clone())
            .build_v2_data()
            .write_manifest_file()
            .await
            .unwrap();

        let bytes = std::fs::read(&path).unwrap();
        let reader = AvroReader::new(bytes.as_slice()).unwrap();
        let meta = reader.user_metadata();
        let value: serde_json::Value = serde_json::from_slice(&meta["partition-spec"]).unwrap();
        assert!(
            value.is_array(),
            "partition-spec must be a JSON array: {value}"
        );
        assert_eq!(meta["partition-spec-id"], b"1");
        assert_eq!(
            ManifestMetadata::parse(meta).unwrap().partition_spec,
            partition_spec
        );
    }
}
