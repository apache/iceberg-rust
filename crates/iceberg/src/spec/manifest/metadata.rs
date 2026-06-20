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
use crate::error::Result;
use crate::spec::{PartitionField, SchemaId, SchemaRef, TableMetadataRef};
use crate::{Error, ErrorKind};

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
    /// Parse from metadata in avro file.
    pub fn parse(meta: &HashMap<String, Vec<u8>>) -> Result<Self> {
        Self::parse_with(meta, None)
    }

    /// Parse from the avro file's key-value metadata, preferring the table
    /// metadata's schema and partition spec (looked up by the manifest's
    /// `schema-id` / `partition-spec-id`) over the manifest's self-described
    /// `schema` / `partition-spec` keys.
    ///
    /// A manifest's embedded `schema` key is redundant with the authoritative
    /// table metadata, and some writers (e.g. duckdb-iceberg) store a
    /// non-conformant value there (the manifest_entry Avro record schema rather
    /// than the Iceberg table schema). When `table_metadata` is provided and
    /// contains the referenced schema and spec, those are used and the
    /// manifest's own `schema` / `partition-spec` keys are not parsed — mirroring
    /// iceberg-java's `ManifestReader(specsById)`, whose reading of the schema
    /// from manifest file metadata is deprecated. When `table_metadata` is
    /// `None` (or does not contain the referenced ids) the manifest's own
    /// metadata is parsed, preserving the previous self-describing behaviour.
    pub fn parse_with(
        meta: &HashMap<String, Vec<u8>>,
        table_metadata: Option<&TableMetadataRef>,
    ) -> Result<Self> {
        let schema_id: i32 = meta
            .get("schema-id")
            .map(|bs| {
                String::from_utf8_lossy(bs).parse().map_err(|err| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Fail to parse schema id in manifest metadata",
                    )
                    .with_source(err)
                })
            })
            .transpose()?
            .unwrap_or(0);
        let spec_id: i32 = meta
            .get("partition-spec-id")
            .map(|bs| {
                String::from_utf8_lossy(bs).parse().map_err(|err| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Fail to parse partition spec id in manifest metadata",
                    )
                    .with_source(err)
                })
            })
            .transpose()?
            .unwrap_or(0);
        let format_version = if let Some(bs) = meta.get("format-version") {
            serde_json::from_slice::<FormatVersion>(bs).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Fail to parse format version in manifest metadata",
                )
                .with_source(err)
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

        // Prefer the authoritative table schema + partition spec when available,
        // bypassing the manifest's redundant (and sometimes non-conformant)
        // `schema` / `partition-spec` metadata keys.
        if let Some(table_metadata) = table_metadata
            && let (Some(schema), Some(partition_spec)) = (
                table_metadata.schema_by_id(schema_id),
                table_metadata.partition_spec_by_id(spec_id),
            )
        {
            return Ok(ManifestMetadata {
                schema: schema.clone(),
                schema_id,
                partition_spec: partition_spec.as_ref().clone(),
                format_version,
                content,
            });
        }

        // Fallback: parse the schema + partition spec from the manifest's own
        // key-value metadata (the manifest is self-describing).
        let schema = Arc::new({
            let bs = meta.get("schema").ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "schema is required in manifest metadata but not found",
                )
            })?;
            serde_json::from_slice::<Schema>(bs).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Fail to parse schema in manifest metadata",
                )
                .with_source(err)
            })?
        });
        let fields = {
            let bs = meta.get("partition-spec").ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "partition-spec is required in manifest metadata but not found",
                )
            })?;
            serde_json::from_slice::<Vec<PartitionField>>(bs).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Fail to parse partition spec in manifest metadata",
                )
                .with_source(err)
            })?
        };
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(spec_id)
            .add_unbound_fields(fields.into_iter().map(|f| f.into_unbound()))?
            .build()?;

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
