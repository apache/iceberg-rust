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

//! Decode REST content-file JSON into [`FileScanTask`]s.
//!
//! REST `data-file` / `delete-files` entries are Iceberg content-file JSON
//! (`file-path`, `file-format`, kebab-case). That is not the Avro snake_case
//! payload [`iceberg::spec::deserialize_data_file_from_json`] expects.

use iceberg::expr::BoundPredicate;
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{
    DataContentType, DataFileFormat, Literal, NameMapping, SchemaRef, Struct, TableMetadataRef,
};
use iceberg::{Error, ErrorKind, Result};
use serde::Deserialize;
use serde_json::Value;

use crate::scan_planning::RestFileScanTask;

/// Per-scan context needed to materialize tasks.
pub(crate) struct ConvertContext {
    pub(crate) metadata: TableMetadataRef,
    pub(crate) snapshot_schema: SchemaRef,
    pub(crate) project_field_ids: Vec<i32>,
    pub(crate) case_sensitive: bool,
    pub(crate) bound_filter: Option<BoundPredicate>,
    pub(crate) name_mapping: Option<std::sync::Arc<NameMapping>>,
    pub(crate) unified_partition_type: Option<std::sync::Arc<iceberg::spec::StructType>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RestContentFile {
    file_path: String,
    file_format: String,
    file_size_in_bytes: u64,
    #[serde(default)]
    record_count: Option<u64>,
    #[serde(default)]
    content: Option<i32>,
    #[serde(default)]
    spec_id: i32,
    #[serde(default)]
    partition: Option<Value>,
    #[serde(default)]
    equality_ids: Option<Vec<i32>>,
    #[serde(default)]
    key_metadata: Option<Value>,
    #[serde(default)]
    first_row_id: Option<i64>,
}

/// Decode expanded REST file-scan-tasks and delete-files into domain tasks.
pub(crate) fn decode_scan_tasks(
    files: Vec<RestFileScanTask>,
    delete_files: Vec<Value>,
    ctx: &ConvertContext,
) -> Result<Vec<FileScanTask>> {
    if files.is_empty() && delete_files.is_empty() {
        return Ok(Vec::new());
    }

    let deletes = delete_files
        .iter()
        .map(to_delete_file)
        .collect::<Result<Vec<_>>>()?;

    files
        .into_iter()
        .map(|task| to_file_scan_task(task, &deletes, ctx))
        .collect()
}

fn parse_content_file(value: &Value) -> Result<RestContentFile> {
    serde_json::from_value(value.clone()).map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            "failed to decode REST content-file JSON",
        )
        .with_source(e)
    })
}

fn to_delete_file(value: &Value) -> Result<FileScanTaskDeleteFile> {
    let rcf = parse_content_file(value)?;
    let content = rcf
        .content
        .map(DataContentType::try_from)
        .transpose()?
        .unwrap_or(DataContentType::PositionDeletes);
    Ok(FileScanTaskDeleteFile::builder()
        .with_file_path(rcf.file_path)
        .with_file_size_in_bytes(rcf.file_size_in_bytes)
        .with_file_type(content)
        .with_partition_spec_id(rcf.spec_id)
        .with_equality_ids(rcf.equality_ids)
        .with_key_metadata(decode_key_metadata(rcf.key_metadata.as_ref()))
        .build())
}

fn to_file_scan_task(
    task: RestFileScanTask,
    all_deletes: &[FileScanTaskDeleteFile],
    ctx: &ConvertContext,
) -> Result<FileScanTask> {
    let rcf = parse_content_file(&task.data_file)?;
    let spec = ctx
        .metadata
        .partition_spec_by_id(rcf.spec_id)
        .cloned()
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "scan plan referenced unknown partition spec id {}",
                    rcf.spec_id
                ),
            )
        })?;

    let data_file_format = rcf.file_format.parse::<DataFileFormat>().map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!(
                "unsupported data file format {:?} in scan plan",
                rcf.file_format
            ),
        )
        .with_source(e)
    })?;

    let partition = decode_partition(&rcf, spec.as_ref(), &ctx.snapshot_schema)?;
    let deletes = match task.delete_file_references {
        Some(refs) => refs
            .into_iter()
            .map(|idx| {
                let usize_idx = usize::try_from(idx).map_err(|_| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("delete-file-reference {idx} out of range"),
                    )
                })?;
                all_deletes.get(usize_idx).cloned().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("delete-file-reference {idx} out of range"),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
    };

    Ok(FileScanTask::builder()
        .with_file_size_in_bytes(rcf.file_size_in_bytes)
        .with_start(0)
        .with_length(rcf.file_size_in_bytes)
        .with_record_count(rcf.record_count)
        .with_first_row_id(rcf.first_row_id)
        .with_data_file_path(rcf.file_path)
        .with_data_file_format(data_file_format)
        .with_schema(ctx.snapshot_schema.clone())
        .with_project_field_ids(ctx.project_field_ids.clone())
        .with_predicate(ctx.bound_filter.clone())
        .with_deletes(deletes)
        .with_partition(Some(partition))
        .with_partition_spec(Some(spec))
        .with_name_mapping(ctx.name_mapping.clone())
        .with_unified_partition_type(ctx.unified_partition_type.clone())
        .with_case_sensitive(ctx.case_sensitive)
        .with_key_metadata(decode_key_metadata(rcf.key_metadata.as_ref()))
        .build())
}

fn decode_partition(
    rcf: &RestContentFile,
    spec: &iceberg::spec::PartitionSpec,
    schema: &SchemaRef,
) -> Result<Struct> {
    let partition_type = spec.partition_type(schema)?;
    let fields = partition_type.fields();
    if fields.is_empty() {
        return Ok(Struct::empty());
    }

    let Some(value) = rcf.partition.as_ref() else {
        return Ok(Struct::from_iter(fields.iter().map(|_| None)));
    };

    let mut literals: Vec<Option<Literal>> = Vec::with_capacity(fields.len());
    match value {
        Value::Array(values) => {
            if values.len() != fields.len() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "partition array has {} values but spec expects {}",
                        values.len(),
                        fields.len()
                    ),
                ));
            }
            for (field, v) in fields.iter().zip(values) {
                literals.push(Literal::try_from_json(v.clone(), &field.field_type)?);
            }
        }
        Value::Object(map) => {
            for field in fields {
                let v = map
                    .get(&field.id.to_string())
                    .cloned()
                    .unwrap_or(Value::Null);
                literals.push(Literal::try_from_json(v, &field.field_type)?);
            }
        }
        Value::Null => {
            return Ok(Struct::from_iter(fields.iter().map(|_| None)));
        }
        other => {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("unexpected partition encoding in scan plan: {other}"),
            ));
        }
    }

    Ok(Struct::from_iter(literals))
}

fn decode_key_metadata(value: Option<&Value>) -> Option<Box<[u8]>> {
    match value {
        Some(Value::Array(values)) => {
            let bytes: Vec<u8> = values
                .iter()
                .filter_map(|v| v.as_u64().map(|n| n as u8))
                .collect();
            if bytes.is_empty() {
                None
            } else {
                Some(bytes.into_boxed_slice())
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{DataContentType, DataFileFormat, TableMetadataRef};
    use serde_json::json;

    use super::*;
    use crate::scan_planning::RestFileScanTask;

    fn unpartitioned_metadata() -> TableMetadataRef {
        let json = r#"{
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000001",
            "location": "s3://bucket/t",
            "last-sequence-number": 1,
            "last-updated-ms": 1,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "int"}
                ]
            }],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "current-snapshot-id": -1,
            "snapshots": []
        }"#;
        Arc::new(serde_json::from_str(json).unwrap())
    }

    fn ctx() -> ConvertContext {
        let metadata = unpartitioned_metadata();
        let schema: SchemaRef = metadata.current_schema().clone();
        ConvertContext {
            metadata,
            snapshot_schema: schema,
            project_field_ids: vec![1],
            case_sensitive: true,
            bound_filter: None,
            name_mapping: None,
            unified_partition_type: None,
        }
    }

    #[test]
    fn empty_plan_decodes_to_no_tasks() {
        let tasks = decode_scan_tasks(vec![], vec![], &ctx()).unwrap();
        assert!(tasks.is_empty());
    }

    #[test]
    fn data_file_json_becomes_file_scan_task() {
        let file = RestFileScanTask {
            data_file: json!({
                "content": 0,
                "file-path": "s3://bucket/f.parquet",
                "file-format": "PARQUET",
                "spec-id": 0,
                "partition": {},
                "record-count": 7,
                "file-size-in-bytes": 128
            }),
            delete_file_references: None,
            residual_filter: None,
        };
        let tasks = decode_scan_tasks(vec![file], vec![], &ctx()).unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].data_file_path, "s3://bucket/f.parquet");
        assert_eq!(tasks[0].file_size_in_bytes, 128);
        assert_eq!(tasks[0].length, 128);
        assert_eq!(tasks[0].data_file_format, DataFileFormat::Parquet);
        assert_eq!(tasks[0].record_count, Some(7));
    }

    #[test]
    fn delete_file_references_are_resolved() {
        let file = RestFileScanTask {
            data_file: json!({
                "file-path": "s3://bucket/f.parquet",
                "file-format": "parquet",
                "file-size-in-bytes": 10
            }),
            delete_file_references: Some(vec![0]),
            residual_filter: None,
        };
        let deletes = vec![json!({
            "content": 1,
            "file-path": "s3://bucket/d.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 3
        })];
        let tasks = decode_scan_tasks(vec![file], deletes, &ctx()).unwrap();
        assert_eq!(tasks[0].deletes.len(), 1);
        assert_eq!(tasks[0].deletes[0].file_path, "s3://bucket/d.parquet");
        assert_eq!(
            tasks[0].deletes[0].file_type,
            DataContentType::PositionDeletes
        );
    }

    #[test]
    fn unknown_spec_id_is_data_invalid() {
        let file = RestFileScanTask {
            data_file: json!({
                "file-path": "s3://bucket/f.parquet",
                "file-format": "parquet",
                "file-size-in-bytes": 10,
                "spec-id": 99
            }),
            delete_file_references: None,
            residual_filter: None,
        };
        let err = decode_scan_tasks(vec![file], vec![], &ctx()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }
}
