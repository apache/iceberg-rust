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
use serde_json::Value;

use crate::scan_planning::{RestContentFile, RestFileScanTask};

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

/// Decode one ScanTasks payload's file-scan-tasks against that payload's delete-files.
pub(crate) fn decode_scan_tasks(
    files: Vec<RestFileScanTask>,
    delete_files: Vec<RestContentFile>,
    ctx: &ConvertContext,
) -> Result<Vec<FileScanTask>> {
    if files.is_empty() && delete_files.is_empty() {
        return Ok(Vec::new());
    }

    let deletes = delete_files
        .into_iter()
        .map(to_delete_file)
        .collect::<Result<Vec<_>>>()?;

    files
        .into_iter()
        .map(|task| to_file_scan_task(task, &deletes, ctx))
        .collect()
}

fn to_delete_file(rcf: RestContentFile) -> Result<FileScanTaskDeleteFile> {
    let content = match rcf.content {
        Some(DataContentType::PositionDeletes) => DataContentType::PositionDeletes,
        Some(DataContentType::EqualityDeletes) => DataContentType::EqualityDeletes,
        Some(DataContentType::Data) | None => {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "delete file {} is missing a valid content type",
                    rcf.file_path
                ),
            ));
        }
    };
    Ok(FileScanTaskDeleteFile::builder()
        .with_file_path(rcf.file_path)
        .with_file_size_in_bytes(rcf.file_size_in_bytes)
        .with_file_type(content)
        .with_partition_spec_id(rcf.spec_id)
        .with_equality_ids(rcf.equality_ids)
        .with_key_metadata(rcf.key_metadata)
        .build())
}

fn to_file_scan_task(
    task: RestFileScanTask,
    all_deletes: &[FileScanTaskDeleteFile],
    ctx: &ConvertContext,
) -> Result<FileScanTask> {
    let rcf = task.data_file;
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
        .with_key_metadata(rcf.key_metadata)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{DataContentType, DataFileFormat, TableMetadataRef};
    use serde_json::json;

    use super::*;
    use crate::scan_planning::{RestContentFile, RestFileScanTask};

    fn content_file(value: Value) -> RestContentFile {
        serde_json::from_value(value).unwrap()
    }

    fn delete_files(values: Vec<Value>) -> Vec<RestContentFile> {
        values.into_iter().map(content_file).collect()
    }

    fn file_task(data_file: Value) -> RestFileScanTask {
        RestFileScanTask {
            data_file: content_file(data_file),
            delete_file_references: None,
            residual_filter: None,
        }
    }

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
        let file = file_task(json!({
            "content": 0,
            "file-path": "s3://bucket/f.parquet",
            "file-format": "PARQUET",
            "spec-id": 0,
            "partition": {},
            "record-count": 7,
            "file-size-in-bytes": 128
        }));
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
        let mut file = file_task(json!({
            "file-path": "s3://bucket/f.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 10
        }));
        file.delete_file_references = Some(vec![0]);
        let deletes = delete_files(vec![json!({
            "content": 1,
            "file-path": "s3://bucket/d.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 3
        })]);
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
        let file = file_task(json!({
            "file-path": "s3://bucket/f.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 10,
            "spec-id": 99
        }));
        let err = decode_scan_tasks(vec![file], vec![], &ctx()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn content_string_data_decodes() {
        let file = file_task(json!({
            "content": "data",
            "file-path": "s3://bucket/f.parquet",
            "file-format": "PARQUET",
            "spec-id": 0,
            "file-size-in-bytes": 128
        }));
        let tasks = decode_scan_tasks(vec![file], vec![], &ctx()).unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].data_file_path, "s3://bucket/f.parquet");
    }

    #[test]
    fn content_string_position_deletes_decodes() {
        let mut file = file_task(json!({
            "file-path": "s3://bucket/f.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 10
        }));
        file.delete_file_references = Some(vec![0]);
        let deletes = delete_files(vec![json!({
            "content": "position-deletes",
            "file-path": "s3://bucket/d.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 3
        })]);
        let tasks = decode_scan_tasks(vec![file], deletes, &ctx()).unwrap();
        assert_eq!(tasks[0].deletes.len(), 1);
        assert_eq!(tasks[0].deletes[0].file_path, "s3://bucket/d.parquet");
        assert_eq!(
            tasks[0].deletes[0].file_type,
            DataContentType::PositionDeletes
        );
    }

    #[test]
    fn content_integer_ordinals_still_decode() {
        let mut file = file_task(json!({
            "content": 0,
            "file-path": "s3://bucket/f.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 10
        }));
        file.delete_file_references = Some(vec![0]);
        let deletes = delete_files(vec![json!({
            "content": 1,
            "file-path": "s3://bucket/d.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 3
        })]);
        let tasks = decode_scan_tasks(vec![file], deletes, &ctx()).unwrap();
        assert_eq!(
            tasks[0].deletes[0].file_type,
            DataContentType::PositionDeletes
        );
    }

    #[test]
    fn delete_file_without_content_is_data_invalid() {
        let mut file = file_task(json!({
            "file-path": "s3://bucket/f.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 10
        }));
        file.delete_file_references = Some(vec![0]);
        let deletes = delete_files(vec![json!({
            "file-path": "s3://bucket/d.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 3
        })]);
        let err = decode_scan_tasks(vec![file], deletes, &ctx()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn hex_key_metadata_decodes_to_bytes() {
        let file = file_task(json!({
            "file-path": "s3://bucket/f.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 10,
            "key-metadata": "00000000000000000000000000000000"
        }));
        let tasks = decode_scan_tasks(vec![file], vec![], &ctx()).unwrap();
        assert_eq!(tasks[0].key_metadata.as_deref(), Some([0u8; 16].as_slice()));
    }

    #[test]
    fn non_hex_key_metadata_is_rejected() {
        let err = serde_json::from_value::<RestContentFile>(json!({
            "file-path": "s3://bucket/f.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 10,
            "key-metadata": "not-hex"
        }));
        assert!(err.is_err());
    }
}
