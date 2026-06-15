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

//! Conversion from REST wire content-file objects to
//! [`iceberg::scan::FileScanTask`]s.
//!
//! `FileScanTask`s are built directly through their public builders, so this
//! never needs access to `iceberg`'s crate-private `DataFile` internals. The
//! row-level predicate is the client's own scan filter (already bound), not the
//! server's `residual-filter`, which keeps results identical to native planning.

use iceberg::expr::BoundPredicate;
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{DataFileFormat, Literal, SchemaRef, Struct, TableMetadataRef};
use iceberg::{Error, ErrorKind, Result};
use serde_json::Value;

use super::types::{RestContentFile, RestFileScanTask};

/// Resolved per-scan context needed to materialize tasks.
pub(crate) struct ConvertContext {
    pub(crate) metadata: TableMetadataRef,
    pub(crate) snapshot_schema: SchemaRef,
    pub(crate) project_field_ids: Vec<i32>,
    pub(crate) case_sensitive: bool,
    /// The client's bound scan filter, applied as the per-task row predicate.
    pub(crate) bound_filter: Option<BoundPredicate>,
}

/// Convert a `delete-files[]` entry into a [`FileScanTaskDeleteFile`].
pub(crate) fn to_delete_file(rcf: &RestContentFile) -> FileScanTaskDeleteFile {
    FileScanTaskDeleteFile::builder()
        .with_file_path(rcf.file_path.clone())
        .with_file_size_in_bytes(rcf.file_size_in_bytes)
        .with_file_type(rcf.content.into())
        .with_partition_spec_id(rcf.spec_id)
        .with_equality_ids(rcf.equality_ids.clone())
        .build()
}

/// Convert a `file-scan-tasks[]` entry into a [`FileScanTask`], resolving its
/// delete references against the response's shared `delete-files` list.
pub(crate) fn to_file_scan_task(
    task: RestFileScanTask,
    all_deletes: &[FileScanTaskDeleteFile],
    ctx: &ConvertContext,
) -> Result<FileScanTask> {
    let rcf = task.data_file;

    let spec = ctx
        .metadata
        .partition_spec_by_id(rcf.spec_id)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Scan plan referenced unknown partition spec id {}",
                    rcf.spec_id
                ),
            )
        })?;

    let data_file_format = rcf.file_format.parse::<DataFileFormat>().map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Unsupported data file format {:?} in scan plan",
                rcf.file_format
            ),
        )
        .with_source(e)
    })?;

    let partition = decode_partition(&rcf, spec, &ctx.snapshot_schema)?;

    let deletes = match task.delete_file_references {
        Some(refs) => refs
            .into_iter()
            .map(|idx| {
                all_deletes.get(idx).cloned().ok_or_else(|| {
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
        .with_record_count(Some(rcf.record_count))
        .with_data_file_path(rcf.file_path)
        .with_data_file_format(data_file_format)
        .with_schema(ctx.snapshot_schema.clone())
        .with_project_field_ids(ctx.project_field_ids.clone())
        .with_predicate(ctx.bound_filter.clone())
        .with_deletes(deletes)
        .with_partition(Some(partition))
        .with_partition_spec(Some(spec.clone()))
        .with_name_mapping(None)
        .with_case_sensitive(ctx.case_sensitive)
        .build())
}

/// Decode the wire `partition` value into a [`Struct`] keyed by the spec's
/// partition type. Accepts both the positional-array and field-id-keyed-object
/// encodings; an absent partition yields an empty struct.
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
        // Partitioned table but no partition data supplied: treat all as null.
        return Ok(Struct::from_iter(fields.iter().map(|_| None)));
    };

    let mut literals: Vec<Option<Literal>> = Vec::with_capacity(fields.len());
    match value {
        Value::Array(values) => {
            if values.len() != fields.len() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Partition array has {} values but spec expects {}",
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
                format!("Unexpected partition encoding in scan plan: {other}"),
            ));
        }
    }

    Ok(Struct::from_iter(literals))
}
