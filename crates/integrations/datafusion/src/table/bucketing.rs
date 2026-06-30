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

//! Distribution of pre-planned [`FileScanTask`]s into per-partition buckets for
//! eager multi-partition scans.
//!
//! Tasks are distributed by *count*: each task is hashed (on its identity
//! partition key when available, otherwise on its data file path) and placed in
//! `hash % n_partitions`. This evens out the number of files per bucket but is
//! unaware of `file_size_in_bytes`, so a table mixing one large file with many
//! small ones can pile most of the bytes into a single bucket and serialize the
//! query on that partition.
//!
//! A size-aware strategy — first-fit-decreasing bin-packing on
//! `file_size_in_bytes` (optionally with a target split size), mirroring
//! iceberg-java's `TableScanUtil.planTaskGroups` / `BinPacking` — would spread
//! the work more evenly. The byte size is already carried on each
//! [`FileScanTask`], so this is a fairly contained extension; it is tracked as a
//! follow-up in <https://github.com/apache/iceberg-rust/issues/128>.

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema, TimeUnit};
use datafusion::common::hash_utils::create_hashes;
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use iceberg::arrow::PrimitiveLiteralArrayBuilder;
use iceberg::scan::FileScanTask;
use iceberg::spec::{Literal, Transform};
use iceberg::table::Table;

/// Identity-partitioned column that is also present in the output projection
/// and whose Arrow type can be reconstructed from a `Literal` for hashing.
pub(super) struct IdentityCol {
    pub(super) name: String,
    /// Position of this column in the *output* schema (after projection).
    pub(super) output_idx: usize,
    /// Position of this column inside the partition spec's `fields()` slice,
    /// matching the slot order of `FileScanTask::partition`.
    pub(super) spec_field_idx: usize,
    pub(super) output_dtype: DataType,
}

/// Inspect the table's default partition spec and return the list of identity
/// columns that can support a [`Partitioning::Hash`] declaration. Returns
/// `None` if any condition is violated:
///   - the source column for an identity field is not in the output projection
///   - the source column's Arrow type is not currently supported by
///     the identity hash materialization path
///   - the table has spec evolution (>1 historical specs), since older files
///     may carry a partition tuple that does not align with the default spec
///
/// Returning `None` forces the scan to declare `UnknownPartitioning` even if
/// bucketing succeeds.
pub(super) fn compute_identity_cols(
    table: &Table,
    output_schema: &ArrowSchema,
) -> Option<Vec<IdentityCol>> {
    let metadata = table.metadata();
    // iceberg-java is less conservative here: it intersects the identity fields
    // present in every spec (`Partitioning.groupingKeyType` /
    // `commonActiveFieldIds`) and still reports a grouping key on the columns
    // that are identity-partitioned across all of them. We deliberately bail
    // out on any spec evolution instead, because the bucketing path aligns each
    // task's partition slot to the *default* spec and `FileScanTask` does not
    // yet carry its own spec id to disambiguate. Tracked as a follow-up in
    // <https://github.com/apache/iceberg-rust/issues/2658>.
    if metadata.partition_specs_iter().len() > 1 {
        return None;
    }
    let spec = metadata.default_partition_spec();
    let table_schema = metadata.current_schema();

    let mut cols = Vec::new();
    for (spec_field_idx, pf) in spec.fields().iter().enumerate() {
        if pf.transform != Transform::Identity {
            continue;
        }
        let source_field = table_schema.field_by_id(pf.source_id)?;
        let output_idx = output_schema.index_of(source_field.name.as_str()).ok()?;
        let output_dtype = output_schema.field(output_idx).data_type().clone();
        if !is_supported_dtype(&output_dtype) {
            return None;
        }
        cols.push(IdentityCol {
            name: source_field.name.clone(),
            output_idx,
            spec_field_idx,
            output_dtype,
        });
    }
    Some(cols)
}

fn is_supported_dtype(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Date32
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Timestamp(TimeUnit::Nanosecond, _)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Decimal128(_, _)
            | DataType::FixedSizeBinary(_)
    )
}

/// Distribute `tasks` across `n_partitions` buckets. When `identity_cols`
/// describes a non-empty, hashable identity key, each task is hashed on
/// that key using DataFusion's repartition hash so the resulting partitioning
/// matches what `RepartitionExec` would produce on the same data. Tasks
/// missing partition data fall back to hashing `data_file_path`, which still
/// distributes evenly but breaks the `Hash` contract; the second tuple
/// element flags whether every task supplied a full identity key.
pub(super) fn bucket_tasks(
    tasks: Vec<FileScanTask>,
    n_partitions: usize,
    identity_cols: Option<&[IdentityCol]>,
) -> (Vec<Vec<FileScanTask>>, bool) {
    if n_partitions == 0 {
        return (Vec::new(), tasks.is_empty());
    }
    let mut buckets: Vec<Vec<FileScanTask>> = (0..n_partitions).map(|_| Vec::new()).collect();
    let mut all_full_key = true;
    let cols = identity_cols.unwrap_or(&[]);
    let identity_hashes = identity_hashes_for_tasks(&tasks, cols);

    for (task_idx, task) in tasks.into_iter().enumerate() {
        let bucket_idx = match &identity_hashes {
            Some(identity_hashes) if identity_hashes.full_key_by_task[task_idx] => {
                (identity_hashes.hashes[task_idx] % n_partitions as u64) as usize
            }
            None => {
                all_full_key = false;
                fallback_hash(&task) as usize % n_partitions
            }
            Some(_) => {
                all_full_key = false;
                fallback_hash(&task) as usize % n_partitions
            }
        };
        buckets[bucket_idx].push(task);
    }
    (buckets, all_full_key)
}

struct IdentityHashes {
    hashes: Vec<u64>,
    full_key_by_task: Vec<bool>,
}

/// Hash all identity-partition values using [`REPARTITION_RANDOM_STATE`] so the
/// bucket assignment matches DataFusion's hash-repartition convention. The
/// returned `full_key_by_task` marks rows whose task supplied every identity key
/// slot with a supported non-null literal.
fn identity_hashes_for_tasks(
    tasks: &[FileScanTask],
    cols: &[IdentityCol],
) -> Option<IdentityHashes> {
    if cols.is_empty() {
        return None;
    }

    let mut builders = cols
        .iter()
        .map(|col| PrimitiveLiteralArrayBuilder::try_new(&col.output_dtype, tasks.len()))
        .collect::<iceberg::Result<Vec<_>>>()
        .ok()?;
    let mut full_key_by_task = Vec::with_capacity(tasks.len());

    for task in tasks {
        let partition_fields = task.partition.as_ref().map(|partition| partition.fields());
        let mut full_key = partition_fields.is_some();

        for (builder, col) in builders.iter_mut().zip(cols) {
            let lit = partition_fields
                .and_then(|fields| fields.get(col.spec_field_idx))
                .and_then(|lit| lit.as_ref());
            let prim_lit = lit.and_then(|lit| match lit {
                Literal::Primitive(prim) => Some(prim),
                _ => None,
            });
            let appended = builder.append_or_null(prim_lit).ok()?;
            full_key = full_key && appended;
        }
        full_key_by_task.push(full_key);
    }

    let arrays = builders
        .into_iter()
        .map(PrimitiveLiteralArrayBuilder::finish)
        .collect::<iceberg::Result<Vec<_>>>()
        .ok()?;
    let mut hashes = vec![0u64; tasks.len()];
    create_hashes(
        &arrays,
        REPARTITION_RANDOM_STATE.random_state(),
        &mut hashes,
    )
    .ok()?;
    Some(IdentityHashes {
        hashes,
        full_key_by_task,
    })
}

/// Deterministic per-file fallback used when identity hashing cannot produce a
/// bucket. The hash function does not need to match DataFusion's because any
/// task taking this path causes the scan to drop to `UnknownPartitioning`.
fn fallback_hash(task: &FileScanTask) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    task.data_file_path.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        ArrayRef, Decimal128Array, Int32Array, StringArray, TimestampMicrosecondArray,
    };
    use iceberg::spec::{
        DataFileFormat, Literal, NestedField, PrimitiveType, Schema, Struct, Type,
    };

    use super::*;

    fn scan_task(file_idx: usize, partition: Option<Struct>) -> FileScanTask {
        FileScanTask {
            file_size_in_bytes: 1,
            start: 0,
            length: 1,
            record_count: Some(1),
            data_file_path: format!("/tmp/file_{file_idx}.parquet"),
            data_file_format: DataFileFormat::Parquet,
            schema: Arc::new(
                Schema::builder()
                    .with_schema_id(0)
                    .with_fields(vec![
                        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::required(2, "name", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])
                    .build()
                    .unwrap(),
            ),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes: Vec::new(),
            partition,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        }
    }

    fn bucket_by_file_index(
        buckets: &[Vec<FileScanTask>],
        file_count: usize,
    ) -> Vec<Option<usize>> {
        let mut actual_bucket_by_file = vec![None; file_count];
        for (bucket_idx, bucket) in buckets.iter().enumerate() {
            for task in bucket {
                let file_idx = task
                    .data_file_path()
                    .strip_suffix(".parquet")
                    .and_then(|path| path.rsplit_once("file_").map(|(_, idx)| idx))
                    .and_then(|idx| idx.parse::<usize>().ok())
                    .expect("test data file path should include its row index");
                actual_bucket_by_file[file_idx] = Some(bucket_idx);
            }
        }
        actual_bucket_by_file
    }

    #[test]
    fn bucket_tasks_hashes_multiple_identity_columns() {
        let rows = [(1, "a"), (2, "b"), (1, "b"), (3, "c"), (2, "a")];
        let tasks = rows
            .iter()
            .enumerate()
            .map(|(idx, (id, name))| {
                scan_task(
                    idx,
                    Some(Struct::from_iter(vec![
                        Some(Literal::int(*id)),
                        Some(Literal::string(*name)),
                    ])),
                )
            })
            .collect::<Vec<_>>();
        let cols = vec![
            IdentityCol {
                name: "id".to_string(),
                output_idx: 0,
                spec_field_idx: 0,
                output_dtype: DataType::Int32,
            },
            IdentityCol {
                name: "name".to_string(),
                output_idx: 1,
                spec_field_idx: 1,
                output_dtype: DataType::Utf8,
            },
        ];
        let n_partitions = 4_usize;

        let (buckets, all_full_key) = bucket_tasks(tasks, n_partitions, Some(&cols));

        assert!(all_full_key);
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, name)| *name).collect::<Vec<_>>(),
            )),
        ];
        let mut hashes = vec![0_u64; rows.len()];
        create_hashes(
            &arrays,
            REPARTITION_RANDOM_STATE.random_state(),
            &mut hashes,
        )
        .unwrap();

        let actual_bucket_by_file = bucket_by_file_index(&buckets, rows.len());
        for (file_idx, hash) in hashes.iter().enumerate() {
            let expected_bucket = (hash % n_partitions as u64) as usize;
            assert_eq!(actual_bucket_by_file[file_idx], Some(expected_bucket));
        }
    }

    #[test]
    fn bucket_tasks_hashes_decimal_and_timestamp_identity_columns() {
        let rows = [
            (100_i128, 1_740_600_000_000_000_i64),
            (200_i128, 1_740_600_100_000_000_i64),
            (100_i128, 1_740_600_200_000_000_i64),
        ];
        let tasks = rows
            .iter()
            .enumerate()
            .map(|(idx, (price, ts))| {
                scan_task(
                    idx,
                    Some(Struct::from_iter(vec![
                        Some(Literal::decimal(*price)),
                        Some(Literal::timestamp(*ts)),
                    ])),
                )
            })
            .collect::<Vec<_>>();
        let timestamp_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let cols = vec![
            IdentityCol {
                name: "price".to_string(),
                output_idx: 0,
                spec_field_idx: 0,
                output_dtype: DataType::Decimal128(18, 2),
            },
            IdentityCol {
                name: "ts".to_string(),
                output_idx: 1,
                spec_field_idx: 1,
                output_dtype: timestamp_type,
            },
        ];
        let n_partitions = 4_usize;

        let (buckets, all_full_key) = bucket_tasks(tasks, n_partitions, Some(&cols));

        assert!(all_full_key);
        let decimal_array =
            Decimal128Array::from(rows.iter().map(|(price, _)| *price).collect::<Vec<_>>())
                .with_precision_and_scale(18, 2)
                .unwrap();
        let timestamp_array =
            TimestampMicrosecondArray::from(rows.iter().map(|(_, ts)| *ts).collect::<Vec<_>>())
                .with_timezone("UTC");
        let arrays: Vec<ArrayRef> = vec![Arc::new(decimal_array), Arc::new(timestamp_array)];
        let mut hashes = vec![0_u64; rows.len()];
        create_hashes(
            &arrays,
            REPARTITION_RANDOM_STATE.random_state(),
            &mut hashes,
        )
        .unwrap();

        let actual_bucket_by_file = bucket_by_file_index(&buckets, rows.len());
        for (file_idx, hash) in hashes.iter().enumerate() {
            let expected_bucket = (hash % n_partitions as u64) as usize;
            assert_eq!(actual_bucket_by_file[file_idx], Some(expected_bucket));
        }
    }

    #[test]
    fn bucket_tasks_falls_back_per_task_for_missing_identity_key() {
        let tasks = vec![
            scan_task(0, Some(Struct::from_iter(vec![Some(Literal::string("a"))]))),
            scan_task(1, Some(Struct::from_iter(vec![None::<Literal>]))),
            scan_task(2, Some(Struct::from_iter(vec![Some(Literal::string("c"))]))),
            scan_task(3, None),
        ];
        let expected_tasks = tasks.clone();
        let cols = vec![IdentityCol {
            name: "name".to_string(),
            output_idx: 1,
            spec_field_idx: 0,
            output_dtype: DataType::Utf8,
        }];
        let n_partitions = 5_usize;

        let (buckets, all_full_key) = bucket_tasks(tasks, n_partitions, Some(&cols));

        assert!(!all_full_key);
        let arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("c"),
            None,
        ]))];
        let mut hashes = vec![0_u64; expected_tasks.len()];
        create_hashes(
            &arrays,
            REPARTITION_RANDOM_STATE.random_state(),
            &mut hashes,
        )
        .unwrap();

        let actual_bucket_by_file = bucket_by_file_index(&buckets, expected_tasks.len());
        for file_idx in [0_usize, 2] {
            let expected_bucket = (hashes[file_idx] % n_partitions as u64) as usize;
            assert_eq!(actual_bucket_by_file[file_idx], Some(expected_bucket));
        }
        for file_idx in [1_usize, 3] {
            let expected_bucket = fallback_hash(&expected_tasks[file_idx]) as usize % n_partitions;
            assert_eq!(actual_bucket_by_file[file_idx], Some(expected_bucket));
        }
    }
}
