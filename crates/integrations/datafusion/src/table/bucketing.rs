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

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray,
};
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::hash_utils::create_hashes;
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use iceberg::scan::FileScanTask;
use iceberg::spec::{Literal, PrimitiveLiteral, Transform};
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
///     [`literal_to_array`]
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
            | DataType::Date32
    )
}

/// Distribute `tasks` across `n_partitions` buckets. When `identity_cols`
/// describes a non-empty, hashable identity key, each task is hashed on
/// that key using DataFusion's repartition hash so the resulting partitioning
/// matches what `RepartitionExec` would produce on the same data. Tasks
/// missing partition data fall back to hashing `data_file_path`, which still
/// distributes evenly but breaks the `Hash` contract — the second tuple
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

    for task in tasks {
        let bucket_idx = match identity_hash(&task, cols) {
            Some(h) => (h % n_partitions as u64) as usize,
            None => {
                all_full_key = false;
                fallback_hash(&task) as usize % n_partitions
            }
        };
        buckets[bucket_idx].push(task);
    }
    (buckets, all_full_key)
}

/// Hash the identity-partition values of `task` using
/// [`REPARTITION_RANDOM_STATE`] so the bucket assignment matches DataFusion's
/// hash-repartition convention. Returns `None` if the task lacks partition
/// data or any required slot is null/unsupported.
fn identity_hash(task: &FileScanTask, cols: &[IdentityCol]) -> Option<u64> {
    if cols.is_empty() {
        return None;
    }
    let partition = task.partition.as_ref()?;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
    for col in cols {
        let lit = partition.fields().get(col.spec_field_idx)?.as_ref()?;
        arrays.push(literal_to_array(lit, &col.output_dtype)?);
    }
    let mut hashes = vec![0u64; 1];
    create_hashes(
        &arrays,
        REPARTITION_RANDOM_STATE.random_state(),
        &mut hashes,
    )
    .ok()?;
    Some(hashes[0])
}

/// Deterministic per-file fallback used when `identity_hash` cannot produce a
/// bucket. The hash function does not need to match DataFusion's because any
/// task taking this path causes the scan to drop to `UnknownPartitioning`.
fn fallback_hash(task: &FileScanTask) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    task.data_file_path.hash(&mut hasher);
    hasher.finish()
}

/// Materialize a single-element Arrow array of `dt` holding the value of
/// `lit`. The Arrow type must match what DataFusion will see for this column
/// at scan time, otherwise `create_hashes` would dispatch on a different type
/// and produce a hash that disagrees with DataFusion's row-wise hashing.
fn literal_to_array(lit: &Literal, dt: &DataType) -> Option<ArrayRef> {
    let prim = match lit {
        Literal::Primitive(p) => p,
        _ => return None,
    };
    Some(match (prim, dt) {
        (PrimitiveLiteral::Boolean(v), DataType::Boolean) => Arc::new(BooleanArray::from(vec![*v])),
        (PrimitiveLiteral::Int(v), DataType::Int32) => Arc::new(Int32Array::from(vec![*v])),
        (PrimitiveLiteral::Int(v), DataType::Date32) => Arc::new(Date32Array::from(vec![*v])),
        (PrimitiveLiteral::Long(v), DataType::Int64) => Arc::new(Int64Array::from(vec![*v])),
        (PrimitiveLiteral::Float(v), DataType::Float32) => Arc::new(Float32Array::from(vec![v.0])),
        (PrimitiveLiteral::Double(v), DataType::Float64) => {
            Arc::new(Float64Array::from(vec![v.0]))
        }
        (PrimitiveLiteral::String(v), DataType::Utf8) => {
            Arc::new(StringArray::from(vec![v.as_str()]))
        }
        _ => return None,
    })
}
