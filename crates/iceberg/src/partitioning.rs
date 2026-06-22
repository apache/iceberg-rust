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

//! Partition type utilities for Iceberg tables.

use crate::spec::{NestedField, NestedFieldRef, PartitionSpec, Schema, StructType, Transform};
use crate::{Error, ErrorKind, Result};

/// Computes the unified partition type across all partition specs in the table.
///
/// This is equivalent to Java's `Partitioning.partitionType(table)`. The result is a
/// StructType containing all partition fields ever used across all specs, enabling correct
/// representation of the `_partition` metadata column when partition evolution has occurred.
///
/// Matches Java's behavior:
/// - Specs are sorted by spec_id in descending order (newer specs first), so newer field
///   names take precedence when deduplicating by field_id.
/// - Void transform fields (dropped partition columns) are skipped.
/// - Fields are deduplicated by field_id — each unique field_id appears exactly once.
///
/// # Arguments
/// * `partition_specs` - Iterator over all partition specs in the table
/// * `schema` - The current table schema (needed to determine result types of transforms)
pub fn compute_unified_partition_type<'a>(
    partition_specs: impl Iterator<Item = &'a PartitionSpec>,
    schema: &Schema,
) -> Result<StructType> {
    let mut seen_field_ids = std::collections::HashSet::new();
    let mut struct_fields: Vec<NestedFieldRef> = Vec::new();

    // Sort specs by spec_id descending (newer first) to match Java's behavior:
    // newer field names take precedence when deduplicating by field_id.
    let mut specs: Vec<&PartitionSpec> = partition_specs.collect();
    specs.sort_by(|a, b| b.spec_id().cmp(&a.spec_id()));

    for spec in specs {
        for field in spec.fields() {
            if seen_field_ids.contains(&field.field_id) {
                continue;
            }

            // Skip void transforms (dropped partition columns)
            if matches!(field.transform, Transform::Void) {
                continue;
            }

            seen_field_ids.insert(field.field_id);

            let source_field = schema.field_by_id(field.source_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "No column with source column id {} in schema for partition field {}",
                        field.source_id, field.name
                    ),
                )
            })?;

            let res_type = field.transform.result_type(&source_field.field_type)?;
            let nested = NestedField::optional(field.field_id, &field.name, res_type).into();
            struct_fields.push(nested);
        }
    }

    Ok(StructType::new(struct_fields))
}
