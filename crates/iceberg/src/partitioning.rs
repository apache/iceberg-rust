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

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use crate::error::invalid_data;
use crate::spec::{
    NestedField, NestedFieldRef, PartitionField, PartitionSpec, Schema, StructType, Transform, Type,
};
use crate::{Error, ErrorKind, Result};

/// Computes the unified partition type across all partition specs in the table.
///
/// This is equivalent to Java's `Partitioning.partitionType(table)`. The result is a
/// StructType containing all partition fields ever used across all specs, enabling correct
/// representation of the `_partition` metadata column when partition evolution has occurred.
///
/// Matches Java's `buildPartitionProjectionType` behavior:
/// - Specs are sorted by spec_id in descending order (newer specs first), so newer field
///   names take precedence when deduplicating by field_id.
/// - Unknown transforms cause an error.
/// - Fields whose source column was dropped from the schema are skipped.
/// - Two specs defining the same field_id must be compatible (same source, compatible
///   transforms); V1 tables do not guarantee field ids are unique across specs.
/// - When a newer spec marks a field as Void (dropped) but an older spec has it with a
///   real transform, the older spec's type is preserved while the newer spec's name is kept.
/// - Fields are deduplicated by field_id; each unique field_id appears exactly once.
/// - Output fields are sorted by field_id ascending.
///
/// # Arguments
/// * `partition_specs` - Iterator over all partition specs in the table
/// * `schema` - The current table schema (needed to determine result types of transforms)
pub fn compute_unified_partition_type<'a>(
    partition_specs: impl Iterator<Item = &'a PartitionSpec>,
    schema: &Schema,
) -> Result<StructType> {
    let mut specs: Vec<&PartitionSpec> = partition_specs.collect();
    specs.sort_by_key(|s| Reverse(s.spec_id()));

    let active_field_ids = all_active_field_ids(specs.iter().copied(), schema);

    let mut field_map: HashMap<i32, &PartitionField> = HashMap::new();
    let mut type_map: HashMap<i32, Type> = HashMap::new();
    let mut name_map: HashMap<i32, String> = HashMap::new();

    for spec in &specs {
        for field in spec.fields() {
            let field_id = field.field_id;

            // Reject unknown transforms up front: we cannot determine their result type,
            // so we cannot build a partition column for them. This check must precede the
            // active_field_ids filter below, otherwise an unknown transform could be
            // silently skipped.
            if matches!(field.transform, Transform::Unknown) {
                return Err(invalid_data!(
                    "Partition field '{}' uses an unknown transform whose result type \
                         cannot be determined",
                    field.name
                ));
            }

            if !active_field_ids.contains(&field_id) {
                continue;
            }

            let source_field = match schema.field_by_id(field.source_id) {
                Some(f) => f,
                None => continue,
            };

            match field_map.get(&field_id) {
                None => {
                    let res_type = field.transform.result_type(&source_field.field_type)?;
                    field_map.insert(field_id, field);
                    type_map.insert(field_id, res_type);
                    name_map.insert(field_id, field.name.clone());
                }
                Some(existing) => {
                    // V1 tables do not guarantee field ids are unique across specs, so two
                    // specs may define the same field id. They must be compatible.
                    if !equivalent_ignoring_names(field, existing) {
                        return Err(invalid_data!(
                            "Conflicting partition fields for field id {field_id}: \
                                 '{}' and '{}'",
                            field.name,
                            existing.name
                        ));
                    }

                    // Use the correct type for dropped partitions in v1 tables: if the
                    // newer spec voided the field but an older spec has a real transform,
                    // keep the older spec's type.
                    if is_void_transform(existing) && !is_void_transform(field) {
                        let res_type = field.transform.result_type(&source_field.field_type)?;
                        field_map.insert(field_id, field);
                        type_map.insert(field_id, res_type);
                    }
                }
            }
        }
    }

    let mut field_ids: Vec<i32> = field_map.keys().copied().collect();
    field_ids.sort();

    let struct_fields = field_ids
        .into_iter()
        .map(|fid| -> Result<NestedFieldRef> {
            let name = name_map.get(&fid).ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Missing name for partition field {fid}"),
                )
            })?;
            let ty = type_map.remove(&fid).ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Missing type for partition field {fid}"),
                )
            })?;
            Ok(NestedField::optional(fid, name, ty).into())
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StructType::new(struct_fields))
}

fn is_void_transform(field: &PartitionField) -> bool {
    matches!(field.transform, Transform::Void)
}

/// Two partition fields with the same field id are compatible if they share the same
/// source id and have compatible transforms. Matches Java's
/// `Partitioning.equivalentIgnoringNames`.
fn equivalent_ignoring_names(field: &PartitionField, other: &PartitionField) -> bool {
    field.field_id == other.field_id
        && field.source_id == other.source_id
        && compatible_transforms(&field.transform, &other.transform)
}

/// Transforms are compatible if they are equal, or if either is Void (a dropped field).
/// Matches Java's `Partitioning.compatibleTransforms`.
fn compatible_transforms(t1: &Transform, t2: &Transform) -> bool {
    t1 == t2 || matches!(t1, Transform::Void) || matches!(t2, Transform::Void)
}

fn all_active_field_ids<'a>(
    partition_specs: impl Iterator<Item = &'a PartitionSpec>,
    schema: &Schema,
) -> HashSet<i32> {
    partition_specs
        .flat_map(|spec| spec.fields().iter())
        .filter(|field| schema.field_by_id(field.source_id).is_some())
        .map(|field| field.field_id)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::spec::{NestedField, PrimitiveType, Transform, Type, UnboundPartitionSpec};

    fn test_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "data", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "ts", Type::Primitive(PrimitiveType::Timestamp)).into(),
                NestedField::required(4, "category", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap()
    }

    fn build_spec(
        schema: &Schema,
        spec_id: i32,
        fields: Vec<(i32, &str, Transform)>,
    ) -> PartitionSpec {
        let mut builder = UnboundPartitionSpec::builder().with_spec_id(spec_id);
        for (source_id, name, transform) in fields {
            builder = builder
                .add_partition_field(source_id, name, transform)
                .unwrap();
        }
        builder.build().bind(schema.clone()).unwrap()
    }

    #[test]
    fn test_single_spec_identity() {
        let schema = test_schema();
        let spec = build_spec(&schema, 0, vec![(4, "category", Transform::Identity)]);

        let result = compute_unified_partition_type([&spec].into_iter(), &schema).unwrap();
        assert_eq!(result.fields().len(), 1);
        assert_eq!(result.fields()[0].name, "category");
        assert_eq!(
            *result.fields()[0].field_type,
            Type::Primitive(PrimitiveType::String)
        );
    }

    #[test]
    fn test_single_spec_with_year_transform() {
        let schema = test_schema();
        let spec = build_spec(&schema, 0, vec![(3, "ts_year", Transform::Year)]);

        let result = compute_unified_partition_type([&spec].into_iter(), &schema).unwrap();
        assert_eq!(result.fields().len(), 1);
        assert_eq!(result.fields()[0].name, "ts_year");
        assert_eq!(
            *result.fields()[0].field_type,
            Type::Primitive(PrimitiveType::Int)
        );
    }

    #[test]
    fn test_unpartitioned() {
        let schema = test_schema();
        let spec = PartitionSpec::unpartition_spec();
        let result = compute_unified_partition_type([&spec].into_iter(), &schema).unwrap();
        assert!(result.fields().is_empty());
    }

    #[test]
    fn test_multiple_fields_sorted_by_id() {
        let schema = test_schema();
        let spec = build_spec(&schema, 0, vec![
            (3, "ts_year", Transform::Year),
            (4, "category", Transform::Identity),
        ]);

        let result = compute_unified_partition_type([&spec].into_iter(), &schema).unwrap();
        assert_eq!(result.fields().len(), 2);
        assert!(result.fields()[0].id < result.fields()[1].id);
    }

    #[test]
    fn test_newer_name_takes_precedence() {
        let schema = test_schema();

        // Spec 0: old name
        let spec_v0 = PartitionSpec::builder(Arc::new(schema.clone()))
            .with_spec_id(0)
            .add_unbound_field(crate::spec::UnboundPartitionField {
                source_id: 4,
                field_id: Some(1000),
                name: "cat_old".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .build()
            .unwrap();

        // Spec 1: newer name, same field_id
        let spec_v1 = PartitionSpec::builder(Arc::new(schema.clone()))
            .with_spec_id(1)
            .add_unbound_field(crate::spec::UnboundPartitionField {
                source_id: 4,
                field_id: Some(1000),
                name: "cat_new".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .build()
            .unwrap();

        let result =
            compute_unified_partition_type([&spec_v0, &spec_v1].into_iter(), &schema).unwrap();
        assert_eq!(result.fields().len(), 1);
        assert_eq!(result.fields()[0].name, "cat_new");
    }

    #[test]
    fn test_void_replaced_by_older_non_void() {
        let schema = test_schema();

        // Spec 0 (older): category partitioned by identity
        let spec_v0 = PartitionSpec::builder(Arc::new(schema.clone()))
            .with_spec_id(0)
            .add_unbound_field(crate::spec::UnboundPartitionField {
                source_id: 4,
                field_id: Some(1000),
                name: "category".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .build()
            .unwrap();

        // Spec 1 (newer): same field_id voided (partition dropped)
        let spec_v1 = PartitionSpec::builder(Arc::new(schema.clone()))
            .with_spec_id(1)
            .add_unbound_field(crate::spec::UnboundPartitionField {
                source_id: 4,
                field_id: Some(1000),
                name: "category_v2".to_string(),
                transform: Transform::Void,
            })
            .unwrap()
            .build()
            .unwrap();

        let result =
            compute_unified_partition_type([&spec_v0, &spec_v1].into_iter(), &schema).unwrap();

        assert_eq!(result.fields().len(), 1);
        // Name from newer spec
        assert_eq!(result.fields()[0].name, "category_v2");
        // Type from older non-void spec
        assert_eq!(
            *result.fields()[0].field_type,
            Type::Primitive(PrimitiveType::String)
        );
    }

    #[test]
    fn test_dropped_source_column_skipped() {
        // Schema without field 4 (category was dropped)
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "data", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        // Spec references source_id=4 which no longer exists in the schema.
        // Deserialize directly since the builder rejects unknown source columns.
        let spec = serde_json::from_value::<PartitionSpec>(serde_json::json!({
            "spec-id": 0,
            "fields": [{
                "source-id": 4,
                "field-id": 1000,
                "name": "category",
                "transform": "identity"
            }]
        }))
        .unwrap();

        let result = compute_unified_partition_type([&spec].into_iter(), &schema).unwrap();
        assert!(result.fields().is_empty());
    }

    #[test]
    fn test_evolution_adds_new_field() {
        let schema = test_schema();

        // Spec 0: partition by category
        let spec_v0 = build_spec(&schema, 0, vec![(4, "category", Transform::Identity)]);

        // Spec 1: partition by category + ts_year
        let spec_v1 = PartitionSpec::builder(Arc::new(schema.clone()))
            .with_spec_id(1)
            .add_unbound_field(crate::spec::UnboundPartitionField {
                source_id: 4,
                field_id: Some(spec_v0.fields()[0].field_id),
                name: "category".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .add_partition_field("ts", "ts_year", Transform::Year)
            .unwrap()
            .build()
            .unwrap();

        let result =
            compute_unified_partition_type([&spec_v0, &spec_v1].into_iter(), &schema).unwrap();
        assert_eq!(result.fields().len(), 2);
    }

    #[test]
    fn test_unknown_transform_errors() {
        let schema = test_schema();

        // A spec using an unknown transform. Deserialize directly since the builder
        // validates transforms.
        let spec = serde_json::from_value::<PartitionSpec>(serde_json::json!({
            "spec-id": 0,
            "fields": [{
                "source-id": 4,
                "field-id": 1000,
                "name": "category",
                "transform": "unknown"
            }]
        }))
        .unwrap();

        let err = compute_unified_partition_type([&spec].into_iter(), &schema).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_conflicting_partition_fields_error() {
        let schema = test_schema();

        // Spec 0: field id 1000 -> source 4 (category), identity
        let spec_v0 = serde_json::from_value::<PartitionSpec>(serde_json::json!({
            "spec-id": 0,
            "fields": [{
                "source-id": 4,
                "field-id": 1000,
                "name": "category",
                "transform": "identity"
            }]
        }))
        .unwrap();

        // Spec 1: field id 1000 reused for a different source (ts) and transform (year).
        // This conflicts with spec 0 and must be rejected.
        let spec_v1 = serde_json::from_value::<PartitionSpec>(serde_json::json!({
            "spec-id": 1,
            "fields": [{
                "source-id": 3,
                "field-id": 1000,
                "name": "ts_year",
                "transform": "year"
            }]
        }))
        .unwrap();

        let err =
            compute_unified_partition_type([&spec_v0, &spec_v1].into_iter(), &schema).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }
}
