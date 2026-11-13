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

use std::collections::HashSet;

use super::StatisticsPartitioning;
use crate::ErrorKind;
use crate::spec::{Literal, NestedField, PartitionSpec, PrimitiveType, Schema, Struct, StructType};

fn schema(fields: Vec<(i32, PrimitiveType)>) -> Schema {
    Schema::builder()
        .with_fields(
            fields.into_iter().map(|(id, ty)| {
                NestedField::optional(id, format!("source_{id}"), ty.into()).into()
            }),
        )
        .build()
        .unwrap()
}

fn spec(id: i32, fields: Vec<(i32, i32, &str, &str)>) -> PartitionSpec {
    serde_json::from_value(serde_json::json!({
        "spec-id": id,
        "fields": fields.into_iter().map(|(source, field, name, transform)| {
            serde_json::json!({
                "source-id": source, "field-id": field,
                "name": name, "transform": transform,
            })
        }).collect::<Vec<_>>()
    }))
    .unwrap()
}

fn physical_type(fields: Vec<(i32, PrimitiveType)>) -> StructType {
    StructType::new(
        fields
            .into_iter()
            .map(|(id, ty)| NestedField::optional(id, format!("physical_{id}"), ty.into()).into())
            .collect(),
    )
}

fn tuple(values: Vec<Option<Literal>>) -> Struct {
    values.into_iter().collect()
}

fn assert_invalid<T: std::fmt::Debug>(result: crate::Result<T>, message: &str) {
    let error = result.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::DataInvalid);
    assert!(error.message().contains(message), "{error}");
}

#[test]
fn test_full_union_retains_dropped_sources() {
    let old_schema = schema(vec![(1, PrimitiveType::String)]);
    let current_schema = schema(vec![(2, PrimitiveType::Int)]);
    let old = spec(0, vec![(1, 1000, "region", "identity")]);
    let current = spec(1, vec![(2, 1001, "id", "identity")]);
    let result =
        StatisticsPartitioning::new(&[&current, &old], &[&current_schema, &old_schema], &[])
            .unwrap();
    assert_eq!(
        result.partition_type(),
        &StructType::new(vec![
            NestedField::optional(1000, "region", PrimitiveType::String.into()).into(),
            NestedField::optional(1001, "id", PrimitiveType::Int.into()).into(),
        ])
    );
    let active = crate::partitioning::compute_unified_partition_type(
        [&old, &current].into_iter(),
        &current_schema,
    )
    .unwrap();
    assert_eq!(active.fields().len(), 1);
    assert_eq!(active.fields()[0].id, 1001);
}

#[test]
fn test_reordered_renamed_and_removed_fields_roundtrip() {
    let sources = schema(vec![
        (1, PrimitiveType::String),
        (2, PrimitiveType::Int),
        (3, PrimitiveType::Long),
    ]);
    let old = spec(0, vec![
        (2, 1002, "id", "identity"),
        (1, 1000, "region", "identity"),
    ]);
    let current = spec(1, vec![
        (1, 1000, "new_region", "identity"),
        (3, 1005, "bucket", "bucket[8]"),
    ]);
    let resolver = StatisticsPartitioning::new(&[&old, &current], &[&sources], &[]).unwrap();
    let fields = resolver.partition_type().fields();
    assert_eq!(
        fields
            .iter()
            .map(|field| (field.id, field.name.as_str(), field.required))
            .collect::<Vec<_>>(),
        vec![
            (1000, "new_region", false),
            (1002, "id", false),
            (1005, "bucket", false)
        ]
    );
    let physical = physical_type(vec![
        (1000, PrimitiveType::String),
        (1002, PrimitiveType::Int),
    ]);
    let key = resolver
        .mapping(0, &physical)
        .unwrap()
        .key(&tuple(vec![
            Some(Literal::string("us")),
            Some(Literal::int(7)),
        ]))
        .unwrap();
    assert_eq!(key.spec_id(), 0);
    assert_eq!(
        key.partition(),
        &tuple(vec![Some(Literal::int(7)), Some(Literal::string("us"))])
    );
    let unified = resolver.unified_partition(&key).unwrap();
    assert_eq!(
        unified,
        tuple(vec![
            Some(Literal::string("us")),
            Some(Literal::int(7)),
            None
        ])
    );
    assert_eq!(
        resolver
            .mapping(0, resolver.partition_type())
            .unwrap()
            .key(&unified)
            .unwrap(),
        key
    );
}

#[test]
fn test_manifest_evidence_recovers_a_dropped_source() {
    let historical = spec(0, vec![(1, 1000, "dropped", "identity")]);
    let physical = physical_type(vec![(1000, PrimitiveType::String)]);
    let resolver = StatisticsPartitioning::new(&[&historical], &[], &[(0, &physical)]).unwrap();
    assert_eq!(
        *resolver.partition_type().fields()[0].field_type,
        PrimitiveType::String.into()
    );
    assert_invalid(
        StatisticsPartitioning::new(&[&historical], &[], &[]),
        "Cannot resolve historical type",
    );
    let unknown = schema(vec![(1, PrimitiveType::Unknown)]);
    assert_invalid(
        StatisticsPartitioning::new(&[&historical], &[&unknown], &[]),
        "Cannot resolve historical type",
    );
}

#[test]
fn test_fixed_result_transforms_need_no_source_evidence() {
    for (transform, expected) in [
        ("bucket[8]", PrimitiveType::Int),
        ("year", PrimitiveType::Int),
        ("month", PrimitiveType::Int),
        ("hour", PrimitiveType::Int),
        ("day", PrimitiveType::Date),
    ] {
        let historical = spec(0, vec![(1, 1000, "dropped", transform)]);
        let resolver = StatisticsPartitioning::new(&[&historical], &[], &[]).unwrap();
        assert_eq!(
            *resolver.partition_type().fields()[0].field_type,
            expected.into()
        );
    }
}

#[test]
fn test_promotions_canonicalize_keys_and_hashes() {
    for (old_type, new_type, old_value, new_value) in [
        (
            PrimitiveType::Int,
            PrimitiveType::Long,
            Literal::int(i32::MAX),
            Literal::long(i32::MAX),
        ),
        (
            PrimitiveType::Float,
            PrimitiveType::Double,
            Literal::float(1.5_f32),
            Literal::double(1.5),
        ),
        (
            PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            },
            PrimitiveType::Decimal {
                precision: 18,
                scale: 2,
            },
            Literal::decimal(1234),
            Literal::decimal(1234),
        ),
    ] {
        let historical = spec(0, vec![(1, 1000, "value", "identity")]);
        let old_schema = schema(vec![(1, old_type.clone())]);
        let new_schema = schema(vec![(1, new_type.clone())]);
        let old_physical = physical_type(vec![(1000, old_type)]);
        let new_physical = physical_type(vec![(1000, new_type.clone())]);
        for schemas in [[&old_schema, &new_schema], [&new_schema, &old_schema]] {
            let resolver = StatisticsPartitioning::new(&[&historical], &schemas, &[
                (0, &old_physical),
                (0, &new_physical),
            ])
            .unwrap();
            assert_eq!(
                *resolver.partition_type().fields()[0].field_type,
                new_type.clone().into()
            );
            let old_key = resolver
                .mapping(0, &old_physical)
                .unwrap()
                .key(&tuple(vec![Some(old_value.clone())]))
                .unwrap();
            let new_key = resolver
                .mapping(0, &new_physical)
                .unwrap()
                .key(&tuple(vec![Some(new_value.clone())]))
                .unwrap();
            assert_eq!(old_key, new_key);
            assert_eq!(HashSet::from([old_key, new_key]).len(), 1);
        }
    }
}

#[test]
fn test_physical_only_promotions_are_resolved() {
    let historical = spec(0, vec![(1, 1000, "value", "truncate[10]")]);
    let narrow = physical_type(vec![(1000, PrimitiveType::Int)]);
    let wide = physical_type(vec![(1000, PrimitiveType::Long)]);
    let resolver =
        StatisticsPartitioning::new(&[&historical], &[], &[(0, &wide), (0, &narrow)]).unwrap();
    assert_eq!(
        *resolver.partition_type().fields()[0].field_type,
        PrimitiveType::Long.into()
    );
    assert_invalid(
        resolver.mapping(0, &physical_type(vec![(1000, PrimitiveType::Double)])),
        "Cannot promote",
    );
}

#[test]
fn test_conflicting_types_are_rejected() {
    for (left, right) in [
        (PrimitiveType::Int, PrimitiveType::String),
        (PrimitiveType::Int, PrimitiveType::Double),
        (PrimitiveType::Timestamp, PrimitiveType::Timestamptz),
        (PrimitiveType::Fixed(16), PrimitiveType::Uuid),
        (
            PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            },
            PrimitiveType::Decimal {
                precision: 18,
                scale: 3,
            },
        ),
    ] {
        let historical = spec(0, vec![(1, 1000, "value", "identity")]);
        let left_schema = schema(vec![(1, left)]);
        let right_schema = schema(vec![(1, right)]);
        assert_invalid(
            StatisticsPartitioning::new(&[&historical], &[&left_schema, &right_schema], &[]),
            "Conflicting types",
        );
    }
}

#[test]
fn test_source_conflicts_are_not_hidden_by_fixed_outputs() {
    let historical = spec(0, vec![(1, 1000, "bucket", "bucket[8]")]);
    let left_schema = schema(vec![(1, PrimitiveType::Int)]);
    let right_schema = schema(vec![(1, PrimitiveType::String)]);
    assert_invalid(
        StatisticsPartitioning::new(&[&historical], &[&left_schema, &right_schema], &[]),
        "Conflicting types",
    );
}

#[test]
fn test_fixed_manifest_outputs_cannot_be_promoted() {
    let historical = spec(0, vec![(1, 1000, "bucket", "bucket[8]")]);
    let invalid = physical_type(vec![(1000, PrimitiveType::Long)]);
    assert_invalid(
        StatisticsPartitioning::new(&[&historical], &[], &[(0, &invalid)]),
        "Invalid physical result type",
    );
}

#[test]
fn test_v1_void_replacement_keeps_type_and_latest_name() {
    let old = spec(0, vec![(1, 1000, "old_name", "identity")]);
    let void = spec(1, vec![(1, 1000, "new_name", "void")]);
    let old_physical = physical_type(vec![(1000, PrimitiveType::String)]);
    let void_physical = physical_type(vec![(1000, PrimitiveType::Int)]);
    let resolver = StatisticsPartitioning::new(&[&old, &void], &[], &[
        (0, &old_physical),
        (1, &void_physical),
    ])
    .unwrap();
    assert_eq!(resolver.partition_type().fields()[0].name, "new_name");
    assert_eq!(
        *resolver.partition_type().fields()[0].field_type,
        PrimitiveType::String.into()
    );
    let mapping = resolver.mapping(1, &void_physical).unwrap();
    let key = mapping.key(&tuple(vec![None])).unwrap();
    assert_eq!(resolver.unified_partition(&key).unwrap(), tuple(vec![None]));
    assert_invalid(
        mapping.key(&tuple(vec![Some(Literal::int(1))])),
        "Non-null value for void",
    );
    let omitted = resolver
        .mapping(1, &StructType::new(vec![]))
        .unwrap()
        .key(&Struct::empty())
        .unwrap();
    assert_eq!(omitted, key);
}

#[test]
fn test_void_only_uses_available_type_or_int_fallback() {
    let void = spec(0, vec![(1, 1000, "void", "void")]);
    let sources = schema(vec![(1, PrimitiveType::String)]);
    let physical = physical_type(vec![(1000, PrimitiveType::Long)]);
    for (schemas, manifests, expected) in [
        (vec![&sources], vec![], PrimitiveType::String),
        (vec![&sources], vec![(0, &physical)], PrimitiveType::String),
        (vec![], vec![(0, &physical)], PrimitiveType::Long),
        (vec![], vec![], PrimitiveType::Int),
    ] {
        let resolver = StatisticsPartitioning::new(&[&void], &schemas, &manifests).unwrap();
        assert_eq!(
            *resolver.partition_type().fields()[0].field_type,
            expected.into()
        );
    }
}

#[test]
fn test_conflicting_field_definitions_are_rejected_even_through_void() {
    let old = spec(0, vec![(1, 1000, "value", "identity")]);
    let different_source = spec(1, vec![(2, 1000, "value", "void")]);
    assert_invalid(
        StatisticsPartitioning::new(&[&old, &different_source], &[], &[]),
        "Conflicting definitions",
    );
    let conflicting = spec(1, vec![(1, 1000, "value", "truncate[8]")]);
    let newest_void = spec(2, vec![(1, 1000, "value", "void")]);
    assert_invalid(
        StatisticsPartitioning::new(&[&newest_void, &old, &conflicting], &[], &[]),
        "Conflicting definitions",
    );
}

#[test]
fn test_unknown_and_zero_width_transforms_are_rejected() {
    for transform in ["unknown", "bucket[0]", "truncate[0]"] {
        let historical = spec(0, vec![(1, 1000, "value", transform)]);
        assert_invalid(
            StatisticsPartitioning::new(&[&historical], &[], &[]),
            "Invalid transform",
        );
    }
}

#[test]
fn test_missing_necessary_field_differs_from_present_null() {
    let sources = schema(vec![(1, PrimitiveType::String), (2, PrimitiveType::Int)]);
    let old = spec(0, vec![(1, 1000, "region", "identity")]);
    let current = spec(1, vec![(2, 1001, "id", "identity")]);
    let resolver = StatisticsPartitioning::new(&[&old, &current], &[&sources], &[]).unwrap();
    let imported = physical_type(vec![(1001, PrimitiveType::Int)]);
    assert_invalid(
        resolver.mapping(0, &imported),
        "Missing necessary partition field 1000",
    );
    // Missing 1000 is irrelevant to spec 1; its physically present null is safe.
    let key = resolver
        .mapping(1, &imported)
        .unwrap()
        .key(&tuple(vec![None]))
        .unwrap();
    assert_eq!(key.partition(), &tuple(vec![None]));
    assert_eq!(
        resolver.unified_partition(&key).unwrap(),
        tuple(vec![None, None])
    );
}

#[test]
fn test_spec_id_preserves_distinct_keys() {
    let sources = schema(vec![(1, PrimitiveType::Int)]);
    let old = spec(0, vec![(1, 1000, "id", "identity")]);
    let current = spec(1, vec![(1, 1000, "id", "identity")]);
    let resolver = StatisticsPartitioning::new(&[&old, &current], &[&sources], &[]).unwrap();
    let values = tuple(vec![Some(Literal::int(5))]);
    let old_key = resolver
        .mapping(0, resolver.partition_type())
        .unwrap()
        .key(&values)
        .unwrap();
    let current_key = resolver
        .mapping(1, resolver.partition_type())
        .unwrap()
        .key(&values)
        .unwrap();
    assert_ne!(old_key, current_key);
    assert_eq!(HashSet::from([old_key, current_key]).len(), 2);
}

#[test]
fn test_float_keys_preserve_signed_zero_and_normalize_nan() {
    let sources = schema(vec![(1, PrimitiveType::Double)]);
    let historical = spec(0, vec![(1, 1000, "value", "identity")]);
    let resolver = StatisticsPartitioning::new(&[&historical], &[&sources], &[]).unwrap();
    let narrow = physical_type(vec![(1000, PrimitiveType::Float)]);
    let mapping = resolver.mapping(0, &narrow).unwrap();
    let negative = mapping
        .key(&tuple(vec![Some(Literal::float(-0.0_f32))]))
        .unwrap();
    let positive = mapping
        .key(&tuple(vec![Some(Literal::float(0.0_f32))]))
        .unwrap();
    assert_ne!(negative, positive);
    assert_eq!(HashSet::from([negative, positive]).len(), 2);
    let float_nan = mapping
        .key(&tuple(vec![Some(Literal::float(f32::NAN))]))
        .unwrap();
    let double_nan = resolver
        .mapping(0, resolver.partition_type())
        .unwrap()
        .key(&tuple(vec![Some(Literal::double(f64::NAN))]))
        .unwrap();
    assert_eq!(float_nan, double_nan);
    assert_eq!(HashSet::from([float_nan, double_nan]).len(), 1);
}

#[test]
fn test_empty_and_unpartitioned_specs_have_empty_keys() {
    let unpartitioned = PartitionSpec::unpartition_spec();
    let resolver = StatisticsPartitioning::new(&[&unpartitioned], &[], &[]).unwrap();
    assert!(resolver.partition_type().fields().is_empty());
    let key = resolver
        .mapping(0, resolver.partition_type())
        .unwrap()
        .key(&Struct::empty())
        .unwrap();
    assert_eq!(key.partition(), &Struct::empty());
    assert_eq!(resolver.unified_partition(&key).unwrap(), Struct::empty());
    assert!(
        StatisticsPartitioning::new(&[], &[], &[])
            .unwrap()
            .partition_type()
            .fields()
            .is_empty()
    );
}

#[test]
fn test_unpartitioned_keys_survive_evolution_to_partitioned() {
    let unpartitioned = PartitionSpec::unpartition_spec();
    let current = spec(1, vec![(1, 1000, "id", "identity")]);
    let sources = schema(vec![(1, PrimitiveType::Int)]);
    let resolver =
        StatisticsPartitioning::new(&[&unpartitioned, &current], &[&sources], &[]).unwrap();
    let key = resolver
        .mapping(0, &StructType::new(vec![]))
        .unwrap()
        .key(&Struct::empty())
        .unwrap();
    let unified = resolver.unified_partition(&key).unwrap();
    assert_eq!(unified, tuple(vec![None]));
    assert_eq!(
        resolver
            .mapping(0, resolver.partition_type())
            .unwrap()
            .key(&unified)
            .unwrap(),
        key
    );
}

#[test]
fn test_reconstruction_rejects_narrowing_and_scale_changes() {
    let historical = spec(0, vec![(1, 1000, "value", "identity")]);
    for (target, physical) in [
        (PrimitiveType::Int, PrimitiveType::Long),
        (PrimitiveType::Float, PrimitiveType::Double),
        (
            PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            },
            PrimitiveType::Decimal {
                precision: 18,
                scale: 2,
            },
        ),
        (
            PrimitiveType::Decimal {
                precision: 18,
                scale: 2,
            },
            PrimitiveType::Decimal {
                precision: 9,
                scale: 3,
            },
        ),
    ] {
        let sources = schema(vec![(1, target)]);
        let resolver = StatisticsPartitioning::new(&[&historical], &[&sources], &[]).unwrap();
        assert_invalid(
            resolver.mapping(0, &physical_type(vec![(1000, physical)])),
            "Cannot promote",
        );
    }
}

#[test]
fn test_nonprimitive_and_unknown_physical_types_are_rejected() {
    let historical = spec(0, vec![(1, 1000, "value", "identity")]);
    let sources = schema(vec![(1, PrimitiveType::Int)]);
    let resolver = StatisticsPartitioning::new(&[&historical], &[&sources], &[]).unwrap();
    let unknown = physical_type(vec![(1000, PrimitiveType::Unknown)]);
    assert_invalid(resolver.mapping(0, &unknown), "Unknown type");
    let nested = StructType::new(vec![
        NestedField::optional(1000, "value", StructType::new(vec![]).into()).into(),
    ]);
    assert_invalid(resolver.mapping(0, &nested), "Non-primitive type");
    assert_invalid(
        StatisticsPartitioning::new(&[&historical], &[], &[(0, &nested)]),
        "Non-primitive type",
    );
    let nested_source = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "source", StructType::new(vec![]).into()).into(),
        ])
        .build()
        .unwrap();
    assert_invalid(
        StatisticsPartitioning::new(&[&historical], &[&nested_source], &[]),
        "Non-primitive type",
    );
}

#[test]
fn test_duplicate_specs_and_fields_are_rejected() {
    let historical = spec(0, vec![(1, 1000, "value", "void")]);
    assert_invalid(
        StatisticsPartitioning::new(&[&historical, &historical], &[], &[]),
        "Duplicate partition spec",
    );
    let duplicate = spec(0, vec![
        (1, 1000, "first", "void"),
        (1, 1000, "second", "void"),
    ]);
    assert_invalid(
        StatisticsPartitioning::new(&[&duplicate], &[], &[]),
        "Duplicate partition field",
    );
    let physical = physical_type(vec![(1000, PrimitiveType::Int), (1000, PrimitiveType::Int)]);
    assert_invalid(
        StatisticsPartitioning::new(&[&historical], &[], &[(0, &physical)]),
        "Duplicate physical",
    );
    let resolver = StatisticsPartitioning::new(&[&historical], &[], &[]).unwrap();
    assert_invalid(resolver.mapping(0, &physical), "Duplicate physical");
}

#[test]
fn test_unknown_specs_and_extraneous_manifest_fields_are_rejected() {
    let historical = spec(0, vec![(1, 1000, "value", "void")]);
    let physical = physical_type(vec![(1001, PrimitiveType::Int)]);
    assert_invalid(
        StatisticsPartitioning::new(&[&historical], &[], &[(1, &physical)]),
        "Unknown manifest partition spec",
    );
    assert_invalid(
        StatisticsPartitioning::new(&[&historical], &[], &[(0, &physical)]),
        "absent from spec",
    );
    let resolver = StatisticsPartitioning::new(&[&historical], &[], &[]).unwrap();
    assert_invalid(resolver.mapping(1, &physical), "Unknown partition spec");
}

#[test]
fn test_invalid_value_types_lengths_and_decimal_precision_are_rejected() {
    let historical = spec(0, vec![(1, 1000, "value", "identity")]);
    for (ty, value, expected) in [
        (
            PrimitiveType::Int,
            Literal::string("invalid"),
            "Invalid value",
        ),
        (
            PrimitiveType::Fixed(2),
            Literal::binary(vec![1]),
            "Invalid fixed value length",
        ),
        (
            PrimitiveType::Decimal {
                precision: 2,
                scale: 0,
            },
            Literal::decimal(100),
            "Decimal value exceeds precision",
        ),
    ] {
        let sources = schema(vec![(1, ty)]);
        let resolver = StatisticsPartitioning::new(&[&historical], &[&sources], &[]).unwrap();
        let mapping = resolver.mapping(0, resolver.partition_type()).unwrap();
        assert_invalid(mapping.key(&tuple(vec![Some(value)])), expected);
        assert_invalid(mapping.key(&Struct::empty()), "expected 1");
        assert_invalid(mapping.key(&tuple(vec![None, None])), "expected 1");
    }
}
