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

//! Historical partition types and field-ID mappings for partition statistics.

// Kept internal until the statistics codec and computation consume these helpers.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};

use crate::Result;
use crate::error::invalid_data;
use crate::spec::{
    Literal, NestedField, PartitionField, PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema,
    Struct, StructType, Transform, Type,
};

/// A full historical partition type and the original field order of each spec.
///
/// Unlike the active-field projection used by ordinary scans, this retains fields
/// whose source columns have been dropped. It performs no storage access.
#[derive(Debug)]
pub(crate) struct StatisticsPartitioning {
    partition_type: StructType,
    specs: HashMap<i32, Vec<SpecField>>,
}

#[derive(Debug)]
struct SpecField {
    id: i32,
    unified_position: usize,
    is_void: bool,
}

/// A key in original spec order, with values promoted to the full historical type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StatisticsPartitionKey {
    spec_id: i32,
    partition: Struct,
}

impl StatisticsPartitionKey {
    pub(crate) fn spec_id(&self) -> i32 {
        self.spec_id
    }

    pub(crate) fn partition(&self) -> &Struct {
        &self.partition
    }
}

/// A reusable plan for recovering one spec's keys from a physical partition layout.
#[derive(Debug)]
pub(crate) struct StatisticsPartitionMapping {
    spec_id: i32,
    physical_len: usize,
    fields: Vec<ValueMapping>,
}

#[derive(Debug)]
struct ValueMapping {
    id: i32,
    position: Option<usize>,
    source_type: Option<PrimitiveType>,
    target_type: PrimitiveType,
    is_void: bool,
}

impl StatisticsPartitioning {
    /// Resolves types from known specs, current/retained schemas, and optional
    /// physical manifest partition types paired with their spec IDs.
    ///
    /// Manifest types must come from the physical writer schema, rather than the
    /// embedded table schema. The caller owns extraction of that evidence.
    pub(crate) fn new(
        specs: &[&PartitionSpec],
        schemas: &[&Schema],
        manifest_types: &[(i32, &StructType)],
    ) -> Result<Self> {
        let mut ordered_specs = specs.to_vec();
        ordered_specs.sort_by_key(|spec| std::cmp::Reverse(spec.spec_id()));
        let mut specs_by_id = HashMap::new();
        let mut definitions: BTreeMap<i32, PartitionField> = BTreeMap::new();
        for spec in &ordered_specs {
            if specs_by_id.insert(spec.spec_id(), *spec).is_some() {
                return Err(invalid_data!(
                    "Duplicate partition spec ID {}",
                    spec.spec_id()
                ));
            }
            let mut seen = std::collections::HashSet::new();
            for field in spec.fields() {
                if !seen.insert(field.field_id) {
                    return Err(invalid_data!(
                        "Duplicate partition field ID {} in spec {}",
                        field.field_id,
                        spec.spec_id()
                    ));
                }
                if matches!(
                    field.transform,
                    Transform::Unknown | Transform::Bucket(0) | Transform::Truncate(0)
                ) {
                    return Err(invalid_data!(
                        "Invalid transform {} for partition field {}",
                        field.transform,
                        field.field_id
                    ));
                }
                if let Some(existing) = definitions.get_mut(&field.field_id) {
                    if existing.source_id != field.source_id
                        || (existing.transform != Transform::Void
                            && field.transform != Transform::Void
                            && existing.transform != field.transform)
                    {
                        return Err(invalid_data!(
                            "Conflicting definitions for partition field {}",
                            field.field_id
                        ));
                    }
                    // Keep the newest name, but use the historical non-void transform.
                    if existing.transform == Transform::Void {
                        existing.transform = field.transform;
                    }
                } else {
                    definitions.insert(field.field_id, field.clone());
                }
            }
        }

        for (spec_id, physical_type) in manifest_types {
            let spec = specs_by_id
                .get(spec_id)
                .ok_or_else(|| invalid_data!("Unknown manifest partition spec ID {spec_id}"))?;
            physical_positions(physical_type)?;
            for field in physical_type.fields() {
                if !spec
                    .fields()
                    .iter()
                    .any(|definition| definition.field_id == field.id)
                {
                    return Err(invalid_data!(
                        "Manifest partition field {} is absent from spec {spec_id}",
                        field.id
                    ));
                }
            }
        }

        let mut fields = Vec::with_capacity(definitions.len());
        for (id, definition) in &definitions {
            let mut source_type = None;
            for schema in schemas {
                if let Some(source) = schema.field_by_id(definition.source_id)
                    && *source.field_type != Type::Primitive(PrimitiveType::Unknown)
                {
                    merge_type(
                        &mut source_type,
                        concrete_type(&source.field_type, *id)?,
                        *id,
                    )?;
                }
            }
            let has_source_type = source_type.is_some();
            let mut output_type = match source_type {
                Some(source) => Some(concrete_type(
                    &definition.transform.result_type(&source.into())?,
                    *id,
                )?),
                None => fixed_result_type(definition.transform),
            };

            for (spec_id, physical_type) in manifest_types {
                let spec = specs_by_id[spec_id];
                let Some(spec_field) = spec.fields().iter().find(|field| field.field_id == *id)
                else {
                    continue;
                };
                // A void field stores only nulls; a writer's placeholder type cannot
                // replace a historical non-void type or a known source type.
                if spec_field.transform == Transform::Void
                    && (definition.transform != Transform::Void || has_source_type)
                {
                    continue;
                }
                if let Some(physical) = physical_type.fields().iter().find(|field| field.id == *id)
                    && *physical.field_type != Type::Primitive(PrimitiveType::Unknown)
                {
                    let candidate = concrete_type(&physical.field_type, *id)?;
                    if fixed_result_type(definition.transform)
                        .is_some_and(|expected| expected != candidate)
                    {
                        return Err(invalid_data!(
                            "Invalid physical result type for partition field {id}: {candidate:?}"
                        ));
                    }
                    // Identity and truncate preserve the source type. Validate that
                    // physical evidence is a possible output of the transform.
                    if matches!(
                        definition.transform,
                        Transform::Identity | Transform::Truncate(_)
                    ) {
                        definition
                            .transform
                            .result_type(&candidate.clone().into())?;
                    }
                    merge_type(&mut output_type, candidate, *id)?;
                }
            }
            let ty = output_type
                .or_else(|| (definition.transform == Transform::Void).then_some(PrimitiveType::Int))
                .ok_or_else(|| {
                    invalid_data!(
                        "Cannot resolve historical type for partition field {id} (source {})",
                        definition.source_id
                    )
                })?;
            fields.push(NestedField::optional(*id, &definition.name, ty.into()).into());
        }

        let partition_type = StructType::new(fields);
        let positions = physical_positions(&partition_type)?;
        let specs = ordered_specs
            .into_iter()
            .map(|spec| {
                (
                    spec.spec_id(),
                    spec.fields()
                        .iter()
                        .map(|field| SpecField {
                            id: field.field_id,
                            unified_position: positions[&field.field_id],
                            is_void: field.transform == Transform::Void,
                        })
                        .collect(),
                )
            })
            .collect();
        Ok(Self {
            partition_type,
            specs,
        })
    }

    pub(crate) fn partition_type(&self) -> &StructType {
        &self.partition_type
    }

    /// Compiles a field-ID mapping from the original physical layout, before
    /// projection or default filling. Missing non-void fields are errors even if
    /// a decoder would supply nulls; omitted fields outside this spec are safe.
    pub(crate) fn mapping(
        &self,
        spec_id: i32,
        physical_type: &StructType,
    ) -> Result<StatisticsPartitionMapping> {
        let spec = self.spec(spec_id)?;
        let positions = physical_positions(physical_type)?;
        let fields = spec
            .iter()
            .map(|field| {
                let position = positions.get(&field.id).copied();
                let target_type = concrete_type(
                    &self.partition_type.fields()[field.unified_position].field_type,
                    field.id,
                )?;
                let source_type = if field.is_void {
                    None
                } else {
                    let position = position.ok_or_else(|| {
                        invalid_data!(
                            "Missing necessary partition field {} for spec {spec_id}",
                            field.id
                        )
                    })?;
                    let source =
                        concrete_type(&physical_type.fields()[position].field_type, field.id)?;
                    if !can_promote(&source, &target_type) {
                        return Err(invalid_data!(
                            "Cannot promote partition field {} from {source:?} to {target_type:?}",
                            field.id
                        ));
                    }
                    Some(source)
                };
                Ok(ValueMapping {
                    id: field.id,
                    position,
                    source_type,
                    target_type,
                    is_void: field.is_void,
                })
            })
            .collect::<Result<_>>()?;
        Ok(StatisticsPartitionMapping {
            spec_id,
            physical_len: physical_type.fields().len(),
            fields,
        })
    }

    /// Projects a canonical per-spec key into the full optional partition layout.
    pub(crate) fn unified_partition(&self, key: &StatisticsPartitionKey) -> Result<Struct> {
        let spec = self.spec(key.spec_id)?;
        check_tuple_len(&key.partition, spec.len())?;
        let mut values = vec![None; self.partition_type.fields().len()];
        for (field, value) in spec.iter().zip(key.partition.iter()) {
            if let Some(value) = value {
                if field.is_void {
                    return Err(invalid_data!(
                        "Non-null value for void partition field {}",
                        field.id
                    ));
                }
                let ty = concrete_type(
                    &self.partition_type.fields()[field.unified_position].field_type,
                    field.id,
                )?;
                values[field.unified_position] = Some(normalize(value, &ty, &ty, field.id)?);
            }
        }
        Ok(values.into_iter().collect())
    }

    fn spec(&self, id: i32) -> Result<&[SpecField]> {
        self.specs
            .get(&id)
            .map(Vec::as_slice)
            .ok_or_else(|| invalid_data!("Unknown partition spec ID {id}"))
    }
}

impl StatisticsPartitionMapping {
    /// Recovers original spec order and canonicalizes values before equality/hashing.
    /// Void fields are always null and can be recovered without physical storage.
    pub(crate) fn key(&self, physical_partition: &Struct) -> Result<StatisticsPartitionKey> {
        check_tuple_len(physical_partition, self.physical_len)?;
        let partition = self
            .fields
            .iter()
            .map(|field| {
                let value = field
                    .position
                    .and_then(|position| physical_partition[position].as_ref());
                if field.is_void {
                    if value.is_some() {
                        return Err(invalid_data!(
                            "Non-null value for void partition field {}",
                            field.id
                        ));
                    }
                    return Ok(None);
                }
                value
                    .map(|value| {
                        normalize(
                            value,
                            field
                                .source_type
                                .as_ref()
                                .expect("Non-void mappings have a source type"),
                            &field.target_type,
                            field.id,
                        )
                    })
                    .transpose()
            })
            .collect::<Result<Struct>>()?;
        Ok(StatisticsPartitionKey {
            spec_id: self.spec_id,
            partition,
        })
    }
}

fn physical_positions(ty: &StructType) -> Result<HashMap<i32, usize>> {
    let mut positions = HashMap::new();
    for (position, field) in ty.fields().iter().enumerate() {
        if positions.insert(field.id, position).is_some() {
            return Err(invalid_data!(
                "Duplicate physical partition field ID {}",
                field.id
            ));
        }
    }
    Ok(positions)
}

fn concrete_type(ty: &Type, field_id: i32) -> Result<PrimitiveType> {
    let Type::Primitive(primitive) = ty else {
        return Err(invalid_data!(
            "Non-primitive type for partition field {field_id}: {ty}"
        ));
    };
    if matches!(primitive, PrimitiveType::Unknown) {
        return Err(invalid_data!("Unknown type for partition field {field_id}"));
    }
    if let PrimitiveType::Decimal { precision, scale } = primitive
        && (*precision == 0 || *precision > 38 || scale > precision)
    {
        return Err(invalid_data!(
            "Invalid decimal type for partition field {field_id}: {ty}"
        ));
    }
    Ok(primitive.clone())
}

fn fixed_result_type(transform: Transform) -> Option<PrimitiveType> {
    match transform {
        Transform::Bucket(_) | Transform::Year | Transform::Month | Transform::Hour => {
            Some(PrimitiveType::Int)
        }
        Transform::Day => Some(PrimitiveType::Date),
        _ => None,
    }
}

fn can_promote(source: &PrimitiveType, target: &PrimitiveType) -> bool {
    source == target
        || matches!(
            (source, target),
            (PrimitiveType::Int, PrimitiveType::Long)
                | (PrimitiveType::Float, PrimitiveType::Double)
        )
        || matches!((source, target),
            (PrimitiveType::Decimal { precision: source_precision, scale: source_scale },
             PrimitiveType::Decimal { precision: target_precision, scale: target_scale })
             if source_precision <= target_precision && source_scale == target_scale)
}

fn merge_type(
    resolved: &mut Option<PrimitiveType>,
    candidate: PrimitiveType,
    id: i32,
) -> Result<()> {
    match resolved {
        Some(existing) if can_promote(existing, &candidate) => *existing = candidate,
        Some(existing) if can_promote(&candidate, existing) => {}
        Some(existing) => {
            return Err(invalid_data!(
                "Conflicting types for partition field {id}: {existing:?} and {candidate:?}"
            ));
        }
        None => *resolved = Some(candidate),
    }
    Ok(())
}

fn check_tuple_len(tuple: &Struct, expected: usize) -> Result<()> {
    if tuple.fields().len() != expected {
        return Err(invalid_data!(
            "Partition tuple has {} values, expected {expected}",
            tuple.fields().len()
        ));
    }
    Ok(())
}

fn normalize(
    value: &Literal,
    source: &PrimitiveType,
    target: &PrimitiveType,
    id: i32,
) -> Result<Literal> {
    let Literal::Primitive(primitive) = value else {
        return Err(invalid_data!(
            "Non-primitive value for partition field {id}"
        ));
    };
    if !source.compatible(primitive) {
        return Err(invalid_data!(
            "Invalid value for partition field {id} of type {source:?}: {value:?}"
        ));
    }
    match (source, primitive) {
        (PrimitiveType::Decimal { precision, .. }, PrimitiveLiteral::Int128(value))
            if value
                .unsigned_abs()
                .checked_ilog10()
                .map_or(1, |digits| digits + 1)
                > *precision =>
        {
            return Err(invalid_data!(
                "Decimal value exceeds precision for partition field {id}"
            ));
        }
        (PrimitiveType::Fixed(size), PrimitiveLiteral::Binary(value))
            if u64::try_from(value.len()).ok() != Some(*size) =>
        {
            return Err(invalid_data!(
                "Invalid fixed value length for partition field {id}"
            ));
        }
        _ => {}
    }
    match (source, target, primitive) {
        (PrimitiveType::Int, PrimitiveType::Long, PrimitiveLiteral::Int(value)) => {
            Ok(Literal::long(*value))
        }
        (PrimitiveType::Float, PrimitiveType::Double, PrimitiveLiteral::Float(value)) => {
            Ok(Literal::double(f64::from(value.0)))
        }
        _ => Ok(value.clone()),
    }
}

#[cfg(test)]
mod tests;
