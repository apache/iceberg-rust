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

//! Column projection for `ArrowReader`: building the Parquet projection mask
//! from Iceberg field IDs, and mapping field IDs between Iceberg and Parquet
//! (including fallback handling for files without embedded IDs).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::{
    DataType, Field, FieldRef, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ProjectionMask};
use parquet::schema::types::{SchemaDescriptor, Type as ParquetType};

use super::{ArrowReader, CollectFieldIdVisitor};
use crate::arrow::schema::validate_unshredded_variant;
use crate::arrow::{VariantExtensionType, arrow_schema_to_schema, arrow_type_to_type};
use crate::error::{Result, invalid_data};
use crate::expr::BoundPredicate;
use crate::expr::visitors::bound_predicate_visitor::visit;
use crate::spec::{
    LIST_FIELD_NAME, MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME, MappedField, NameMapping,
    NestedField, PrimitiveType, Schema, Type,
};
use crate::{Error, ErrorKind};

impl ArrowReader {
    pub(super) fn build_field_id_set_and_map(
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
        predicate: &BoundPredicate,
        use_position_fallback: bool,
    ) -> Result<(HashSet<i32>, HashMap<i32, usize>)> {
        // Collects all Iceberg field IDs referenced in the filter predicate
        let mut collector = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut collector, predicate)?;

        let iceberg_field_ids = collector.field_ids();
        let field_id_map =
            Self::resolve_field_id_map(parquet_schema, arrow_schema, use_position_fallback)?;

        Ok((iceberg_field_ids, field_id_map))
    }

    /// Resolves the field-id-to-leaf-column-index map for a Parquet file
    pub(super) fn resolve_field_id_map(
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
        use_position_fallback: bool,
    ) -> Result<HashMap<i32, usize>> {
        Ok(match build_field_id_map(parquet_schema)? {
            Some(map) => map,
            // No embedded field IDs and no name mapping: position-based fallback
            None if use_position_fallback => build_fallback_field_id_map(parquet_schema),
            // No embedded field IDs, but a name mapping assigned them to the Arrow
            // schema: resolve columns through the mapped Arrow field-id metadata
            None => build_field_id_map_from_arrow_schema(arrow_schema),
        })
    }

    pub(super) fn get_arrow_projection_mask(
        field_ids: &[i32],
        iceberg_schema_of_task: &Schema,
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
        use_fallback: bool, // Position-based fallback: file lacks embedded field IDs and no name mapping assigned any
    ) -> Result<ProjectionMask> {
        fn type_promotion_is_valid(
            file_type: Option<&PrimitiveType>,
            projected_type: Option<&PrimitiveType>,
        ) -> bool {
            match (file_type, projected_type) {
                (Some(lhs), Some(rhs)) if lhs == rhs => true,
                (Some(PrimitiveType::Int), Some(PrimitiveType::Long)) => true,
                (Some(PrimitiveType::Float), Some(PrimitiveType::Double)) => true,
                (
                    Some(PrimitiveType::Decimal {
                        precision: file_precision,
                        scale: file_scale,
                    }),
                    Some(PrimitiveType::Decimal {
                        precision: requested_precision,
                        scale: requested_scale,
                    }),
                ) if requested_precision >= file_precision && file_scale == requested_scale => true,
                // Uuid will be store as Fixed(16) in parquet file, so the read back type will be Fixed(16).
                (Some(PrimitiveType::Fixed(16)), Some(PrimitiveType::Uuid)) => true,
                _ => false,
            }
        }

        if field_ids.is_empty() {
            return Ok(ProjectionMask::all());
        }

        if use_fallback {
            // Position-based projection selects whole top-level columns without checking a
            // variant's storage layout.
            for field_id in field_ids {
                if let Some(field) = iceberg_schema_of_task.field_by_id(*field_id)
                    && type_contains_variant(&field.field_type)
                {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        "Reading variant columns from Parquet files without field ids is not supported yet",
                    ));
                }
            }
            // Position-based projection necessary because file lacks embedded field IDs
            Self::get_arrow_projection_mask_fallback(field_ids, parquet_schema)
        } else {
            // Field-ID-based projection using embedded field IDs from Parquet metadata

            // Parquet's columnar format requires leaf-level (not top-level struct/list/map) projection
            let mut projected = ProjectedFields::default();
            for field_id in field_ids {
                let field = iceberg_schema_of_task.field_by_id(*field_id);
                if let Some(field) = field {
                    projected.include(field);
                }
            }

            Self::get_arrow_projection_mask_with_field_ids(
                &projected,
                iceberg_schema_of_task,
                parquet_schema,
                arrow_schema,
                type_promotion_is_valid,
            )
        }
    }

    /// Standard projection using embedded field IDs from Parquet metadata.
    /// For iceberg-java compatibility with ParquetSchemaUtil.pruneColumns().
    fn get_arrow_projection_mask_with_field_ids(
        projected: &ProjectedFields,
        iceberg_schema_of_task: &Schema,
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
        type_promotion_is_valid: fn(Option<&PrimitiveType>, Option<&PrimitiveType>) -> bool,
    ) -> Result<ProjectionMask> {
        let leaf_field_ids = &projected.leaf_ids;
        // Maps field_id → leaf column indices. `Vec` because a variant contributes two
        // leaves (metadata + value) under a single field id.
        let mut column_map: HashMap<i32, Vec<usize>> = HashMap::new();
        let fields = arrow_schema.fields();
        // HashSet for O(1) membership checks instead of O(n) slice scans.
        let leaf_field_id_set: HashSet<i32> = leaf_field_ids.iter().copied().collect();
        // Keep presence leaves here too, or a map whose value struct lost every child
        // keeps only its key and fails conversion.
        let presence: BTreeSet<usize> =
            presence_leaves(fields, &projected.struct_ids, &leaf_field_id_set)
                .into_iter()
                .flat_map(|candidate| candidate.leaves)
                .collect();

        let projected_variant_ids: HashSet<i32> = leaf_field_ids
            .iter()
            .copied()
            .filter(|id| {
                iceberg_schema_of_task
                    .field_by_id(*id)
                    .is_some_and(|f| matches!(f.field_type.as_ref(), Type::Variant(_)))
            })
            .collect();

        // A variant's metadata/value leaves carry no field id, so the field-id scan below
        // never finds them. Like iceberg-java's `PruneColumns`, the whole variant group is
        // projected: record, for every Arrow leaf inside a projected variant, the variant's
        // field id (numbering matches `filter_leaves`).
        //
        // Tagging the projected variants with the `arrow.parquet.variant` extension lets
        // `arrow_schema_to_schema` fold each storage struct into `Type::Variant` without
        // descending into its id-less sub-fields.
        let (variant_leaves, tagged_fields) = if projected_variant_ids.is_empty() {
            (HashMap::new(), fields.clone())
        } else {
            let mut leaf_idx_to_variant_field_id = HashMap::new();
            let mut leaf_idx = 0usize;
            Self::collect_variant_leaves(
                fields,
                &mut leaf_idx,
                None,
                &projected_variant_ids,
                &mut leaf_idx_to_variant_field_id,
            )?;
            (
                leaf_idx_to_variant_field_id,
                Self::attach_variant_extensions(fields, &projected_variant_ids),
            )
        };

        // Pre-project only the fields that have been selected, possibly avoiding converting
        // some Arrow types that are not yet supported. Variant leaves are included here too
        // (they carry no field id) so nested containers — e.g. a map's key_value struct —
        // stay well-formed for `arrow_schema_to_schema` below.
        let projected_arrow_schema = ArrowSchema::new_with_metadata(
            tagged_fields.filter_leaves(|idx, f| {
                variant_leaves.contains_key(&idx)
                    || presence.contains(&idx)
                    || parquet_field_id(f)
                        .is_some_and(|field_id| leaf_field_id_set.contains(&field_id))
            }),
            arrow_schema.metadata().clone(),
        );
        let iceberg_schema = arrow_schema_to_schema(&projected_arrow_schema)?;

        tagged_fields.filter_leaves(|idx, field| {
            // Variant sub-fields: the parent group carries the field id, not the leaf, and
            // `Type::Variant` is not a primitive, so skip the type-promotion check (matching
            // iceberg-java, which projects the variant group whole with no type check).
            if let Some(&variant_field_id) = variant_leaves.get(&idx) {
                column_map.entry(variant_field_id).or_default().push(idx);
                return true;
            }

            let Some(field_id) = parquet_field_id(field) else {
                return false;
            };

            let iceberg_field = iceberg_schema_of_task.field_by_id(field_id);
            let parquet_iceberg_field = iceberg_schema.field_by_id(field_id);

            if iceberg_field.is_none() || parquet_iceberg_field.is_none() {
                return false;
            }

            if !type_promotion_is_valid(
                parquet_iceberg_field
                    .unwrap()
                    .field_type
                    .as_primitive_type(),
                iceberg_field.unwrap().field_type.as_primitive_type(),
            ) {
                return false;
            }

            column_map.entry(field_id).or_default().push(idx);
            true
        });

        // Schema evolution: New columns may not exist in old Parquet files.
        // We only project existing columns; RecordBatchTransformer adds default/NULL values.
        let mut indices = Vec::with_capacity(leaf_field_ids.len() + presence.len());
        for field_id in leaf_field_ids {
            if let Some(col_indices) = column_map.get(field_id) {
                indices.extend_from_slice(col_indices);
            }
        }

        // Read only for struct validity; `RecordBatchTransformer` drops these leaves.
        indices.extend(presence);

        if indices.is_empty() {
            // Edge case: All requested columns are new (don't exist in file).
            // Project all columns so RecordBatchTransformer has a batch to transform.
            Ok(ProjectionMask::all())
        } else {
            Ok(ProjectionMask::leaves(parquet_schema, indices))
        }
    }

    /// Walks an Arrow `Fields` tree, recording `leaf_idx → variant_field_id` for every
    /// leaf that sits inside a projected variant column (top-level or nested in a
    /// struct/list/map).
    ///
    /// The leaf numbering must match [`arrow_schema::Fields::filter_leaves`], since the
    /// resulting map is consulted by index in the `filter_leaves` passes above. In
    /// particular it mirrors `filter_leaves`' handling of `Dictionary`/`RunEndEncoded`
    /// (unwrapped to their value type) and `Union` (each member counted) — otherwise a
    /// mismatch would silently shift every variant leaf after it onto the wrong column.
    fn collect_variant_leaves(
        fields: &Fields,
        leaf_idx: &mut usize,
        variant_parent: Option<i32>,
        projected_variant_ids: &HashSet<i32>,
        leaf_idx_to_variant_field_id: &mut HashMap<usize, i32>,
    ) -> Result<()> {
        for field in fields {
            Self::collect_variant_leaves_in_field(
                field,
                leaf_idx,
                variant_parent,
                projected_variant_ids,
                leaf_idx_to_variant_field_id,
            )?;
        }
        Ok(())
    }

    fn collect_variant_leaves_in_field(
        field: &FieldRef,
        leaf_idx: &mut usize,
        variant_parent: Option<i32>,
        projected_variant_ids: &HashSet<i32>,
        leaf_idx_to_variant_field_id: &mut HashMap<usize, i32>,
    ) -> Result<()> {
        // Once inside a variant, stay inside; otherwise check whether this field is itself
        // a projected variant column.
        let effective_variant = match variant_parent {
            Some(variant_field_id) => Some(variant_field_id),
            None => {
                let entered = Self::variant_field_id(field, projected_variant_ids);
                if let Some(variant_field_id) = entered {
                    validate_unshredded_variant(field, variant_field_id)?;
                }
                entered
            }
        };

        // Mirror `Fields::filter_leaves`: unwrap `Dictionary`/`RunEndEncoded` to their value
        // type before deciding whether this is a leaf or a nested type to descend into.
        let data_type = match field.data_type() {
            DataType::Dictionary(_, value) => value.as_ref(),
            DataType::RunEndEncoded(_, value) => value.data_type(),
            other => other,
        };

        match data_type {
            DataType::Struct(subfields) => {
                Self::collect_variant_leaves(
                    subfields,
                    leaf_idx,
                    effective_variant,
                    projected_variant_ids,
                    leaf_idx_to_variant_field_id,
                )?;
            }
            DataType::List(inner_field)
            | DataType::LargeList(inner_field)
            | DataType::FixedSizeList(inner_field, _)
            | DataType::Map(inner_field, _) => {
                Self::collect_variant_leaves_in_field(
                    inner_field,
                    leaf_idx,
                    effective_variant,
                    projected_variant_ids,
                    leaf_idx_to_variant_field_id,
                )?;
            }
            DataType::Union(union_fields, _) => {
                for (_, inner_field) in union_fields.iter() {
                    Self::collect_variant_leaves_in_field(
                        inner_field,
                        leaf_idx,
                        effective_variant,
                        projected_variant_ids,
                        leaf_idx_to_variant_field_id,
                    )?;
                }
            }
            _ => {
                if let Some(variant_field_id) = effective_variant {
                    leaf_idx_to_variant_field_id.insert(*leaf_idx, variant_field_id);
                }
                *leaf_idx += 1;
            }
        }
        Ok(())
    }

    /// Returns `field`'s field id, read from the file or assigned by name mapping, if it is one
    /// of the projected variants.
    fn variant_field_id(field: &Field, projected_variant_ids: &HashSet<i32>) -> Option<i32> {
        parquet_field_id(field).filter(|id| projected_variant_ids.contains(id))
    }

    /// Returns `fields` with the canonical `arrow.parquet.variant` extension attached to every
    /// projected variant (recursing into struct/list/map to reach nested variants).
    ///
    /// A group is a variant when its field id resolves to `Type::Variant` in the Iceberg schema,
    /// so files without the Parquet `VARIANT` annotation are read too.
    fn attach_variant_extensions(fields: &Fields, projected_variant_ids: &HashSet<i32>) -> Fields {
        fields
            .iter()
            .map(|field| Self::attach_variant_extension(field, projected_variant_ids))
            .collect()
    }

    fn attach_variant_extension(
        field: &FieldRef,
        projected_variant_ids: &HashSet<i32>,
    ) -> FieldRef {
        // A variant's storage is a struct; tag it and don't descend — its metadata/value
        // children are not themselves variants.
        if Self::variant_field_id(field, projected_variant_ids).is_some()
            && matches!(field.data_type(), DataType::Struct(_))
        {
            return Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_extension_type(VariantExtensionType),
            );
        }
        // Otherwise recurse into containers to reach nested variants.
        let data_type = match field.data_type() {
            DataType::Struct(children) => DataType::Struct(Self::attach_variant_extensions(
                children,
                projected_variant_ids,
            )),
            DataType::List(child) => {
                DataType::List(Self::attach_variant_extension(child, projected_variant_ids))
            }
            DataType::LargeList(child) => {
                DataType::LargeList(Self::attach_variant_extension(child, projected_variant_ids))
            }
            DataType::FixedSizeList(child, len) => DataType::FixedSizeList(
                Self::attach_variant_extension(child, projected_variant_ids),
                *len,
            ),
            DataType::Map(child, sorted) => DataType::Map(
                Self::attach_variant_extension(child, projected_variant_ids),
                *sorted,
            ),
            // `collect_variant_leaves` descends Dictionary, RunEndEncoded and Union only to keep
            // leaf numbering in step with `filter_leaves`; Parquet-derived schemas never place a
            // variant under them.
            _ => return field.clone(),
        };
        Arc::new(field.as_ref().clone().with_data_type(data_type))
    }

    /// Fallback projection for Parquet files without field IDs.
    /// Uses position-based matching: field ID N → column position N-1.
    /// Projects entire top-level columns (including nested content) for iceberg-java compatibility.
    fn get_arrow_projection_mask_fallback(
        field_ids: &[i32],
        parquet_schema: &SchemaDescriptor,
    ) -> Result<ProjectionMask> {
        // Position-based: field_id N → column N-1 (field IDs are 1-indexed)
        let parquet_root_fields = parquet_schema.root_schema().get_fields();
        let mut root_indices = vec![];

        for field_id in field_ids.iter() {
            let parquet_pos = (*field_id - 1) as usize;

            if parquet_pos < parquet_root_fields.len() {
                root_indices.push(parquet_pos);
            }
            // RecordBatchTransformer adds missing columns with NULL values
        }

        if root_indices.is_empty() {
            Ok(ProjectionMask::all())
        } else {
            Ok(ProjectionMask::roots(parquet_schema, root_indices))
        }
    }
}

/// Whether `field_type` is, or transitively contains, a variant type.
fn type_contains_variant(field_type: &Type) -> bool {
    match field_type {
        Type::Variant(_) => true,
        Type::Struct(s) => s
            .fields()
            .iter()
            .any(|f| type_contains_variant(&f.field_type)),
        Type::List(l) => type_contains_variant(&l.element_field.field_type),
        Type::Map(m) => {
            type_contains_variant(&m.key_field.field_type)
                || type_contains_variant(&m.value_field.field_type)
        }
        Type::Primitive(_) => false,
    }
}

/// Build the map of parquet field id to Parquet column index in the schema.
/// Returns None if the Parquet file doesn't have field IDs embedded (e.g., migrated tables).
pub(super) fn build_field_id_map(
    parquet_schema: &SchemaDescriptor,
) -> Result<Option<HashMap<i32, usize>>> {
    let mut column_map = HashMap::new();

    for (idx, field) in parquet_schema.columns().iter().enumerate() {
        let field_type = field.self_type();
        match field_type {
            ParquetType::PrimitiveType { basic_info, .. } => {
                if !basic_info.has_id() {
                    return Ok(None);
                }
                column_map.insert(basic_info.id(), idx);
            }
            ParquetType::GroupType { .. } => {
                return Err(invalid_data!(
                    "Leaf column in schema should be primitive type but got {field_type:?}"
                ));
            }
        };
    }

    Ok(Some(column_map))
}

/// Finds the Parquet leaf column index carrying `field_id` by its embedded id.
///
/// Unlike [`build_field_id_map`], a leaf without an embedded id does not abort the
/// search -- it is simply skipped. This tolerates files that legitimately mix id-bearing
/// and id-less leaves, e.g. a Variant column whose internal metadata/value leaves are
/// required by the spec to have no field id, alongside a reserved metadata column that
/// does carry its id.
pub(super) fn find_leaf_by_field_id(
    parquet_schema: &SchemaDescriptor,
    field_id: i32,
) -> Option<usize> {
    parquet_schema.columns().iter().position(|col| {
        matches!(
            col.self_type(),
            ParquetType::PrimitiveType { basic_info, .. }
                if basic_info.has_id() && basic_info.id() == field_id
        )
    })
}

/// Build a fallback field ID map for Parquet files without embedded field IDs.
///
/// Returns the number of primitive (leaf) columns in a Parquet type, recursing into groups.
fn leaf_count(ty: &parquet::schema::types::Type) -> usize {
    if ty.is_primitive() {
        1
    } else {
        ty.get_fields().iter().map(|f| leaf_count(f)).sum()
    }
}

/// Builds a mapping from fallback field IDs to leaf column indices for Parquet files
/// without embedded field IDs. Returns entries only for primitive top-level fields.
///
/// Must use top-level field positions (not leaf column positions) to stay consistent
/// with `add_fallback_field_ids_to_arrow_schema`, which assigns ordinal IDs to
/// top-level Arrow fields. Using leaf positions instead would produce wrong indices
/// when nested types (struct/list/map) expand into multiple leaf columns.
///
/// Mirrors iceberg-java's ParquetSchemaUtil.addFallbackIds() which iterates
/// fileSchema.getFields() assigning ordinal IDs to top-level fields.
pub(super) fn build_fallback_field_id_map(
    parquet_schema: &SchemaDescriptor,
) -> HashMap<i32, usize> {
    let mut column_map = HashMap::new();
    let mut leaf_idx = 0;

    for (top_pos, field) in parquet_schema.root_schema().get_fields().iter().enumerate() {
        let field_id = (top_pos + 1) as i32;
        if field.is_primitive() {
            column_map.insert(field_id, leaf_idx);
        }
        leaf_idx += leaf_count(field);
    }

    column_map
}

/// Builds a mapping from field IDs to leaf column indices using the field-id metadata
/// carried by the Arrow schema.
///
/// Used for Parquet files without embedded field IDs when a name mapping has assigned
/// IDs to the Arrow schema (see [`apply_name_mapping_to_arrow_schema`]): the Parquet
/// schema descriptor itself still has no IDs, but the Arrow leaves are flattened in the
/// same depth-first order as Parquet leaf columns, so the Arrow leaf index lines up with
/// the Parquet column index. Columns the mapping did not match carry no field-id
/// metadata and are simply absent from the map.
fn build_field_id_map_from_arrow_schema(arrow_schema: &ArrowSchemaRef) -> HashMap<i32, usize> {
    let mut column_map = HashMap::new();
    arrow_schema.fields().filter_leaves(|idx, field| {
        if let Some(field_id) = parquet_field_id(field) {
            column_map.insert(field_id, idx);
        }
        false
    });
    column_map
}

/// Returns the field ID in `field`'s `PARQUET:field_id` metadata, if it has a valid one.
fn parquet_field_id(field: &Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|field_id| i32::from_str(field_id).ok())
}

/// Field ids a projection reads from the file.
#[derive(Default)]
struct ProjectedFields {
    leaf_ids: Vec<i32>,
    /// Non-empty structs, which may need a presence leaf.
    struct_ids: HashSet<i32>,
}

impl ProjectedFields {
    /// Recursively extract leaf field IDs because Parquet projection works at the leaf column level.
    /// Nested types (struct/list/map) are flattened in Parquet's columnar format.
    fn include(&mut self, field: &NestedField) {
        match field.field_type.as_ref() {
            Type::Primitive(_) => {
                self.leaf_ids.push(field.id);
            }
            Type::Struct(struct_type) => {
                if !struct_type.fields().is_empty() {
                    self.struct_ids.insert(field.id);
                }
                for nested_field in struct_type.fields() {
                    self.include(nested_field);
                }
            }
            Type::List(list_type) => {
                self.include(&list_type.element_field);
            }
            Type::Map(map_type) => {
                self.include(&map_type.key_field);
                self.include(&map_type.value_field);
            }
            // A variant is an Iceberg leaf type but a Parquet group: only the parent
            // group carries the field id, so we project it like a primitive. Its
            // metadata/value leaves are resolved later via `collect_variant_leaves`.
            Type::Variant(_) => {
                self.leaf_ids.push(field.id);
            }
        }
    }
}

struct PresenceCandidate {
    repetition: u32,
    /// Leaf indexes to read: one leaf, or a whole map.
    leaves: Range<usize>,
}

/// One file leaf under each projected struct that no selected leaf covers, so the
/// struct keeps its definition level. Like Java's `PresenceColumnSelector`, inner
/// structs go first, and the leaf with the lowest repetition level wins, then the first.
fn presence_leaves(
    fields: &Fields,
    projected_structs: &HashSet<i32>,
    leaf_field_ids: &HashSet<i32>,
) -> Vec<PresenceCandidate> {
    if projected_structs.is_empty() {
        return Vec::new();
    }
    let mut walk = PresenceWalk {
        projected_structs,
        leaf_field_ids,
        index: 0,
        chosen: Vec::new(),
        pick: true,
    };
    for field in fields {
        walk.visit(field, 0, false);
    }
    walk.chosen
}

struct PresenceWalk<'a> {
    projected_structs: &'a HashSet<i32>,
    leaf_field_ids: &'a HashSet<i32>,
    index: usize,
    chosen: Vec<PresenceCandidate>,
    /// Whether a struct here may record a presence leaf.
    pick: bool,
}

impl PresenceWalk<'_> {
    /// Returns whether a selected leaf is under `field`, and if not, the best presence
    /// candidate. A candidate also goes through the projection's type check, so what it
    /// reads must convert and every field on its path needs a field id. Candidates are
    /// only built under a projected struct, since nothing else can use them.
    fn visit(
        &mut self,
        field: &Field,
        repetition: u32,
        in_projected: bool,
    ) -> (bool, Option<PresenceCandidate>) {
        // Unwrap once, like `Fields::filter_leaves`, to count leaves the same way.
        let data_type = match field.data_type() {
            DataType::Dictionary(_, value) => value.as_ref(),
            DataType::RunEndEncoded(_, values) => values.data_type(),
            other => other,
        };
        let id = parquet_field_id(field);
        let (covered, best) = match data_type {
            DataType::Struct(children) => {
                let projected = id.is_some_and(|id| self.projected_structs.contains(&id));
                let mut covered = false;
                let mut best: Option<PresenceCandidate> = None;
                for child in children {
                    let (child_covered, child_best) =
                        self.visit(child, repetition, in_projected || projected);
                    covered |= child_covered;
                    if let Some(c) = child_best
                        && best.as_ref().is_none_or(|b| c.repetition < b.repetition)
                    {
                        best = Some(c);
                    }
                }
                if self.pick
                    && !covered
                    && projected
                    && let Some(leaf) = best.take()
                {
                    self.chosen.push(leaf);
                    covered = true;
                }
                (covered, best)
            }
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _) => self.visit(child, repetition + 1, in_projected),
            // Iceberg can't convert a list view. Count its leaves, but don't record
            // presence under it, including for a projected struct nested inside.
            DataType::ListView(child) | DataType::LargeListView(child) => {
                let pick = self.pick;
                self.pick = false;
                let (covered, _) = self.visit(child, repetition + 1, false);
                self.pick = pick;
                (covered, None)
            }
            DataType::Map(entries, _) => {
                let start = self.index;
                // Parquet drops a map unless both its key and value are read, so the
                // candidate is the whole map and its entries need none of their own.
                let (covered, _) = self.visit(entries, repetition + 1, false);
                let readable =
                    in_projected && !covered && arrow_type_to_type(field.data_type()).is_ok();
                let leaf = readable.then_some(PresenceCandidate {
                    repetition: repetition + 1,
                    leaves: start..self.index,
                });
                (covered, leaf)
            }
            _ => {
                let start = self.index;
                self.index += 1;
                let covered = id.is_some_and(|id| self.leaf_field_ids.contains(&id));
                let readable =
                    in_projected && !covered && arrow_type_to_type(field.data_type()).is_ok();
                let leaf = readable.then_some(PresenceCandidate {
                    repetition,
                    leaves: start..self.index,
                });
                (covered, leaf)
            }
        };
        let best = best.filter(|_| !covered && id.is_some());
        (covered, best)
    }
}

/// Apply name mapping to Arrow schema for Parquet files lacking field IDs.
///
/// Assigns Iceberg field IDs based on names at every depth using the name mapping,
/// enabling correct projection on migrated files (e.g., from Hive/Spark via add_files).
///
/// Per Iceberg spec Column Projection rule #2:
/// "Use schema.name-mapping.default metadata to map field id to columns without field id"
/// https://iceberg.apache.org/spec/#column-projection
///
/// Corresponds to Java's ParquetSchemaUtil.applyNameMapping() and ApplyNameMapping visitor.
/// The key difference is Java operates on Parquet MessageType, while we operate on Arrow Schema.
///
/// # Arguments
/// * `arrow_schema` - Arrow schema from Parquet file (without field IDs)
/// * `name_mapping` - Name mapping from table metadata (TableProperties.DEFAULT_NAME_MAPPING)
///
/// # Returns
/// Arrow schema with field IDs assigned based on name mapping
pub(super) fn apply_name_mapping_to_arrow_schema(
    arrow_schema: ArrowSchemaRef,
    name_mapping: &NameMapping,
) -> Result<Arc<ArrowSchema>> {
    debug_assert!(
        arrow_schema
            .fields()
            .iter()
            .next()
            .is_none_or(|f| f.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none()),
        "Schema already has field IDs - name mapping should not be applied"
    );

    let mut mapped_fields_by_name = HashMap::with_capacity(name_mapping.fields().len());
    for mapped_field in name_mapping.fields() {
        for name in mapped_field.names() {
            // Preserve the existing first-match behavior for duplicate aliases.
            mapped_fields_by_name
                .entry(name.as_str())
                .or_insert(mapped_field);
        }
    }

    let fields_with_mapped_ids = try_map_fields(arrow_schema.fields(), |field| {
        let mapped_field = mapped_fields_by_name.get(field.name().as_str()).copied();
        apply_name_mapping_to_field(field, mapped_field)
    })?;

    Ok(Arc::new(ArrowSchema::new_with_metadata(
        fields_with_mapped_ids,
        arrow_schema.metadata().clone(),
    )))
}

/// Assigns IDs from `mapped_field` to `field` and its nested fields, as Java's ApplyNameMapping
/// does. List elements match `element`, and map entries match `key` and `value`. A field
/// without an entry, or whose entry has no field ID, keeps no ID, and so do its nested fields.
fn apply_name_mapping_to_field(
    field: &FieldRef,
    mapped_field: Option<&MappedField>,
) -> Result<FieldRef> {
    let Some((mapped_field, field_id)) =
        mapped_field.zip(mapped_field.and_then(MappedField::field_id))
    else {
        return Ok(Arc::clone(field));
    };
    let map_nested = |nested: &FieldRef, name: &str| {
        let mapped_nested = find_mapped_field(mapped_field.fields().iter().map(Arc::as_ref), name);
        apply_name_mapping_to_field(nested, mapped_nested)
    };

    let data_type = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(try_map_fields(fields, |nested| {
            map_nested(nested, nested.name())
        })?),
        DataType::List(element) => DataType::List(map_nested(element, LIST_FIELD_NAME)?),
        DataType::LargeList(element) => DataType::LargeList(map_nested(element, LIST_FIELD_NAME)?),
        DataType::FixedSizeList(element, size) => {
            DataType::FixedSizeList(map_nested(element, LIST_FIELD_NAME)?, *size)
        }
        // The mapping has no entry for Arrow's entries struct. The shape checks are defensive,
        // because parquet-rs builds every map as a struct of key and value.
        DataType::Map(entries, sorted) => {
            let DataType::Struct(entry_fields) = entries.data_type() else {
                return Err(invalid_data!(
                    "Map field `{}` must have struct type",
                    field.name()
                ));
            };
            let [key, value] = &entry_fields[..] else {
                return Err(invalid_data!(
                    "Map field `{}` must have exactly 2 fields, found {}",
                    field.name(),
                    entry_fields.len()
                ));
            };
            let entry_fields = Fields::from([
                map_nested(key, MAP_KEY_FIELD_NAME)?,
                map_nested(value, MAP_VALUE_FIELD_NAME)?,
            ]);
            let entries = entries
                .as_ref()
                .clone()
                .with_data_type(DataType::Struct(entry_fields));
            DataType::Map(Arc::new(entries), *sorted)
        }
        // `Fields::filter_leaves` treats `ListView` and `LargeListView` as leaves, so projection
        // cannot select their elements even with IDs.
        other => other.clone(),
    };

    let mut field = field.as_ref().clone().with_data_type(data_type);
    field
        .metadata_mut()
        .insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
    Ok(Arc::new(field))
}

/// Returns the first entry in `mapped_fields` that lists `name`.
fn find_mapped_field<'a>(
    mapped_fields: impl IntoIterator<Item = &'a MappedField>,
    name: &str,
) -> Option<&'a MappedField> {
    mapped_fields
        .into_iter()
        .find(|mapped_field| mapped_field.names().iter().any(|n| n == name))
}

/// Maps `fields` into a `Fields` sized up front, which `collect::<Result<_>>()` does not do.
fn try_map_fields(
    fields: &Fields,
    mut map_field: impl FnMut(&FieldRef) -> Result<FieldRef>,
) -> Result<Fields> {
    let mut mapped = Vec::with_capacity(fields.len());
    for field in fields {
        mapped.push(map_field(field)?);
    }
    Ok(Fields::from(mapped))
}

/// Rewrites file-side Arrow `FixedSizeList` hints to `List`.
///
/// parquet-rs picks its list reader from the embedded Arrow schema. Iceberg has no
/// fixed-size list type, so a `FixedSizeList` hint must become `List` before the stream
/// reader is built. That uses the 3-level LIST reader (no null-row padding, no
/// `PromotePlan` conversion). Returns `None` when the schema has no `FixedSizeList`.
/// Unchanged fields are shared; nothing is allocated before the first change.
///
/// Same placement as the list-view rewrite in #3467: call this before name mapping and
/// fallback IDs so nested IDs land on the rewritten `List` fields.
pub(super) fn coerce_fixed_size_lists(arrow_schema: &ArrowSchemaRef) -> Option<ArrowSchemaRef> {
    let fields = coerce_fixed_size_lists_in_fields(arrow_schema.fields())?;
    Some(Arc::new(ArrowSchema::new_with_metadata(
        fields,
        arrow_schema.metadata().clone(),
    )))
}

/// Returns `fields` with `FixedSizeList` rewritten to `List`, or `None` if they have none.
fn coerce_fixed_size_lists_in_fields(fields: &Fields) -> Option<Fields> {
    let (index, first) = fields
        .iter()
        .enumerate()
        .find_map(|(index, field)| Some((index, coerce_fixed_size_lists_in_field(field)?)))?;
    let rest = fields
        .iter()
        .skip(index + 1)
        .map(coerce_fixed_size_lists_or_share);
    Some(
        fields
            .iter()
            .take(index)
            .cloned()
            .chain([first])
            .chain(rest)
            .collect(),
    )
}

/// Returns `field` with `FixedSizeList` rewritten to `List`, or `None` if it has none.
fn coerce_fixed_size_lists_in_field(field: &FieldRef) -> Option<FieldRef> {
    let data_type = match field.data_type() {
        DataType::FixedSizeList(element, _) => {
            DataType::List(coerce_fixed_size_lists_or_share(element))
        }
        DataType::List(element) => DataType::List(coerce_fixed_size_lists_in_field(element)?),
        DataType::LargeList(element) => {
            DataType::LargeList(coerce_fixed_size_lists_in_field(element)?)
        }
        DataType::Map(entries, sorted) => {
            DataType::Map(coerce_fixed_size_lists_in_field(entries)?, *sorted)
        }
        DataType::Struct(fields) => DataType::Struct(coerce_fixed_size_lists_in_fields(fields)?),
        _ => return None,
    };
    Some(Arc::new(field.as_ref().clone().with_data_type(data_type)))
}

fn coerce_fixed_size_lists_or_share(field: &FieldRef) -> FieldRef {
    coerce_fixed_size_lists_in_field(field).unwrap_or_else(|| Arc::clone(field))
}

/// Add position-based fallback field IDs to Arrow schema for Parquet files lacking them.
/// Enables projection on migrated files (e.g., from Hive/Spark).
///
/// Why at schema level (not per-batch): Efficiency - avoids repeated schema modification.
/// Why only top-level: Nested projection uses leaf column indices, not parent struct IDs.
/// Why 1-indexed: Compatibility with iceberg-java's ParquetSchemaUtil.addFallbackIds().
pub(super) fn add_fallback_field_ids_to_arrow_schema(
    arrow_schema: &ArrowSchemaRef,
) -> Arc<ArrowSchema> {
    debug_assert!(
        arrow_schema
            .fields()
            .iter()
            .next()
            .is_none_or(|f| f.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none()),
        "Schema already has field IDs"
    );

    let fields_with_fallback_ids: Vec<_> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(pos, field)| {
            let mut metadata = field.metadata().clone();
            let field_id = (pos + 1) as i32; // 1-indexed for Java compatibility
            metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());

            Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                .with_metadata(metadata)
        })
        .collect();

    Arc::new(ArrowSchema::new_with_metadata(
        fields_with_fallback_ids,
        arrow_schema.metadata().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
    use futures::TryStreamExt;
    use parquet::arrow::{
        ArrowWriter, PARQUET_FIELD_ID_META_KEY, ProjectionMask, parquet_to_arrow_schema,
    };
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;
    use tempfile::TempDir;

    use crate::arrow::{ArrowReader, ArrowReaderBuilder};
    use crate::expr::{Bind, Reference};
    use crate::io::FileIO;
    use crate::scan::{FileScanTask, FileScanTaskStream};
    use crate::spec::{
        DataFileFormat, Datum, MappedField, NameMapping, NestedField, PrimitiveType, Schema,
        StructType, Type, VariantType,
    };
    use crate::{ErrorKind, Runtime};

    #[test]
    fn test_name_mapping_duplicate_alias_preserves_first_match() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "name",
            DataType::Utf8,
            true,
        )]));
        for (first_id, expected) in [(Some(2), Some("2")), (None, None)] {
            let name_mapping = NameMapping::new(vec![
                MappedField::new(first_id, vec!["name".to_string()], vec![]),
                MappedField::new(Some(4), vec!["name".to_string()], vec![]),
            ]);

            let mapped =
                super::apply_name_mapping_to_arrow_schema(arrow_schema.clone(), &name_mapping)
                    .unwrap();

            assert_eq!(
                mapped.fields()[0]
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .map(String::as_str),
                expected,
                "first matching field ID: {first_id:?}"
            );
        }
    }

    #[test]
    fn test_arrow_projection_mask() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![1])
                .with_fields(vec![
                    NestedField::required(1, "c1", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(2, "c2", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(
                        3,
                        "c3",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 38,
                            scale: 3,
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("c1", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            // Type not supported
            Field::new("c2", DataType::Duration(TimeUnit::Microsecond), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
            ),
            // Precision is beyond the supported range
            Field::new("c3", DataType::Decimal128(39, 3), true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
        ]));

        let message_type = "
message schema {
  required binary c1 (STRING) = 1;
  optional int32 c2 (INTEGER(8,true)) = 2;
  optional fixed_len_byte_array(17) c3 (DECIMAL(39,3)) = 3;
}
    ";
        let parquet_type = parse_message_type(message_type).expect("should parse schema");
        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_type));

        // Try projecting the fields c2 and c3 with the unsupported data types
        let err = ArrowReader::get_arrow_projection_mask(
            &[1, 2, 3],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.to_string(),
            "DataInvalid => Unsupported Arrow data type: Duration(µs)".to_string()
        );

        // Omitting field c2, we still get an error due to c3 being selected
        let err = ArrowReader::get_arrow_projection_mask(
            &[1, 3],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.to_string(),
            "DataInvalid => Failed to create decimal type, source: DataInvalid => Decimals with precision larger than 38 are not supported: 39".to_string()
        );

        // Finally avoid selecting fields with unsupported data types
        let mask = ArrowReader::get_arrow_projection_mask(
            &[1],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .expect("Some ProjectionMask");
        assert_eq!(mask, ProjectionMask::leaves(&parquet_schema, vec![0]));
    }

    fn field_id_meta(id: i32) -> HashMap<String, String> {
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())])
    }

    /// Arrow `Struct(metadata: Binary, value: Binary)` — the unshredded variant layout.
    fn variant_arrow_fields() -> Fields {
        Fields::from(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, false),
        ])
    }

    /// A variant storage group as it arrives from Parquet: `Struct(metadata, value)` with the
    /// field id on the group and no field ids on the sub-fields. The `arrow.parquet.variant`
    /// extension is intentionally ABSENT — the read path recovers variant identity from the
    /// Iceberg schema and self-attaches it, so this exercises files that lack the annotation.
    fn variant_arrow_field(name: &str, id: i32) -> Field {
        Field::new(name, DataType::Struct(variant_arrow_fields()), false)
            .with_metadata(field_id_meta(id))
    }

    /// A variant is a Parquet group whose leaves carry no field id, so it is projected via
    /// its enclosing group's field id and all of its leaves are read together.
    #[test]
    fn test_arrow_projection_mask_variant() {
        // c1 (String, id 1) + v (Variant, id 2).
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "c1", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "v", Type::Variant(VariantType)).into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("c1", DataType::Utf8, false).with_metadata(field_id_meta(1)),
            variant_arrow_field("v", 2),
        ]));
        let message_type = "
message schema {
  required binary c1 (STRING) = 1;
  required group v = 2 {
    required binary metadata;
    required binary value;
  }
}
";
        let parquet_schema =
            SchemaDescriptor::new(Arc::new(parse_message_type(message_type).unwrap()));

        // Both fields: all three leaves.
        let mask = ArrowReader::get_arrow_projection_mask(
            &[1, 2],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap();
        assert_eq!(mask, ProjectionMask::leaves(&parquet_schema, vec![0, 1, 2]));

        // Variant only: its two leaves (metadata, value).
        let mask_variant = ArrowReader::get_arrow_projection_mask(
            &[2],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap();
        assert_eq!(
            mask_variant,
            ProjectionMask::leaves(&parquet_schema, vec![1, 2])
        );

        // Primitive only: variant leaves must not leak in.
        let mask_primitive = ArrowReader::get_arrow_projection_mask(
            &[1],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap();
        assert_eq!(
            mask_primitive,
            ProjectionMask::leaves(&parquet_schema, vec![0])
        );
    }

    /// A shredded variant (with a `typed_value` sub-field) must be rejected, not silently
    /// projected as metadata/value only.
    #[test]
    fn test_arrow_projection_mask_variant_shredded_is_rejected() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "c1", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "v", Type::Variant(VariantType)).into(),
                ])
                .build()
                .unwrap(),
        );
        let shredded = Field::new(
            "v",
            DataType::Struct(Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("value", DataType::Binary, true),
                Field::new("typed_value", DataType::Int64, true),
            ])),
            false,
        )
        .with_metadata(field_id_meta(2));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("c1", DataType::Utf8, false).with_metadata(field_id_meta(1)),
            shredded,
        ]));
        let message_type = "
message schema {
  required binary c1 (STRING) = 1;
  required group v = 2 {
    required binary metadata;
    optional binary value;
    optional int64 typed_value;
  }
}
";
        let parquet_schema =
            SchemaDescriptor::new(Arc::new(parse_message_type(message_type).unwrap()));

        let err = ArrowReader::get_arrow_projection_mask(
            &[1, 2],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap_err();
        assert_eq!(
            (err.kind(), err.message()),
            (
                ErrorKind::FeatureUnsupported,
                "Reading shredded variant column `v` (field id 2) is not supported yet, see https://github.com/apache/iceberg-rust/issues/3606"
            )
        );

        // An unprojected shredded sibling does not affect other columns.
        let mask = ArrowReader::get_arrow_projection_mask(
            &[1],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap();
        assert_eq!(mask, ProjectionMask::leaves(&parquet_schema, vec![0]));
    }

    #[test]
    fn test_arrow_projection_mask_validates_variant_storage() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "c1", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "v", Type::Variant(VariantType)).into(),
            ])
            .build()
            .unwrap();
        let binary = |name: &str| Field::new(name, DataType::Binary, true);
        let metadata = || Field::new("metadata", DataType::Binary, false);
        let mask_for = |storage: DataType| {
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("c1", DataType::Utf8, false).with_metadata(field_id_meta(1)),
                Field::new("v", storage, false).with_metadata(field_id_meta(2)),
            ]));
            // Only the leaf count matters to the mask; two variant leaves throughout.
            let parquet_schema = SchemaDescriptor::new(Arc::new(
                parse_message_type(
                    "message schema {
                       required binary c1 (STRING) = 1;
                       required group v = 2 { required binary a; required binary b; }
                     }",
                )
                .unwrap(),
            ));
            ArrowReader::get_arrow_projection_mask(
                &[1, 2],
                &schema,
                &parquet_schema,
                &arrow_schema,
                false,
            )
            .map(|mask| (mask, parquet_schema))
        };
        let error_of = |storage: DataType| {
            let err = mask_for(storage).unwrap_err();
            (err.kind(), err.message().to_string())
        };
        let invalid = |problem: &str| {
            (
                ErrorKind::DataInvalid,
                format!("Variant column `v` (field id 2) {problem}"),
            )
        };

        assert_eq!(
            error_of(DataType::Struct(Fields::from(vec![
                metadata(),
                binary("value"),
                binary("foo"),
            ]))),
            invalid("has fields other than `metadata` and `value`")
        );
        assert_eq!(
            error_of(DataType::Struct(Fields::from(vec![
                metadata(),
                binary("foo"),
            ]))),
            invalid("is missing the `value` field")
        );
        assert_eq!(
            error_of(DataType::Struct(Fields::from(vec![
                binary("metadata"),
                binary("value"),
            ]))),
            invalid("has an optional `metadata` field, which must be required")
        );
        assert_eq!(
            error_of(DataType::Struct(Fields::from(vec![binary("value")]))),
            invalid("is missing the `metadata` field")
        );
        assert_eq!(
            error_of(DataType::Struct(Fields::from(vec![
                Field::new("metadata", DataType::Utf8, false),
                binary("value"),
            ]))),
            invalid("has a non-binary `metadata` field: Utf8")
        );
        assert_eq!(
            error_of(DataType::Binary),
            invalid("must be stored as a group, found Binary")
        );
        assert_eq!(
            error_of(DataType::Struct(Fields::from(vec![
                binary("metadata"),
                Field::new("typed_value", DataType::Int64, true),
            ]))),
            (
                ErrorKind::FeatureUnsupported,
                "Reading shredded variant column `v` (field id 2) is not supported yet, see https://github.com/apache/iceberg-rust/issues/3606".to_string()
            )
        );

        // Children are matched by name, and any Arrow binary type holds the bytes.
        let (mask, parquet_schema) = mask_for(DataType::Struct(Fields::from(vec![
            Field::new("value", DataType::LargeBinary, true),
            Field::new("metadata", DataType::BinaryView, false),
        ])))
        .unwrap();
        assert_eq!(mask, ProjectionMask::leaves(&parquet_schema, vec![0, 1, 2]));
    }

    /// A variant nested inside a struct must have both of its sub-leaves projected.
    #[test]
    fn test_arrow_projection_mask_variant_nested_in_struct() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "parent",
                        Type::Struct(StructType::new(vec![
                            NestedField::required(2, "c2", Type::Primitive(PrimitiveType::String))
                                .into(),
                            NestedField::required(3, "v", Type::Variant(VariantType)).into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let parent_fields = Fields::from(vec![
            Field::new("c2", DataType::Utf8, false).with_metadata(field_id_meta(2)),
            variant_arrow_field("v", 3),
        ]);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("parent", DataType::Struct(parent_fields), false)
                .with_metadata(field_id_meta(1)),
        ]));
        let message_type = "
message schema {
  required group parent = 1 {
    required binary c2 (STRING) = 2;
    required group v = 3 {
      required binary metadata;
      required binary value;
    }
  }
}
";
        let parquet_schema =
            SchemaDescriptor::new(Arc::new(parse_message_type(message_type).unwrap()));

        // Nested variant: both of its leaves.
        let mask_variant = ArrowReader::get_arrow_projection_mask(
            &[3],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap();
        assert_eq!(
            mask_variant,
            ProjectionMask::leaves(&parquet_schema, vec![1, 2]),
            "variant nested in a struct was dropped from projection"
        );

        // Sibling primitive: no variant leaves.
        let mask_primitive = ArrowReader::get_arrow_projection_mask(
            &[2],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap();
        assert_eq!(
            mask_primitive,
            ProjectionMask::leaves(&parquet_schema, vec![0])
        );

        // Both.
        let mask_both = ArrowReader::get_arrow_projection_mask(
            &[2, 3],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap();
        assert_eq!(
            mask_both,
            ProjectionMask::leaves(&parquet_schema, vec![0, 1, 2])
        );
    }

    /// `map<string, variant>`: the map's key leaf and the variant value's metadata/value
    /// leaves must all be projected, and the pruned schema must stay a well-formed 2-field
    /// map (regression test for the projected-schema path).
    #[test]
    fn test_arrow_projection_mask_map_with_variant_value() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "m",
                        Type::Map(crate::spec::MapType::required(
                            2,
                            Type::Primitive(PrimitiveType::String),
                            3,
                            Type::Variant(VariantType),
                        )),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let entries = Field::new(
            "key_value",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false).with_metadata(field_id_meta(2)),
                variant_arrow_field("value", 3),
            ])),
            false,
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("m", DataType::Map(Arc::new(entries), false), false)
                .with_metadata(field_id_meta(1)),
        ]));
        let message_type = "
message schema {
  required group m (MAP) = 1 {
    repeated group key_value {
      required binary key (STRING) = 2;
      required group value = 3 {
        required binary metadata;
        required binary value;
      }
    }
  }
}
";
        let parquet_schema =
            SchemaDescriptor::new(Arc::new(parse_message_type(message_type).unwrap()));

        let mask = ArrowReader::get_arrow_projection_mask(
            &[1],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap();
        assert_eq!(mask, ProjectionMask::leaves(&parquet_schema, vec![0, 1, 2]));
    }

    /// `list<variant>`: the variant element's metadata/value leaves must both be projected.
    #[test]
    fn test_arrow_projection_mask_list_with_variant() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "l",
                        Type::List(crate::spec::ListType::new(
                            NestedField::list_element(2, Type::Variant(VariantType), true).into(),
                        )),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "l",
                DataType::List(Arc::new(variant_arrow_field("element", 2))),
                false,
            )
            .with_metadata(field_id_meta(1)),
        ]));
        let message_type = "
message schema {
  required group l (LIST) = 1 {
    repeated group list {
      required group element = 2 {
        required binary metadata;
        required binary value;
      }
    }
  }
}
";
        let parquet_schema =
            SchemaDescriptor::new(Arc::new(parse_message_type(message_type).unwrap()));

        let mask = ArrowReader::get_arrow_projection_mask(
            &[1],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap();
        assert_eq!(mask, ProjectionMask::leaves(&parquet_schema, vec![0, 1]));
    }

    /// `collect_variant_leaves` must number leaves exactly like `Fields::filter_leaves`,
    /// including `filter_leaves`' `Dictionary`/`RunEndEncoded` unwrapping and `Union` descent.
    /// Otherwise a sibling of those kinds shifts every following variant leaf onto the wrong
    /// column.
    #[test]
    fn test_collect_variant_leaves_numbering_matches_filter_leaves() {
        use arrow_schema::{UnionFields, UnionMode};

        let union_fields: UnionFields = [
            (0i8, Arc::new(Field::new("i", DataType::Int32, false))),
            (1i8, Arc::new(Field::new("l", DataType::Int64, false))),
        ]
        .into_iter()
        .collect();

        // Siblings that each expand to multiple leaves under `filter_leaves`, placed before the
        // variant: a Dictionary(Struct{a,b}), a Union{i32,i64}, and a RunEndEncoded(Struct{x,y})
        // — 2 leaves each. `filter_leaves` unwraps Dictionary/RunEndEncoded to their value type
        // and descends Union members; our walk must match.
        let dict_struct = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ]))),
        );
        let union = DataType::Union(union_fields, UnionMode::Sparse);
        let run_end_encoded = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new(
                "values",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int32, false),
                    Field::new("y", DataType::Int32, false),
                ])),
                true,
            )),
        );
        let int_item = || Arc::new(Field::new("item", DataType::Int32, false));
        let list_of_struct = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("p", DataType::Int32, false),
                Field::new("q", DataType::Int32, false),
            ])),
            false,
        )));
        let map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", DataType::Int32, true),
                ])),
                false,
            )),
            false,
        );
        let fields = Fields::from(vec![
            Field::new("d", dict_struct, false),
            Field::new("u", union, false),
            Field::new("r", run_end_encoded, false),
            Field::new("ls", list_of_struct, false),
            Field::new("ll", DataType::LargeList(int_item()), false),
            Field::new("fl", DataType::FixedSizeList(int_item(), 2), false),
            Field::new("m", map, false),
            variant_arrow_field("v", 2),
            Field::new(
                "w",
                DataType::List(Arc::new(variant_arrow_field("element", 3))),
                false,
            ),
        ]);

        let mut leaf_idx_to_variant_field_id = HashMap::new();
        let mut leaf_idx = 0usize;
        ArrowReader::collect_variant_leaves(
            &fields,
            &mut leaf_idx,
            None,
            &HashSet::from([2, 3]),
            &mut leaf_idx_to_variant_field_id,
        )
        .unwrap();

        let mut arrow_leaf_names = vec![];
        let _ = fields.filter_leaves(|idx, field| {
            assert_eq!(idx, arrow_leaf_names.len());
            arrow_leaf_names.push(field.name().clone());
            false
        });
        assert_eq!(arrow_leaf_names, [
            "a", "b", "i", "l", "x", "y", "p", "q", "item", "item", "keys", "values", "metadata",
            "value", "metadata", "value",
        ]);
        assert_eq!(leaf_idx, arrow_leaf_names.len());
        assert_eq!(
            leaf_idx_to_variant_field_id,
            HashMap::from([(12, 2), (13, 2), (14, 3), (15, 3)])
        );
    }

    /// Test schema evolution: reading old Parquet file (with only column 'a')
    /// using a newer table schema (with columns 'a' and 'b').
    /// This tests that:
    /// 1. get_arrow_projection_mask allows missing columns
    /// 2. RecordBatchTransformer adds missing column 'b' with NULL values
    #[tokio::test]
    async fn test_schema_evolution_add_column() {
        use arrow_array::{Array, Int32Array};

        // New table schema: columns 'a' and 'b' (b was added later, file only has 'a')
        let new_schema = Arc::new(
            Schema::builder()
                .with_schema_id(2)
                .with_fields(vec![
                    NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Create Arrow schema for old Parquet file (only has column 'a')
        let arrow_schema_old = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        // Write old Parquet file with only column 'a'
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let data_a = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema_old.clone(), vec![data_a]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{table_location}/old_file.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        // Read the old Parquet file using the NEW schema (with column 'b')
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/old_file.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/old_file.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(new_schema.clone())
                .with_project_field_ids(vec![1, 2]) // Request both columns 'a' and 'b'
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Verify we got the correct data
        assert_eq!(result.len(), 1);
        let batch = &result[0];

        // Should have 2 columns now
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        // Column 'a' should have the original data
        let col_a = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(col_a.values(), &[1, 2, 3]);

        // Column 'b' should be all NULLs (it didn't exist in the old file)
        let col_b = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(col_b.null_count(), 3);
        assert!(col_b.is_null(0));
        assert!(col_b.is_null(1));
        assert!(col_b.is_null(2));
    }

    /// Test reading Parquet files without field ID metadata (e.g., migrated tables).
    /// This exercises the position-based fallback path.
    ///
    /// Corresponds to Java's ParquetSchemaUtil.addFallbackIds() + pruneColumnsFallback()
    /// in /parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java
    #[tokio::test]
    async fn test_read_parquet_file_without_field_ids() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "age", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Parquet file from a migrated table - no field ID metadata
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let name_data = vec!["Alice", "Bob", "Charlie"];
        let age_data = vec![30, 25, 35];

        use arrow_array::Int32Array;
        let name_col = Arc::new(StringArray::from(name_data.clone())) as ArrayRef;
        let age_col = Arc::new(Int32Array::from(age_data.clone())) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![name_col, age_col]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        // Verify position-based mapping: field_id 1 → position 0, field_id 2 → position 1
        let name_array = batch.column(0).as_string::<i32>();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");
        assert_eq!(name_array.value(2), "Charlie");

        let age_array = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(age_array.value(0), 30);
        assert_eq!(age_array.value(1), 25);
        assert_eq!(age_array.value(2), 35);
    }

    /// Regression test for #2403: when a Parquet file lacks embedded field IDs but a
    /// name mapping is present, projection must use the field IDs assigned by the name
    /// mapping — not the position-based fallback (field_id N → column N-1).
    ///
    /// The scenario uses a file whose physical column order does not line up with the
    /// Iceberg field IDs: physical columns are `[name, subdept]` while the mapping
    /// assigns `name → 2` and `subdept → 4`. The position fallback would project
    /// field 2 from physical column 1 (`subdept`) and drop field 4 (column 3 is out of
    /// range), silently misreading data.
    #[tokio::test]
    async fn test_read_parquet_with_name_mapping_uses_mapped_field_ids() {
        // Iceberg schema: physical file order does NOT match field-id order.
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(3, "dept", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(4, "subdept", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        // Migrated Parquet file: no field-id metadata, physical columns [name, subdept].
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("subdept", DataType::Utf8, true),
        ]));

        let name_mapping = Arc::new(NameMapping::new(vec![
            MappedField::new(Some(2), vec!["name".to_string()], vec![]),
            MappedField::new(Some(4), vec!["subdept".to_string()], vec![]),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let name_col = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef;
        let subdept_col = Arc::new(StringArray::from(vec!["comms", "tax", "audit"])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![name_col, subdept_col]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![2, 4])
                .with_case_sensitive(false)
                .with_name_mapping(Some(name_mapping))
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        // field 2 (`name`) must come from physical column 0, not be NULL-filled.
        let name_array = batch.column(0).as_string::<i32>();
        assert_eq!(
            name_array.null_count(),
            0,
            "`name` was NULL-filled: name mapping was ignored and position fallback was used"
        );
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");
        assert_eq!(name_array.value(2), "Charlie");

        // field 4 (`subdept`) must come from physical column 1.
        let subdept_array = batch.column(1).as_string::<i32>();
        assert_eq!(subdept_array.null_count(), 0);
        assert_eq!(subdept_array.value(0), "comms");
        assert_eq!(subdept_array.value(1), "tax");
        assert_eq!(subdept_array.value(2), "audit");
    }

    /// Regression test for #2403, predicate side: with a name mapping present, predicate
    /// pushdown must resolve field IDs via the mapping rather than the position fallback,
    /// which would evaluate the filter against the wrong physical column.
    #[tokio::test]
    async fn test_predicate_on_name_mapped_file_uses_mapped_field_ids() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(3, "dept", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(4, "subdept", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("subdept", DataType::Utf8, true),
        ]));

        let name_mapping = Arc::new(NameMapping::new(vec![
            MappedField::new(Some(2), vec!["name".to_string()], vec![]),
            MappedField::new(Some(4), vec!["subdept".to_string()], vec![]),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        // Engineered so that filtering the wrong physical column yields different rows:
        // `name` and `subdept` both contain the value "Alice", on different rows.
        let name_col = Arc::new(StringArray::from(vec!["Alice", "Bob", "Sue"])) as ArrayRef;
        let subdept_col = Arc::new(StringArray::from(vec!["Bob", "Alice", "Alice"])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![name_col, subdept_col]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let predicate = Reference::new("name").equal_to(Datum::string("Alice"));

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current())
            .with_row_group_filtering_enabled(true)
            .with_row_selection_enabled(true)
            .build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![2, 4])
                .with_case_sensitive(false)
                .with_name_mapping(Some(name_mapping))
                .with_predicate(Some(predicate.bind(schema, true).unwrap()))
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 1,
            "filter `name = \"Alice\"` matched the wrong rows: predicate was evaluated \
             against the wrong physical column"
        );

        let batch = &result[0];
        let name_array = batch.column(0).as_string::<i32>();
        assert_eq!(name_array.value(0), "Alice");
        let subdept_array = batch.column(1).as_string::<i32>();
        assert_eq!(subdept_array.value(0), "Bob");
    }

    /// Regression test for #3374: a struct in a name-mapped file reads correctly next to a
    /// top-level column, and predicates resolve its fields.
    #[tokio::test]
    async fn test_read_parquet_with_name_mapping_maps_nested_fields() {
        use arrow_array::types::Int32Type;
        use arrow_array::{Int32Array, StructArray};

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(
                        2,
                        "s",
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(3, "a", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let name_mapping = Arc::new(NameMapping::new(vec![
            MappedField::new(Some(1), vec!["id".to_string()], vec![]),
            MappedField::new(Some(2), vec!["s".to_string()], vec![MappedField::new(
                Some(3),
                vec!["a".to_string()],
                vec![],
            )]),
        ]));

        // No field IDs at any level.
        let s_col = StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
        )]);
        let to_write = RecordBatch::try_from_iter_with_nullable(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                true,
            ),
            ("s", Arc::new(s_col) as ArrayRef, true),
        ])
        .unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/1.parquet", tmp_dir.path().to_str().unwrap());
        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), None).unwrap();
        writer.write(&to_write).unwrap();
        writer.close().unwrap();

        let file_size = std::fs::metadata(&file_path).unwrap().len();
        let tasks = |predicate| {
            Box::pin(futures::stream::iter([Ok(FileScanTask::builder()
                .with_file_size_in_bytes(file_size)
                .with_start(0)
                .with_length(0)
                .with_data_file_path(file_path.clone())
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .with_name_mapping(Some(name_mapping.clone()))
                .with_predicate(predicate)
                .build()
                .unwrap())])) as FileScanTaskStream
        };
        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();

        let result = reader
            .clone()
            .read(tasks(None))
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();
        let s = result[0].column(1).as_struct();
        assert_eq!(s.null_count(), 0);
        assert_eq!(
            s.column(0).as_primitive::<Int32Type>(),
            &Int32Array::from(vec![10, 20, 30])
        );

        // `s.a` resolves, so the row filter rejects the nested predicate as it does for files
        // with embedded IDs, rather than matching no rows.
        let predicate = Reference::new("s.a")
            .greater_than(Datum::int(15))
            .bind(schema.clone(), true)
            .unwrap();
        let err = reader
            .read(tasks(Some(predicate)))
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    /// Every nested Arrow type gets IDs, with list and map entries matched by their spec names.
    #[test]
    fn test_apply_name_mapping_to_nested_arrow_types() {
        use arrow_schema::Fields;

        use super::apply_name_mapping_to_arrow_schema;
        use crate::arrow::arrow_schema_to_schema;

        let field = |name: &str, data_type| Field::new(name, data_type, true);
        let item = |data_type| Arc::new(field("item", data_type));
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                field("values", DataType::Int32),
            ])),
            false,
        );
        let x = Fields::from(vec![field("x", DataType::Int32)]);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            field("id", DataType::Int32),
            // `s.id` must take its ID from the entry for `s`, not from the top-level `id`.
            field(
                "s",
                DataType::Struct(Fields::from(vec![field("id", DataType::Int32)])),
            ),
            field("list", DataType::List(item(DataType::Int32))),
            field("fixed", DataType::FixedSizeList(item(DataType::Int32), 2)),
            field("large", DataType::LargeList(item(DataType::Struct(x)))),
            field("map", DataType::Map(Arc::new(entries), true)),
        ]));
        let entry =
            |id, name: &str, fields| MappedField::new(Some(id), vec![name.to_string()], fields);
        let name_mapping = NameMapping::new(vec![
            entry(1, "id", vec![]),
            entry(2, "s", vec![entry(3, "id", vec![])]),
            entry(4, "list", vec![entry(5, "element", vec![])]),
            entry(6, "fixed", vec![entry(7, "element", vec![])]),
            entry(8, "large", vec![entry(9, "element", vec![entry(
                10,
                "x",
                vec![],
            )])]),
            entry(11, "map", vec![
                entry(12, "key", vec![]),
                entry(13, "value", vec![]),
            ]),
        ]);

        let mapped = apply_name_mapping_to_arrow_schema(arrow_schema, &name_mapping).unwrap();

        // The list size and the map ordering carry over.
        assert!(matches!(
            mapped.field(3).data_type(),
            DataType::FixedSizeList(_, 2)
        ));
        assert!(matches!(
            mapped.field(5).data_type(),
            DataType::Map(_, true)
        ));
        // The conversion fails on any field without an ID.
        let schema = arrow_schema_to_schema(&mapped).unwrap();
        let names: Vec<_> = (1..=13)
            .map(|id| schema.name_by_field_id(id).unwrap_or_default())
            .collect();
        assert_eq!(names, [
            "id",
            "s",
            "s.id",
            "list",
            "list.element",
            "fixed",
            "fixed.element",
            "large",
            "large.element",
            "large.element.x",
            "map",
            "map.key",
            "map.value",
        ]);
    }

    /// A field without an entry, or whose entry has no field ID, keeps no ID, and so do its
    /// nested fields.
    #[test]
    fn test_apply_name_mapping_skips_unmapped_fields() {
        use arrow_schema::Fields;

        use super::{apply_name_mapping_to_arrow_schema, build_field_id_map_from_arrow_schema};

        let int = |name: &str| Field::new(name, DataType::Int32, true);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "s",
                DataType::Struct(Fields::from(vec![int("a"), int("b")])),
                true,
            ),
            Field::new("t", DataType::Struct(Fields::from(vec![int("c")])), true),
        ]));
        let entry = |id, name: &str, fields| MappedField::new(id, vec![name.to_string()], fields);
        let name_mapping = NameMapping::new(vec![
            // No entry for `s.b`.
            entry(Some(1), "s", vec![entry(Some(2), "a", vec![])]),
            entry(None, "t", vec![entry(Some(3), "c", vec![])]),
        ]);

        let mapped = apply_name_mapping_to_arrow_schema(arrow_schema, &name_mapping).unwrap();

        // Only `s.a`, leaf 0, resolves to an ID.
        assert_eq!(
            build_field_id_map_from_arrow_schema(&mapped),
            HashMap::from([(2, 0)])
        );
    }

    /// Test reading Parquet files without field IDs with partial projection.
    /// Only a subset of columns are requested, verifying position-based fallback
    /// handles column selection correctly.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_partial_projection() {
        use arrow_array::Int32Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "col1", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "col2", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(3, "col3", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(4, "col4", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("col1", DataType::Utf8, false),
            Field::new("col2", DataType::Int32, false),
            Field::new("col3", DataType::Utf8, false),
            Field::new("col4", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let col1_data = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let col2_data = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;
        let col3_data = Arc::new(StringArray::from(vec!["c", "d"])) as ArrayRef;
        let col4_data = Arc::new(Int32Array::from(vec![30, 40])) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
            col1_data, col2_data, col3_data, col4_data,
        ])
        .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 3])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let col1_array = batch.column(0).as_string::<i32>();
        assert_eq!(col1_array.value(0), "a");
        assert_eq!(col1_array.value(1), "b");

        let col3_array = batch.column(1).as_string::<i32>();
        assert_eq!(col3_array.value(0), "c");
        assert_eq!(col3_array.value(1), "d");
    }

    /// Test reading Parquet files without field IDs with schema evolution.
    /// The Iceberg schema has more fields than the Parquet file, testing that
    /// missing columns are filled with NULLs.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_schema_evolution() {
        use arrow_array::{Array, Int32Array};

        // Schema with field 3 added after the file was written
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "age", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "city", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let name_data = Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef;
        let age_data = Arc::new(Int32Array::from(vec![30, 25])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![name_data, age_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2, 3])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let name_array = batch.column(0).as_string::<i32>();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");

        let age_array = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(age_array.value(0), 30);
        assert_eq!(age_array.value(1), 25);

        // Verify missing column filled with NULLs
        let city_array = batch.column(2).as_string::<i32>();
        assert_eq!(city_array.null_count(), 2);
        assert!(city_array.is_null(0));
        assert!(city_array.is_null(1));
    }

    /// Test reading Parquet files without field IDs that have multiple row groups.
    /// This ensures the position-based fallback works correctly across row group boundaries.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_multiple_row_groups() {
        use arrow_array::Int32Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "value", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        // Small row group size to create multiple row groups
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_write_batch_size(2)
            .set_max_row_group_row_count(Some(2))
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();

        // Write 6 rows in 3 batches (will create 3 row groups)
        for batch_num in 0..3 {
            let name_data = Arc::new(StringArray::from(vec![
                format!("name_{}", batch_num * 2),
                format!("name_{}", batch_num * 2 + 1),
            ])) as ArrayRef;
            let value_data =
                Arc::new(Int32Array::from(vec![batch_num * 2, batch_num * 2 + 1])) as ArrayRef;

            let batch =
                RecordBatch::try_new(arrow_schema.clone(), vec![name_data, value_data]).unwrap();
            writer.write(&batch).expect("Writing batch");
        }
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert!(!result.is_empty());

        let mut all_names = Vec::new();
        let mut all_values = Vec::new();

        for batch in &result {
            let name_array = batch.column(0).as_string::<i32>();
            let value_array = batch
                .column(1)
                .as_primitive::<arrow_array::types::Int32Type>();

            for i in 0..batch.num_rows() {
                all_names.push(name_array.value(i).to_string());
                all_values.push(value_array.value(i));
            }
        }

        assert_eq!(all_names.len(), 6);
        assert_eq!(all_values.len(), 6);

        for i in 0..6 {
            assert_eq!(all_names[i], format!("name_{i}"));
            assert_eq!(all_values[i], i as i32);
        }
    }

    /// Test reading Parquet files without field IDs with nested types (struct).
    /// Java's pruneColumnsFallback() projects entire top-level columns including nested content,
    /// but only top-level fields get fallback ids, so struct fields read as null.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_reads_struct_fields_as_null() {
        use arrow_array::{Int32Array, StructArray};
        use arrow_schema::Fields;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(
                        2,
                        "person",
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(
                                3,
                                "name",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            NestedField::optional(4, "age", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "person",
                DataType::Struct(Fields::from(vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new("age", DataType::Int32, false),
                ])),
                false,
            ),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let id_data = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let name_data = Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef;
        let age_data = Arc::new(Int32Array::from(vec![30, 25])) as ArrayRef;
        let person_data = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                name_data,
            ),
            (
                Arc::new(Field::new("age", DataType::Int32, false)),
                age_data,
            ),
        ])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![id_data, person_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let id_array = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);

        let person_array = batch.column(1).as_struct();
        assert_eq!(person_array.num_columns(), 2);
        assert_eq!(person_array.null_count(), 0);
        assert_eq!(person_array.column(0).null_count(), 2);
        assert_eq!(person_array.column(1).null_count(), 2);
    }

    /// Test reading Parquet files without field IDs with schema evolution - column added in the middle.
    /// When a new column is inserted between existing columns in the schema order,
    /// the fallback projection must correctly map field IDs to output positions.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_schema_evolution_add_column_in_middle() {
        use arrow_array::{Array, Int32Array};

        let arrow_schema_old = Arc::new(ArrowSchema::new(vec![
            Field::new("col0", DataType::Int32, true),
            Field::new("col1", DataType::Int32, true),
        ]));

        // New column added between existing columns: col0 (id=1), newCol (id=5), col1 (id=2)
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "col0", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(5, "newCol", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "col1", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let col0_data = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let col1_data = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema_old.clone(), vec![col0_data, col1_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 5, 2])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let result_col0 = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(result_col0.value(0), 1);
        assert_eq!(result_col0.value(1), 2);

        // New column should be NULL (doesn't exist in old file)
        let result_newcol = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(result_newcol.null_count(), 2);
        assert!(result_newcol.is_null(0));
        assert!(result_newcol.is_null(1));

        let result_col1 = batch
            .column(2)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(result_col1.value(0), 10);
        assert_eq!(result_col1.value(1), 20);
    }

    /// Test reading Parquet files without field IDs with a filter that eliminates all row groups.
    /// During development of field ID mapping, we saw a panic when row_selection_enabled=true and
    /// all row groups are filtered out.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_filter_eliminates_all_rows() {
        use arrow_array::{Float64Array, Int32Array};

        // Schema with fields that will use fallback IDs 1, 2, 3
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "value", Type::Primitive(PrimitiveType::Double))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        // Write data where all ids are >= 10
        let id_data = Arc::new(Int32Array::from(vec![10, 11, 12])) as ArrayRef;
        let name_data = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let value_data = Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![id_data, name_data, value_data])
                .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        // Filter that eliminates all row groups: id < 5
        let predicate = Reference::new("id").less_than(Datum::int(5));

        // Enable both row_group_filtering and row_selection - triggered the panic
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current())
            .with_row_group_filtering_enabled(true)
            .with_row_selection_enabled(true)
            .build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2, 3])
                .with_case_sensitive(false)
                .with_predicate(Some(predicate.bind(schema, true).unwrap()))
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        // Should no longer panic
        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Should return empty results
        assert!(result.is_empty() || result.iter().all(|batch| batch.num_rows() == 0));
    }

    /// Test bucket partitioning reads source column from data file (not partition metadata).
    ///
    /// This is an integration test verifying the complete ArrowReader pipeline with bucket partitioning.
    /// It corresponds to TestRuntimeFiltering tests in Iceberg Java (e.g., testRenamedSourceColumnTable).
    ///
    /// # Iceberg Spec Requirements
    ///
    /// Per the Iceberg spec "Column Projection" section:
    /// > "Return the value from partition metadata if an **Identity Transform** exists for the field"
    ///
    /// This means:
    /// - Identity transforms (e.g., `identity(dept)`) use constants from partition metadata
    /// - Non-identity transforms (e.g., `bucket(4, id)`) must read source columns from data files
    /// - Partition metadata for bucket transforms stores bucket numbers (0-3), NOT source values
    ///
    /// Java's PartitionUtil.constantsMap() implements this via:
    /// ```java
    /// if (field.transform().isIdentity()) {
    ///     idToConstant.put(field.sourceId(), converted);
    /// }
    /// ```
    ///
    /// # What This Test Verifies
    ///
    /// This test ensures the full ArrowReader → RecordBatchTransformer pipeline correctly handles
    /// bucket partitioning when FileScanTask provides partition_spec and partition_data:
    ///
    /// - Parquet file has field_id=1 named "id" with actual data [1, 5, 9, 13]
    /// - FileScanTask specifies partition_spec with bucket(4, id) and partition_data with bucket=1
    /// - RecordBatchTransformer.constants_map() excludes bucket-partitioned field from constants
    /// - ArrowReader correctly reads [1, 5, 9, 13] from the data file
    /// - Values are NOT replaced with constant 1 from partition metadata
    ///
    /// # Why This Matters
    ///
    /// Without correct handling:
    /// - Runtime filtering would break (e.g., `WHERE id = 5` would fail)
    /// - Query results would be incorrect (all rows would have id=1)
    /// - Bucket partitioning would be unusable for query optimization
    ///
    /// # References
    /// - Iceberg spec: format/spec.md "Column Projection" + "Partition Transforms"
    /// - Java test: spark/src/test/java/.../TestRuntimeFiltering.java
    /// - Java impl: core/src/main/java/org/apache/iceberg/util/PartitionUtil.java
    #[tokio::test]
    async fn test_bucket_partitioning_reads_source_column_from_file() {
        use arrow_array::Int32Array;

        use crate::spec::{Literal, PartitionSpec, Struct, Transform};

        // Iceberg schema with id and name columns
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Partition spec: bucket(4, id)
        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .add_partition_field("id", "id_bucket", Transform::Bucket(4))
                .unwrap()
                .build()
                .unwrap(),
        );

        // Partition data: bucket value is 1
        let partition_data = Struct::from_iter(vec![Some(Literal::int(1))]);

        // Create Arrow schema with field IDs for Parquet file
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        // Write Parquet file with data
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let id_data = Arc::new(Int32Array::from(vec![1, 5, 9, 13])) as ArrayRef;
        let name_data =
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave"])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![id_data, name_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{}/data.parquet", &table_location)).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        // Read the Parquet file with partition spec and data
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/data.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/data.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .with_partition(Some(partition_data))
                .with_partition_spec(Some(partition_spec))
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Verify we got the correct data
        assert_eq!(result.len(), 1);
        let batch = &result[0];

        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 4);

        // The id column MUST contain actual values from the Parquet file [1, 5, 9, 13],
        // NOT the constant partition value 1
        let id_col = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 5);
        assert_eq!(id_col.value(2), 9);
        assert_eq!(id_col.value(3), 13);

        let name_col = batch.column(1).as_string::<i32>();
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");
        assert_eq!(name_col.value(2), "Charlie");
        assert_eq!(name_col.value(3), "Dave");
    }

    /// Regression for <https://github.com/apache/iceberg-rust/issues/2306>:
    /// predicate on a column after nested types in a migrated file (no field IDs).
    /// Schema has struct, list, and map columns before the predicate target (`id`),
    /// exercising the fallback field ID mapping across all nested type variants.
    #[tokio::test]
    async fn test_predicate_on_migrated_file_with_nested_types() {
        use serde::{Deserialize, Serialize};
        use serde_arrow::schema::{SchemaLike, TracingOptions};

        #[derive(Serialize, Deserialize)]
        struct Person {
            name: String,
            age: i32,
        }

        #[derive(Serialize, Deserialize)]
        struct Row {
            person: Person,
            people: Vec<Person>,
            props: std::collections::BTreeMap<String, String>,
            id: i32,
        }

        let rows = vec![
            Row {
                person: Person {
                    name: "Alice".into(),
                    age: 30,
                },
                people: vec![Person {
                    name: "Alice".into(),
                    age: 30,
                }],
                props: [("k1".into(), "v1".into())].into(),
                id: 1,
            },
            Row {
                person: Person {
                    name: "Bob".into(),
                    age: 25,
                },
                people: vec![Person {
                    name: "Bob".into(),
                    age: 25,
                }],
                props: [("k2".into(), "v2".into())].into(),
                id: 2,
            },
            Row {
                person: Person {
                    name: "Carol".into(),
                    age: 40,
                },
                people: vec![Person {
                    name: "Carol".into(),
                    age: 40,
                }],
                props: [("k3".into(), "v3".into())].into(),
                id: 3,
            },
        ];

        let tracing_options = TracingOptions::default()
            .map_as_struct(false)
            .strings_as_large_utf8(false)
            .sequence_as_large_list(false);
        let fields = Vec::<arrow_schema::FieldRef>::from_type::<Row>(tracing_options).unwrap();
        let arrow_schema = Arc::new(ArrowSchema::new(fields.clone()));
        let batch = serde_arrow::to_record_batch(&fields, &rows).unwrap();

        // Fallback field IDs: person=1, people=2, props=3, id=4
        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "person",
                        Type::Struct(StructType::new(vec![
                            NestedField::required(
                                5,
                                "name",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            NestedField::required(6, "age", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                    NestedField::required(
                        2,
                        "people",
                        Type::List(crate::spec::ListType {
                            element_field: NestedField::required(
                                7,
                                "element",
                                Type::Struct(StructType::new(vec![
                                    NestedField::required(
                                        8,
                                        "name",
                                        Type::Primitive(PrimitiveType::String),
                                    )
                                    .into(),
                                    NestedField::required(
                                        9,
                                        "age",
                                        Type::Primitive(PrimitiveType::Int),
                                    )
                                    .into(),
                                ])),
                            )
                            .into(),
                        }),
                    )
                    .into(),
                    NestedField::required(
                        3,
                        "props",
                        Type::Map(crate::spec::MapType {
                            key_field: NestedField::required(
                                10,
                                "key",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            value_field: NestedField::required(
                                11,
                                "value",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                        }),
                    )
                    .into(),
                    NestedField::required(4, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/1.parquet");

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();

        let predicate = Reference::new("id").greater_than(Datum::int(1));

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
            .with_row_group_filtering_enabled(true)
            .with_row_selection_enabled(true)
            .build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
                .with_start(0)
                .with_length(0)
                .with_data_file_path(file_path)
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(iceberg_schema.clone())
                .with_project_field_ids(vec![4])
                .with_case_sensitive(false)
                .with_predicate(Some(predicate.bind(iceberg_schema, true).unwrap()))
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let ids: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(ids, vec![2, 3]);
    }

    /// Reads a file written with an older nested schema through an evolved one. This relies
    /// on parquet-rs attaching `PARQUET:field_id` to struct children, list elements, and map
    /// keys and values, which the hand-built fixtures in `record_batch_transformer` assume.
    #[tokio::test]
    async fn test_read_evolved_nested_fields_by_field_id() {
        use serde_json::{Value, json};

        use crate::arrow::schema_to_arrow_schema;
        use crate::spec::PrimitiveType::{Int, Long};
        use crate::spec::{ListType, MapType};

        fn struct_of(fields: &[(i32, &str, PrimitiveType)]) -> Type {
            Type::Struct(StructType::new(
                fields
                    .iter()
                    .map(|(id, name, ty)| {
                        NestedField::optional(*id, *name, Type::Primitive(ty.clone())).into()
                    })
                    .collect(),
            ))
        }
        fn schema(s: Type, element: Type, value: Type) -> Schema {
            let string = Type::Primitive(PrimitiveType::String);
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(Int)).into(),
                    NestedField::optional(2, "s", s).into(),
                    NestedField::optional(
                        3,
                        "l",
                        Type::List(ListType::new(
                            NestedField::list_element(7, element, true).into(),
                        )),
                    )
                    .into(),
                    NestedField::optional(
                        4,
                        "m",
                        Type::Map(MapType::required(9, string, 10, value)),
                    )
                    .into(),
                ])
                .build()
                .unwrap()
        }
        fn to_batch(schema: &Schema, rows: Value) -> RecordBatch {
            let fields = schema_to_arrow_schema(schema).unwrap().fields().to_vec();
            serde_arrow::to_record_batch(&fields, &rows).unwrap()
        }

        let old_schema = schema(
            struct_of(&[(5, "a", Int), (6, "b", PrimitiveType::String)]),
            struct_of(&[(8, "x", Int)]),
            struct_of(&[(11, "x", Int)]),
        );
        // Reorders `s`, adds children before, between, and after existing ones, and
        // promotes int to long.
        let new_schema = Arc::new(schema(
            struct_of(&[
                (6, "b", PrimitiveType::String),
                (12, "c", Int),
                (5, "a", Long),
            ]),
            struct_of(&[(13, "y", Int), (8, "x", Int)]),
            struct_of(&[(11, "x", Long), (14, "y", Int)]),
        ));
        let old_batch = to_batch(
            &old_schema,
            json!([
                {"id": 1, "s": {"a": 1, "b": "p"}, "l": [{"x": 10}, {"x": 11}], "m": {"k1": {"x": 100}}},
                {"id": 2, "s": null, "l": null, "m": {"k2": {"x": 200}}},
                {"id": 3, "s": {"a": 3, "b": null}, "l": [{"x": 30}], "m": null},
            ]),
        );
        let expected = to_batch(
            &new_schema,
            json!([
                {
                    "id": 1,
                    "s": {"b": "p", "c": null, "a": 1},
                    "l": [{"y": null, "x": 10}, {"y": null, "x": 11}],
                    "m": {"k1": {"x": 100, "y": null}},
                },
                {"id": 2, "s": null, "l": null, "m": {"k2": {"x": 200, "y": null}}},
                {"id": 3, "s": {"b": null, "c": null, "a": 3}, "l": [{"y": null, "x": 30}], "m": null},
            ]),
        );

        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/old.parquet", tmp_dir.path().to_str().unwrap());
        let mut writer =
            ArrowWriter::try_new(File::create(&file_path).unwrap(), old_batch.schema(), None)
                .unwrap();
        writer.write(&old_batch).unwrap();
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
                .with_start(0)
                .with_length(0)
                .with_data_file_path(file_path)
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(new_schema)
                .with_project_field_ids(vec![1, 2, 3, 4])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();
        assert_eq!(result, vec![expected]);
    }

    // ---------------------------------------------------------------------
    // End-to-end variant read tests (synthetic Parquet).
    //
    // Unless a test says otherwise, the files carry a field id on the variant group, none on
    // its leaves, and no Parquet VARIANT annotation. `value` is required, as in iceberg-java's
    // unshredded layout.
    // ---------------------------------------------------------------------

    // Known variant bytes the reader must round-trip unchanged. `value` is distinct per row,
    // so a dropped or reordered row is caught, not just the struct shape.
    const VARIANT_METADATA: [&[u8]; 3] = [b"\x01", b"\x01", b"\x01"];
    const VARIANT_VALUE: [&[u8]; 3] = [b"\x0a", b"\x0b", b"\x0c"];

    /// The 3-row variant `StructArray` for [`VARIANT_METADATA`]/[`VARIANT_VALUE`].
    fn variant_struct_array() -> ArrayRef {
        use arrow_array::{BinaryArray, StructArray};
        let metadata = Arc::new(BinaryArray::from(VARIANT_METADATA.to_vec())) as ArrayRef;
        let value = Arc::new(BinaryArray::from(VARIANT_VALUE.to_vec())) as ArrayRef;
        Arc::new(StructArray::new(
            variant_arrow_fields(),
            vec![metadata, value],
            None,
        )) as ArrayRef
    }

    /// Asserts `field` is a variant and `column` holds the bytes of [`variant_struct_array`]
    /// in the table schema's Arrow type.
    fn assert_variant(field: &Field, column: &ArrayRef) {
        use arrow_array::StructArray;
        let DataType::Struct(table_fields) =
            crate::arrow::type_to_arrow_type(&Type::Variant(VariantType)).unwrap()
        else {
            unreachable!()
        };
        let expected = StructArray::new(
            table_fields,
            variant_struct_array().as_struct().columns().to_vec(),
            None,
        );
        assert_eq!(field.extension_type_name(), Some("arrow.parquet.variant"));
        assert_eq!(column.as_struct(), &expected);
    }

    /// Asserts the top-level variant `v` of `batch` round-tripped.
    fn assert_top_level_variant(batch: &RecordBatch) {
        let schema = batch.schema();
        assert_variant(
            schema.field_with_name("v").unwrap(),
            batch.column_by_name("v").unwrap(),
        );
    }

    /// Asserts the `id` column holds exactly `[1, 2, 3]`.
    fn assert_id_values(col: &ArrayRef) {
        let ids = col.as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(ids.values(), &[1, 2, 3]);
    }

    fn write_variant_parquet(
        arrow_schema: Arc<ArrowSchema>,
        columns: Vec<ArrayRef>,
        dir: &str,
    ) -> String {
        let batch = RecordBatch::try_new(arrow_schema.clone(), columns).unwrap();
        let path = format!("{dir}/variant.parquet");
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), arrow_schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    async fn try_read_variant(
        schema: Arc<Schema>,
        path: &str,
        project_field_ids: Vec<i32>,
        name_mapping: Option<Arc<NameMapping>>,
    ) -> crate::Result<Vec<RecordBatch>> {
        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(path.to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(project_field_ids)
            .with_name_mapping(name_mapping)
            .with_case_sensitive(false)
            .build()?;
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        reader.read(tasks)?.stream().try_collect().await
    }

    /// Reads `path`, checking that each batch's schema matches its columns, nested field
    /// metadata included.
    async fn read_variant(
        schema: Arc<Schema>,
        path: &str,
        project_field_ids: Vec<i32>,
    ) -> Vec<RecordBatch> {
        let batches = try_read_variant(schema, path, project_field_ids, None)
            .await
            .unwrap();
        for batch in &batches {
            RecordBatch::try_new(batch.schema(), batch.columns().to_vec()).unwrap();
        }
        batches
    }

    /// `id` (int, id 1) + optional `v` (variant, id 2).
    fn top_level_variant_schema() -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "v", Type::Variant(VariantType)).into(),
                ])
                .build()
                .unwrap(),
        )
    }

    /// Writes [`top_level_variant_schema`] rows with `v` stored as `variant_field`.
    fn write_top_level_variant_as(dir: &str, id_field: Field, variant_field: Field) -> String {
        use arrow_array::{Int32Array, StructArray};
        let DataType::Struct(storage) = variant_field.data_type() else {
            unreachable!()
        };
        let variant = Arc::new(StructArray::new(
            storage.clone(),
            variant_struct_array().as_struct().columns().to_vec(),
            None,
        )) as ArrayRef;
        let arrow_schema = Arc::new(ArrowSchema::new(vec![id_field, variant_field]));
        let id = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        write_variant_parquet(arrow_schema, vec![id, variant], dir)
    }

    fn write_top_level_variant(dir: &str) -> Arc<Schema> {
        write_top_level_variant_as(
            dir,
            Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(1)),
            variant_arrow_field("v", 2),
        );
        top_level_variant_schema()
    }

    #[tokio::test]
    async fn test_read_variant_full_scan() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let schema = write_top_level_variant(dir);

        let batches = read_variant(schema, &format!("{dir}/variant.parquet"), vec![1, 2]).await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_id_values(batches[0].column_by_name("id").unwrap());
        assert_top_level_variant(&batches[0]);
    }

    #[tokio::test]
    async fn test_read_variant_only() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let schema = write_top_level_variant(dir);

        let batches = read_variant(schema, &format!("{dir}/variant.parquet"), vec![2]).await;
        assert_eq!(batches[0].num_rows(), 3);
        assert_top_level_variant(&batches[0]);
        assert!(batches[0].column_by_name("id").is_none());
    }

    #[tokio::test]
    async fn test_read_variant_sibling_only_excludes_variant() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let schema = write_top_level_variant(dir);

        let batches = read_variant(schema, &format!("{dir}/variant.parquet"), vec![1]).await;
        assert_eq!(batches[0].num_rows(), 3);
        assert_id_values(batches[0].column_by_name("id").unwrap());
        assert!(batches[0].column_by_name("v").is_none());
    }

    /// Null variant rows stay null when the variant is rebuilt into the table type, here because
    /// the file's `value` is required and the table type's is nullable.
    #[tokio::test]
    async fn test_read_variant_with_null_rows() {
        use arrow_array::StructArray;
        use arrow_buffer::NullBuffer;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let with_null_row = |fields: Fields| {
            Arc::new(StructArray::new(
                fields,
                variant_struct_array().as_struct().columns().to_vec(),
                Some(NullBuffer::from(vec![true, false, true])),
            )) as ArrayRef
        };
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("v", DataType::Struct(variant_arrow_fields()), true)
                .with_metadata(field_id_meta(2)),
        ]));
        let path = write_variant_parquet(
            arrow_schema,
            vec![with_null_row(variant_arrow_fields())],
            dir,
        );

        let batches = read_variant(top_level_variant_schema(), &path, vec![2]).await;
        let DataType::Struct(table_fields) =
            crate::arrow::type_to_arrow_type(&Type::Variant(VariantType)).unwrap()
        else {
            unreachable!()
        };
        let schema = batches[0].schema();
        assert_eq!(
            schema.field_with_name("v").unwrap().extension_type_name(),
            Some("arrow.parquet.variant")
        );
        assert_eq!(
            batches[0].column_by_name("v").unwrap(),
            &with_null_row(table_fields)
        );
    }

    /// A file written before `v` was added to the table reads `v` as null in every row.
    #[tokio::test]
    async fn test_read_variant_missing_from_file() {
        use arrow_array::{Int32Array, new_null_array};
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(1)),
        ]));
        let id = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let path = write_variant_parquet(arrow_schema, vec![id], dir);
        let table_type = crate::arrow::type_to_arrow_type(&Type::Variant(VariantType)).unwrap();

        for project_field_ids in [vec![1, 2], vec![2]] {
            let batches = read_variant(top_level_variant_schema(), &path, project_field_ids).await;
            let schema = batches[0].schema();
            assert_eq!(
                schema.field_with_name("v").unwrap().extension_type_name(),
                Some("arrow.parquet.variant")
            );
            assert_eq!(
                batches[0].column_by_name("v").unwrap(),
                &new_null_array(&table_type, 3)
            );
        }
    }

    /// The file stores the variant exactly as the table schema's Arrow type, so only the
    /// extension has to be added.
    #[tokio::test]
    async fn test_read_variant_stored_as_table_type_gets_extension() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let storage = Fields::from(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
        ]);
        let path = write_top_level_variant_as(
            dir,
            Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(1)),
            Field::new("v", DataType::Struct(storage), true).with_metadata(field_id_meta(2)),
        );

        let batches = read_variant(top_level_variant_schema(), &path, vec![1, 2]).await;
        assert_id_values(batches[0].column_by_name("id").unwrap());
        assert_top_level_variant(&batches[0]);
    }

    #[tokio::test]
    async fn test_read_variant_matches_storage_fields_by_name() {
        use arrow_array::{LargeBinaryArray, StructArray};
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let storage = Fields::from(vec![
            Field::new("value", DataType::LargeBinary, false),
            Field::new("metadata", DataType::LargeBinary, false),
        ]);
        let variant = Arc::new(StructArray::new(
            storage.clone(),
            vec![
                Arc::new(LargeBinaryArray::from(VARIANT_VALUE.to_vec())) as ArrayRef,
                Arc::new(LargeBinaryArray::from(VARIANT_METADATA.to_vec())) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("v", DataType::Struct(storage), false).with_metadata(field_id_meta(2)),
        ]));
        let path = write_variant_parquet(arrow_schema, vec![variant], dir);

        let batches = read_variant(top_level_variant_schema(), &path, vec![2]).await;
        assert_top_level_variant(&batches[0]);
    }

    #[tokio::test]
    async fn test_read_variant_with_dictionary_storage() {
        use arrow_array::BinaryArray;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let dictionary =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary));
        let storage = Fields::from(vec![
            Field::new("metadata", dictionary.clone(), false),
            Field::new("value", dictionary.clone(), false),
        ]);
        let encode = |bytes: [&[u8]; 3]| {
            arrow_cast::cast(&BinaryArray::from(bytes.to_vec()), &dictionary).unwrap()
        };
        let variant = Arc::new(arrow_array::StructArray::new(
            storage.clone(),
            vec![encode(VARIANT_METADATA), encode(VARIANT_VALUE)],
            None,
        )) as ArrayRef;
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("v", DataType::Struct(storage), false).with_metadata(field_id_meta(2)),
        ]));
        let path = write_variant_parquet(arrow_schema, vec![variant], dir);

        let batches = read_variant(top_level_variant_schema(), &path, vec![2]).await;
        assert_top_level_variant(&batches[0]);
    }

    /// The variant group carries the Parquet VARIANT annotation, as iceberg-java writes it.
    #[tokio::test]
    async fn test_read_variant_with_parquet_annotation() {
        use parquet::basic::{LogicalType, Repetition, Type as PhysicalType};
        use parquet::data_type::{ByteArray, ByteArrayType, Int32Type};
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type as ParquetType;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let binary = |name: &str| {
            Arc::new(
                ParquetType::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )
        };
        let id = ParquetType::primitive_type_builder("id", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_id(Some(1))
            .build()
            .unwrap();
        let variant = ParquetType::group_type_builder("v")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::variant(Some(1))))
            .with_id(Some(2))
            .with_fields(vec![binary("metadata"), binary("value")])
            .build()
            .unwrap();
        let parquet_schema = ParquetType::group_type_builder("schema")
            .with_fields(vec![Arc::new(id), Arc::new(variant)])
            .build()
            .unwrap();
        let path = format!("{dir}/annotated.parquet");
        let mut writer = SerializedFileWriter::new(
            File::create(&path).unwrap(),
            Arc::new(parquet_schema),
            Arc::new(WriterProperties::builder().build()),
        )
        .unwrap();
        let mut row_group = writer.next_row_group().unwrap();
        let mut column = row_group.next_column().unwrap().unwrap();
        column
            .typed::<Int32Type>()
            .write_batch(&[1, 2, 3], None, None)
            .unwrap();
        column.close().unwrap();
        for bytes in [VARIANT_METADATA, VARIANT_VALUE] {
            let values: Vec<ByteArray> =
                bytes.iter().map(|b| ByteArray::from(b.to_vec())).collect();
            let mut column = row_group.next_column().unwrap().unwrap();
            column
                .typed::<ByteArrayType>()
                .write_batch(&values, Some(&[1, 1, 1]), None)
                .unwrap();
            column.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();

        let batches = read_variant(top_level_variant_schema(), &path, vec![1, 2]).await;
        assert_id_values(batches[0].column_by_name("id").unwrap());
        assert_top_level_variant(&batches[0]);
    }

    #[tokio::test]
    async fn test_read_variant_with_name_mapping() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let path = write_top_level_variant_as(
            dir,
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Struct(variant_arrow_fields()), false),
        );
        let name_mapping = Arc::new(NameMapping::new(vec![
            MappedField::new(Some(1), vec!["id".to_string()], vec![]),
            MappedField::new(Some(2), vec!["v".to_string()], vec![]),
        ]));

        let batches = try_read_variant(
            top_level_variant_schema(),
            &path,
            vec![1, 2],
            Some(name_mapping),
        )
        .await
        .unwrap();
        assert_id_values(batches[0].column_by_name("id").unwrap());
        assert_top_level_variant(&batches[0]);
    }

    #[tokio::test]
    async fn test_read_variant_without_field_ids_is_rejected() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let path = write_top_level_variant_as(
            dir,
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Struct(variant_arrow_fields()), false),
        );

        let err = try_read_variant(top_level_variant_schema(), &path, vec![1, 2], None)
            .await
            .unwrap_err();
        assert_eq!(
            (err.kind(), err.message()),
            (
                ErrorKind::FeatureUnsupported,
                "Reading variant columns from Parquet files without field ids is not supported yet"
            )
        );
    }

    #[tokio::test]
    async fn test_read_variant_nested_in_struct() {
        use arrow_array::{Int32Array, StructArray};
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        // id (int, 1) + nested struct (2) { payload variant (3) }.
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(
                        2,
                        "nested",
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(3, "payload", Type::Variant(VariantType)).into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let nested_fields = Fields::from(vec![variant_arrow_field("payload", 3)]);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(1)),
            Field::new("nested", DataType::Struct(nested_fields.clone()), false)
                .with_metadata(field_id_meta(2)),
        ]));
        let id = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let nested = Arc::new(StructArray::new(
            nested_fields,
            vec![variant_struct_array()],
            None,
        )) as ArrayRef;
        let path = write_variant_parquet(arrow_schema, vec![id, nested], dir);

        let batches = read_variant(schema, &path, vec![2]).await;
        assert_eq!(batches[0].num_rows(), 3);
        let nested = batches[0].column_by_name("nested").unwrap().as_struct();
        let DataType::Struct(nested_fields) = nested.data_type() else {
            unreachable!()
        };
        assert_variant(&nested_fields[0], nested.column(0));
    }

    /// A null `nested` row reads as null with `payload` stored optional, and stored required as
    /// iceberg-java writes a required variant, where `payload` has no null buffer of its own.
    #[tokio::test]
    async fn test_read_variant_nested_in_null_struct_row() {
        use arrow_array::StructArray;
        use arrow_buffer::NullBuffer;
        let nested_nulls = NullBuffer::from(vec![true, false, true]);
        for payload_required in [false, true] {
            let tmp = TempDir::new().unwrap();
            let dir = tmp.path().to_str().unwrap();
            let payload = if payload_required {
                NestedField::required(3, "payload", Type::Variant(VariantType))
            } else {
                NestedField::optional(3, "payload", Type::Variant(VariantType))
            };
            let nested_type = Type::Struct(StructType::new(vec![payload.into()]));
            let schema = Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(2, "nested", nested_type.clone()).into(),
                    ])
                    .build()
                    .unwrap(),
            );
            let payload_nulls = (!payload_required).then(|| nested_nulls.clone());
            let nested_with = |nested_fields: Fields, variant_fields: Fields| {
                Arc::new(StructArray::new(
                    nested_fields,
                    vec![Arc::new(StructArray::new(
                        variant_fields,
                        variant_struct_array().as_struct().columns().to_vec(),
                        payload_nulls.clone(),
                    )) as ArrayRef],
                    Some(nested_nulls.clone()),
                )) as ArrayRef
            };
            let file_nested_fields = Fields::from(vec![
                Field::new(
                    "payload",
                    DataType::Struct(variant_arrow_fields()),
                    !payload_required,
                )
                .with_metadata(field_id_meta(3)),
            ]);
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("nested", DataType::Struct(file_nested_fields.clone()), true)
                    .with_metadata(field_id_meta(2)),
            ]));
            let path = write_variant_parquet(
                arrow_schema,
                vec![nested_with(file_nested_fields, variant_arrow_fields())],
                dir,
            );

            let batches = read_variant(schema, &path, vec![2]).await;
            let DataType::Struct(table_nested_fields) =
                crate::arrow::type_to_arrow_type(&nested_type).unwrap()
            else {
                unreachable!()
            };
            let DataType::Struct(table_variant_fields) =
                crate::arrow::type_to_arrow_type(&Type::Variant(VariantType)).unwrap()
            else {
                unreachable!()
            };
            assert_eq!(
                table_nested_fields[0].extension_type_name(),
                Some("arrow.parquet.variant")
            );
            let nested = batches[0].column_by_name("nested").unwrap();
            assert_eq!(
                nested,
                &nested_with(table_nested_fields, table_variant_fields)
            );
            // Struct equality skips children under a null parent, so pin `payload`'s own nulls.
            assert_eq!(nested.as_struct().column(0).nulls(), payload_nulls.as_ref());
        }
    }

    #[tokio::test]
    async fn test_read_variant_in_list() {
        use arrow_array::{Int32Array, ListArray};
        use arrow_buffer::OffsetBuffer;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        // id (int, 1) + l list<variant> (2, element 3); 3 rows, each list holds one variant.
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(
                        2,
                        "l",
                        Type::List(crate::spec::ListType::new(
                            NestedField::list_element(3, Type::Variant(VariantType), true).into(),
                        )),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let element_field = Arc::new(variant_arrow_field("element", 3));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(1)),
            Field::new("l", DataType::List(element_field.clone()), false)
                .with_metadata(field_id_meta(2)),
        ]));
        let id = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let list = Arc::new(ListArray::new(
            element_field,
            OffsetBuffer::new(vec![0, 1, 2, 3].into()),
            variant_struct_array(),
            None,
        )) as ArrayRef;
        let path = write_variant_parquet(arrow_schema, vec![id, list], dir);

        let batches = read_variant(schema, &path, vec![2]).await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        let list = batches[0].column_by_name("l").unwrap().as_list::<i32>();
        let DataType::List(element_field) = list.data_type() else {
            unreachable!()
        };
        // Three single-element lists → the flattened element values are the 3 variants.
        assert_variant(element_field, list.values());
    }

    #[tokio::test]
    async fn test_read_variant_in_map() {
        use arrow_array::{Int32Array, MapArray, StringArray, StructArray};
        use arrow_buffer::OffsetBuffer;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        // id (int, 1) + m map<string, variant> (2; key 3, value 4); 3 rows, each map one entry.
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(
                        2,
                        "m",
                        Type::Map(crate::spec::MapType::new(
                            NestedField::map_key_element(3, Type::Primitive(PrimitiveType::String))
                                .into(),
                            NestedField::map_value_element(4, Type::Variant(VariantType), true)
                                .into(),
                        )),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let key_field = Field::new("key", DataType::Utf8, false).with_metadata(field_id_meta(3));
        let value_field = variant_arrow_field("value", 4);
        let entry_fields = Fields::from(vec![key_field, value_field]);
        let entries_field = Arc::new(Field::new(
            "key_value",
            DataType::Struct(entry_fields.clone()),
            false,
        ));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(1)),
            Field::new("m", DataType::Map(entries_field.clone(), false), false)
                .with_metadata(field_id_meta(2)),
        ]));
        let id = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let entries = StructArray::new(
            entry_fields,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
                variant_struct_array(),
            ],
            None,
        );
        let map = Arc::new(MapArray::new(
            entries_field,
            OffsetBuffer::new(vec![0, 1, 2, 3].into()),
            entries,
            None,
            false,
        )) as ArrayRef;
        let path = write_variant_parquet(arrow_schema, vec![id, map], dir);

        let batches = read_variant(schema, &path, vec![2]).await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        let map = batches[0].column_by_name("m").unwrap().as_map();
        // The map's value column holds the 3 variants.
        assert_variant(&map.entries().fields()[1], map.values());
    }

    #[test]
    fn test_projection_mask_reads_one_presence_leaf() {
        // `s` is struct<6: b list<7: int>, w struct<14: int>, 8: u struct<16: uint64, 5: a int>,
        // 3: c int>, and the table replaced every child. Presence must be `u.a`: the first
        // leaf with the lowest repetition level that the type check accepts. `v` holds only
        // a map, which is read whole, so it must convert.
        let parquet_schema = SchemaDescriptor::new(Arc::new(
            parse_message_type(
                "
                message schema {
                  required int32 id = 1;
                  optional group s = 2 {
                    optional group b (LIST) = 6 {
                      repeated group list {
                        optional int32 element = 7;
                      }
                    }
                    optional group w {
                      optional int32 a2 = 14;
                    }
                    optional group u = 8 {
                      optional int64 bad (INTEGER(64, false)) = 16;
                      optional int32 a = 5;
                    }
                    optional int32 c = 3;
                  }
                  optional group v = 4 {
                    optional group m (MAP) = 10 {
                      repeated group key_value {
                        required binary key (STRING) = 11;
                        optional int64 value (INTEGER(64, false)) = 12;
                      }
                    }
                  }
                }
                ",
            )
            .unwrap(),
        ));
        let file_arrow = Arc::new(parquet_to_arrow_schema(&parquet_schema, None).unwrap());
        let table = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(
                        2,
                        "s",
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(9, "n", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                    NestedField::optional(
                        4,
                        "v",
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(13, "n", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let mask = |ids: &[i32]| {
            ArrowReader::get_arrow_projection_mask(ids, &table, &parquet_schema, &file_arrow, false)
                .unwrap()
        };
        assert_eq!(
            mask(&[1, 2]),
            ProjectionMask::leaves(&parquet_schema, vec![0, 4])
        );
        assert_eq!(mask(&[2]), ProjectionMask::leaves(&parquet_schema, vec![4]));
        assert_eq!(mask(&[1]), ProjectionMask::leaves(&parquet_schema, vec![0]));
        assert_eq!(
            mask(&[1, 4]),
            ProjectionMask::leaves(&parquet_schema, vec![0])
        );

        // An `ARROW:schema` hint can wrap leaves in types `filter_leaves` descends through,
        // so the walk must count leaves the same way. Presence for `h` is `d` at leaf 4.
        // For `g`, `lv2` would win on order, but Iceberg can't convert a list view, so
        // presence is `e` at leaf 6.
        let parquet_schema = SchemaDescriptor::new(Arc::new(
            parse_message_type(
                "
                message schema {
                  optional group h = 20 {
                    optional group fl (LIST) = 21 {
                      repeated group list {
                        optional group element = 22 {
                          optional int32 p = 23;
                          optional int32 q = 24;
                        }
                      }
                    }
                    optional group lv (LIST) = 25 {
                      repeated group list {
                        optional group element = 26 {
                          optional int32 p = 27;
                          optional int32 q = 28;
                        }
                      }
                    }
                    optional binary d (STRING) = 29;
                  }
                  optional group g = 31 {
                    optional group lv2 (LIST) = 32 {
                      repeated group list {
                        optional int32 element = 33;
                      }
                    }
                    optional group e (LIST) = 34 {
                      repeated group list {
                        optional int32 element = 35;
                      }
                    }
                  }
                }
                ",
            )
            .unwrap(),
        ));
        // With a hint, parquet-rs takes field ids from the hint's metadata.
        let field = |name: &str, data_type, id: i32| {
            Field::new(name, data_type, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )]))
        };
        let element = |id: i32| {
            let pq = vec![
                field("p", DataType::Int32, id + 1),
                field("q", DataType::Int32, id + 2),
            ];
            Arc::new(field("element", DataType::Struct(pq.into()), id))
        };
        let int_element = |id: i32| Arc::new(field("element", DataType::Int32, id));
        let hint = ArrowSchema::new(vec![
            field(
                "h",
                DataType::Struct(Fields::from(vec![
                    field("fl", DataType::FixedSizeList(element(22), 2), 21),
                    field("lv", DataType::ListView(element(26)), 25),
                    field(
                        "d",
                        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                        29,
                    ),
                ])),
                20,
            ),
            field(
                "g",
                DataType::Struct(Fields::from(vec![
                    field("lv2", DataType::ListView(int_element(33)), 32),
                    field("e", DataType::List(int_element(35)), 34),
                ])),
                31,
            ),
        ]);
        let metadata = vec![parquet::file::metadata::KeyValue::new(
            parquet::arrow::ARROW_SCHEMA_META_KEY.to_string(),
            parquet::arrow::encode_arrow_schema(&hint),
        )];
        let file_arrow =
            Arc::new(parquet_to_arrow_schema(&parquet_schema, Some(&metadata)).unwrap());
        let replaced = |id, name, child_id| {
            NestedField::optional(
                id,
                name,
                Type::Struct(StructType::new(vec![
                    NestedField::optional(child_id, "n", Type::Primitive(PrimitiveType::Int))
                        .into(),
                ])),
            )
            .into()
        };
        let table = Schema::builder()
            .with_fields(vec![replaced(20, "h", 30), replaced(31, "g", 36)])
            .build()
            .unwrap();
        assert_eq!(
            ArrowReader::get_arrow_projection_mask(
                &[20, 31],
                &table,
                &parquet_schema,
                &file_arrow,
                false
            )
            .unwrap(),
            ProjectionMask::leaves(&parquet_schema, vec![4, 6])
        );

        // `element` is still projected, and the file nests it under a list view.
        // Recording `p` would keep the list view in the projected schema and fail
        // conversion, so presence for `h` is `d` at leaf 2.
        let parquet_schema = SchemaDescriptor::new(Arc::new(
            parse_message_type(
                "
                message schema {
                  optional group h = 20 {
                    optional group lv (LIST) = 25 {
                      repeated group list {
                        optional group element = 26 {
                          optional int32 p = 27;
                          optional int32 q = 28;
                        }
                      }
                    }
                    optional binary d (STRING) = 29;
                  }
                }
                ",
            )
            .unwrap(),
        ));
        let hint = ArrowSchema::new(vec![field(
            "h",
            DataType::Struct(Fields::from(vec![
                field("lv", DataType::ListView(element(26)), 25),
                field("d", DataType::Utf8, 29),
            ])),
            20,
        )]);
        let metadata = vec![parquet::file::metadata::KeyValue::new(
            parquet::arrow::ARROW_SCHEMA_META_KEY.to_string(),
            parquet::arrow::encode_arrow_schema(&hint),
        )];
        let file_arrow =
            Arc::new(parquet_to_arrow_schema(&parquet_schema, Some(&metadata)).unwrap());
        let table = Schema::builder()
            .with_fields(vec![
                NestedField::optional(
                    20,
                    "h",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(
                            26,
                            "element",
                            Type::Struct(StructType::new(vec![
                                NestedField::optional(30, "n", Type::Primitive(PrimitiveType::Int))
                                    .into(),
                            ])),
                        )
                        .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap();
        assert_eq!(
            ArrowReader::get_arrow_projection_mask(
                &[20],
                &table,
                &parquet_schema,
                &file_arrow,
                false
            )
            .unwrap(),
            ProjectionMask::leaves(&parquet_schema, vec![2])
        );
    }

    /// Cases from Java's `ReadFormatModelTests` for #17320, without initial-defaults:
    /// every requested child of a struct is missing from the file, so its validity must
    /// come from another leaf under it.
    #[tokio::test]
    async fn test_struct_presence_when_requested_children_missing() {
        use serde_json::{Value, json};

        use crate::arrow::schema_to_arrow_schema;
        use crate::spec::{ListType, MapType, NestedFieldRef};

        fn int(id: i32, name: &str) -> NestedFieldRef {
            NestedField::optional(id, name, Type::Primitive(PrimitiveType::Int)).into()
        }
        fn int_struct(id: i32) -> Type {
            Type::Struct(StructType::new(vec![int(id, "x")]))
        }
        // The table schema re-adds every struct child under a new id. `t` and `u` replace
        // their only child, a list and a map, so presence comes from a repeated leaf.
        fn schema(table: bool) -> Schema {
            let id = |file_id: i32| if table { file_id + 100 } else { file_id };
            let t = if table {
                Type::Struct(StructType::new(vec![int(110, "n")]))
            } else {
                Type::Struct(StructType::new(vec![
                    NestedField::optional(
                        10,
                        "tags",
                        Type::List(ListType::new(
                            NestedField::list_element(
                                11,
                                Type::Primitive(PrimitiveType::Int),
                                false,
                            )
                            .into(),
                        )),
                    )
                    .into(),
                ]))
            };
            let u = if table {
                Type::Struct(StructType::new(vec![int(123, "n")]))
            } else {
                Type::Struct(StructType::new(vec![
                    NestedField::optional(
                        23,
                        "attrs",
                        Type::Map(MapType::optional(
                            24,
                            Type::Primitive(PrimitiveType::String),
                            25,
                            Type::Primitive(PrimitiveType::Int),
                        )),
                    )
                    .into(),
                ]))
            };
            let s = Type::Struct(StructType::new(vec![
                int(id(5), "x"),
                NestedField::optional(7, "inner", int_struct(id(8))).into(),
            ]));
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "s", s).into(),
                    NestedField::optional(3, "t", t).into(),
                    NestedField::optional(6, "u", u).into(),
                    NestedField::optional(
                        4,
                        "m",
                        Type::Map(MapType::optional(
                            13,
                            Type::Primitive(PrimitiveType::String),
                            14,
                            int_struct(id(15)),
                        )),
                    )
                    .into(),
                    NestedField::optional(
                        17,
                        "l",
                        Type::List(ListType::new(
                            NestedField::list_element(18, int_struct(id(19)), false).into(),
                        )),
                    )
                    .into(),
                    // Java skips presence for a struct that can never be null. Here the
                    // struct still needs a column, or it reads as a missing required field.
                    NestedField::required(21, "r", int_struct(id(22))).into(),
                ])
                .build()
                .unwrap()
        }
        fn to_batch(schema: &Schema, rows: Value) -> RecordBatch {
            let fields = schema_to_arrow_schema(schema).unwrap().fields().to_vec();
            serde_arrow::to_record_batch(&fields, &rows).unwrap()
        }

        let file_batch = to_batch(
            &schema(false),
            json!([
                {"id": 1, "s": {"x": null, "inner": {"x": 5}}, "t": {"tags": [1]}, "u": {"attrs": {"a": 1}},
                 "m": {"k": {"x": 7}, "k2": null}, "l": [{"x": 1}, null], "r": {"x": 1}},
                {"id": 2, "s": {"x": 1, "inner": null}, "t": null, "u": null,
                 "m": null, "l": [], "r": {"x": 2}},
                {"id": 3, "s": null, "t": {"tags": null}, "u": {"attrs": null},
                 "m": {}, "l": null, "r": {"x": null}},
                {"id": 4, "s": {"x": 2, "inner": {"x": null}}, "t": {"tags": [2, 3, 4]}, "u": {"attrs": {}},
                 "m": {"k": null}, "l": [null], "r": {"x": 4}},
            ]),
        );
        let table_schema = Arc::new(schema(true));
        let expected = to_batch(
            &table_schema,
            json!([
                {"id": 1, "s": {"x": null, "inner": {"x": null}}, "t": {"n": null}, "u": {"n": null},
                 "m": {"k": {"x": null}, "k2": null}, "l": [{"x": null}, null], "r": {"x": null}},
                {"id": 2, "s": {"x": null, "inner": null}, "t": null, "u": null,
                 "m": null, "l": [], "r": {"x": null}},
                {"id": 3, "s": null, "t": {"n": null}, "u": {"n": null},
                 "m": {}, "l": null, "r": {"x": null}},
                {"id": 4, "s": {"x": null, "inner": {"x": null}}, "t": {"n": null}, "u": {"n": null},
                 "m": {"k": null}, "l": [null], "r": {"x": null}},
            ]),
        );

        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/old.parquet", tmp_dir.path().to_str().unwrap());
        let mut writer =
            ArrowWriter::try_new(File::create(&file_path).unwrap(), file_batch.schema(), None)
                .unwrap();
        writer.write(&file_batch).unwrap();
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(table_schema)
            .with_project_field_ids(vec![1, 2, 3, 6, 4, 17, 21])
            .with_case_sensitive(false)
            .build()
            .unwrap())])) as FileScanTaskStream;
        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();
        assert_eq!(result, vec![expected]);
    }

    #[test]
    fn test_coerce_fixed_size_lists_rewrites_nested() {
        let item = |ty| Arc::new(Field::new("item", ty, true));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("plain", DataType::Int32, true),
            Field::new(
                "fsl",
                DataType::FixedSizeList(item(DataType::Int32), 2),
                true,
            ),
            Field::new(
                "nested",
                DataType::Struct(Fields::from(vec![Field::new(
                    "inner",
                    DataType::FixedSizeList(item(DataType::Int32), 3),
                    true,
                )])),
                true,
            ),
            Field::new(
                "list_of_fsl",
                DataType::List(Arc::new(Field::new(
                    "element",
                    DataType::FixedSizeList(item(DataType::Int32), 2),
                    true,
                ))),
                true,
            ),
        ]));

        let coerced = super::coerce_fixed_size_lists(&schema).expect("should rewrite");
        assert!(matches!(
            coerced.field_with_name("fsl").unwrap().data_type(),
            DataType::List(_)
        ));
        let DataType::Struct(fields) = coerced.field_with_name("nested").unwrap().data_type()
        else {
            panic!("nested should stay Struct");
        };
        assert!(matches!(fields[0].data_type(), DataType::List(_)));
        let DataType::List(element) = coerced.field_with_name("list_of_fsl").unwrap().data_type()
        else {
            panic!("list_of_fsl should stay List");
        };
        assert!(matches!(element.data_type(), DataType::List(_)));
        assert!(super::coerce_fixed_size_lists(&coerced).is_none());
    }

    /// parquet-rs can return a LIST as `FixedSizeList` when the file's embedded Arrow schema
    /// says so. Rewrite the hint to `List` before reading so the scan never sees FixedSizeList.
    #[tokio::test]
    async fn test_read_fixed_size_list_column_as_list() {
        use arrow_array::{FixedSizeListArray, Int32Array};
        use arrow_buffer::{NullBuffer, OffsetBuffer};

        use crate::spec::ListType;

        let with_id = |field: Field, id: i32| {
            field.with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )]))
        };

        // Row 0: [1, 2]; row 1: null; row 2: [5, 6].
        let values = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(5),
            Some(6),
        ])) as ArrayRef;
        let row_nulls = NullBuffer::from(vec![true, false, true]);
        let optional_element = Arc::new(with_id(Field::new("element", DataType::Int32, true), 3));
        let required_element = Arc::new(with_id(Field::new("element", DataType::Int32, false), 5));
        let optional_fsl = FixedSizeListArray::new(
            Arc::clone(&optional_element),
            2,
            Arc::clone(&values),
            Some(row_nulls.clone()),
        );
        let required_fsl =
            FixedSizeListArray::new(Arc::clone(&required_element), 2, values, Some(row_nulls));
        let file_schema = Arc::new(ArrowSchema::new(vec![
            with_id(
                Field::new(
                    "optional",
                    DataType::FixedSizeList(optional_element, 2),
                    true,
                ),
                2,
            ),
            with_id(
                Field::new(
                    "required",
                    DataType::FixedSizeList(required_element, 2),
                    true,
                ),
                4,
            ),
            with_id(Field::new("id", DataType::Int32, true), 1),
        ]));
        let batch = RecordBatch::try_new(file_schema.clone(), vec![
            Arc::new(optional_fsl) as ArrayRef,
            Arc::new(required_fsl) as ArrayRef,
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        ])
        .unwrap();

        let tmp_dir = TempDir::new().unwrap();
        let path = format!("{}/fsl.parquet", tmp_dir.path().to_str().unwrap());
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), file_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        2,
                        "optional",
                        Type::List(ListType::new(
                            NestedField::list_element(
                                3,
                                Type::Primitive(PrimitiveType::Int),
                                false,
                            )
                            .into(),
                        )),
                    )
                    .into(),
                    NestedField::optional(
                        4,
                        "required",
                        Type::List(ListType::new(
                            NestedField::list_element(5, Type::Primitive(PrimitiveType::Int), true)
                                .into(),
                        )),
                    )
                    .into(),
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(std::fs::metadata(&path).unwrap().len())
                .with_start(0)
                .with_length(0)
                .with_data_file_path(path)
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(table_schema)
                .with_project_field_ids(vec![1, 2, 4])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        let out = &result[0];
        assert_eq!(out.num_rows(), 3);

        let expected_offsets = OffsetBuffer::new(vec![0, 2, 2, 4].into());
        let expected_values = Arc::new(Int32Array::from(vec![1, 2, 5, 6])) as ArrayRef;
        let expected_nulls = NullBuffer::from(vec![true, false, true]);
        let schema = out.schema();
        for (name, element_nullable) in [("optional", true), ("required", false)] {
            let col = schema.index_of(name).unwrap();
            let list = out.column(col).as_list::<i32>();
            assert_eq!(list.offsets(), &expected_offsets, "{name} offsets");
            assert_eq!(
                list.values().as_ref(),
                expected_values.as_ref(),
                "{name} values"
            );
            assert_eq!(list.nulls(), Some(&expected_nulls), "{name} nulls");
            let DataType::List(element) = schema.field(col).data_type() else {
                panic!("{name} should be List");
            };
            assert_eq!(
                element.is_nullable(),
                element_nullable,
                "{name} element nullability"
            );
        }
        let id_col = schema.index_of("id").unwrap();
        assert_eq!(
            out.column(id_col).as_ref(),
            &Int32Array::from(vec![1, 2, 3]) as &dyn Array
        );
    }

    /// Migrated files have no field IDs. Cover name mapping and fallback IDs with a null row,
    /// nested FixedSizeList shapes, and a required top-level element.
    #[tokio::test]
    async fn test_read_fixed_size_list_without_field_ids() {
        use arrow_array::{FixedSizeListArray, Int32Array, ListArray, MapArray, StructArray};
        use arrow_buffer::{NullBuffer, OffsetBuffer};

        use crate::spec::{ListType, MapType};

        let item = |nullable| Arc::new(Field::new("item", DataType::Int32, nullable));
        let values = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(5),
            Some(6),
        ])) as ArrayRef;
        let row_nulls = NullBuffer::from(vec![true, false, true]);
        let fsl = |nullable_element| {
            FixedSizeListArray::new(
                item(nullable_element),
                2,
                Arc::clone(&values),
                Some(row_nulls.clone()),
            )
        };

        let top = fsl(false);
        let in_struct = StructArray::new(
            Fields::from(vec![Field::new(
                "inner",
                DataType::FixedSizeList(item(true), 2),
                true,
            )]),
            vec![Arc::new(fsl(true)) as ArrayRef],
            None,
        );
        let map_entries = StructArray::new(
            Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::FixedSizeList(item(true), 2), true),
            ]),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
                Arc::new(FixedSizeListArray::new(
                    item(true),
                    2,
                    Arc::new(Int32Array::from(vec![
                        Some(10),
                        Some(11),
                        Some(20),
                        Some(21),
                    ])) as ArrayRef,
                    None,
                )) as ArrayRef,
            ],
            None,
        );
        let as_map_value = MapArray::new(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(map_entries.fields().clone()),
                false,
            )),
            OffsetBuffer::new(vec![0, 1, 1, 2].into()),
            map_entries,
            Some(row_nulls.clone()),
            false,
        );
        let list_of_fsl = ListArray::new(
            Arc::new(Field::new(
                "element",
                DataType::FixedSizeList(item(true), 2),
                true,
            )),
            OffsetBuffer::new(vec![0, 1, 1, 2].into()),
            Arc::new(FixedSizeListArray::new(
                item(true),
                2,
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(5), Some(6)])) as ArrayRef,
                None,
            )) as ArrayRef,
            Some(row_nulls.clone()),
        );
        let nested_fsl = FixedSizeListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::FixedSizeList(item(true), 2),
                true,
            )),
            1,
            Arc::new(FixedSizeListArray::new(
                item(true),
                2,
                Arc::clone(&values),
                None,
            )) as ArrayRef,
            Some(row_nulls),
        );

        let file_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("top", DataType::FixedSizeList(item(false), 2), true),
            Field::new(
                "s",
                DataType::Struct(Fields::from(vec![Field::new(
                    "inner",
                    DataType::FixedSizeList(item(true), 2),
                    true,
                )])),
                true,
            ),
            Field::new(
                "m",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::FixedSizeList(item(true), 2), true),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            ),
            Field::new(
                "list_of_fsl",
                DataType::List(Arc::new(Field::new(
                    "element",
                    DataType::FixedSizeList(item(true), 2),
                    true,
                ))),
                true,
            ),
            Field::new(
                "nested_fsl",
                DataType::FixedSizeList(
                    Arc::new(Field::new(
                        "item",
                        DataType::FixedSizeList(item(true), 2),
                        true,
                    )),
                    1,
                ),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(file_schema.clone(), vec![
            Arc::new(top) as ArrayRef,
            Arc::new(in_struct) as ArrayRef,
            Arc::new(as_map_value) as ArrayRef,
            Arc::new(list_of_fsl) as ArrayRef,
            Arc::new(nested_fsl) as ArrayRef,
        ])
        .unwrap();

        let tmp_dir = TempDir::new().unwrap();
        let path = format!("{}/fsl_no_ids.parquet", tmp_dir.path().to_str().unwrap());
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), file_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file_size = std::fs::metadata(&path).unwrap().len();

        let int_ty = Type::Primitive(PrimitiveType::Int);
        let list_of = |id, required| {
            Type::List(ListType::new(
                NestedField::list_element(id, int_ty.clone(), required).into(),
            ))
        };
        let table_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "top", list_of(6, true)).into(),
                    NestedField::optional(
                        2,
                        "s",
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(7, "inner", list_of(8, false)).into(),
                        ])),
                    )
                    .into(),
                    NestedField::optional(
                        3,
                        "m",
                        Type::Map(MapType::optional(
                            9,
                            Type::Primitive(PrimitiveType::String),
                            10,
                            list_of(11, false),
                        )),
                    )
                    .into(),
                    NestedField::optional(
                        4,
                        "list_of_fsl",
                        Type::List(ListType::new(
                            NestedField::list_element(12, list_of(13, false), false).into(),
                        )),
                    )
                    .into(),
                    NestedField::optional(
                        5,
                        "nested_fsl",
                        Type::List(ListType::new(
                            NestedField::list_element(14, list_of(15, false), false).into(),
                        )),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let name_mapping = Arc::new(NameMapping::new(vec![
            MappedField::new(Some(1), vec!["top".to_string()], vec![MappedField::new(
                Some(6),
                vec!["element".to_string()],
                vec![],
            )]),
            MappedField::new(Some(2), vec!["s".to_string()], vec![MappedField::new(
                Some(7),
                vec!["inner".to_string()],
                vec![MappedField::new(
                    Some(8),
                    vec!["element".to_string()],
                    vec![],
                )],
            )]),
            MappedField::new(Some(3), vec!["m".to_string()], vec![
                MappedField::new(Some(9), vec!["key".to_string()], vec![]),
                MappedField::new(Some(10), vec!["value".to_string()], vec![MappedField::new(
                    Some(11),
                    vec!["element".to_string()],
                    vec![],
                )]),
            ]),
            MappedField::new(Some(4), vec!["list_of_fsl".to_string()], vec![
                MappedField::new(Some(12), vec!["element".to_string()], vec![
                    MappedField::new(Some(13), vec!["element".to_string()], vec![]),
                ]),
            ]),
            MappedField::new(Some(5), vec!["nested_fsl".to_string()], vec![
                MappedField::new(Some(14), vec!["element".to_string()], vec![
                    MappedField::new(Some(15), vec!["element".to_string()], vec![]),
                ]),
            ]),
        ]));

        let assert_top_list = |out: &RecordBatch| {
            assert_eq!(out.num_rows(), 3);
            let top = out
                .column(out.schema().index_of("top").unwrap())
                .as_list::<i32>();
            assert_eq!(
                top.nulls(),
                Some(&NullBuffer::from(vec![true, false, true]))
            );
            assert_eq!(
                top.values().as_ref(),
                &Int32Array::from(vec![1, 2, 5, 6]) as &dyn Array
            );
            let schema = out.schema();
            for name in ["s", "m", "list_of_fsl", "nested_fsl"] {
                let field = schema.field_with_name(name).unwrap();
                assert!(
                    !matches!(field.data_type(), DataType::FixedSizeList(_, _)),
                    "{name} should not stay FixedSizeList: {:?}",
                    field.data_type()
                );
            }
        };

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let mapped_tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(file_size)
                .with_start(0)
                .with_length(0)
                .with_data_file_path(path.clone())
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(Arc::clone(&table_schema))
                .with_project_field_ids(vec![1, 2, 3, 4, 5])
                .with_case_sensitive(false)
                .with_name_mapping(Some(Arc::clone(&name_mapping)))
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;
        let mapped = reader
            .read(mapped_tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();
        assert_eq!(mapped.len(), 1);
        assert_top_list(&mapped[0]);

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let fallback_tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask::builder()
                .with_file_size_in_bytes(file_size)
                .with_start(0)
                .with_length(0)
                .with_data_file_path(path)
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(table_schema)
                .with_project_field_ids(vec![1, 2, 3, 4, 5])
                .with_case_sensitive(false)
                .build()
                .unwrap())]
            .into_iter(),
        )) as FileScanTaskStream;
        let fallback = reader
            .read(fallback_tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();
        assert_eq!(fallback.len(), 1);
        assert_top_list(&fallback[0]);
    }
}
