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

use super::utils::try_insert_field;
use super::*;

/// Reassigns `schema`'s field ids, reusing ids from `base` for fields whose full name is unchanged
/// and drawing fresh ids from `start_from` upwards for everything else.
///
/// `start_from` must be past every id the table has ever assigned, not merely past `base`'s ids:
/// pass `table_metadata.last_column_id() + 1`, which also reserves the ids of columns already
/// dropped from `base`. Seeding from a schema's `highest_field_id() + 1` can hand a new column the
/// id of a dropped one. A reused id does not consume a fresh one, so a `start_from` that is too low
/// yields duplicate ids, which surface only as a generic error from `build()`.
///
/// The returned `schema_id` is carried over unchanged and is not authoritative; it is arbitrated by
/// [`TableMetadataBuilder::add_schema`](crate::spec::TableMetadataBuilder::add_schema).
pub(crate) fn assign_fresh_ids(schema: Schema, base: &Schema, start_from: i32) -> Result<Schema> {
    let Schema {
        r#struct,
        schema_id,
        identifier_field_ids,
        alias_to_id,
        id_to_name,
        ..
    } = schema;
    let mut assigner = AssignFreshIds::new(id_to_name, base, start_from);
    let fields = assigner.assign_fields(r#struct.fields().to_vec())?;
    let identifier_field_ids = assigner.apply_to_identifier_fields(identifier_field_ids)?;
    let alias_to_id = assigner.apply_to_aliases(alias_to_id)?;

    Schema::builder()
        .with_schema_id(schema_id)
        .with_fields(fields)
        .with_identifier_field_ids(identifier_field_ids)
        .with_alias(alias_to_id)
        .build()
}

struct AssignFreshIds {
    next_field_id: i32,
    target_names: HashMap<i32, String>,
    base_ids: HashMap<String, i32>,
    old_to_new_id: HashMap<i32, i32>,
}

impl AssignFreshIds {
    fn new(target_names: HashMap<i32, String>, base: &Schema, start_from: i32) -> Self {
        Self {
            next_field_id: start_from,
            target_names,
            base_ids: base
                .field_id_to_name_map()
                .iter()
                .map(|(id, name)| (name.clone(), *id))
                .collect(),
            old_to_new_id: HashMap::new(),
        }
    }

    /// Returns `base`'s id when the field's full name is unchanged, otherwise consumes a fresh id by
    /// advancing `next_field_id`.
    fn resolve_or_assign_id(&mut self, old_id: i32) -> Result<i32> {
        if let Some(id) = self
            .target_names
            .get(&old_id)
            .and_then(|name| self.base_ids.get(name))
        {
            return Ok(*id);
        }

        let id = self.next_field_id;
        self.next_field_id = self.next_field_id.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Field ID overflowed, cannot add more fields",
            )
        })?;
        Ok(id)
    }

    fn assign_fields(&mut self, fields: Vec<NestedFieldRef>) -> Result<Vec<NestedFieldRef>> {
        let outer_fields = fields
            .into_iter()
            .map(|field| {
                let new_id = self.resolve_or_assign_id(field.id)?;
                try_insert_field(&mut self.old_to_new_id, field.id, new_id)?;
                Ok(Arc::new(Arc::unwrap_or_clone(field).with_id(new_id)))
            })
            .collect::<Result<Vec<_>>>()?;

        outer_fields
            .into_iter()
            .map(|field| {
                if field.field_type.is_primitive() {
                    Ok(field)
                } else {
                    let mut field = Arc::unwrap_or_clone(field);
                    *field.field_type = self.assign_type(*field.field_type)?;
                    Ok(Arc::new(field))
                }
            })
            .collect()
    }

    fn assign_type(&mut self, field_type: Type) -> Result<Type> {
        match field_type {
            Type::Primitive(primitive) => Ok(Type::Primitive(primitive)),
            Type::Struct(r#struct) => Ok(Type::Struct(StructType::new(
                self.assign_fields(r#struct.fields().to_vec())?,
            ))),
            Type::List(list) => {
                let new_id = self.resolve_or_assign_id(list.element_field.id)?;
                try_insert_field(&mut self.old_to_new_id, list.element_field.id, new_id)?;
                let mut element_field = Arc::unwrap_or_clone(list.element_field);
                element_field.id = new_id;
                *element_field.field_type = self.assign_type(*element_field.field_type)?;
                Ok(Type::List(ListType {
                    element_field: Arc::new(element_field),
                }))
            }
            Type::Map(map) => {
                // Key and value ids are resolved before recursing into either, matching Java and
                // intentionally unlike `ReassignFieldIds`, which recurses the key first.
                let new_key_id = self.resolve_or_assign_id(map.key_field.id)?;
                let new_value_id = self.resolve_or_assign_id(map.value_field.id)?;
                try_insert_field(&mut self.old_to_new_id, map.key_field.id, new_key_id)?;
                try_insert_field(&mut self.old_to_new_id, map.value_field.id, new_value_id)?;

                let mut key_field = Arc::unwrap_or_clone(map.key_field);
                key_field.id = new_key_id;
                *key_field.field_type = self.assign_type(*key_field.field_type)?;

                let mut value_field = Arc::unwrap_or_clone(map.value_field);
                value_field.id = new_value_id;
                *value_field.field_type = self.assign_type(*value_field.field_type)?;

                Ok(Type::Map(MapType {
                    key_field: Arc::new(key_field),
                    value_field: Arc::new(value_field),
                }))
            }
            Type::Variant(variant) => Ok(Type::Variant(variant)),
        }
    }

    fn apply_to_identifier_fields(&self, field_ids: HashSet<i32>) -> Result<HashSet<i32>> {
        field_ids
            .into_iter()
            .map(|id| {
                self.old_to_new_id.get(&id).copied().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Identifier Field ID {id} not found"),
                    )
                })
            })
            .collect()
    }

    fn apply_to_aliases(&self, aliases: BiHashMap<String, i32>) -> Result<BiHashMap<String, i32>> {
        aliases
            .into_iter()
            .map(|(name, id)| {
                self.old_to_new_id
                    .get(&id)
                    .copied()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Field with id {id} for alias {name} not found"),
                        )
                    })
                    .map(|new_id| (name, new_id))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{Literal, VariantType};

    fn empty_schema() -> Schema {
        Schema::builder().build().unwrap()
    }

    #[test]
    fn test_assign_fresh_ids() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(0, "a", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(1, "c", Type::Primitive(PrimitiveType::Int))
                    .with_initial_default(Literal::int(23))
                    .with_write_default(Literal::int(34))
                    .into(),
                NestedField::required(2, "B", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(11, "a", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(12, "c", Type::Primitive(PrimitiveType::Int))
                    .with_initial_default(Literal::int(23))
                    .with_write_default(Literal::int(34))
                    .into(),
                NestedField::required(13, "B", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let assigned = assign_fresh_ids(schema, &empty_schema(), 11).unwrap();

        assert_eq!(assigned.as_struct(), expected.as_struct());
    }

    #[test]
    fn test_assign_fresh_ids_with_type() {
        for test_type in [
            Type::Primitive(PrimitiveType::Boolean),
            Type::Primitive(PrimitiveType::Int),
            Type::Primitive(PrimitiveType::Long),
            Type::Primitive(PrimitiveType::Float),
            Type::Primitive(PrimitiveType::Double),
            Type::Primitive(PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            }),
            Type::Primitive(PrimitiveType::Date),
            Type::Primitive(PrimitiveType::Time),
            Type::Primitive(PrimitiveType::Timestamp),
            Type::Primitive(PrimitiveType::Timestamptz),
            Type::Primitive(PrimitiveType::TimestampNs),
            Type::Primitive(PrimitiveType::TimestamptzNs),
            Type::Primitive(PrimitiveType::String),
            Type::Primitive(PrimitiveType::Uuid),
            Type::Primitive(PrimitiveType::Fixed(16)),
            Type::Primitive(PrimitiveType::Binary),
            Type::Variant(VariantType),
        ] {
            let schema = Schema::builder()
                .with_fields(vec![
                    NestedField::required(0, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(1, "data", test_type.clone()).into(),
                ])
                .build()
                .unwrap();
            let expected = Schema::builder()
                .with_fields(vec![
                    NestedField::required(11, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(12, "data", test_type).into(),
                ])
                .build()
                .unwrap();

            let assigned = assign_fresh_ids(schema, &empty_schema(), 11).unwrap();

            assert_eq!(assigned.as_struct(), expected.as_struct());
        }
    }

    #[test]
    fn test_assign_fresh_ids_reuses_full_names_and_assigns_new_ids_in_correct_order() {
        let base = Schema::builder()
            .with_fields(vec![
                NestedField::required(
                    1,
                    "nested",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(2, "a", Type::Primitive(PrimitiveType::Int)).into(),
                    ])),
                )
                .into(),
                NestedField::optional(
                    3,
                    "items",
                    Type::List(ListType::new(
                        NestedField::list_element(4, Type::Primitive(PrimitiveType::String), false)
                            .into(),
                    )),
                )
                .into(),
                NestedField::optional(
                    5,
                    "properties",
                    Type::Map(MapType::optional(
                        6,
                        Type::Primitive(PrimitiveType::String),
                        7,
                        Type::Primitive(PrimitiveType::Long),
                    )),
                )
                .into(),
                NestedField::required(8, "x", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(9, "dropped", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let replacement = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids([18])
            .with_alias(BiHashMap::from_iter([("a_alias".to_string(), 12)]))
            .with_fields(vec![
                NestedField::required(
                    10,
                    "nested",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(11, "b", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(12, "a", Type::Primitive(PrimitiveType::Int)).into(),
                    ])),
                )
                .into(),
                NestedField::optional(
                    13,
                    "items",
                    Type::List(ListType::new(
                        NestedField::list_element(
                            14,
                            Type::Primitive(PrimitiveType::String),
                            false,
                        )
                        .into(),
                    )),
                )
                .into(),
                NestedField::optional(
                    15,
                    "properties",
                    Type::Map(MapType::optional(
                        16,
                        Type::Primitive(PrimitiveType::String),
                        17,
                        Type::Primitive(PrimitiveType::Long),
                    )),
                )
                .into(),
                NestedField::required(18, "x", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(19, "z", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let assigned = assign_fresh_ids(replacement, &base, 10).unwrap();

        assert_eq!(assigned.field_by_name("nested").unwrap().id, 1);
        assert_eq!(assigned.field_by_name("nested.a").unwrap().id, 2);
        assert_eq!(assigned.field_by_name("items").unwrap().id, 3);
        assert_eq!(assigned.field_by_name("items.element").unwrap().id, 4);
        assert_eq!(assigned.field_by_name("properties").unwrap().id, 5);
        assert_eq!(assigned.field_by_name("properties.key").unwrap().id, 6);
        assert_eq!(assigned.field_by_name("properties.value").unwrap().id, 7);
        assert_eq!(assigned.field_by_name("x").unwrap().id, 8);
        assert_eq!(assigned.field_by_name("z").unwrap().id, 10);
        assert_eq!(assigned.field_by_name("nested.b").unwrap().id, 11);
        assert_eq!(assigned.identifier_field_ids().collect::<Vec<_>>(), vec![8]);
        assert_eq!(assigned.field_by_alias("a_alias").unwrap().id, 2);
        assert_eq!(assigned.highest_field_id(), 11);
    }

    #[test]
    fn test_assign_fresh_ids_does_not_reuse_dropped_column_ids() {
        // The table once had ids 1..=5; only 1 and 3 are still present in the current schema, so
        // `highest_field_id()` of 3 would hand a new column the dropped id 4.
        let base = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(3, "b", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let replacement = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "fresh", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let assigned = assign_fresh_ids(replacement, &base, 6).unwrap();

        assert_eq!(assigned.field_by_name("a").unwrap().id, 1);
        assert_eq!(assigned.field_by_name("fresh").unwrap().id, 6);
    }

    #[test]
    fn test_assign_fresh_ids_rejects_alias_for_dropped_field() {
        // `build()` validates `identifier_field_ids` but not `alias_to_id`, so an alias left
        // pointing at a field the replacement drops reaches `apply_to_aliases`.
        let base = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "dropped", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let replacement = Schema::builder()
            .with_alias(BiHashMap::from_iter([("dropped_alias".to_string(), 2)]))
            .with_fields(vec![
                NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let err = assign_fresh_ids(replacement, &base, 3).unwrap_err();

        assert!(
            err.message()
                .contains("Field with id 2 for alias dropped_alias not found")
        );
    }

    #[test]
    fn test_assign_fresh_ids_assigns_map_ids_before_nested_types() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(
                    1,
                    "map",
                    Type::Map(MapType::required(
                        2,
                        Type::Struct(StructType::new(vec![
                            NestedField::required(
                                3,
                                "key_nested",
                                Type::Primitive(PrimitiveType::Int),
                            )
                            .into(),
                        ])),
                        4,
                        Type::Struct(StructType::new(vec![
                            NestedField::required(
                                5,
                                "value_nested",
                                Type::Primitive(PrimitiveType::Int),
                            )
                            .into(),
                        ])),
                    )),
                )
                .into(),
                NestedField::required(6, "tail", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let assigned = assign_fresh_ids(schema, &empty_schema(), 11).unwrap();

        assert_eq!(assigned.field_by_name("map").unwrap().id, 11);
        assert_eq!(assigned.field_by_name("tail").unwrap().id, 12);
        assert_eq!(assigned.field_by_name("map.key").unwrap().id, 13);
        assert_eq!(assigned.field_by_name("map.value").unwrap().id, 14);
        assert_eq!(assigned.field_by_name("map.key.key_nested").unwrap().id, 15);
        assert_eq!(
            assigned.field_by_name("map.value.value_nested").unwrap().id,
            16
        );
    }
}
