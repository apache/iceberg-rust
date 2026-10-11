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
use crate::error::invalid_data;

/// Rebuilds field trees using a caller-supplied ID assignment strategy.
///
/// Schema reassignment visits struct siblings before their children and records
/// an old-to-new mapping. New columns instead use depth-first traversal and may
/// contain repeated placeholder IDs, so they do not record a mapping.
pub(crate) struct ReassignFieldIds<'a> {
    assign_id: Box<dyn FnMut(i32) -> Result<i32> + 'a>,
    old_to_new_id: Option<HashMap<i32, i32>>,
    siblings_first: bool,
    map_ids_first: bool,
}

impl<'a> ReassignFieldIds<'a> {
    pub(crate) fn new(mut start_from: i32) -> Self {
        Self::with_strategy(
            move |_| {
                let id = start_from;
                start_from = start_from
                    .checked_add(1)
                    .ok_or_else(|| invalid_data!("Field ID overflowed, cannot add more fields"))?;
                Ok(id)
            },
            false,
        )
    }

    /// Use a custom strategy, for example one that reuses IDs from a base schema.
    /// When `map_ids_first` is true, assign both map IDs before visiting either
    /// subtree, matching Java's fresh-ID assignment order. Otherwise, visit the
    /// key subtree before assigning the value ID, preserving schema reassignment.
    pub(crate) fn with_strategy(
        assign_id: impl FnMut(i32) -> Result<i32> + 'a,
        map_ids_first: bool,
    ) -> Self {
        Self {
            assign_id: Box::new(assign_id),
            old_to_new_id: Some(HashMap::new()),
            siblings_first: true,
            map_ids_first,
        }
    }

    /// Assign new columns IDs after `last_id`, updating it as IDs are allocated.
    pub(crate) fn for_new_fields(last_id: &'a mut i32) -> Self {
        let mut reassigner = Self::with_strategy(
            move |_| {
                *last_id = last_id
                    .checked_add(1)
                    .ok_or_else(|| invalid_data!("Field ID overflowed, cannot add more fields"))?;
                Ok(*last_id)
            },
            false,
        );
        reassigner.old_to_new_id = None;
        reassigner.siblings_first = false;
        reassigner
    }

    pub(crate) fn reassign_field_ids(
        &mut self,
        fields: Vec<NestedFieldRef>,
    ) -> Result<Vec<NestedFieldRef>> {
        if !self.siblings_first {
            return fields
                .into_iter()
                .map(|field| self.reassign_field(field))
                .collect();
        }

        // Visit fields on the same level first, then their nested fields.
        let outer_fields = fields
            .into_iter()
            .map(|field| self.assign_field_id(field))
            .collect::<Result<Vec<_>>>()?;
        outer_fields
            .into_iter()
            .map(|field| self.reassign_children(field))
            .collect()
    }

    fn assign_field_id(&mut self, field: NestedFieldRef) -> Result<NestedFieldRef> {
        let new_id = (self.assign_id)(field.id)?;
        if let Some(mapping) = &mut self.old_to_new_id {
            try_insert_field(mapping, field.id, new_id)?;
        }
        Ok(Arc::new(Arc::unwrap_or_clone(field).with_id(new_id)))
    }

    pub(crate) fn reassign_field(&mut self, field: NestedFieldRef) -> Result<NestedFieldRef> {
        let field = self.assign_field_id(field)?;
        self.reassign_children(field)
    }

    fn reassign_children(&mut self, field: NestedFieldRef) -> Result<NestedFieldRef> {
        if field.field_type.is_primitive() {
            return Ok(field);
        }
        let mut field = Arc::unwrap_or_clone(field);
        *field.field_type = self.reassign_ids_visit_type(*field.field_type)?;
        Ok(Arc::new(field))
    }

    fn reassign_ids_visit_type(&mut self, field_type: Type) -> Result<Type> {
        match field_type {
            Type::Primitive(s) => Ok(Type::Primitive(s)),
            Type::Struct(s) => {
                let new_fields = self.reassign_field_ids(s.fields().to_vec())?;
                Ok(Type::Struct(StructType::new(new_fields)))
            }
            Type::List(l) => Ok(Type::List(ListType {
                element_field: self.reassign_field(l.element_field)?,
            })),
            Type::Map(m) => {
                let key_field = self.assign_field_id(m.key_field)?;
                let (key_field, value_field) = if self.map_ids_first {
                    let value_field = self.assign_field_id(m.value_field)?;
                    (self.reassign_children(key_field)?, value_field)
                } else {
                    // Preserve the legacy order: the entire key subtree gets IDs
                    // before the value field itself does.
                    let key_field = self.reassign_children(key_field)?;
                    (key_field, self.assign_field_id(m.value_field)?)
                };
                Ok(Type::Map(MapType {
                    key_field,
                    value_field: self.reassign_children(value_field)?,
                }))
            }
            Type::Variant(v) => Ok(Type::Variant(v)),
        }
    }

    fn mapped_id(&self, id: i32) -> Option<i32> {
        self.old_to_new_id.as_ref()?.get(&id).copied()
    }

    pub fn apply_to_identifier_fields(&self, field_ids: HashSet<i32>) -> Result<HashSet<i32>> {
        field_ids
            .into_iter()
            .map(|id| {
                self.mapped_id(id)
                    .ok_or_else(|| invalid_data!("Identifier Field ID {id} not found"))
            })
            .collect()
    }

    pub fn apply_to_aliases(
        &self,
        alias: BiHashMap<String, i32>,
    ) -> Result<BiHashMap<String, i32>> {
        alias
            .into_iter()
            .map(|(name, id)| {
                self.mapped_id(id)
                    .ok_or_else(|| invalid_data!("Field with id {id} for alias {name} not found"))
                    .map(|new_id| (name, new_id))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::schema::tests::table_schema_nested;

    #[test]
    fn test_map_id_assignment_orders() {
        let fields = vec![
            NestedField::optional(
                10,
                "map",
                Type::Map(MapType::new(
                    NestedField::map_key_element(
                        20,
                        Type::Struct(StructType::new(vec![
                            NestedField::required(30, "key_child", PrimitiveType::Int.into())
                                .into(),
                        ])),
                    )
                    .into(),
                    NestedField::map_value_element(
                        40,
                        Type::List(ListType::new(
                            NestedField::list_element(50, PrimitiveType::Int.into(), false).into(),
                        )),
                        false,
                    )
                    .into(),
                )),
            )
            .into(),
            NestedField::required(60, "id", PrimitiveType::Int.into()).into(),
        ];

        // Struct siblings always precede their children. Only the value ID and
        // nested key ID switch places between the two map traversal orders.
        for (map_ids_first, key_child_id, value_id) in [(false, 3, 4), (true, 4, 3)] {
            let mut next_id = 0;
            let mut reassigner = if map_ids_first {
                ReassignFieldIds::with_strategy(
                    move |_| {
                        let id = next_id;
                        next_id += 1;
                        Ok(id)
                    },
                    true,
                )
            } else {
                ReassignFieldIds::new(0)
            };
            let schema = Schema::builder()
                .with_fields(reassigner.reassign_field_ids(fields.clone()).unwrap())
                .build()
                .unwrap();
            for (name, id) in [
                ("map", 0),
                ("id", 1),
                ("map.key", 2),
                ("map.key.key_child", key_child_id),
                ("map.value", value_id),
                ("map.value.element", 5),
            ] {
                assert_eq!(schema.field_by_name(name).unwrap().id, id, "{name}");
            }
            assert_eq!(
                reassigner
                    .apply_to_aliases(BiHashMap::from_iter([
                        ("key_alias".to_string(), 30),
                        ("value_alias".to_string(), 40),
                    ]))
                    .unwrap(),
                BiHashMap::from_iter([
                    ("key_alias".to_string(), key_child_id),
                    ("value_alias".to_string(), value_id),
                ]),
            );
        }
    }

    #[test]
    fn test_strategy_reuses_ids_by_name() {
        let base = Schema::builder()
            .with_fields([NestedField::required(7, "id", PrimitiveType::Int.into()).into()])
            .build()
            .unwrap();
        let schema = Schema::builder()
            .with_fields([
                NestedField::required(10, "id", PrimitiveType::Int.into()).into(),
                NestedField::optional(20, "new", PrimitiveType::String.into()).into(),
            ])
            .build()
            .unwrap();
        let mut next_id = base.highest_field_id();
        let mut reassigner = ReassignFieldIds::with_strategy(
            |old_id| {
                let name = schema.name_by_field_id(old_id).unwrap();
                if let Some(field) = base.field_by_name(name) {
                    Ok(field.id)
                } else {
                    next_id += 1;
                    Ok(next_id)
                }
            },
            true,
        );
        let fields = reassigner
            .reassign_field_ids(schema.as_struct().fields().to_vec())
            .unwrap();
        assert_eq!(fields[0].id, 7);
        assert_eq!(fields[1].id, 8);
        assert_eq!(
            reassigner
                .apply_to_identifier_fields(HashSet::from([10]))
                .unwrap(),
            HashSet::from([7]),
        );
        assert_eq!(
            reassigner
                .apply_to_aliases(BiHashMap::from_iter([("new_alias".to_string(), 20)]))
                .unwrap(),
            BiHashMap::from_iter([("new_alias".to_string(), 8)]),
        );
        assert!(
            reassigner
                .apply_to_identifier_fields(HashSet::from([99]))
                .unwrap_err()
                .message()
                .contains("Identifier Field ID 99 not found")
        );
        assert!(
            reassigner
                .apply_to_aliases(BiHashMap::from_iter([("missing".to_string(), 99)]))
                .unwrap_err()
                .message()
                .contains("Field with id 99 for alias missing not found")
        );
    }

    #[test]
    fn test_assignment_overflow() {
        let field: NestedFieldRef =
            NestedField::optional(0, "field", PrimitiveType::Int.into()).into();
        let error = ReassignFieldIds::new(i32::MAX)
            .reassign_field(field.clone())
            .unwrap_err();
        assert!(error.message().contains("Field ID overflowed"));

        let mut last_id = i32::MAX - 1;
        let assigned = ReassignFieldIds::for_new_fields(&mut last_id)
            .reassign_field(field.clone())
            .unwrap();
        assert_eq!(assigned.id, i32::MAX);
        let error = ReassignFieldIds::for_new_fields(&mut last_id)
            .reassign_field(field)
            .unwrap_err();
        assert!(error.message().contains("Field ID overflowed"));
        assert_eq!(last_id, i32::MAX);
    }

    #[test]
    fn test_reassign_ids() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![3])
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 3)]))
            .with_fields(vec![
                NestedField::optional(5, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(4, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();

        let reassigned_schema = schema
            .into_builder()
            .with_reassigned_field_ids(0)
            .build()
            .unwrap();

        let expected = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 1)]))
            .with_fields(vec![
                NestedField::optional(0, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(1, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();

        pretty_assertions::assert_eq!(expected, reassigned_schema);
        assert_eq!(reassigned_schema.highest_field_id(), 2);
    }

    #[test]
    fn test_reassign_ids_variant() {
        use crate::spec::VariantType;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(5, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "data", Type::Variant(VariantType)).into(),
            ])
            .build()
            .unwrap();

        let reassigned = schema
            .into_builder()
            .with_reassigned_field_ids(0)
            .build()
            .unwrap();

        // Variant has no sub-fields, so it survives reassignment unchanged; only the
        // top-level field ids shift (id → 0, data → 1).
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(0, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(1, "data", Type::Variant(VariantType)).into(),
            ])
            .build()
            .unwrap();

        pretty_assertions::assert_eq!(expected, reassigned);
    }

    #[test]
    fn test_reassigned_ids_nested() {
        let schema = table_schema_nested();
        let reassigned_schema = schema
            .into_builder()
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 2)]))
            .with_reassigned_field_ids(0)
            .build()
            .unwrap();

        let expected = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 1)]))
            .with_fields(vec![
                NestedField::optional(0, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(1, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                NestedField::required(
                    3,
                    "qux",
                    Type::List(ListType {
                        element_field: NestedField::list_element(
                            7,
                            Type::Primitive(PrimitiveType::String),
                            true,
                        )
                        .into(),
                    }),
                )
                .into(),
                NestedField::required(
                    4,
                    "quux",
                    Type::Map(MapType {
                        key_field: NestedField::map_key_element(
                            8,
                            Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(
                            9,
                            Type::Map(MapType {
                                key_field: NestedField::map_key_element(
                                    10,
                                    Type::Primitive(PrimitiveType::String),
                                )
                                .into(),
                                value_field: NestedField::map_value_element(
                                    11,
                                    Type::Primitive(PrimitiveType::Int),
                                    true,
                                )
                                .into(),
                            }),
                            true,
                        )
                        .into(),
                    }),
                )
                .into(),
                NestedField::required(
                    5,
                    "location",
                    Type::List(ListType {
                        element_field: NestedField::list_element(
                            12,
                            Type::Struct(StructType::new(vec![
                                NestedField::optional(
                                    13,
                                    "latitude",
                                    Type::Primitive(PrimitiveType::Float),
                                )
                                .into(),
                                NestedField::optional(
                                    14,
                                    "longitude",
                                    Type::Primitive(PrimitiveType::Float),
                                )
                                .into(),
                            ])),
                            true,
                        )
                        .into(),
                    }),
                )
                .into(),
                NestedField::optional(
                    6,
                    "person",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(15, "name", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(16, "age", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap();

        pretty_assertions::assert_eq!(expected, reassigned_schema);
        assert_eq!(reassigned_schema.highest_field_id(), 16);
        assert_eq!(reassigned_schema.field_by_id(6).unwrap().name, "person");
        assert_eq!(reassigned_schema.field_by_id(16).unwrap().name, "age");
    }

    #[test]
    fn test_reassign_ids_fails_with_duplicate_ids() {
        let reassigned_schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![5])
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 3)]))
            .with_fields(vec![
                NestedField::required(5, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .with_reassigned_field_ids(0)
            .build()
            .unwrap_err();

        assert!(reassigned_schema.message().contains("'field.id' 3"));
    }
}
