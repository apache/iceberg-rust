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

/*!
 * Partitioning
 */
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::transform::Transform;
use super::{NestedField, Schema, StructType};
use crate::{Error, ErrorKind, Result};

/// Reference to [`PartitionSpec`].
pub type PartitionSpecRef = Arc<PartitionSpec>;
/// Partition fields capture the transform from table data to partition values.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    /// A source column id from the table’s schema
    pub source_id: i32,
    /// A partition field id that is used to identify a partition field and is unique within a partition spec.
    /// In v2 table metadata, it is unique across all partition specs.
    pub field_id: i32,
    /// A partition name.
    pub name: String,
    /// A transform that is applied to the source column to produce a partition value.
    pub transform: Transform,
}

impl PartitionField {
    /// To unbound partition field
    pub fn to_unbound(self) -> UnboundPartitionField {
        UnboundPartitionField {
            source_id: self.source_id,
            partition_id: Some(self.field_id),
            name: self.name,
            transform: self.transform,
        }
    }
}

///  Partition spec that defines how to produce a tuple of partition values from a record.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    /// Identifier for PartitionSpec
    pub spec_id: i32,
    /// Details of the partition spec
    pub fields: Vec<PartitionField>,
}

impl PartitionSpec {
    /// Create partition spec builer
    pub fn builder() -> PartitionSpecBuilder {
        PartitionSpecBuilder::default()
    }

    /// Returns if the partition spec is unpartitioned.
    ///
    /// A [`PartitionSpec`] is unpartitioned if it has no fields or all fields are [`Transform::Void`] transform.
    pub fn is_unpartitioned(&self) -> bool {
        self.fields.is_empty()
            || self
                .fields
                .iter()
                .all(|f| matches!(f.transform, Transform::Void))
    }

    /// Returns the partition type of this partition spec.
    pub fn partition_type(&self, schema: &Schema) -> Result<StructType> {
        let mut fields = Vec::with_capacity(self.fields.len());
        for partition_field in &self.fields {
            let field = schema
                .field_by_id(partition_field.source_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "No column with source column id {} in schema {:?}",
                            partition_field.source_id, schema
                        ),
                    )
                })?;
            let res_type = partition_field.transform.result_type(&field.field_type)?;
            let field =
                NestedField::optional(partition_field.field_id, &partition_field.name, res_type)
                    .into();
            fields.push(field);
        }
        Ok(StructType::new(fields))
    }

    /// Turn this partition spec into an unbound partition spec.
    pub fn to_unbound(self) -> UnboundPartitionSpec {
        UnboundPartitionSpec {
            spec_id: Some(self.spec_id),
            fields: self
                .fields
                .into_iter()
                .map(|f| UnboundPartitionField {
                    source_id: f.source_id,
                    partition_id: Some(f.field_id),
                    name: f.name,
                    transform: f.transform,
                })
                .collect(),
        }
    }
}

/// Reference to [`UnboundPartitionSpec`].
pub type UnboundPartitionSpecRef = Arc<UnboundPartitionSpec>;
/// Unbound partition field can be built without a schema and later bound to a schema.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct UnboundPartitionField {
    /// A source column id from the table’s schema
    pub source_id: i32,
    /// A partition field id that is used to identify a partition field and is unique within a partition spec.
    /// In v2 table metadata, it is unique across all partition specs.
    #[builder(default, setter(strip_option))]
    pub partition_id: Option<i32>,
    /// A partition name.
    pub name: String,
    /// A transform that is applied to the source column to produce a partition value.
    pub transform: Transform,
}

/// Unbound partition spec can be built without a schema and later bound to a schema.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Builder)]
#[serde(rename_all = "kebab-case")]
#[builder(setter(prefix = "with"))]
pub struct UnboundPartitionSpec {
    /// Identifier for PartitionSpec
    #[builder(default, setter(strip_option))]
    pub spec_id: Option<i32>,
    /// Details of the partition spec
    #[builder(setter(each(name = "with_unbound_partition_field")))]
    pub fields: Vec<UnboundPartitionField>,
}

impl UnboundPartitionSpec {
    /// Create unbound partition spec builer
    pub fn builder() -> UnboundPartitionSpecBuilder {
        UnboundPartitionSpecBuilder::default()
    }
}

/// Create valid partition specs for a given schema.
#[derive(Debug, Default)]
pub struct PartitionSpecBuilder {
    spec_id: i32,
    last_assigned_field_id: i32,
    fields: Vec<PartitionField>,
}

impl PartitionSpecBuilder {
    pub(crate) const UNPARTITIONED_LAST_ASSIGNED_ID: i32 = 999;

    /// Create a new partition spec builder with the given schema.
    pub fn new() -> Self {
        Self {
            spec_id: 0,
            fields: vec![],
            last_assigned_field_id: Self::UNPARTITIONED_LAST_ASSIGNED_ID,
        }
    }

    /// Accessor to the current last assigned field id.
    pub fn last_assigned_field_id(&self) -> i32 {
        self.last_assigned_field_id
    }

    /// Set the last assigned field id for the partition spec.
    /// This is useful when re-binding partition specs.
    pub fn with_last_assigned_field_id(mut self, last_assigned_field_id: i32) -> Self {
        self.last_assigned_field_id = last_assigned_field_id;
        self
    }

    /// Set the spec id for the partition spec.
    pub fn with_spec_id(mut self, spec_id: i32) -> Self {
        self.spec_id = spec_id;
        self
    }

    /// Add a new partition field to the partition spec.
    pub fn with_partition_field(mut self, field: PartitionField) -> Result<Self> {
        self.check_name_set_and_unique(&field.name)?;
        self.check_for_redundant_partitions(field.source_id, &field.transform)?;
        self.check_partition_id_unique(field.field_id)?;

        self.fields.push(field);
        Ok(self)
    }

    /// Add a new partition field to the partition spec.
    /// Field ID is auto assigned if not set.
    pub fn with_unbound_partition_field(mut self, field: UnboundPartitionField) -> Result<Self> {
        self.check_name_set_and_unique(&field.name)?;
        self.check_for_redundant_partitions(field.source_id, &field.transform)?;
        if let Some(partition_id) = field.partition_id {
            self.check_partition_id_unique(partition_id)?;
        }

        let partition_id = if let Some(partition_id) = field.partition_id {
            self.last_assigned_field_id = std::cmp::max(self.last_assigned_field_id, partition_id);
            partition_id
        } else {
            self.increment_and_get_next_field_id()?
        };

        let field = PartitionField {
            source_id: field.source_id,
            field_id: partition_id,
            name: field.name,
            transform: field.transform,
        };

        self.fields.push(field);
        Ok(self)
    }

    /// Add multiple unbound partition fields to the partition spec.
    /// Field IDs are auto assigned if not set.
    pub fn with_unbound_fields(
        self,
        fields: impl IntoIterator<Item = UnboundPartitionField>,
    ) -> Result<Self> {
        let mut builder = self;
        for field in fields {
            builder = builder.with_unbound_partition_field(field)?;
        }
        Ok(builder)
    }

    /// Add multiple partition fields to the partition spec.
    pub fn with_fields(self, fields: impl IntoIterator<Item = PartitionField>) -> Result<Self> {
        let mut builder = self;
        for field in fields {
            builder = builder.with_partition_field(field)?;
        }
        Ok(builder)
    }

    /// Build the unbound partition spec.
    pub fn build_unbound(self) -> UnboundPartitionSpec {
        UnboundPartitionSpec {
            spec_id: Some(self.spec_id),
            fields: self.fields.into_iter().map(|f| f.to_unbound()).collect(),
        }
    }

    /// Build a bound partition spec with the given schema.
    pub fn build(self, schema: &Schema) -> Result<PartitionSpec> {
        let mut fields = Vec::with_capacity(self.fields.len());
        for field in self.fields {
            Self::check_name_does_not_collide_with_schema(&field, schema)?;
            Self::check_transform_compatibility(&field, schema)?;
            fields.push(PartitionField {
                source_id: field.source_id,
                field_id: field.field_id,
                name: field.name,
                transform: field.transform,
            });
        }
        Ok(PartitionSpec {
            spec_id: self.spec_id,
            fields,
        })
    }

    /// Build a partition spec without validating it against a schema.
    /// This can lead to invalid partition specs. Use with caution.
    pub fn build_unchecked(self) -> PartitionSpec {
        PartitionSpec {
            spec_id: self.spec_id,
            fields: self.fields,
        }
    }

    /// Ensure that the partition name is unique among the partition fields and is not empty.
    fn check_name_set_and_unique(&self, name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot use empty partition name",
            ));
        }

        if self.partition_names().contains(name) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot use partition name more than once: {}", name),
            ));
        }
        Ok(())
    }

    /// Check field / partition_id unique within the partition spec if set
    fn check_partition_id_unique(&self, field_id: i32) -> Result<()> {
        if self.fields.iter().any(|f| f.field_id == field_id) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot use field id more than once in one PartitionSpec: {}",
                    field_id
                ),
            ));
        }

        Ok(())
    }

    /// Ensure that the partition name is unique among columns in the schema.
    /// Duplicate names are allowed if:
    /// 1. The column is sourced from the column with the same name.
    /// 2. AND the transformation is identity
    fn check_name_does_not_collide_with_schema(
        field: &PartitionField,
        schema: &Schema,
    ) -> Result<()> {
        match schema.field_by_name(field.name.as_str()) {
            Some(schema_collision) => {
                if field.transform == Transform::Identity {
                    if schema_collision.id == field.source_id {
                        Ok(())
                    } else {
                        Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Cannot create identity partition sourced from different field in schema. Field name '{}' has id `{}` in schema but partition source id is `{}`",
                                field.name, schema_collision.id, field.source_id
                            ),
                        ))
                    }
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot create partition with name: '{}' that conflicts with schema field and is not an identity transform.",
                            field.name
                        ),
                    ))
                }
            }
            None => Ok(()),
        }
    }

    /// For a single source-column transformations must be unique.
    fn check_for_redundant_partitions(&self, source_id: i32, transform: &Transform) -> Result<()> {
        let collision = self.fields.iter().find(|f| {
            f.source_id == source_id && f.transform.dedup_name() == transform.dedup_name()
        });

        if let Some(collision) = collision {
            Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add redundant partition with source id `{}` and transform `{}`. A partition with the same source id and transform already exists with name `{}`",
                    source_id, transform.dedup_name(), collision.name
                ),
            ))
        } else {
            Ok(())
        }
    }

    /// Ensure that the transformation of the field is compatible with type of the field
    /// in the schema. Implicitly also checks if the source field exists in the schema.
    fn check_transform_compatibility(field: &PartitionField, schema: &Schema) -> Result<()> {
        let schema_field = schema.field_by_id(field.source_id).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot find partition source field with id `{}` in schema",
                    field.source_id
                ),
            )
        })?;

        if field.transform != Transform::Void {
            if !schema_field.field_type.is_primitive() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot partition by non-primitive source field: '{}'.",
                        schema_field.field_type
                    ),
                ));
            }

            if field
                .transform
                .result_type(&schema_field.field_type)
                .is_err()
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Invalid source type: '{}' for transform: '{}'.",
                        schema_field.field_type,
                        field.transform.dedup_name()
                    ),
                ));
            }
        }

        Ok(())
    }

    fn increment_and_get_next_field_id(&mut self) -> Result<i32> {
        if self.last_assigned_field_id == i32::MAX {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot assign more partition fields. Field id overflow.",
            ));
        }
        self.last_assigned_field_id += 1;
        Ok(self.last_assigned_field_id)
    }

    fn partition_names(&self) -> std::collections::HashSet<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::Type;

    #[test]
    fn test_partition_spec() {
        let spec = r#"
        {
        "spec-id": 1,
        "fields": [ {
            "source-id": 4,
            "field-id": 1000,
            "name": "ts_day",
            "transform": "day"
            }, {
            "source-id": 1,
            "field-id": 1001,
            "name": "id_bucket",
            "transform": "bucket[16]"
            }, {
            "source-id": 2,
            "field-id": 1002,
            "name": "id_truncate",
            "transform": "truncate[4]"
            } ]
        }
        "#;

        let partition_spec: PartitionSpec = serde_json::from_str(spec).unwrap();
        assert_eq!(4, partition_spec.fields[0].source_id);
        assert_eq!(1000, partition_spec.fields[0].field_id);
        assert_eq!("ts_day", partition_spec.fields[0].name);
        assert_eq!(Transform::Day, partition_spec.fields[0].transform);

        assert_eq!(1, partition_spec.fields[1].source_id);
        assert_eq!(1001, partition_spec.fields[1].field_id);
        assert_eq!("id_bucket", partition_spec.fields[1].name);
        assert_eq!(Transform::Bucket(16), partition_spec.fields[1].transform);

        assert_eq!(2, partition_spec.fields[2].source_id);
        assert_eq!(1002, partition_spec.fields[2].field_id);
        assert_eq!("id_truncate", partition_spec.fields[2].name);
        assert_eq!(Transform::Truncate(4), partition_spec.fields[2].transform);
    }

    #[test]
    fn test_is_unpartitioned() {
        let partition_spec = PartitionSpec::builder().with_spec_id(1).build_unchecked();
        assert!(
            partition_spec.is_unpartitioned(),
            "Empty partition spec should be unpartitioned"
        );

        let partition_spec = PartitionSpec::builder()
            .with_unbound_fields(vec![
                UnboundPartitionField::builder()
                    .source_id(1)
                    .name("id".to_string())
                    .transform(Transform::Identity)
                    .build(),
                UnboundPartitionField::builder()
                    .source_id(2)
                    .name("name".to_string())
                    .transform(Transform::Void)
                    .build(),
            ])
            .unwrap()
            .with_spec_id(1)
            .build_unchecked();
        assert!(
            !partition_spec.is_unpartitioned(),
            "Partition spec with one non void transform should not be unpartitioned"
        );

        let partition_spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_unbound_fields(vec![
                UnboundPartitionField::builder()
                    .source_id(1)
                    .name("id".to_string())
                    .transform(Transform::Void)
                    .build(),
                UnboundPartitionField::builder()
                    .source_id(2)
                    .name("name".to_string())
                    .transform(Transform::Void)
                    .build(),
            ])
            .unwrap()
            .build_unchecked();
        assert!(
            partition_spec.is_unpartitioned(),
            "Partition spec with all void field should be unpartitioned"
        );
    }

    #[test]
    fn test_unbound_partition_spec() {
        let spec = r#"
		{
		"spec-id": 1,
		"fields": [ {
			"source-id": 4,
			"partition-id": 1000,
			"name": "ts_day",
			"transform": "day"
			}, {
			"source-id": 1,
			"partition-id": 1001,
			"name": "id_bucket",
			"transform": "bucket[16]"
			}, {
			"source-id": 2,
			"partition-id": 1002,
			"name": "id_truncate",
			"transform": "truncate[4]"
			} ]
		}
		"#;

        let partition_spec: UnboundPartitionSpec = serde_json::from_str(spec).unwrap();
        assert_eq!(Some(1), partition_spec.spec_id);

        assert_eq!(4, partition_spec.fields[0].source_id);
        assert_eq!(Some(1000), partition_spec.fields[0].partition_id);
        assert_eq!("ts_day", partition_spec.fields[0].name);
        assert_eq!(Transform::Day, partition_spec.fields[0].transform);

        assert_eq!(1, partition_spec.fields[1].source_id);
        assert_eq!(Some(1001), partition_spec.fields[1].partition_id);
        assert_eq!("id_bucket", partition_spec.fields[1].name);
        assert_eq!(Transform::Bucket(16), partition_spec.fields[1].transform);

        assert_eq!(2, partition_spec.fields[2].source_id);
        assert_eq!(Some(1002), partition_spec.fields[2].partition_id);
        assert_eq!("id_truncate", partition_spec.fields[2].name);
        assert_eq!(Transform::Truncate(4), partition_spec.fields[2].transform);

        let spec = r#"
		{
		"fields": [ {
			"source-id": 4,
			"name": "ts_day",
			"transform": "day"
			} ]
		}
		"#;
        let partition_spec: UnboundPartitionSpec = serde_json::from_str(spec).unwrap();
        assert_eq!(None, partition_spec.spec_id);

        assert_eq!(4, partition_spec.fields[0].source_id);
        assert_eq!(None, partition_spec.fields[0].partition_id);
        assert_eq!("ts_day", partition_spec.fields[0].name);
        assert_eq!(Transform::Day, partition_spec.fields[0].transform);
    }

    #[test]
    fn test_partition_type() {
        let spec = r#"
            {
            "spec-id": 1,
            "fields": [ {
                "source-id": 4,
                "field-id": 1000,
                "name": "ts_day",
                "transform": "day"
                }, {
                "source-id": 1,
                "field-id": 1001,
                "name": "id_bucket",
                "transform": "bucket[16]"
                }, {
                "source-id": 2,
                "field-id": 1002,
                "name": "id_truncate",
                "transform": "truncate[4]"
                } ]
            }
            "#;

        let partition_spec: PartitionSpec = serde_json::from_str(spec).unwrap();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
                NestedField::required(
                    3,
                    "ts",
                    Type::Primitive(crate::spec::PrimitiveType::Timestamp),
                )
                .into(),
                NestedField::required(
                    4,
                    "ts_day",
                    Type::Primitive(crate::spec::PrimitiveType::Timestamp),
                )
                .into(),
                NestedField::required(
                    5,
                    "id_bucket",
                    Type::Primitive(crate::spec::PrimitiveType::Int),
                )
                .into(),
                NestedField::required(
                    6,
                    "id_truncate",
                    Type::Primitive(crate::spec::PrimitiveType::Int),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let partition_type = partition_spec.partition_type(&schema).unwrap();
        assert_eq!(3, partition_type.fields().len());
        assert_eq!(
            *partition_type.fields()[0],
            NestedField::optional(
                partition_spec.fields[0].field_id,
                &partition_spec.fields[0].name,
                Type::Primitive(crate::spec::PrimitiveType::Date)
            )
        );
        assert_eq!(
            *partition_type.fields()[1],
            NestedField::optional(
                partition_spec.fields[1].field_id,
                &partition_spec.fields[1].name,
                Type::Primitive(crate::spec::PrimitiveType::Int)
            )
        );
        assert_eq!(
            *partition_type.fields()[2],
            NestedField::optional(
                partition_spec.fields[2].field_id,
                &partition_spec.fields[2].name,
                Type::Primitive(crate::spec::PrimitiveType::String)
            )
        );
    }

    #[test]
    fn test_partition_empty() {
        let spec = r#"
            {
            "spec-id": 1,
            "fields": []
            }
            "#;

        let partition_spec: PartitionSpec = serde_json::from_str(spec).unwrap();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
                NestedField::required(
                    3,
                    "ts",
                    Type::Primitive(crate::spec::PrimitiveType::Timestamp),
                )
                .into(),
                NestedField::required(
                    4,
                    "ts_day",
                    Type::Primitive(crate::spec::PrimitiveType::Timestamp),
                )
                .into(),
                NestedField::required(
                    5,
                    "id_bucket",
                    Type::Primitive(crate::spec::PrimitiveType::Int),
                )
                .into(),
                NestedField::required(
                    6,
                    "id_truncate",
                    Type::Primitive(crate::spec::PrimitiveType::Int),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let partition_type = partition_spec.partition_type(&schema).unwrap();
        assert_eq!(0, partition_type.fields().len());
    }

    #[test]
    fn test_partition_error() {
        let spec = r#"
        {
        "spec-id": 1,
        "fields": [ {
            "source-id": 4,
            "field-id": 1000,
            "name": "ts_day",
            "transform": "day"
            }, {
            "source-id": 1,
            "field-id": 1001,
            "name": "id_bucket",
            "transform": "bucket[16]"
            }, {
            "source-id": 2,
            "field-id": 1002,
            "name": "id_truncate",
            "transform": "truncate[4]"
            } ]
        }
        "#;

        let partition_spec: PartitionSpec = serde_json::from_str(spec).unwrap();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();

        assert!(partition_spec.partition_type(&schema).is_err());
    }

    #[test]
    fn test_builder_unchecked() {
        let spec = PartitionSpec {
            spec_id: 10,
            fields: vec![
                PartitionField {
                    source_id: 4,
                    field_id: 1000,
                    name: "ts_day".to_string(),
                    transform: Transform::Day,
                },
                PartitionField {
                    source_id: 1,
                    field_id: 1001,
                    name: "id_bucket".to_string(),
                    transform: Transform::Bucket(16),
                },
                PartitionField {
                    source_id: 2,
                    field_id: 1002,
                    name: "id_truncate".to_string(),
                    transform: Transform::Truncate(4),
                },
            ],
        };

        let build_spec = PartitionSpec::builder()
            .with_spec_id(10)
            .with_partition_field(spec.fields[0].clone())
            .unwrap()
            .with_partition_field(spec.fields[1].clone())
            .unwrap()
            .with_partition_field(spec.fields[2].clone())
            .unwrap()
            .build_unchecked();

        assert_eq!(spec, build_spec);
    }

    #[test]
    fn test_build_unbound() {
        let unbound_spec = UnboundPartitionSpec {
            spec_id: Some(10),
            fields: vec![
                UnboundPartitionField {
                    source_id: 4,
                    partition_id: Some(1000),
                    name: "ts_day".to_string(),
                    transform: Transform::Day,
                },
                UnboundPartitionField {
                    source_id: 1,
                    partition_id: Some(1001),
                    name: "id_bucket".to_string(),
                    transform: Transform::Bucket(16),
                },
                UnboundPartitionField {
                    source_id: 2,
                    partition_id: Some(1002),
                    name: "id_truncate".to_string(),
                    transform: Transform::Truncate(4),
                },
            ],
        };

        let build_spec = PartitionSpec::builder()
            .with_spec_id(10)
            .with_unbound_partition_field(unbound_spec.fields[0].clone())
            .unwrap()
            .with_unbound_partition_field(unbound_spec.fields[1].clone())
            .unwrap()
            .with_unbound_partition_field(unbound_spec.fields[2].clone())
            .unwrap()
            .build_unbound();

        assert_eq!(unbound_spec, build_spec);
    }

    #[test]
    fn test_builder_disallow_duplicate_names() {
        PartitionSpec::builder()
            .with_partition_field(PartitionField {
                source_id: 1,
                field_id: 1000,
                name: "ts_day".to_string(),
                transform: Transform::Day,
            })
            .unwrap()
            .with_partition_field(PartitionField {
                source_id: 2,
                field_id: 1001,
                name: "ts_day".to_string(),
                transform: Transform::Day,
            })
            .unwrap_err();
    }

    #[test]
    fn test_builder_disallow_duplicate_field_ids() {
        PartitionSpec::builder()
            .with_partition_field(PartitionField {
                source_id: 1,
                field_id: 1000,
                name: "ts_day".to_string(),
                transform: Transform::Day,
            })
            .unwrap()
            .with_partition_field(PartitionField {
                source_id: 2,
                field_id: 1000,
                name: "id_bucket".to_string(),
                transform: Transform::Bucket(16),
            })
            .unwrap_err();
    }

    #[test]
    fn test_builder_auto_assign_field_ids() {
        let spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_unbound_partition_field(UnboundPartitionField {
                source_id: 1,
                name: "id".to_string(),
                transform: Transform::Identity,
                partition_id: Some(512),
            })
            .unwrap()
            .with_unbound_partition_field(UnboundPartitionField {
                source_id: 2,
                name: "name".to_string(),
                transform: Transform::Void,
                partition_id: None,
            })
            .unwrap()
            // Should keep its ID even if its lower
            .with_unbound_partition_field(UnboundPartitionField {
                source_id: 3,
                name: "ts".to_string(),
                transform: Transform::Year,
                partition_id: Some(1),
            })
            .unwrap()
            .build_unchecked();

        assert_eq!(512, spec.fields[0].field_id);
        assert_eq!(513, spec.fields[1].field_id);
        assert_eq!(1, spec.fields[2].field_id);
    }

    #[test]
    fn test_builder_valid_schema_1() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();

        PartitionSpec::builder()
            .with_spec_id(1)
            .build(&schema)
            .unwrap();

        let spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_unbound_partition_field(UnboundPartitionField {
                source_id: 1,
                partition_id: None,
                name: "id_bucket[16]".to_string(),
                transform: Transform::Bucket(16),
            })
            .unwrap()
            .build(&schema)
            .unwrap();

        assert_eq!(spec, PartitionSpec {
            spec_id: 1,
            fields: vec![PartitionField {
                source_id: 1,
                field_id: 1,
                name: "id_bucket[16]".to_string(),
                transform: Transform::Bucket(16),
            }]
        });
    }

    #[test]
    fn test_collision_with_schema_name() {
        let schema = Schema::builder()
            .with_fields(vec![NestedField::required(
                1,
                "id",
                Type::Primitive(crate::spec::PrimitiveType::Int),
            )
            .into()])
            .build()
            .unwrap();

        PartitionSpec::builder()
            .with_spec_id(1)
            .build(&schema)
            .unwrap();

        let err = PartitionSpec::builder()
            .with_spec_id(1)
            .with_unbound_partition_field(UnboundPartitionField {
                source_id: 1,
                partition_id: None,
                name: "id".to_string(),
                transform: Transform::Bucket(16),
            })
            .unwrap()
            .build(&schema)
            .unwrap_err();
        assert!(err.message().contains("conflicts with schema"))
    }

    #[test]
    fn test_builder_collision_is_ok_for_identity_transforms() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "number",
                    Type::Primitive(crate::spec::PrimitiveType::Int),
                )
                .into(),
            ])
            .build()
            .unwrap();

        PartitionSpec::builder()
            .with_spec_id(1)
            .build(&schema)
            .unwrap();

        PartitionSpec::builder()
            .with_spec_id(1)
            .with_unbound_partition_field(UnboundPartitionField {
                source_id: 1,
                partition_id: None,
                name: "id".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .build(&schema)
            .unwrap();

        // Not OK for different source id
        PartitionSpec::builder()
            .with_spec_id(1)
            .with_unbound_partition_field(UnboundPartitionField {
                source_id: 2,
                partition_id: None,
                name: "id".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .build(&schema)
            .unwrap_err();
    }

    #[test]
    fn test_builder_all_source_ids_must_exist() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
                NestedField::required(
                    3,
                    "ts",
                    Type::Primitive(crate::spec::PrimitiveType::Timestamp),
                )
                .into(),
            ])
            .build()
            .unwrap();

        // Valid
        PartitionSpec::builder()
            .with_spec_id(1)
            .with_unbound_fields(
                vec![
                    UnboundPartitionField {
                        source_id: 1,
                        partition_id: None,
                        name: "id_bucket".to_string(),
                        transform: Transform::Bucket(16),
                    },
                    UnboundPartitionField {
                        source_id: 2,
                        partition_id: None,
                        name: "name".to_string(),
                        transform: Transform::Identity,
                    },
                ]
                .into_iter(),
            )
            .unwrap()
            .build(&schema)
            .unwrap();

        // Invalid
        PartitionSpec::builder()
            .with_spec_id(1)
            .with_unbound_fields(
                vec![
                    UnboundPartitionField {
                        source_id: 1,
                        partition_id: None,
                        name: "id_bucket".to_string(),
                        transform: Transform::Bucket(16),
                    },
                    UnboundPartitionField {
                        source_id: 4,
                        partition_id: None,
                        name: "name".to_string(),
                        transform: Transform::Identity,
                    },
                ]
                .into_iter(),
            )
            .unwrap()
            .build(&schema)
            .unwrap_err();
    }

    #[test]
    fn test_builder_disallows_redundant() {
        let err = PartitionSpec::builder()
            .with_spec_id(1)
            .with_unbound_partition_field(UnboundPartitionField {
                source_id: 1,
                partition_id: None,
                name: "id_bucket[16]".to_string(),
                transform: Transform::Bucket(16),
            })
            .unwrap()
            .with_unbound_partition_field(UnboundPartitionField {
                source_id: 1,
                partition_id: None,
                name: "id_bucket_with_other_name".to_string(),
                transform: Transform::Bucket(16),
            })
            .unwrap_err();
        assert!(err.message().contains("redundant partition"));
    }

    #[test]
    fn test_builder_incompatible_transforms_disallowed() {
        let schema = Schema::builder()
            .with_fields(vec![NestedField::required(
                1,
                "id",
                Type::Primitive(crate::spec::PrimitiveType::Int),
            )
            .into()])
            .build()
            .unwrap();

        PartitionSpec::builder()
            .with_spec_id(1)
            .with_unbound_partition_field(UnboundPartitionField {
                source_id: 1,
                partition_id: None,
                name: "id_year".to_string(),
                transform: Transform::Year,
            })
            .unwrap()
            .build(&schema)
            .unwrap_err();
    }
}
