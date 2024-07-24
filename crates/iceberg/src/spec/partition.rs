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
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use typed_builder::TypedBuilder;

use crate::{Error, ErrorKind};

use super::{transform::Transform, NestedField, Schema, StructType};

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

///  Partition spec that defines how to produce a tuple of partition values from a record.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Builder)]
#[serde(rename_all = "kebab-case")]
#[builder(setter(prefix = "with"))]
pub struct PartitionSpec {
    /// Identifier for PartitionSpec
    pub spec_id: i32,
    /// Details of the partition spec
    #[builder(setter(each(name = "with_partition_field")))]
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
    pub fn partition_type(&self, schema: &Schema) -> Result<StructType, Error> {
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

#[cfg(test)]
mod tests {
    use crate::spec::Type;

    use super::*;

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
        let partition_spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_fields(vec![])
            .build()
            .unwrap();
        assert!(
            partition_spec.is_unpartitioned(),
            "Empty partition spec should be unpartitioned"
        );

        let partition_spec = PartitionSpec::builder()
            .with_partition_field(
                PartitionField::builder()
                    .source_id(1)
                    .field_id(1)
                    .name("id".to_string())
                    .transform(Transform::Identity)
                    .build(),
            )
            .with_partition_field(
                PartitionField::builder()
                    .source_id(2)
                    .field_id(2)
                    .name("name".to_string())
                    .transform(Transform::Void)
                    .build(),
            )
            .with_spec_id(1)
            .build()
            .unwrap();
        assert!(
            !partition_spec.is_unpartitioned(),
            "Partition spec with one non void transform should not be unpartitioned"
        );

        let partition_spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_partition_field(
                PartitionField::builder()
                    .source_id(1)
                    .field_id(1)
                    .name("id".to_string())
                    .transform(Transform::Void)
                    .build(),
            )
            .with_partition_field(
                PartitionField::builder()
                    .source_id(2)
                    .field_id(2)
                    .name("name".to_string())
                    .transform(Transform::Void)
                    .build(),
            )
            .build()
            .unwrap();
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
}
