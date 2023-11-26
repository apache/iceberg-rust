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
use crate::error::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use typed_builder::TypedBuilder;

use super::DEFAULT_SPEC_ID;
use super::{schema::SchemaRef, transform::Transform};

/// Reference to [`PartitionSpec`].
pub type PartitionSpecRef = Arc<PartitionSpec>;
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, TypedBuilder)]
#[serde(rename_all = "kebab-case")]
/// Partition fields capture the transform from table data to partition values.
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Builder)]
#[serde(rename_all = "kebab-case")]
#[builder(setter(prefix = "with"))]
///  Partition spec that defines how to produce a tuple of partition values from a record.
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
}

static PARTITION_DATA_ID_START: i32 = 1000;

/// Reference to [`UnboundPartitionSpec`].
pub type UnboundPartitionSpecRef = Arc<UnboundPartitionSpec>;
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Unbound partition field can be built without a schema and later bound to a schema.
pub struct UnboundPartitionField {
    /// A source column id from the table’s schema
    pub source_id: i32,
    /// A partition field id that is used to identify a partition field and is unique within a partition spec.
    /// In v2 table metadata, it is unique across all partition specs.
    pub partition_id: Option<i32>,
    /// A partition name.
    pub name: String,
    /// A transform that is applied to the source column to produce a partition value.
    pub transform: Transform,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Builder)]
#[serde(rename_all = "kebab-case")]
#[builder(setter(prefix = "with"))]
/// Unbound partition spec can be built without a schema and later bound to a schema.
pub struct UnboundPartitionSpec {
    /// Identifier for PartitionSpec
    pub spec_id: Option<i32>,
    /// Details of the partition spec
    #[builder(setter(each(name = "with_unbound_partition_field")))]
    pub fields: Vec<UnboundPartitionField>,
}

impl UnboundPartitionSpec {
    /// last assigned id for partitioned field
    pub fn unpartitioned_last_assigned_id() -> i32 {
        PARTITION_DATA_ID_START - 1
    }

    /// Create unbound partition spec builer
    pub fn builder() -> UnboundPartitionSpecBuilder {
        UnboundPartitionSpecBuilder::default()
    }

    /// Bind unbound partition spec to a schema
    pub fn bind(&self, schema: SchemaRef) -> Result<PartitionSpec> {
        let mut fields = Vec::with_capacity(self.fields.len());
        let mut last_assigned_field_id: i32 =
            UnboundPartitionSpec::unpartitioned_last_assigned_id();
        for field in &self.fields {
            let field_id = match field.partition_id {
                Some(id) => id,
                None => {
                    last_assigned_field_id += 1;
                    last_assigned_field_id
                }
            };
            match schema.field_by_id(field.source_id) {
                Some(f) => {
                    if f.name != field.name {
                        return Err(Error::new(
                            ErrorKind::Conflict,
                            format!(
                                "Field name {} in partition spec does not match schema",
                                field.name
                            ),
                        ));
                    }
                }
                None => {
                    return Err(Error::new(
                        ErrorKind::Conflict,
                        format!(
                            "Field id {} in partition spec is not in schema",
                            field.source_id
                        ),
                    ));
                }
            }
            last_assigned_field_id = last_assigned_field_id.max(field_id);
            fields.push(PartitionField {
                source_id: field.source_id,
                field_id,
                name: field.name.clone(),
                transform: field.transform,
            });
        }
        let spec_id = match self.spec_id {
            Some(id) => id,
            None => DEFAULT_SPEC_ID,
        };
        Ok(PartitionSpec { spec_id, fields })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_spec() {
        let sort_order = r#"
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

        let partition_spec: PartitionSpec = serde_json::from_str(sort_order).unwrap();
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
}
