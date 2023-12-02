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

use super::transform::Transform;

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
