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
 * Sorting
*/
use crate::error::Result;
use crate::{Error, ErrorKind};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use typed_builder::TypedBuilder;

use super::{schema::SchemaRef, transform::Transform};

/// Reference to [`SortOrder`].
pub type SortOrderRef = Arc<SortOrder>;
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
/// Sort direction in a partition, either ascending or descending
pub enum SortDirection {
    /// Ascending
    #[serde(rename = "asc")]
    Ascending,
    /// Descending
    #[serde(rename = "desc")]
    Descending,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
/// Describes the order of null values when sorted.
pub enum NullOrder {
    #[serde(rename = "nulls-first")]
    /// Nulls are stored first
    First,
    #[serde(rename = "nulls-last")]
    /// Nulls are stored last
    Last,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, TypedBuilder)]
#[serde(rename_all = "kebab-case")]
/// Entry for every column that is to be sorted
pub struct SortField {
    /// A source column id from the table’s schema
    pub source_id: i32,
    /// A transform that is used to produce values to be sorted on from the source column.
    pub transform: Transform,
    /// A sort direction, that can only be either asc or desc
    pub direction: SortDirection,
    /// A null order that describes the order of null values when sorted.
    pub null_order: NullOrder,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Builder, Default)]
#[serde(rename_all = "kebab-case")]
#[builder(setter(prefix = "with"))]
/// A sort order is defined by a sort order id and a list of sort fields.
/// The order of the sort fields within the list defines the order in which the sort is applied to the data.
pub struct SortOrder {
    /// Identifier for SortOrder, order_id `0` is no sort order.
    #[builder(default)]
    pub order_id: i64,
    /// Details of the sort
    #[builder(setter(each(name = "with_sort_field")), default)]
    pub fields: Vec<SortField>,
}

impl SortOrder {
    /// Create sort order builder
    pub fn builder() -> SortOrderBuilder {
        SortOrderBuilder::default()
    }

    /// Returns true if the sort order is unsorted.
    ///
    /// A [`SortOrder`] is unsorted if it has no sort fields.
    pub fn is_unsorted(&self) -> bool {
        self.fields.is_empty()
    }

    /// Converts to an unbound sort order
    pub fn to_unbound(&self) -> UnboundSortOrder {
        UnboundSortOrder::builder()
            .with_order_id(self.order_id)
            .with_fields(
                self.fields
                    .iter()
                    .map(|x| UnboundSortField {
                        source_id: x.source_id,
                        transform: x.transform,
                        direction: x.direction,
                        null_order: x.null_order,
                    })
                    .collect_vec(),
            )
            .build()
            .unwrap()
    }
}

/// Reference to [`UnboundSortOrder`].
pub type UnboundSortOrderRef = Arc<UnboundSortOrder>;

/// Entry for every column that is to be sorted
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct UnboundSortField {
    /// A source column id from the table’s schema
    pub source_id: i32,
    /// A transform that is used to produce values to be sorted on from the source column.
    pub transform: Transform,
    /// A sort direction, that can only be either asc or desc
    pub direction: SortDirection,
    /// A null order that describes the order of null values when sorted.
    pub null_order: NullOrder,
}

/// Unbound sort order can be later bound to a schema.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Builder, Default)]
#[serde(rename_all = "kebab-case")]
#[builder(setter(prefix = "with"))]
#[builder(build_fn(skip))]
pub struct UnboundSortOrder {
    /// Identifier for the SortOrder, order_id `0` is no sort order.
    pub order_id: i64,
    /// Details of the sort
    #[builder(setter(each(name = "with_sort_field")))]
    pub fields: Vec<UnboundSortField>,
}

impl UnboundSortOrder {
    /// Create unbound sort order builder
    pub fn builder() -> UnboundSortOrderBuilder {
        UnboundSortOrderBuilder::default()
    }

    /// Create an unbound unsorted order
    fn unsorted_order() -> UnboundSortOrder {
        UnboundSortOrder {
            order_id: 0,
            fields: Vec::new(),
        }
    }

    /// Bind unbound partition spec to a schema
    pub fn bind(&self, _schema: SchemaRef) -> Result<SortOrder> {
        todo!()
    }
}

impl UnboundSortOrderBuilder {
    /// Creates a new unbound sort order.
    pub fn build(&self) -> Result<UnboundSortOrder> {
        let fields = self.fields.clone().unwrap_or(Vec::new());
        return match (self.order_id, fields.as_slice()) {
            (Some(0) | None, []) => Ok(UnboundSortOrder::unsorted_order()),
            (_, []) => Err(Error::new(
                ErrorKind::Unexpected,
                "Unsorted order ID must be 0",
            )),
            (Some(0), [..]) => Err(Error::new(
                ErrorKind::Unexpected,
                "Sort order ID 0 is reserved for unsorted order",
            )),
            (_, [..]) => Ok(UnboundSortOrder {
                order_id: self.order_id.unwrap_or(1),
                fields: fields.to_vec(),
            }),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_field() {
        let spec = r#"
        {
            "transform": "bucket[4]",
            "source-id": 3,
            "direction": "desc",
            "null-order": "nulls-last"
         }
        "#;

        let field: SortField = serde_json::from_str(spec).unwrap();
        assert_eq!(Transform::Bucket(4), field.transform);
        assert_eq!(3, field.source_id);
        assert_eq!(SortDirection::Descending, field.direction);
        assert_eq!(NullOrder::Last, field.null_order);
    }

    #[test]
    fn test_sort_order() {
        let spec = r#"
        {
        "order-id": 1,
        "fields": [ {
            "transform": "identity",
            "source-id": 2,
            "direction": "asc",
            "null-order": "nulls-first"
         }, {
            "transform": "bucket[4]",
            "source-id": 3,
            "direction": "desc",
            "null-order": "nulls-last"
         } ]
        }
        "#;

        let order: SortOrder = serde_json::from_str(spec).unwrap();
        assert_eq!(Transform::Identity, order.fields[0].transform);
        assert_eq!(2, order.fields[0].source_id);
        assert_eq!(SortDirection::Ascending, order.fields[0].direction);
        assert_eq!(NullOrder::First, order.fields[0].null_order);

        assert_eq!(Transform::Bucket(4), order.fields[1].transform);
        assert_eq!(3, order.fields[1].source_id);
        assert_eq!(SortDirection::Descending, order.fields[1].direction);
        assert_eq!(NullOrder::Last, order.fields[1].null_order);
    }

    #[test]
    fn test_unbound_sort_order() {
        let spec = r#"
        {
        "order-id": 1,
        "fields": [ {
            "transform": "identity",
            "source-id": 2,
            "direction": "asc",
            "null-order": "nulls-first"
         }, {
            "transform": "bucket[4]",
            "source-id": 3,
            "direction": "desc",
            "null-order": "nulls-last"
         } ]
        }
        "#;

        let order: UnboundSortOrder = serde_json::from_str(spec).unwrap();
        assert_eq!(Transform::Identity, order.fields[0].transform);
        assert_eq!(2, order.fields[0].source_id);
        assert_eq!(SortDirection::Ascending, order.fields[0].direction);
        assert_eq!(NullOrder::First, order.fields[0].null_order);

        assert_eq!(Transform::Bucket(4), order.fields[1].transform);
        assert_eq!(3, order.fields[1].source_id);
        assert_eq!(SortDirection::Descending, order.fields[1].direction);
        assert_eq!(NullOrder::Last, order.fields[1].null_order);
    }
}
