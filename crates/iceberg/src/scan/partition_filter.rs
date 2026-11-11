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

use super::cache::{ExpressionEvaluatorCache, PartitionFilterCache};
use crate::Result;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::spec::{PartitionSpecRef, SchemaRef, Struct};

/// Checks whether a data file's partition can hold rows that match a filter, as a table scan
/// does when it plans files.
#[derive(Debug)]
pub struct PartitionFilter {
    schema: SchemaRef,
    case_sensitive: bool,
    filter: BoundPredicate,
    partition_filters: PartitionFilterCache,
    evaluators: ExpressionEvaluatorCache,
}

impl PartitionFilter {
    /// Creates a partition filter for `filter` on a table with `schema`.
    ///
    /// # Errors
    ///
    /// Returns an error if `filter` doesn't bind to `schema`.
    pub fn try_new(filter: Predicate, schema: SchemaRef, case_sensitive: bool) -> Result<Self> {
        let filter = filter.rewrite_not().bind(schema.clone(), case_sensitive)?;
        Ok(Self {
            schema,
            case_sensitive,
            filter,
            partition_filters: PartitionFilterCache::new(),
            evaluators: ExpressionEvaluatorCache::new(),
        })
    }

    /// Returns whether a data file with `partition`, written with `partition_spec`, can hold rows
    /// that match the filter.
    ///
    /// # Errors
    ///
    /// Returns an error if the filter can't be projected onto `partition_spec`, or if `partition`
    /// doesn't match the spec's fields.
    pub fn can_match(&self, partition_spec: &PartitionSpecRef, partition: &Struct) -> Result<bool> {
        let partition_filter = self.partition_filters.get(
            partition_spec,
            &self.schema,
            self.case_sensitive,
            &self.filter,
        )?;
        self.evaluators
            .get(partition_spec.spec_id(), &partition_filter)?
            .eval_partition(partition)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::PartitionFilter;
    use crate::Result;
    use crate::expr::{Predicate, Reference};
    use crate::spec::{
        Datum, Literal, NestedField, PartitionSpec, PartitionSpecRef, PrimitiveType, Schema,
        SchemaRef, Struct, Transform, Type,
    };

    /// A table with columns `id` and `day`, partitioned by `id` and the month of `day`.
    fn table() -> Result<(SchemaRef, PartitionSpecRef)> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "day", Type::Primitive(PrimitiveType::Date)).into(),
                ])
                .build()?,
        );
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_partition_field("id", "id", Transform::Identity)?
            .add_partition_field("day", "day_month", Transform::Month)?
            .build()?;
        Ok((schema, Arc::new(spec)))
    }

    /// A partition value of [`table`]'s spec. `month` counts months since 1970-01.
    fn partition(id: Option<i32>, month: Option<i32>) -> Struct {
        Struct::from_iter([id.map(Literal::int), month.map(Literal::int)])
    }

    fn can_match(filter: Predicate, partitions: &[Struct]) -> Result<Vec<bool>> {
        let (schema, spec) = table()?;
        let filter = PartitionFilter::try_new(filter, schema, true)?;
        partitions
            .iter()
            .map(|partition| filter.can_match(&spec, partition))
            .collect()
    }

    #[test]
    fn matches_the_partition_of_a_transformed_value() -> Result<()> {
        // 648 is 2024-01 and 649 is 2024-02.
        let filter = Reference::new("day").equal_to(Datum::date_from_str("2024-02-14")?);
        let partitions = [partition(Some(1), Some(648)), partition(Some(1), Some(649))];
        assert_eq!(can_match(filter, &partitions)?, vec![false, true]);
        Ok(())
    }

    #[test]
    fn matches_a_null_partition_only_when_the_filter_can_match_null() -> Result<()> {
        let partitions = [partition(None, Some(648)), partition(Some(1), Some(648))];
        let is_null = Reference::new("id").is_null();
        assert_eq!(can_match(is_null, &partitions)?, vec![true, false]);
        let equal = Reference::new("id").equal_to(Datum::int(1));
        assert_eq!(can_match(equal, &partitions)?, vec![false, true]);
        Ok(())
    }

    #[test]
    fn rewrites_a_not_before_projecting_it() -> Result<()> {
        let filter = !Reference::new("id").less_than(Datum::int(5));
        let partitions = [partition(Some(3), Some(648)), partition(Some(7), Some(648))];
        assert_eq!(can_match(filter, &partitions)?, vec![false, true]);
        Ok(())
    }

    #[test]
    fn matches_every_file_of_an_unpartitioned_spec() -> Result<()> {
        let (schema, _) = table()?;
        let filter =
            PartitionFilter::try_new(Reference::new("id").equal_to(Datum::int(1)), schema, true)?;
        let spec = Arc::new(PartitionSpec::unpartition_spec());
        assert!(filter.can_match(&spec, &Struct::empty())?);
        Ok(())
    }

    #[test]
    fn rejects_a_filter_on_a_missing_column() -> Result<()> {
        let (schema, _) = table()?;
        let filter = Reference::new("missing").equal_to(Datum::int(1));
        assert!(PartitionFilter::try_new(filter, schema, true).is_err());
        Ok(())
    }
}
