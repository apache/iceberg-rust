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

use crate::expr::visitors::bound_predicate_visitor::{visit, BoundPredicateVisitor};
use crate::expr::{BoundPredicate, BoundReference};
use crate::spec::{Datum, FieldSummary, ManifestFile};
use crate::Result;
use fnv::FnvHashSet;

#[derive(Debug)]
/// Evaluates a [`ManifestFile`] to see if the partition summaries
/// match a provided [`BoundPredicate`].
///
/// Used by [`TableScan`] to prune the list of [`ManifestFile`]s
/// in which data might be found that matches the TableScan's filter.
pub(crate) struct ManifestEvaluator {
    partition_filter: BoundPredicate,
    case_sensitive: bool,
}

impl ManifestEvaluator {
    pub(crate) fn new(partition_filter: BoundPredicate, case_sensitive: bool) -> Self {
        Self {
            partition_filter,
            case_sensitive,
        }
    }

    /// Evaluate this `ManifestEvaluator`'s filter predicate against the
    /// provided [`ManifestFile`]'s partitions. Used by [`TableScan`] to
    /// see if this `ManifestFile` could possibly contain data that matches
    /// the scan's filter.
    pub(crate) fn eval(&self, manifest_file: &ManifestFile) -> Result<bool> {
        if manifest_file.partitions.is_empty() {
            return Ok(true);
        }

        let mut evaluator = ManifestFilterVisitor::new(self, &manifest_file.partitions);

        visit(&mut evaluator, &self.partition_filter)
    }
}

struct ManifestFilterVisitor<'a> {
    manifest_evaluator: &'a ManifestEvaluator,
    partitions: &'a Vec<FieldSummary>,
}

impl<'a> ManifestFilterVisitor<'a> {
    fn new(manifest_evaluator: &'a ManifestEvaluator, partitions: &'a Vec<FieldSummary>) -> Self {
        ManifestFilterVisitor {
            manifest_evaluator,
            partitions,
        }
    }
}

// Remove this annotation once all todos have been removed
#[allow(unused_variables)]
impl BoundPredicateVisitor for ManifestFilterVisitor<'_> {
    type T = bool;

    fn always_true(&mut self) -> crate::Result<bool> {
        Ok(true)
    }

    fn always_false(&mut self) -> crate::Result<bool> {
        Ok(false)
    }

    fn and(&mut self, lhs: bool, rhs: bool) -> crate::Result<bool> {
        Ok(lhs && rhs)
    }

    fn or(&mut self, lhs: bool, rhs: bool) -> crate::Result<bool> {
        Ok(lhs || rhs)
    }

    fn not(&mut self, inner: bool) -> crate::Result<bool> {
        Ok(!inner)
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        Ok(self.field_summary_for_reference(reference).contains_null)
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        Ok(self
            .field_summary_for_reference(reference)
            .contains_nan
            .is_some())
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        todo!()
    }
}

impl ManifestFilterVisitor<'_> {
    fn field_summary_for_reference(&self, reference: &BoundReference) -> &FieldSummary {
        let pos = reference.accessor().position();
        &self.partitions[pos]
    }
}

#[cfg(test)]
mod test {
    use crate::expr::visitors::inclusive_projection::InclusiveProjection;
    use crate::expr::visitors::manifest_evaluator::ManifestEvaluator;
    use crate::expr::{
        Bind, BoundPredicate, Predicate, PredicateOperator, Reference, UnaryExpression,
    };
    use crate::spec::{
        FieldSummary, ManifestContentType, ManifestFile, NestedField, PartitionField,
        PartitionSpec, PartitionSpecRef, PrimitiveType, Schema, SchemaRef, Transform, Type,
    };
    use crate::Result;
    use std::sync::Arc;

    fn create_schema_and_partition_spec() -> Result<(SchemaRef, PartitionSpecRef)> {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Float),
            ))])
            .build()?;

        let spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_fields(vec![PartitionField::builder()
                .source_id(1)
                .name("a".to_string())
                .field_id(1)
                .transform(Transform::Identity)
                .build()])
            .build()
            .unwrap();

        Ok((Arc::new(schema), Arc::new(spec)))
    }

    fn create_schema_and_partition_spec_with_id_mismatch() -> Result<(SchemaRef, PartitionSpecRef)>
    {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Float),
            ))])
            .build()?;

        let spec = PartitionSpec::builder()
            .with_spec_id(999)
            .with_fields(vec![PartitionField::builder()
                .source_id(1)
                .name("a".to_string())
                .field_id(1)
                .transform(Transform::Identity)
                .build()])
            .build()
            .unwrap();

        Ok((Arc::new(schema), Arc::new(spec)))
    }

    fn create_manifest_file(partitions: Vec<FieldSummary>) -> ManifestFile {
        ManifestFile {
            manifest_path: "/test/path".to_string(),
            manifest_length: 0,
            partition_spec_id: 1,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 0,
            added_data_files_count: None,
            existing_data_files_count: None,
            deleted_data_files_count: None,
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            partitions,
            key_metadata: vec![],
        }
    }

    fn create_partition_schema(
        partition_spec: &PartitionSpecRef,
        schema: &Schema,
    ) -> Result<SchemaRef> {
        let partition_type = partition_spec.partition_type(schema)?;

        let partition_fields: Vec<_> = partition_type.fields().iter().map(Arc::clone).collect();

        let partition_schema = Arc::new(
            Schema::builder()
                .with_schema_id(partition_spec.spec_id)
                .with_fields(partition_fields)
                .build()?,
        );

        Ok(partition_schema)
    }

    fn create_partition_filter(
        partition_spec: PartitionSpecRef,
        partition_schema: SchemaRef,
        filter: &BoundPredicate,
        case_sensitive: bool,
    ) -> Result<BoundPredicate> {
        let mut inclusive_projection = InclusiveProjection::new(partition_spec);

        let partition_filter = inclusive_projection
            .project(filter)?
            .rewrite_not()
            .bind(partition_schema, case_sensitive)?;

        Ok(partition_filter)
    }

    fn create_manifest_evaluator(
        schema: SchemaRef,
        partition_spec: PartitionSpecRef,
        filter: &BoundPredicate,
        case_sensitive: bool,
    ) -> Result<ManifestEvaluator> {
        let partition_schema = create_partition_schema(&partition_spec, &schema)?;
        let partition_filter = create_partition_filter(
            partition_spec,
            partition_schema.clone(),
            filter,
            case_sensitive,
        )?;

        Ok(ManifestEvaluator::new(partition_filter, case_sensitive))
    }

    #[test]
    fn test_manifest_file_empty_partitions() -> Result<()> {
        let case_sensitive = false;

        let (schema, partition_spec) = create_schema_and_partition_spec()?;

        let filter = Predicate::AlwaysTrue.bind(schema.clone(), case_sensitive)?;

        let manifest_file = create_manifest_file(vec![]);

        let manifest_evaluator =
            create_manifest_evaluator(schema, partition_spec, &filter, case_sensitive)?;

        let result = manifest_evaluator.eval(&manifest_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_manifest_file_trivial_partition_passing_filter() -> Result<()> {
        let case_sensitive = true;

        let (schema, partition_spec) = create_schema_and_partition_spec()?;

        let filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("a"),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let manifest_file = create_manifest_file(vec![FieldSummary {
            contains_null: true,
            contains_nan: None,
            lower_bound: None,
            upper_bound: None,
        }]);

        let manifest_evaluator =
            create_manifest_evaluator(schema, partition_spec, &filter, case_sensitive)?;

        let result = manifest_evaluator.eval(&manifest_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_manifest_file_trivial_partition_rejected_filter() -> Result<()> {
        let case_sensitive = true;

        let (schema, partition_spec) = create_schema_and_partition_spec()?;

        let filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("a"),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let manifest_file = create_manifest_file(vec![FieldSummary {
            contains_null: false,
            contains_nan: None,
            lower_bound: None,
            upper_bound: None,
        }]);

        let manifest_evaluator =
            create_manifest_evaluator(schema, partition_spec, &filter, case_sensitive)?;

        let result = manifest_evaluator.eval(&manifest_file).unwrap();

        assert!(!result);

        Ok(())
    }
}
