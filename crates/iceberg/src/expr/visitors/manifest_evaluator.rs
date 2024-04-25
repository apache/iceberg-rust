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
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::{Bind, BoundPredicate, BoundReference};
use crate::spec::{Datum, FieldSummary, ManifestFile, PartitionSpecRef, Schema, SchemaRef};
use crate::{Error, ErrorKind};
use fnv::FnvHashSet;
use std::sync::Arc;

/// Evaluates [`ManifestFile`]s to see if their partition summary matches a provided
/// [`BoundPredicate`]. Used by [`TableScan`] to filter down the list of [`ManifestFile`]s
/// in which data might be found that matches the TableScan's filter.
pub(crate) struct ManifestEvaluator {
    partition_schema: SchemaRef,
    partition_filter: BoundPredicate,
    case_sensitive: bool,
}

impl ManifestEvaluator {
    pub(crate) fn new(
        partition_spec: PartitionSpecRef,
        table_schema: SchemaRef,
        filter: BoundPredicate,
        case_sensitive: bool,
    ) -> crate::Result<Self> {
        let partition_type = partition_spec.partition_type(&table_schema)?;

        // this is needed as SchemaBuilder.with_fields expects an iterator over
        // Arc<NestedField> rather than &Arc<NestedField>
        let cloned_partition_fields: Vec<_> =
            partition_type.fields().iter().map(Arc::clone).collect();

        // The partition_schema's schema_id is set to the partition
        // spec's spec_id here, and used to perform a sanity check
        // during eval to confirm that it matches the spec_id
        // of the ManifestFile we're evaluating
        let partition_schema = Schema::builder()
            .with_schema_id(partition_spec.spec_id)
            .with_fields(cloned_partition_fields)
            .build()?;

        let partition_schema_ref = Arc::new(partition_schema);

        let mut inclusive_projection = InclusiveProjection::new(partition_spec.clone());
        let unbound_partition_filter = inclusive_projection.project(&filter)?;

        let partition_filter = unbound_partition_filter
            .rewrite_not()
            .bind(partition_schema_ref.clone(), case_sensitive)?;

        Ok(Self {
            partition_schema: partition_schema_ref,
            partition_filter,
            case_sensitive,
        })
    }

    /// Evaluate this `ManifestEvaluator`'s filter predicate against the
    /// provided [`ManifestFile`]'s partitions. Used by [`TableScan`] to
    /// see if this `ManifestFile` could possibly contain data that matches
    /// the scan's filter.
    pub(crate) fn eval(&self, manifest_file: &ManifestFile) -> crate::Result<bool> {
        if manifest_file.partitions.is_empty() {
            return Ok(true);
        }

        // The schema_id of self.partition_schema is set to the
        // spec_id of the partition spec that this ManifestEvaluator
        // was created from in ManifestEvaluator::new
        if self.partition_schema.schema_id() != manifest_file.partition_spec_id {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Partition ID for manifest file '{}' does not match partition ID for the Scan",
                    &manifest_file.manifest_path
                ),
            ));
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
    use crate::expr::visitors::manifest_evaluator::ManifestEvaluator;
    use crate::expr::{Bind, Predicate, PredicateOperator, Reference, UnaryExpression};
    use crate::spec::{
        FieldSummary, ManifestContentType, ManifestFile, NestedField, PartitionField,
        PartitionSpec, PrimitiveType, Schema, Transform, Type,
    };
    use std::sync::Arc;

    #[test]
    fn test_manifest_file_no_partitions() {
        let (table_schema_ref, partition_spec_ref) = create_test_schema_and_partition_spec();

        let partition_filter = Predicate::AlwaysTrue
            .bind(table_schema_ref.clone(), false)
            .unwrap();

        let case_sensitive = false;

        let manifest_file_partitions = vec![];
        let manifest_file = create_test_manifest_file(manifest_file_partitions);

        let manifest_evaluator = ManifestEvaluator::new(
            partition_spec_ref,
            table_schema_ref,
            partition_filter,
            case_sensitive,
        )
        .unwrap();

        let result = manifest_evaluator.eval(&manifest_file).unwrap();

        assert!(result);
    }

    #[test]
    fn test_manifest_file_trivial_partition_passing_filter() {
        let (table_schema_ref, partition_spec_ref) = create_test_schema_and_partition_spec();

        let partition_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("a"),
        ))
        .bind(table_schema_ref.clone(), true)
        .unwrap();

        let manifest_file_partitions = vec![FieldSummary {
            contains_null: true,
            contains_nan: None,
            lower_bound: None,
            upper_bound: None,
        }];
        let manifest_file = create_test_manifest_file(manifest_file_partitions);

        let manifest_evaluator =
            ManifestEvaluator::new(partition_spec_ref, table_schema_ref, partition_filter, true)
                .unwrap();

        let result = manifest_evaluator.eval(&manifest_file).unwrap();

        assert!(result);
    }

    #[test]
    fn test_manifest_file_partition_id_mismatch_returns_error() {
        let (table_schema_ref, partition_spec_ref) =
            create_test_schema_and_partition_spec_with_id_mismatch();

        let partition_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("a"),
        ))
        .bind(table_schema_ref.clone(), true)
        .unwrap();

        let manifest_file_partitions = vec![FieldSummary {
            contains_null: true,
            contains_nan: None,
            lower_bound: None,
            upper_bound: None,
        }];
        let manifest_file = create_test_manifest_file(manifest_file_partitions);

        let manifest_evaluator =
            ManifestEvaluator::new(partition_spec_ref, table_schema_ref, partition_filter, true)
                .unwrap();

        let result = manifest_evaluator.eval(&manifest_file);

        assert!(result.is_err());
    }

    #[test]
    fn test_manifest_file_trivial_partition_rejected_filter() {
        let (table_schema_ref, partition_spec_ref) = create_test_schema_and_partition_spec();

        let partition_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("a"),
        ))
        .bind(table_schema_ref.clone(), true)
        .unwrap();

        let manifest_file_partitions = vec![FieldSummary {
            contains_null: false,
            contains_nan: None,
            lower_bound: None,
            upper_bound: None,
        }];
        let manifest_file = create_test_manifest_file(manifest_file_partitions);

        let manifest_evaluator =
            ManifestEvaluator::new(partition_spec_ref, table_schema_ref, partition_filter, true)
                .unwrap();

        let result = manifest_evaluator.eval(&manifest_file).unwrap();

        assert!(!result);
    }

    fn create_test_schema_and_partition_spec() -> (Arc<Schema>, Arc<PartitionSpec>) {
        let table_schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Float),
            ))])
            .build()
            .unwrap();
        let table_schema_ref = Arc::new(table_schema);

        let partition_spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_fields(vec![PartitionField::builder()
                .source_id(1)
                .name("a".to_string())
                .field_id(1)
                .transform(Transform::Identity)
                .build()])
            .build()
            .unwrap();
        let partition_spec_ref = Arc::new(partition_spec);
        (table_schema_ref, partition_spec_ref)
    }

    fn create_test_schema_and_partition_spec_with_id_mismatch() -> (Arc<Schema>, Arc<PartitionSpec>)
    {
        let table_schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Float),
            ))])
            .build()
            .unwrap();
        let table_schema_ref = Arc::new(table_schema);

        let partition_spec = PartitionSpec::builder()
            // Spec ID here deliberately doesn't match the one from create_test_manifest_file
            .with_spec_id(999)
            .with_fields(vec![PartitionField::builder()
                .source_id(1)
                .name("a".to_string())
                .field_id(1)
                .transform(Transform::Identity)
                .build()])
            .build()
            .unwrap();
        let partition_spec_ref = Arc::new(partition_spec);
        (table_schema_ref, partition_spec_ref)
    }

    fn create_test_manifest_file(manifest_file_partitions: Vec<FieldSummary>) -> ManifestFile {
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
            partitions: manifest_file_partitions,
            key_metadata: vec![],
        }
    }
}
