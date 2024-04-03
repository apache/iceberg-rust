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

use crate::expr::visitors::bound_predicate_evaluator::BoundPredicateEvaluator;
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::{Bind, BoundPredicate, BoundReference};
use crate::spec::{Datum, FieldSummary, ManifestFile, PartitionSpecRef, Schema, SchemaRef};
use fnv::FnvHashSet;
use std::sync::Arc;

pub(crate) struct ManifestEvaluatorFactory {
    partition_schema: SchemaRef,
    partition_filter: BoundPredicate,
    case_sensitive: bool,
}

impl ManifestEvaluatorFactory {
    pub(crate) fn new(
        partition_spec: PartitionSpecRef,
        table_schema: SchemaRef,
        partition_filter: BoundPredicate,
        case_sensitive: bool,
    ) -> crate::Result<Self> {
        let partition_type = partition_spec.partition_type(&table_schema)?;

        // this is needed as SchemaBuilder.with_fields expects an iterator over
        // Arc<NestedField> rather than &Arc<NestedField>
        let cloned_partition_fields: Vec<_> =
            partition_type.fields().iter().map(Arc::clone).collect();

        let partition_schema = Schema::builder()
            .with_fields(cloned_partition_fields)
            .build()?;

        let partition_schema_ref = Arc::new(partition_schema);

        let inclusive_projection =
            InclusiveProjection::new(table_schema.clone(), partition_spec.clone());
        let unbound_partition_filter = inclusive_projection.project(&partition_filter)?;

        let partition_filter =
            unbound_partition_filter.bind(partition_schema_ref.clone(), case_sensitive)?;

        Ok(Self {
            partition_schema: partition_schema_ref,
            partition_filter,
            case_sensitive,
        })
    }

    pub(crate) fn evaluate(&self, manifest_file: &ManifestFile) -> crate::Result<bool> {
        if manifest_file.partitions.is_empty() {
            return Ok(true);
        }

        let mut evaluator = ManifestEvaluator::new(self, &manifest_file.partitions);

        evaluator.visit(&self.partition_filter)
    }
}

struct ManifestEvaluator<'a> {
    manifest_evaluator_builder: &'a ManifestEvaluatorFactory,
    partitions: &'a Vec<FieldSummary>,
}

impl<'a> ManifestEvaluator<'a> {
    fn new(
        manifest_evaluator_builder: &'a ManifestEvaluatorFactory,
        partitions: &'a Vec<FieldSummary>,
    ) -> Self {
        ManifestEvaluator {
            manifest_evaluator_builder,
            partitions,
        }
    }
}

// Remove this annotation once all todos have been removed
#[allow(unused_variables)]
impl BoundPredicateEvaluator for ManifestEvaluator<'_> {
    fn is_null(&mut self, reference: &BoundReference) -> crate::Result<bool> {
        Ok(self.field_summary_for_reference(reference).contains_null)
    }

    fn not_null(&mut self, reference: &BoundReference) -> crate::Result<bool> {
        todo!()
    }

    fn is_nan(&mut self, reference: &BoundReference) -> crate::Result<bool> {
        Ok(self
            .field_summary_for_reference(reference)
            .contains_nan
            .is_some())
    }

    fn not_nan(&mut self, reference: &BoundReference) -> crate::Result<bool> {
        todo!()
    }

    fn less_than(&mut self, reference: &BoundReference, literal: &Datum) -> crate::Result<bool> {
        todo!()
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn greater_than(&mut self, reference: &BoundReference, literal: &Datum) -> crate::Result<bool> {
        todo!()
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn eq(&mut self, reference: &BoundReference, literal: &Datum) -> crate::Result<bool> {
        todo!()
    }

    fn not_eq(&mut self, reference: &BoundReference, literal: &Datum) -> crate::Result<bool> {
        todo!()
    }

    fn starts_with(&mut self, reference: &BoundReference, literal: &Datum) -> crate::Result<bool> {
        todo!()
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
    ) -> crate::Result<bool> {
        todo!()
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
    ) -> crate::Result<bool> {
        todo!()
    }
}

impl ManifestEvaluator<'_> {
    fn field_summary_for_reference(&self, reference: &BoundReference) -> &FieldSummary {
        let pos = reference.accessor().position();
        &self.partitions[pos as usize]
    }
}
