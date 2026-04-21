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

//! Utilities for reasoning about how a predicate relates to the partitioning of a
//! [`DataFile`]. Useful for partition-aligned row-level operations such as DELETE
//! that is only permitted when it drops whole files.

use std::sync::Arc;

use crate::Result;
use crate::expr::visitors::expression_evaluator::ExpressionEvaluator;
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::visitors::strict_projection::StrictProjection;
use crate::expr::{Bind, Predicate};
use crate::spec::{DataFile, PartitionSpecRef, Schema, SchemaRef};

/// How a predicate relates to the partition value of a single [`DataFile`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionCoverage {
    /// Every row in the file matches the predicate.
    ///
    /// Produced when the strict projection of the predicate onto the partition spec
    /// evaluates to true against the file's partition value.
    AllRowsMatch,
    /// No row in the file can match the predicate.
    ///
    /// Produced when the inclusive projection of the predicate onto the partition
    /// spec evaluates to false against the file's partition value.
    NoRowsMatch,
    /// The file straddles the predicate: some rows may match and some may not.
    ///
    /// Produced when the inclusive projection matches but the strict projection
    /// does not. In this case row-level filtering would be required to answer the
    /// predicate exactly; the partition value alone is insufficient.
    Straddle,
}

/// Classifies a [`DataFile`] against a predicate by evaluating strict and inclusive
/// projections of the predicate onto a single partition spec.
///
/// Construct one filter per partition spec the caller may encounter (tables with
/// evolved specs have multiple). See [`DataFile::partition_spec_id`] to route each
/// file to the right filter.
pub struct PartitionCoverageFilter {
    inclusive: ExpressionEvaluator,
    strict: ExpressionEvaluator,
}

impl PartitionCoverageFilter {
    /// Build a filter for `predicate` against `schema` and `partition_spec`.
    ///
    /// `case_sensitive` controls column-name binding.
    pub fn try_new(
        predicate: &Predicate,
        schema: SchemaRef,
        partition_spec: PartitionSpecRef,
        case_sensitive: bool,
    ) -> Result<Self> {
        let bound = predicate
            .clone()
            .rewrite_not()
            .bind(schema.clone(), case_sensitive)?;

        let partition_type = partition_spec.partition_type(&schema)?;
        let partition_schema = Arc::new(
            Schema::builder()
                .with_schema_id(partition_spec.spec_id())
                .with_fields(partition_type.fields().to_owned())
                .build()?,
        );

        let inclusive = InclusiveProjection::new(partition_spec.clone())
            .project(&bound)?
            .rewrite_not()
            .bind(partition_schema.clone(), case_sensitive)?;

        let strict = StrictProjection::new(partition_spec)
            .strict_project(&bound)?
            .rewrite_not()
            .bind(partition_schema, case_sensitive)?;

        Ok(Self {
            inclusive: ExpressionEvaluator::new(inclusive),
            strict: ExpressionEvaluator::new(strict),
        })
    }

    /// Classify `data_file`'s partition value against this filter.
    pub fn classify(&self, data_file: &DataFile) -> Result<PartitionCoverage> {
        if self.strict.eval(data_file)? {
            return Ok(PartitionCoverage::AllRowsMatch);
        }
        if !self.inclusive.eval(data_file)? {
            return Ok(PartitionCoverage::NoRowsMatch);
        }
        Ok(PartitionCoverage::Straddle)
    }
}
