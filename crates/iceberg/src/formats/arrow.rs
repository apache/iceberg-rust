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

//! `impl DataBatch for RecordBatch` — the shipped default in-memory representation.
//!
//! This is the implementation that engines get for free when they use Arrow.
//! Engines that need a different in-memory representation implement `DataBatch`
//! for their own type and pass the TCK Layer 1 suite.

use std::any::Any;

use arrow_array::RecordBatch;

use super::traits::{ColumnMetrics, DataBatch};
use crate::expr::BoundPredicate;
use crate::spec::{Datum, Schema};
use crate::Result;

impl DataBatch for RecordBatch {
    fn num_rows(&self) -> usize {
        self.num_rows()
    }

    fn field_ids(&self) -> &[i32] {
        // Field IDs are stored per-column in Arrow schema metadata.
        // A production implementation caches them; this stub returns empty.
        &[]
    }

    fn project(&self, _field_ids: &[i32]) -> Result<Self> {
        // Production: use RecordBatchProjector to select columns by field ID.
        todo!("Wire RecordBatchProjector")
    }

    fn filter(&self, _predicate: &BoundPredicate) -> Result<Self> {
        // Production: use PredicateConverter to evaluate against Arrow arrays.
        todo!("Wire PredicateConverter")
    }

    fn column_metrics(&self, _field_id: i32) -> Option<ColumnMetrics> {
        // Production: compute min/max/null/NaN from the Arrow array.
        todo!("Wire column metrics computation")
    }

    fn inject_constants(&self, _constants: &[(i32, Datum)]) -> Result<Self> {
        // Production: append constant-value columns to the batch.
        todo!("Wire constant injection")
    }

    fn evolve_schema(&self, _source: &Schema, _target: &Schema) -> Result<Self> {
        // Production: cast columns for type widening, add null columns for new fields.
        todo!("Wire schema evolution")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
