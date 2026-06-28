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

//! Technology Compatibility Kit (TCK) — Layer 1: DataBatch conformance.
//!
//! This module defines the test harness that any `DataBatch` implementation
//! must pass. A new batch type adds one fixture implementation to enter the
//! entire TCK matrix.
//!
//! # Structure
//!
//! ```text
//! Layer 3: Integration    (format x batch x scenario)
//! Layer 2: Format         (one format, known-good batch type)
//! Layer 1: DataBatch      (one batch type, no format involvement)  ← this module
//! ```
//!
//! When a Layer 3 test fails, Layer 1 and Layer 2 results identify whether
//! the bug is in the batch implementation, the format implementation, or
//! their interaction.
//!
//! # Adding a new DataBatch implementation
//!
//! 1. Implement `DataBatch` for your type.
//! 2. Implement `DataBatchTestFixture` for a fixture that constructs test data.
//! 3. Call the TCK test functions with your fixture.
//! 4. All tests must pass before merging.

use super::DataBatch;

/// Trait for constructing test fixtures for a specific `DataBatch` implementation.
///
/// Each `DataBatch` implementation provides a fixture that knows how to
/// build test data in its native format. This keeps the TCK test logic
/// generic while each implementation constructs its own batch type.
pub trait DataBatchTestFixture {
    /// The concrete `DataBatch` type under test.
    type Batch: DataBatch;

    /// A batch with a single Int32 column (field ID 1), non-null.
    fn int32_batch(&self, num_rows: usize) -> Self::Batch;

    /// A batch with a nullable String column (field ID 2), alternating null/non-null.
    fn nullable_string_batch(&self, num_rows: usize) -> Self::Batch;

    /// A batch with Int32 (field ID 1) and String (field ID 2) columns.
    fn two_column_batch(&self, num_rows: usize) -> Self::Batch;

    /// An empty batch (zero rows) with the same schema as `two_column_batch`.
    fn empty_batch(&self) -> Self::Batch;
}

// ---------------------------------------------------------------------------
// TCK Layer 1 test scenarios
//
// Each function tests one aspect of the DataBatch contract. These are called
// by concrete test modules (one per DataBatch implementation).
// ---------------------------------------------------------------------------

/// Verify num_rows returns the correct count.
pub fn tck_num_rows<F: DataBatchTestFixture>(fixture: &F) {
    let batch = fixture.int32_batch(10);
    assert_eq!(batch.num_rows(), 10);
}

/// Verify num_rows is zero for an empty batch.
pub fn tck_num_rows_empty<F: DataBatchTestFixture>(fixture: &F) {
    let batch = fixture.empty_batch();
    assert_eq!(batch.num_rows(), 0);
}

/// Verify column_metrics returns correct null count for nullable columns.
pub fn tck_column_metrics_null_count<F: DataBatchTestFixture>(fixture: &F) {
    let batch = fixture.nullable_string_batch(6);
    let metrics = batch.column_metrics(2);
    assert!(metrics.is_some(), "field ID 2 should be present");
    let m = metrics.unwrap();
    assert_eq!(m.value_count, 6);
    assert_eq!(m.null_count, 3);
}

/// Verify column_metrics returns None for a field ID not in the batch.
pub fn tck_column_metrics_missing_field<F: DataBatchTestFixture>(fixture: &F) {
    let batch = fixture.int32_batch(5);
    assert!(batch.column_metrics(99).is_none());
}

/// Verify as_any allows downcasting to the concrete batch type.
pub fn tck_as_any_downcast<F: DataBatchTestFixture>(fixture: &F) {
    let batch = fixture.int32_batch(3);
    let any_ref = batch.as_any();
    assert!(
        any_ref.downcast_ref::<F::Batch>().is_some(),
        "as_any must allow downcasting to the concrete batch type"
    );
}

/// Verify project returns an error for a non-existent field ID.
pub fn tck_project_nonexistent_field<F: DataBatchTestFixture>(fixture: &F) {
    let batch = fixture.two_column_batch(5);
    let result = batch.project(&[99]);
    assert!(result.is_err(), "projecting a non-existent field ID must error");
}

// ---------------------------------------------------------------------------
// Future TCK scenarios (stubbed)
// ---------------------------------------------------------------------------

/// Verify filter with a simple equality predicate.
pub fn tck_filter_equality<F: DataBatchTestFixture>(_fixture: &F) {
    todo!("Requires constructing a BoundPredicate for testing")
}

/// Verify inject_constants adds constant columns.
pub fn tck_inject_constants<F: DataBatchTestFixture>(_fixture: &F) {
    todo!("Requires Datum construction for each primitive type")
}

/// Verify evolve_schema handles type promotion (int -> long).
pub fn tck_evolve_schema_promotion<F: DataBatchTestFixture>(_fixture: &F) {
    todo!("Requires constructing source/target Schema pairs")
}

/// Verify evolve_schema handles adding a new nullable column.
pub fn tck_evolve_schema_add_column<F: DataBatchTestFixture>(_fixture: &F) {
    todo!("Requires constructing source/target Schema pairs")
}
