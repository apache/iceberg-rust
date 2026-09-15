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

//! Delta writer module.
//!
//! A delta writer consumes a stream of row-level changes (inserts and deletes)
//! and produces both data files and delete files in a single pass. This is the
//! foundation for the `DeltaWriter` epic (see <https://github.com/apache/iceberg-rust/issues/2218>).
//!
//! This module currently hosts the building blocks for that writer. The first
//! inhabitant is [`record_ops`], which splits a batch carrying the repo's
//! [`_change_type`](crate::metadata_columns::RESERVED_COL_NAME_CHANGE_TYPE)
//! column into separate insert and delete batches: `INSERT`/`UPDATE_AFTER` rows
//! become inserts and `DELETE`/`UPDATE_BEFORE` rows become deletes.
//!
//! [`RecordBatch`]: arrow_array::RecordBatch

// The building blocks here are wired into the DeltaWriter in a later PR (#2218),
// so nothing in non-test crate code calls them yet.
#![allow(dead_code)]

pub mod record_ops;
