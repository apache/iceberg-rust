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

//! Table maintenance actions — engine-agnostic ports of Java's `org.apache.iceberg.actions`
//! that operate on an already-committed [`Table`](crate::table::Table) rather than producing a
//! new snapshot through the [`transaction`](crate::transaction) seam.
//!
//! Unlike a [`TransactionAction`](crate::transaction::TransactionAction) (which appends/rewrites
//! metadata and commits through the catalog), a maintenance action reads the table's current
//! metadata + physical storage and performs out-of-band work — here, physically deleting files
//! that no live snapshot references.
//!
//! # Contents
//!
//! - [`DeleteOrphanFiles`] — list a location and delete files unreachable from any valid
//!   snapshot (the Rust port of Java's `DeleteOrphanFiles` Spark action, minus the Spark
//!   distribution layer). **This action deletes files.**
//!
//! # Relationship to `transaction::expire_cleanup`
//!
//! [`ExpireSnapshotsCleanup`](crate::transaction::ExpireSnapshotsCleanup) deletes files made
//! unreachable by a *specific* expire-snapshots commit (a `before − after` reachability delta).
//! `DeleteOrphanFiles` is the complementary safety net: it deletes files present in storage that
//! *no* valid snapshot references at all — files leaked by failed writes, interrupted compactions,
//! or non-Iceberg processes. The two derive their valid-file universe differently (see
//! [`DeleteOrphanFiles`] for why this module re-derives the *full* reachable set instead of
//! reusing `expire_cleanup`'s delta machinery).

mod delete_orphan_files;

#[cfg(test)]
mod tests;

pub use delete_orphan_files::{DeleteOrphanFiles, DeleteOrphanFilesResult, PrefixMismatchMode};
