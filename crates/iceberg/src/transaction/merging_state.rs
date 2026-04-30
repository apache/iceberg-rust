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

//! Composition root for the merging snapshot producer.
//!
//! Each action that wants Java's `MergingSnapshotProducer` semantics embeds a
//! `MergingState` and routes its commit body through `filter_manifests` and
//! `merge_manifests`. The two stages share a snapshot-level set of table
//! properties (target manifest size, min count to merge, merge enabled) that
//! are read once at construction so a single commit observes consistent
//! values even if the table's properties were edited in flight.
//!
//! Java analog: this struct fills the role that `MergingSnapshotProducer`
//! plays in the Java type hierarchy as a partial mixin around its concrete
//! actions. Using composition (rather than a trait with default methods) was
//! decided in brainstorm §4.2 / §16.1 — it sidesteps the "no mutable state in
//! a default method" limitation of Rust traits.

#![allow(dead_code)] // Wired up in the rewrite_files.rs commit.

use std::collections::HashSet;

use crate::Result;
use crate::spec::{DataFile, ManifestFile, TableProperties};
use crate::table::Table;
use crate::transaction::manifest_filter::ManifestFilterManager;
use crate::transaction::manifest_merge::ManifestMergeManager;
use crate::transaction::snapshot::SnapshotProducer;

pub(crate) struct MergingState {
    target_size_bytes: u64,
    min_count_to_merge: u32,
    merge_enabled: bool,
    data_filter: ManifestFilterManager,
    data_merge: ManifestMergeManager,
}

impl MergingState {
    /// Read the merge-related table properties once and construct a fresh state
    /// for a single commit attempt sequence (one action invocation, possibly
    /// retried). Defaults match Java parity — see brainstorm §3 / §15 Q1.
    pub(crate) fn from_table(table: &Table) -> Self {
        let props = table.metadata().properties();
        let target_size_bytes = parse_or_default(
            props,
            TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES,
            TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
        );
        let min_count_to_merge = parse_or_default(
            props,
            TableProperties::PROPERTY_COMMIT_MANIFEST_MIN_MERGE_COUNT,
            TableProperties::PROPERTY_COMMIT_MANIFEST_MIN_MERGE_COUNT_DEFAULT,
        );
        let merge_enabled = parse_or_default(
            props,
            TableProperties::PROPERTY_COMMIT_MANIFEST_MERGE_ENABLED,
            TableProperties::PROPERTY_COMMIT_MANIFEST_MERGE_ENABLED_DEFAULT,
        );

        Self {
            target_size_bytes,
            min_count_to_merge,
            merge_enabled,
            data_filter: ManifestFilterManager::new(),
            data_merge: ManifestMergeManager::new(
                target_size_bytes,
                min_count_to_merge,
                merge_enabled,
            ),
        }
    }

    /// Mark a file path for removal during the next filter pass.
    pub(crate) fn delete(&mut self, file: &DataFile) {
        self.data_filter.delete(file);
    }

    /// Run the residual-rewrite pass over `current_manifests`. Returns the new
    /// manifest list with each replaced manifest either dropped (all alive
    /// entries were marked for deletion) or rewritten as a residual containing
    /// only the survivors with their original sequence numbers preserved.
    pub(crate) async fn filter_manifests(
        &mut self,
        producer: &SnapshotProducer<'_>,
        current_manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        self.data_filter
            .filter_manifests(producer, current_manifests)
            .await
    }

    /// Run the bin-pack merge pass over `unmerged`. The first element of
    /// `unmerged` is treated as the "first" manifest (the one this commit just
    /// produced) and exempted from being merged into a small bin.
    pub(crate) async fn merge_manifests(
        &self,
        producer: &SnapshotProducer<'_>,
        unmerged: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        self.data_merge.merge_manifests(producer, unmerged).await
    }

    /// Sum of `manifests-replaced` contributions from the filter pass and the
    /// merge pass. Drives the snapshot summary's `manifests-replaced` key.
    pub(crate) fn replaced_manifests_count(&self) -> u64 {
        self.data_filter.replaced_manifests_count()
            + self.data_merge.replaced_manifests_count()
    }

    /// Best-effort cleanup of any cached residual or merged manifest paths
    /// that aren't in `committed_paths`. Errors are logged, never propagated.
    pub(crate) async fn clean_uncommitted(
        &mut self,
        producer: &SnapshotProducer<'_>,
        committed_paths: &HashSet<String>,
    ) {
        self.data_filter
            .clean_uncommitted(producer, committed_paths)
            .await;
        self.data_merge
            .clean_uncommitted(producer, committed_paths)
            .await;
    }

    /// Whether the merge pass is enabled. Exposed so the action can choose to
    /// skip the merge step (and the cost of `merge_manifests`) when the table
    /// has opted out.
    pub(crate) fn merge_enabled(&self) -> bool {
        self.merge_enabled
    }
}

fn parse_or_default<T: std::str::FromStr>(
    props: &std::collections::HashMap<String, String>,
    key: &str,
    default: T,
) -> T {
    props
        .get(key)
        .and_then(|s| s.parse::<T>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::spec::TableProperties;
    use crate::transaction::tests::make_v2_minimal_table;

    #[test]
    fn defaults_when_properties_absent() {
        let table = make_v2_minimal_table();
        let state = MergingState::from_table(&table);
        assert_eq!(
            state.target_size_bytes,
            TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES_DEFAULT
        );
        assert_eq!(
            state.min_count_to_merge,
            TableProperties::PROPERTY_COMMIT_MANIFEST_MIN_MERGE_COUNT_DEFAULT
        );
        assert!(state.merge_enabled);
    }

    #[test]
    fn parse_or_default_uses_present_value() {
        let mut props = HashMap::new();
        props.insert("k".to_string(), "42".to_string());
        let v: u64 = parse_or_default(&props, "k", 7);
        assert_eq!(v, 42);
        let missing: u64 = parse_or_default(&props, "missing", 7);
        assert_eq!(missing, 7);
    }

    #[test]
    fn parse_or_default_falls_back_on_invalid() {
        let mut props = HashMap::new();
        props.insert("k".to_string(), "not-a-number".to_string());
        let v: u64 = parse_or_default(&props, "k", 7);
        assert_eq!(v, 7, "invalid value falls back to default rather than panicking");
    }
}
