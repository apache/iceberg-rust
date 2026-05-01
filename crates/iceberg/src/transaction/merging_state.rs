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
//! Actions that want Java's `MergingSnapshotProducer` semantics embed a
//! `MergingState` and route their commit body through `filter_manifests` and
//! `merge_manifests`. The merge-related table properties are read once at
//! construction, so a single attempt sequence observes consistent values even
//! if the table's properties were edited mid-flight.
//!
//! Java analog: fills the partial-mixin role that `MergingSnapshotProducer`
//! plays in Java's type hierarchy.

use std::collections::HashSet;
use std::sync::Arc;

use crate::Result;
use crate::spec::{DataFile, ManifestFile, TableProperties};
use crate::table::Table;
use crate::transaction::manifest_filter::ManifestFilterManager;
use crate::transaction::manifest_merge::ManifestMergeManager;
use crate::transaction::snapshot::{ManifestProcess, SnapshotProducer};

pub(crate) struct MergingState {
    data_filter: ManifestFilterManager,
    data_merge: ManifestMergeManager,
    delete_filter: ManifestFilterManager,
    delete_merge: ManifestMergeManager,
}

impl MergingState {
    /// Reads the merge-related table properties once. Defaults are Java-parity.
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
            data_filter: ManifestFilterManager::new(),
            data_merge: ManifestMergeManager::new(
                target_size_bytes,
                min_count_to_merge,
                merge_enabled,
            ),
            delete_filter: ManifestFilterManager::new(),
            delete_merge: ManifestMergeManager::new(
                target_size_bytes,
                min_count_to_merge,
                merge_enabled,
            ),
        }
    }

    pub(crate) fn delete(&self, file: &DataFile) {
        match file.content {
            crate::spec::DataContentType::Data => {
                self.data_filter.delete(file.file_path.clone());
            }
            crate::spec::DataContentType::PositionDeletes
            | crate::spec::DataContentType::EqualityDeletes => {
                self.delete_filter.delete(file.file_path.clone());
            }
        }
    }

    pub(crate) async fn filter_manifests(
        &self,
        producer: &SnapshotProducer<'_>,
        current_manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        let mut data_manifests = Vec::new();
        let mut delete_manifests = Vec::new();

        for m in current_manifests {
            match m.content {
                crate::spec::ManifestContentType::Data => data_manifests.push(m),
                crate::spec::ManifestContentType::Deletes => delete_manifests.push(m),
            }
        }

        let (data_res, delete_res) = futures::join!(
            self.data_filter.filter_manifests(producer, data_manifests),
            self.delete_filter
                .filter_manifests(producer, delete_manifests),
        );

        let mut filtered = data_res?;
        filtered.extend(delete_res?);

        Ok(filtered)
    }

    /// `unmerged[0]` is the "first" manifest this commit produced; the merge
    /// manager exempts the bin containing it when it has fewer than
    /// `min_count_to_merge` siblings.
    pub(crate) async fn merge_manifests(
        &self,
        producer: &SnapshotProducer<'_>,
        unmerged: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        if unmerged.is_empty() {
            return Ok(unmerged);
        }

        let mut data_unmerged = Vec::new();
        let mut delete_unmerged = Vec::new();

        for m in unmerged {
            match m.content {
                crate::spec::ManifestContentType::Data => data_unmerged.push(m),
                crate::spec::ManifestContentType::Deletes => delete_unmerged.push(m),
            }
        }

        let (data_res, delete_res) = futures::join!(
            self.data_merge.merge_manifests(producer, data_unmerged),
            self.delete_merge.merge_manifests(producer, delete_unmerged),
        );

        let mut merged = data_res?;
        merged.extend(delete_res?);

        Ok(merged)
    }

    /// Sum of `manifests-replaced` contributions from the filter pass and the
    /// merge pass. Drives the snapshot summary's `manifests-replaced` key.
    pub(crate) fn replaced_manifests_count(&self) -> u64 {
        self.data_filter.replaced_manifests_count()
            + self.data_merge.replaced_manifests_count()
            + self.delete_filter.replaced_manifests_count()
            + self.delete_merge.replaced_manifests_count()
    }

    /// Best-effort cleanup of any cached residual or merged manifest paths
    /// that aren't in `committed_paths`. Errors are logged, never propagated.
    pub(crate) async fn clean_uncommitted(
        &self,
        file_io: &crate::io::FileIO,
        committed_paths: &HashSet<String>,
    ) {
        futures::join!(
            self.data_filter.clean_uncommitted(file_io, committed_paths),
            self.data_merge.clean_uncommitted(file_io, committed_paths),
            self.delete_filter
                .clean_uncommitted(file_io, committed_paths),
            self.delete_merge
                .clean_uncommitted(file_io, committed_paths),
        );
    }
}

/// `Arc<MergingState>` is the natural shape callers hold; impl-on-Arc lets the
/// action pass it directly to `SnapshotProducer::commit` without a forwarder.
impl ManifestProcess for Arc<MergingState> {
    async fn process_manifests(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        self.merge_manifests(snapshot_produce, manifests).await
    }

    fn replaced_manifests_count(&self) -> u64 {
        MergingState::replaced_manifests_count(self)
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
    use crate::transaction::tests::make_v2_minimal_table;

    #[test]
    fn from_table_constructs_without_panic_when_properties_absent() {
        // `make_v2_minimal_table` carries none of the merge properties; constructor
        // must fall back to defaults rather than failing parse_or_default.
        let table = make_v2_minimal_table();
        let _state = MergingState::from_table(&table);
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
        // Garbage values must not panic; a malformed property setting silently
        // falls back to the Java-parity default.
        assert_eq!(v, 7);
    }
}
