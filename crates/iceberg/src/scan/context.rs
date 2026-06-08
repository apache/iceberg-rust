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

use std::sync::Arc;

use futures::channel::mpsc::Sender;
use futures::{SinkExt, TryFutureExt};

use crate::delete_file_index::DeleteFileIndex;
use crate::expr::visitors::residual_evaluator::ResidualEvaluator;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::io::object_cache::ObjectCache;
use crate::scan::{
    BoundPredicates, ExpressionEvaluatorCache, FileScanTask, ManifestEvaluatorCache,
    PartitionFilterCache,
};
use crate::spec::{
    ManifestContentType, ManifestEntryRef, ManifestFile, ManifestList, SchemaRef, SnapshotRef,
    TableMetadataRef,
};
use crate::{Error, ErrorKind, Result};

/// Wraps a [`ManifestFile`] alongside the objects that are needed
/// to process it in a thread-safe manner
pub(crate) struct ManifestFileContext {
    manifest_file: ManifestFile,

    sender: Sender<ManifestEntryContext>,

    field_ids: Arc<Vec<i32>>,
    bound_predicates: Option<Arc<BoundPredicates>>,
    object_cache: Arc<ObjectCache>,
    snapshot_schema: SchemaRef,
    expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
    delete_file_index: DeleteFileIndex,
    case_sensitive: bool,

    /// The residual evaluator for this manifest's (spec, snapshot filter) pair,
    /// built once per manifest file and shared across its entries. `None` when the
    /// scan has no row filter (every task then carries no per-row predicate).
    residual_evaluator: Option<Arc<ResidualEvaluator>>,
}

/// Wraps a [`ManifestEntryRef`] alongside the objects that are needed
/// to process it in a thread-safe manner
pub(crate) struct ManifestEntryContext {
    pub manifest_entry: ManifestEntryRef,

    pub expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
    pub field_ids: Arc<Vec<i32>>,
    pub bound_predicates: Option<Arc<BoundPredicates>>,
    pub partition_spec_id: i32,
    pub snapshot_schema: SchemaRef,
    pub delete_file_index: DeleteFileIndex,
    pub case_sensitive: bool,

    /// The residual evaluator for the scan's (spec, snapshot filter) pair, shared
    /// across the manifest's entries. `None` when the scan has no row filter.
    pub residual_evaluator: Option<Arc<ResidualEvaluator>>,
}

impl ManifestFileContext {
    /// Consumes this [`ManifestFileContext`], fetching its Manifest from FileIO and then
    /// streaming its constituent [`ManifestEntries`] to the channel provided in the context
    pub(crate) async fn fetch_manifest_and_stream_manifest_entries(self) -> Result<()> {
        let ManifestFileContext {
            object_cache,
            manifest_file,
            bound_predicates,
            snapshot_schema,
            field_ids,
            mut sender,
            expression_evaluator_cache,
            delete_file_index,
            residual_evaluator,
            ..
        } = self;

        let manifest = object_cache.get_manifest(&manifest_file).await?;

        for manifest_entry in manifest.entries() {
            let manifest_entry_context = ManifestEntryContext {
                // TODO: refactor to avoid the expensive ManifestEntry clone
                manifest_entry: manifest_entry.clone(),
                expression_evaluator_cache: expression_evaluator_cache.clone(),
                field_ids: field_ids.clone(),
                partition_spec_id: manifest_file.partition_spec_id,
                bound_predicates: bound_predicates.clone(),
                snapshot_schema: snapshot_schema.clone(),
                delete_file_index: delete_file_index.clone(),
                case_sensitive: self.case_sensitive,
                residual_evaluator: residual_evaluator.clone(),
            };

            sender
                .send(manifest_entry_context)
                .map_err(|_| Error::new(ErrorKind::Unexpected, "mpsc channel SendError"))
                .await?;
        }

        Ok(())
    }
}

impl ManifestEntryContext {
    /// consume this `ManifestEntryContext`, returning a `FileScanTask`
    /// created from it
    pub(crate) async fn into_file_scan_task(self) -> Result<FileScanTask> {
        let deletes = self
            .delete_file_index
            .get_deletes_for_data_file(
                self.manifest_entry.data_file(),
                self.manifest_entry.sequence_number(),
            )
            .await;

        // Compute the PARTITION-REDUCED residual for this file (Java
        // `BaseFileScanTask.residual()` = `residuals.residualFor(file.partition())`).
        //
        // Correctness invariant: every row in a data file belongs to that file's
        // single partition tuple, so the partition-implied conditions the residual
        // drops are TRUE for every row in the file. Filtering rows with the residual
        // therefore yields exactly the same rows as filtering with the full snapshot
        // filter — the reduction is result-preserving, only cheaper. When the scan has
        // no filter (no residual evaluator) the task carries no predicate, and for an
        // unpartitioned spec the residual equals the full filter (behavior unchanged).
        let predicate = self.residual_predicate()?;

        Ok(FileScanTask {
            file_size_in_bytes: self.manifest_entry.file_size_in_bytes(),
            start: 0,
            length: self.manifest_entry.file_size_in_bytes(),
            record_count: Some(self.manifest_entry.record_count()),

            data_file_path: self.manifest_entry.file_path().to_string(),
            data_file_format: self.manifest_entry.file_format(),

            schema: self.snapshot_schema.clone(),
            project_field_ids: self.field_ids.to_vec(),
            predicate,

            deletes,

            // Include partition data and spec from manifest entry
            partition: Some(self.manifest_entry.data_file.partition.clone()),
            // Left `None` deliberately: setting it activates the reader's identity-partition
            // constant-materialization path (Java `PartitionUtil.constantsMap`), whose
            // `record_batch_transformer` has latent type bugs (RunEndEncoded vs the declared
            // column type; no Int->Int64 widening). That path is a separate parity feature,
            // deferred to its own increment; the residual above does not need it.
            partition_spec: None,
            // TODO: Extract name_mapping from table metadata property "schema.name-mapping.default"
            name_mapping: None,
            case_sensitive: self.case_sensitive,
        })
    }

    /// Computes the bound row filter this file's reader should apply: the
    /// partition-reduced residual of the scan's snapshot filter for this file's
    /// partition tuple.
    ///
    /// Returns `None` when the scan has no filter. Otherwise it evaluates the
    /// pre-built [`ResidualEvaluator`] against this file's partition and binds the
    /// resulting (unbound) residual `Predicate` back to the snapshot schema — the
    /// `BoundPredicate` the reader consumes. A residual of `AlwaysTrue` binds to
    /// `BoundPredicate::AlwaysTrue` (the reader applies no per-row filtering); an
    /// `AlwaysFalse` residual binds to `BoundPredicate::AlwaysFalse` (the file
    /// produces no rows).
    fn residual_predicate(&self) -> Result<Option<BoundPredicate>> {
        let Some(residual_evaluator) = self.residual_evaluator.as_ref() else {
            return Ok(None);
        };

        let residual: Predicate =
            residual_evaluator.residual_for(self.manifest_entry.data_file().partition())?;
        let bound = residual.bind(self.snapshot_schema.clone(), self.case_sensitive)?;
        Ok(Some(bound))
    }
}

/// PlanContext wraps a [`SnapshotRef`] alongside all the other
/// objects that are required to perform a scan file plan.
#[derive(Debug)]
pub(crate) struct PlanContext {
    pub snapshot: SnapshotRef,

    pub table_metadata: TableMetadataRef,
    pub snapshot_schema: SchemaRef,
    pub case_sensitive: bool,
    pub predicate: Option<Arc<Predicate>>,
    pub snapshot_bound_predicate: Option<Arc<BoundPredicate>>,
    pub object_cache: Arc<ObjectCache>,
    pub field_ids: Arc<Vec<i32>>,

    pub partition_filter_cache: Arc<PartitionFilterCache>,
    pub manifest_evaluator_cache: Arc<ManifestEvaluatorCache>,
    pub expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
}

impl PlanContext {
    pub(crate) async fn get_manifest_list(&self) -> Result<Arc<ManifestList>> {
        self.object_cache
            .as_ref()
            .get_manifest_list(&self.snapshot, &self.table_metadata)
            .await
    }

    fn get_partition_filter(&self, manifest_file: &ManifestFile) -> Result<Arc<BoundPredicate>> {
        let partition_spec_id = manifest_file.partition_spec_id;

        let partition_filter = self.partition_filter_cache.get(
            partition_spec_id,
            &self.table_metadata,
            &self.snapshot_schema,
            self.case_sensitive,
            self.predicate
                .as_ref()
                .ok_or(Error::new(
                    ErrorKind::Unexpected,
                    "Expected a predicate but none present",
                ))?
                .as_ref()
                .bind(self.snapshot_schema.clone(), self.case_sensitive)?,
        )?;

        Ok(partition_filter)
    }

    pub(crate) fn build_manifest_file_contexts(
        &self,
        manifest_list: Arc<ManifestList>,
        tx_data: Sender<ManifestEntryContext>,
        delete_file_idx: DeleteFileIndex,
        delete_file_tx: Sender<ManifestEntryContext>,
    ) -> Result<Box<impl Iterator<Item = Result<ManifestFileContext>> + 'static>> {
        let mut manifest_files = manifest_list.entries().iter().collect::<Vec<_>>();
        // Sort manifest files to process delete manifests first.
        // This avoids a deadlock where the producer blocks on sending data manifest entries
        // (because the data channel is full) while the delete manifest consumer is waiting
        // for delete manifest entries (which haven't been produced yet).
        // By processing delete manifests first, we ensure the delete consumer can finish,
        // which then allows the data consumer to start draining the data channel.
        manifest_files.sort_by_key(|m| match m.content {
            ManifestContentType::Deletes => 0,
            ManifestContentType::Data => 1,
        });

        // TODO: Ideally we could ditch this intermediate Vec as we return an iterator.
        let mut filtered_mfcs = vec![];
        for manifest_file in manifest_files {
            let tx = if manifest_file.content == ManifestContentType::Deletes {
                delete_file_tx.clone()
            } else {
                tx_data.clone()
            };

            let partition_bound_predicate = if self.predicate.is_some() {
                let partition_bound_predicate = self.get_partition_filter(manifest_file)?;

                // evaluate the ManifestFile against the partition filter. Skip
                // if it cannot contain any matching rows
                if !self
                    .manifest_evaluator_cache
                    .get(
                        manifest_file.partition_spec_id,
                        partition_bound_predicate.clone(),
                    )
                    .eval(manifest_file)?
                {
                    continue;
                }

                Some(partition_bound_predicate)
            } else {
                None
            };

            let mfc = self.create_manifest_file_context(
                manifest_file,
                partition_bound_predicate,
                tx,
                delete_file_idx.clone(),
            )?;

            filtered_mfcs.push(Ok(mfc));
        }

        Ok(Box::new(filtered_mfcs.into_iter()))
    }

    fn create_manifest_file_context(
        &self,
        manifest_file: &ManifestFile,
        partition_filter: Option<Arc<BoundPredicate>>,
        sender: Sender<ManifestEntryContext>,
        delete_file_index: DeleteFileIndex,
    ) -> Result<ManifestFileContext> {
        let bound_predicates =
            if let (Some(ref partition_bound_predicate), Some(snapshot_bound_predicate)) =
                (partition_filter, &self.snapshot_bound_predicate)
            {
                Some(Arc::new(BoundPredicates {
                    partition_bound_predicate: partition_bound_predicate.as_ref().clone(),
                    snapshot_bound_predicate: snapshot_bound_predicate.as_ref().clone(),
                }))
            } else {
                None
            };

        // Resolve the spec this manifest's files were written with (Java
        // `file.spec()`), used to build the residual evaluator below. All files in one
        // manifest share this spec id, so it is resolved once per manifest file.
        let partition_spec = self
            .table_metadata
            .partition_spec_by_id(manifest_file.partition_spec_id)
            .cloned();

        // Build the residual evaluator once per manifest file, sharing it across all
        // entries (the spec + snapshot filter are constant within a manifest). It is
        // only needed when the scan has a row filter — `snapshot_bound_predicate` is
        // `Some` exactly then. When the spec is unpartitioned (or missing) the
        // evaluator returns the filter verbatim, so each task keeps the full filter.
        let residual_evaluator = match (&self.snapshot_bound_predicate, &partition_spec) {
            (Some(snapshot_bound_predicate), Some(spec)) => Some(Arc::new(ResidualEvaluator::of(
                spec.clone(),
                &self.snapshot_schema,
                snapshot_bound_predicate.as_ref().clone(),
                self.case_sensitive,
            )?)),
            (Some(snapshot_bound_predicate), None) => Some(Arc::new(
                ResidualEvaluator::unpartitioned(snapshot_bound_predicate.as_ref().clone()),
            )),
            (None, _) => None,
        };

        Ok(ManifestFileContext {
            manifest_file: manifest_file.clone(),
            bound_predicates,
            sender,
            object_cache: self.object_cache.clone(),
            snapshot_schema: self.snapshot_schema.clone(),
            field_ids: self.field_ids.clone(),
            expression_evaluator_cache: self.expression_evaluator_cache.clone(),
            delete_file_index,
            case_sensitive: self.case_sensitive,
            residual_evaluator,
        })
    }
}
