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

use futures::StreamExt;
use futures::stream::BoxStream;
use tracing::Instrument;
use tracing::field::Empty;

use crate::delete_file_index::DeleteFileIndex;
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
use crate::traced_stream::TracedStream;
use crate::{Error, ErrorKind, Result};

/// Wraps a [`ManifestFile`] alongside the objects that are needed
/// to process it in a thread-safe manner
pub(crate) struct ManifestFileContext {
    manifest_file: ManifestFile,
    field_ids: Arc<Vec<i32>>,
    bound_predicates: Option<Arc<BoundPredicates>>,
    object_cache: Arc<ObjectCache>,
    snapshot_schema: SchemaRef,
    expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
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
    pub(crate) span: tracing::Span,
}

impl ManifestFileContext {
    /// Consumes this [`ManifestFileContext`], fetching its Manifest from FileIO and then
    /// streaming its constituent [`ManifestEntries`]
    pub(crate) async fn fetch_manifest_and_stream_entries(
        self,
        parent_span: tracing::Span,
    ) -> Result<BoxStream<'static, Result<ManifestEntryContext>>> {
        let manifest_span = tracing::debug_span!(
            parent: &parent_span,
            "iceberg.scan.plan.process_manifest",
            iceberg.scan.plan.manifest.file_path = self.manifest_file.manifest_path,
            iceberg.scan.plan.manifest.entries_count = Empty,
        );

        let span = manifest_span.clone();

        let ManifestFileContext {
            object_cache,
            manifest_file,
            bound_predicates,
            snapshot_schema,
            field_ids,
            expression_evaluator_cache,
            ..
        } = self;

        let (manifest, manifest_file) = async move {
            let manifest = object_cache.get_manifest(&manifest_file).await;
            (manifest, manifest_file)
        }
        .instrument(manifest_span.clone())
        .await;
        let manifest = manifest?;

        span.record(
            "iceberg.scan.plan.manifest.entries_count",
            manifest.entries().len(),
        );

        let stream = async_stream::stream! {
            for manifest_entry in manifest.entries() {
                let manifest_entry_span = tracing::debug_span!(
                    parent: span.clone(),
                    "iceberg.scan.plan.process_data_file",
                    iceberg.scam.plan.data_file.file_path = manifest_entry.file_path(),
                    "iceberg.scan.plan_data_file.type" = Empty,
                    iceberg.scan.plan.data_file.skipped = Empty,
                    iceberg.scan.plan.data_file.skipped_reason = Empty,
                );

                yield Ok(ManifestEntryContext {
                    manifest_entry: manifest_entry.clone(),
                    expression_evaluator_cache: expression_evaluator_cache.clone(),
                    field_ids: field_ids.clone(),
                    partition_spec_id: manifest_file.partition_spec_id,
                    bound_predicates: bound_predicates.clone(),
                    snapshot_schema: snapshot_schema.clone(),
                    span: manifest_entry_span,
                });
            }
        }
        .boxed();

        Ok(Box::pin(TracedStream::new(stream, vec![
            manifest_span.clone(),
        ])))
    }

    pub(crate) fn is_delete(&self) -> bool {
        self.manifest_file.content == ManifestContentType::Deletes
    }
}

impl ManifestEntryContext {
    /// consume this `ManifestEntryContext`, returning a `FileScanTask`
    /// created from it
    pub(crate) fn into_file_scan_task(
        self,
        delete_file_index: Arc<DeleteFileIndex>,
    ) -> Result<FileScanTask> {
        let deletes = delete_file_index.get_deletes_for_data_file(
            self.manifest_entry.data_file(),
            self.manifest_entry.sequence_number(),
        );

        Ok(FileScanTask {
            start: 0,
            length: self.manifest_entry.file_size_in_bytes(),
            record_count: Some(self.manifest_entry.record_count()),

            data_file_path: self.manifest_entry.file_path().to_string(),
            data_file_format: self.manifest_entry.file_format(),

            schema: self.snapshot_schema,
            project_field_ids: self.field_ids.to_vec(),
            predicate: self
                .bound_predicates
                .map(|x| x.as_ref().snapshot_bound_predicate.clone()),

            deletes,
        })
    }
}

/// PlanContext wraps a [`SnapshotRef`] alongside all the other
/// objects that are required to perform a scan file plan.
#[derive(Clone, Debug)]
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
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(iceberg.scan.plan.manifest_list.file_path = ?self.snapshot.manifest_list()),
    )]
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

    #[tracing::instrument(
        skip_all,
        level = "debug",
        name = "iceberg.scan.plan.process_manifest_list",
        fields(
            iceberg.scan.plan.manifest_list.entries_count = manifest_list.entries().len(),
        )
    )]
    pub(crate) fn build_manifest_file_contexts(
        &self,
        manifest_list: Arc<ManifestList>,
    ) -> (
        Vec<Result<ManifestFileContext>>,
        Vec<Result<ManifestFileContext>>,
    ) {
        let has_predicate = self.predicate.is_some();

        (0..manifest_list.entries().len())
            .map(move |i| manifest_list.entries()[i].clone())
            .filter_map(move |manifest_file| {
                // TODO: replace closure when `try_blocks` stabilizes
                (|| {
                    let partition_bound_predicate = if has_predicate {
                        let predicate = self.get_partition_filter(&manifest_file)?;

                        if !self
                            .manifest_evaluator_cache
                            .get(manifest_file.partition_spec_id, predicate.clone())
                            .eval(&manifest_file)?
                        {
                            tracing::debug!(
                                iceberg.scan.plan.manifest.file_path = manifest_file.manifest_path,
                                iceberg.scan.plan.manifest.skip_reason = "partition",
                                "iceberg.scan.plan.manifest_file.skipped"
                            );
                            metrics::counter!("iceberg.scan.plan.manifest_file.skipped", "reason" => "partition").increment(1);
                            return Ok(None); // Skip this file.
                        }
                        Some(predicate)
                    } else {
                        None
                    };

                    metrics::counter!("iceberg.scan.plan.manifest_file.included").increment(1);

                    let context = self
                        .create_manifest_file_context(manifest_file, partition_bound_predicate)?;
                    Ok(Some(context))
                })()
                .transpose()
            })
            .partition(|ctx| ctx.as_ref().map_or(true, |ctx| ctx.is_delete()))
    }

    fn create_manifest_file_context(
        &self,
        manifest_file: ManifestFile,
        partition_filter: Option<Arc<BoundPredicate>>,
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

        Ok(ManifestFileContext {
            manifest_file,
            bound_predicates,
            object_cache: self.object_cache.clone(),
            snapshot_schema: self.snapshot_schema.clone(),
            field_ids: self.field_ids.clone(),
            expression_evaluator_cache: self.expression_evaluator_cache.clone(),
        })
    }
}
