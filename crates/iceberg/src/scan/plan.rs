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

//! Drives a scan plan: fans manifests out to their entries and turns the
//! surviving entries into [`FileScanTask`]s.

use futures::channel::mpsc::{Sender, channel};
use futures::{SinkExt, StreamExt, TryStreamExt};

use crate::delete_file_index::DeleteFileIndex;
use crate::expr::BoundPredicate;
use crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
use crate::runtime::Runtime;
use crate::scan::context::{ManifestEntryContext, PlanContext};
use crate::scan::{DeleteFileContext, FileScanTask, FileScanTaskStream};
use crate::spec::DataContentType;
use crate::{Error, ErrorKind, Result};

pub(crate) async fn plan_tasks(
    plan_context: &PlanContext,
    runtime: &Runtime,
    concurrency_limit_manifest_files: usize,
    concurrency_limit_manifest_entries: usize,
) -> Result<FileScanTaskStream> {
    // used to stream ManifestEntryContexts between stages of the file plan operation
    let (manifest_entry_data_ctx_tx, manifest_entry_data_ctx_rx) =
        channel(concurrency_limit_manifest_files);
    let (manifest_entry_delete_ctx_tx, manifest_entry_delete_ctx_rx) =
        channel(concurrency_limit_manifest_files);

    // used to stream the results back to the caller
    let (file_scan_task_tx, file_scan_task_rx) = channel(concurrency_limit_manifest_entries);

    let (delete_file_idx, delete_file_tx) = DeleteFileIndex::new(runtime.clone());

    // get the [`ManifestFile`]s from the [`ManifestList`], filtering out any
    // whose partitions cannot match this
    // scan's filter
    let manifest_file_contexts = plan_context
        .build_manifest_file_contexts(
            manifest_entry_data_ctx_tx,
            delete_file_idx.clone(),
            manifest_entry_delete_ctx_tx,
        )
        .await?;

    let mut channel_for_manifest_error = file_scan_task_tx.clone();
    let mut channel_for_data_manifest_entry_error = file_scan_task_tx.clone();
    let mut channel_for_delete_manifest_entry_error = file_scan_task_tx.clone();

    let rt = runtime.clone();

    // Concurrently load all [`Manifest`]s and stream their [`ManifestEntry`]s
    rt.io().spawn(async move {
        let result = futures::stream::iter(manifest_file_contexts)
            .try_for_each_concurrent(concurrency_limit_manifest_files, |ctx| async move {
                ctx.fetch_manifest_and_stream_manifest_entries().await
            })
            .await;

        if let Err(error) = result {
            let _ = channel_for_manifest_error.send(Err(error)).await;
        }
    });

    // Process the delete file [`ManifestEntry`] stream in parallel
    {
        let rt = rt.clone();
        let rt_inner = rt.clone();
        rt.cpu().spawn(async move {
            let result = manifest_entry_delete_ctx_rx
                .map(|me_ctx| Ok((me_ctx, delete_file_tx.clone())))
                .try_for_each_concurrent(
                    concurrency_limit_manifest_entries,
                    |(manifest_entry_context, tx)| {
                        let rt_inner = rt_inner.clone();
                        async move {
                            rt_inner
                                .cpu()
                                .spawn(async move {
                                    process_delete_manifest_entry(manifest_entry_context, tx).await
                                })
                                .await?
                        }
                    },
                )
                .await;

            if let Err(error) = result {
                let _ = channel_for_delete_manifest_entry_error
                    .send(Err(error))
                    .await;
            }
        });
    }

    // Process the data file [`ManifestEntry`] stream in parallel
    {
        let rt_inner = rt.clone();
        rt.cpu().spawn(async move {
            let result = manifest_entry_data_ctx_rx
                .map(|me_ctx| Ok((me_ctx, file_scan_task_tx.clone())))
                .try_for_each_concurrent(
                    concurrency_limit_manifest_entries,
                    |(manifest_entry_context, tx)| {
                        let rt_inner = rt_inner.clone();
                        async move {
                            rt_inner
                                .cpu()
                                .spawn(async move {
                                    process_data_manifest_entry(manifest_entry_context, tx).await
                                })
                                .await?
                        }
                    },
                )
                .await;

            if let Err(error) = result {
                let _ = channel_for_data_manifest_entry_error.send(Err(error)).await;
            }
        });
    }

    Ok(file_scan_task_rx.boxed())
}

async fn process_delete_manifest_entry(
    manifest_entry_context: ManifestEntryContext,
    mut delete_file_ctx_tx: Sender<DeleteFileContext>,
) -> Result<()> {
    // skip processing this manifest entry if it has been marked as deleted
    if !manifest_entry_context.manifest_entry.is_alive() {
        return Ok(());
    }

    // abort the plan if we encounter a manifest entry that is not for a delete file
    if manifest_entry_context.manifest_entry.content_type() == DataContentType::Data {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Encountered an entry for a data file in a delete manifest",
        ));
    }

    if let Some(ref bound_predicates) = manifest_entry_context.bound_predicates {
        let expression_evaluator_cache = manifest_entry_context.expression_evaluator_cache.as_ref();

        let expression_evaluator = expression_evaluator_cache.get(
            manifest_entry_context.partition_spec_id,
            &bound_predicates.partition_bound_predicate,
        )?;

        // skip any data file whose partition data indicates that it can't contain
        // any data that matches this scan's filter
        if !expression_evaluator.eval(manifest_entry_context.manifest_entry.data_file())? {
            return Ok(());
        }
    }

    delete_file_ctx_tx
        .send(DeleteFileContext {
            manifest_entry: manifest_entry_context.manifest_entry.clone(),
            partition_spec_id: manifest_entry_context.partition_spec_id,
        })
        .await?;

    Ok(())
}

async fn process_data_manifest_entry(
    manifest_entry_context: ManifestEntryContext,
    mut file_scan_task_tx: Sender<Result<FileScanTask>>,
) -> Result<()> {
    // skip processing this manifest entry if it has been marked as deleted
    if !manifest_entry_context.manifest_entry.is_alive() {
        return Ok(());
    }

    // abort the plan if we encounter a manifest entry for a delete file
    if manifest_entry_context.manifest_entry.content_type() != DataContentType::Data {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Encountered an entry for a delete file in a data file manifest",
        ));
    }

    if let Some(ref bound_predicates) = manifest_entry_context.bound_predicates {
        let BoundPredicates {
            snapshot_bound_predicate,
            partition_bound_predicate,
        } = bound_predicates.as_ref();

        let expression_evaluator_cache = manifest_entry_context.expression_evaluator_cache.as_ref();

        let expression_evaluator = expression_evaluator_cache.get(
            manifest_entry_context.partition_spec_id,
            partition_bound_predicate,
        )?;

        // skip any data file whose partition data indicates that it can't contain
        // any data that matches this scan's filter
        if !expression_evaluator.eval(manifest_entry_context.manifest_entry.data_file())? {
            return Ok(());
        }

        // skip any data file whose metrics don't match this scan's filter
        if !InclusiveMetricsEvaluator::eval(
            snapshot_bound_predicate,
            manifest_entry_context.manifest_entry.data_file(),
            false,
        )? {
            return Ok(());
        }
    }

    // congratulations! the manifest entry has made its way through the
    // entire plan without getting filtered out. Create a corresponding
    // FileScanTask and push it to the result stream
    file_scan_task_tx
        .send(Ok(manifest_entry_context.into_file_scan_task().await?))
        .await?;

    Ok(())
}

pub(crate) struct BoundPredicates {
    pub(crate) partition_bound_predicate: BoundPredicate,
    pub(crate) snapshot_bound_predicate: BoundPredicate,
}
