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

//! Each invocation finalizes after its consumer terminates and all contributing futures stop.
//! Reports include partial and failed planning; they do not certify successful or exhaustive planning.
//! Duration includes consumption and worker teardown, excluding reporter delivery. Empty snapshots
//! report, including COW rewrite planning; tables without snapshots do not.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use tracing::instrument::WithSubscriber;

use super::PlanContext;
use crate::expr::{BoundPredicate, Predicate};
use crate::metrics::{MetricsReport, MetricsReporter};
pub(super) use crate::metrics::{ScanMetrics as Metrics, ScanReport};
use crate::spec::{DEFAULT_SCHEMA_ID, NestedField, StructType, Type};
use crate::{Result, Runtime, TableIdent};

impl ScanReport {
    pub(super) fn new(table_ident: TableIdent, context: &PlanContext) -> Self {
        let projected_field_ids = projected_field_ids(context);
        let projected_field_names = projected_field_ids
            .iter()
            .map(|id| {
                let name = context
                    .snapshot_schema
                    .name_by_field_id(*id)
                    .map(str::to_owned)
                    .or_else(|| {
                        crate::metadata_columns::get_metadata_field(*id)
                            .ok()
                            .map(|f| f.name.clone())
                    })
                    .or_else(|| {
                        (*id == crate::metadata_columns::RESERVED_FIELD_ID_PARTITION).then(|| {
                            crate::metadata_columns::RESERVED_COL_NAME_PARTITION.to_owned()
                        })
                    });
                debug_assert!(name.is_some(), "Unresolved scan report field ID {id}");
                name.unwrap_or_else(|| id.to_string())
            })
            .collect();
        Self {
            table_ident,
            snapshot_id: context.snapshot.snapshot_id(),
            schema_id: projected_schema_id(context, &projected_field_ids),
            projected_field_ids,
            projected_field_names,
            filter: context
                .snapshot_bound_predicate
                .as_deref()
                .map(crate::metrics::filter::unbind)
                .unwrap_or_else(|| {
                    context
                        .predicate
                        .as_deref()
                        .cloned()
                        .unwrap_or(Predicate::AlwaysTrue)
                }),
            metrics: Metrics::empty(),
            total_planning_duration: Duration::ZERO,
        }
    }

    pub(super) fn finish(mut self, counters: &Counters, elapsed: Duration) -> Self {
        self.metrics = self.metrics.finish(counters);
        self.filter =
            crate::metrics::filter::sanitize(&self.filter, chrono::Utc::now().timestamp_micros());
        self.total_planning_duration = elapsed;
        self
    }
}

impl Metrics {
    pub(super) fn finish(mut self, counters: &Counters) -> Self {
        self.scanned_data_manifests = counters.scanned_data_manifests.load(Ordering::Relaxed);
        self.scanned_delete_manifests = counters.scanned_delete_manifests.load(Ordering::Relaxed);
        self.result_data_files = counters.result_data_files.load(Ordering::Relaxed);
        self.result_delete_files = counters.result_delete_files.load(Ordering::Relaxed);
        self.skipped_data_files = counters.skipped_data_files.load(Ordering::Relaxed);
        self.skipped_delete_files = counters.skipped_delete_files.load(Ordering::Relaxed);
        self.indexed_delete_files = counters.indexed_delete_files.load(Ordering::Relaxed);
        self.equality_delete_files = counters.equality_delete_files.load(Ordering::Relaxed);
        self.positional_delete_files = counters.positional_delete_files.load(Ordering::Relaxed);
        self.dvs = counters.dvs.load(Ordering::Relaxed);
        self.total_file_size_in_bytes = counters.total_file_size_in_bytes.load(Ordering::Relaxed);
        self.total_delete_file_size_in_bytes = counters
            .total_delete_file_size_in_bytes
            .load(Ordering::Relaxed);
        self
    }
}

/// Keeps reporting state alive until the stream and all planning workers stop.
pub(super) struct ScanReportState {
    pub(super) counters: Arc<Counters>,
    report: Option<ScanReport>,
    reporter: Arc<dyn MetricsReporter>,
    runtime: Runtime,
    dispatcher: tracing::Dispatch,
    started: Instant,
}

impl ScanReportState {
    pub(super) fn new(
        table_ident: TableIdent,
        context: &PlanContext,
        reporter: Arc<dyn MetricsReporter>,
        runtime: Runtime,
    ) -> Self {
        let started = Instant::now();
        Self {
            counters: Arc::new(Counters::default()),
            report: Some(ScanReport::new(table_ident, context)),
            reporter,
            runtime,
            dispatcher: tracing::dispatcher::get_default(Clone::clone),
            started,
        }
    }

    pub(super) fn metrics_mut(&mut self) -> &mut Metrics {
        &mut self.report.as_mut().unwrap().metrics
    }

    pub(super) fn report_on_termination<T: Send + 'static>(
        self: Arc<Self>,
        tasks: BoxStream<'static, Result<T>>,
    ) -> BoxStream<'static, Result<T>> {
        futures::stream::try_unfold((tasks, self), |(mut tasks, reporting)| async move {
            match tasks.next().await {
                Some(task) => task.map(|task| Some((task, (tasks, reporting)))),
                None => Ok(None),
            }
        })
        .boxed()
    }
}

impl Drop for ScanReportState {
    fn drop(&mut self) {
        // Each spawned future, including nested entry workers and the delete index,
        // holds a guard. The last Arc release synchronizes their termination before
        // reading counters; dropping a join handle alone would not provide this barrier.
        let report = self.report.take().unwrap();
        let report = report.finish(&self.counters, self.started.elapsed());
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
            let reporter = self.reporter.clone();
            // Delivery outlives a dropped consumer; no planning worker waits on it.
            let delivery = async move {
                if let Err(payload) = AssertUnwindSafe(async move {
                    reporter.report(MetricsReport::Scan(Box::new(report))).await;
                })
                .catch_unwind()
                .await
                {
                    // A panic payload's destructor can itself panic.
                    std::mem::forget(payload);
                }
            }
            .with_subscriber(self.dispatcher.clone());
            drop(self.runtime.io().spawn(delivery));
        })) {
            // Delivery may run during unwinding. Even a panic payload's destructor can panic.
            std::mem::forget(payload);
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct Counters {
    pub(super) scanned_data_manifests: AtomicU64,
    pub(super) scanned_delete_manifests: AtomicU64,
    pub(super) result_data_files: AtomicU64,
    pub(super) result_delete_files: AtomicU64,
    pub(super) skipped_data_files: AtomicU64,
    pub(super) skipped_delete_files: AtomicU64,
    pub(super) indexed_delete_files: AtomicU64,
    pub(super) equality_delete_files: AtomicU64,
    pub(super) positional_delete_files: AtomicU64,
    pub(super) dvs: AtomicU64,
    pub(super) total_file_size_in_bytes: AtomicU64,
    pub(super) total_delete_file_size_in_bytes: AtomicU64,
}

pub(super) async fn track<T>(
    reporting: Arc<ScanReportState>,
    future: impl Future<Output = T>,
) -> T {
    let result = future.await;
    drop(reporting);
    result
}

fn projected_schema_id(context: &PlanContext, projected_ids: &[i32]) -> i32 {
    let schema = &context.snapshot_schema;
    // Java TypeUtil.project retains the source ID only when the whole schema survives projection.
    if projected_ids
        .iter()
        .all(|id| schema.field_by_id(*id).is_some())
        && schema.as_struct().fields().iter().all(|field| {
            context.field_ids.contains(&field.id) || field_is_projected(field, projected_ids)
        })
    {
        schema.schema_id()
    } else {
        DEFAULT_SCHEMA_ID
    }
}

fn field_is_projected(field: &NestedField, projected_ids: &[i32]) -> bool {
    projected_ids.contains(&field.id)
        && match field.field_type.as_ref() {
            Type::Struct(nested) => nested
                .fields()
                .iter()
                .all(|field| field_is_projected(field, projected_ids)),
            _ => true,
        }
}

fn projected_field_ids(context: &PlanContext) -> Vec<i32> {
    let mut ids = Vec::new();
    for id in context.field_ids.iter() {
        if let Some(field) = context.snapshot_schema.field_by_id(*id) {
            if !matches!(*field.field_type, Type::List(_) | Type::Map(_)) {
                ids.push(*id);
            }
            collect_projected_ids(&field.field_type, &mut ids);
        }
    }
    if let Some(predicate) = &context.snapshot_bound_predicate {
        let mut filter_ids = Vec::new();
        collect_filter_ids(predicate, &mut filter_ids);
        for id in filter_ids {
            if !collect_filter_path(context.snapshot_schema.as_struct(), id, &mut ids) {
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();
    ids.dedup();
    for id in context.field_ids.iter() {
        if context.snapshot_schema.field_by_id(*id).is_none() && !ids.contains(id) {
            ids.push(*id);
        }
    }
    ids
}

fn collect_filter_path(struct_type: &StructType, id: i32, ids: &mut Vec<i32>) -> bool {
    for field in struct_type.fields() {
        if field.id == id {
            if matches!(*field.field_type, Type::List(_) | Type::Map(_)) {
                collect_projected_ids(&field.field_type, ids);
            } else {
                ids.push(id);
            }
            return true;
        }
        if let Type::Struct(nested) = field.field_type.as_ref()
            && collect_filter_path(nested, id, ids)
        {
            ids.push(field.id);
            return true;
        }
    }
    false
}

fn collect_filter_ids(predicate: &BoundPredicate, ids: &mut Vec<i32>) {
    match predicate {
        BoundPredicate::AlwaysTrue | BoundPredicate::AlwaysFalse => {}
        BoundPredicate::And(expr) | BoundPredicate::Or(expr) => {
            for input in expr.inputs() {
                collect_filter_ids(input, ids);
            }
        }
        BoundPredicate::Not(expr) => collect_filter_ids(expr.inputs()[0], ids),
        BoundPredicate::Unary(expr) => ids.push(expr.term().field().id),
        BoundPredicate::Binary(expr) => ids.push(expr.term().field().id),
        BoundPredicate::Set(expr) => ids.push(expr.term().field().id),
    }
}

fn collect_projected_ids(ty: &Type, ids: &mut Vec<i32>) {
    // Mirrors Java TypeUtil.getProjectedIds, including its list/map container rules.
    match ty {
        Type::Struct(struct_type) => {
            for field in struct_type.fields() {
                if !matches!(*field.field_type, Type::List(_) | Type::Map(_)) {
                    ids.push(field.id);
                }
                collect_projected_ids(&field.field_type, ids);
            }
        }
        Type::List(list) => {
            collect_projected_ids(&list.element_field.field_type, ids);
            if matches!(
                *list.element_field.field_type,
                Type::Primitive(_) | Type::Variant(_)
            ) {
                ids.push(list.element_field.id);
            }
        }
        Type::Map(map) => {
            collect_projected_ids(&map.key_field.field_type, ids);
            collect_projected_ids(&map.value_field.field_type, ids);
            if matches!(
                *map.value_field.field_type,
                Type::Primitive(_) | Type::Variant(_)
            ) {
                ids.extend([map.key_field.id, map.value_field.id]);
            }
        }
        Type::Primitive(_) | Type::Variant(_) => {}
    }
}
