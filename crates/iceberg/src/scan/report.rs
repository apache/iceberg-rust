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
use std::time::Instant;

use futures::StreamExt;
use futures::stream::BoxStream;
use tracing::instrument::WithSubscriber;

use super::PlanContext;
use super::metrics::PlanningCounters;
use crate::metadata_columns::{
    RESERVED_COL_NAME_PARTITION, RESERVED_FIELD_ID_PARTITION, get_metadata_field,
};
use crate::metrics::{MetricsReport, MetricsReporter, ScanReport};
use crate::spec::set_iceberg_version;
use crate::{Result, Runtime, TableIdent};

pub(super) fn report_on_completion<T: Send + 'static>(
    tasks: BoxStream<'static, Result<T>>,
    counters: Arc<PlanningCounters>,
    table_ident: TableIdent,
    context: &PlanContext,
    reporter: Arc<dyn MetricsReporter>,
    runtime: Runtime,
    started: Instant,
) -> BoxStream<'static, Result<T>> {
    let snapshot_id = context.snapshot.snapshot_id();
    let schema_id = context.snapshot_schema.schema_id();
    let filter = context.predicate.as_deref().cloned();
    let projected_field_ids = context.field_ids.as_ref().clone();
    let projected_field_names = projected_field_ids
        .iter()
        .map(|id| {
            context
                .snapshot_schema
                .name_by_field_id(*id)
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    if *id == RESERVED_FIELD_ID_PARTITION {
                        RESERVED_COL_NAME_PARTITION.to_owned()
                    } else {
                        get_metadata_field(*id)
                            .expect("validated scan projection")
                            .name
                            .clone()
                    }
                })
        })
        .collect();
    let dispatcher = tracing::dispatcher::get_default(Clone::clone);
    let mut metadata = Default::default();
    set_iceberg_version(&mut metadata);
    metadata.extend(context.report_metadata.clone());
    let reporting = async move {
        let report = MetricsReport::Scan(ScanReport {
            table_ident,
            snapshot_id,
            schema_id,
            filter,
            projected_field_ids,
            projected_field_names,
            metrics: counters.scan_metrics(started.elapsed()),
            metadata,
        });
        drop(
            runtime.io().spawn(
                async move {
                    reporter.report(&report).await;
                }
                .with_subscriber(dispatcher),
            ),
        );
    };
    futures::stream::unfold(
        (tasks, Some(reporting)),
        |(mut tasks, mut reporting)| async move {
            match tasks.next().await {
                Some(task) => {
                    if task.is_err() {
                        reporting = None;
                    }
                    Some((task, (tasks, reporting)))
                }
                None => {
                    if let Some(reporting) = reporting {
                        reporting.await;
                    }
                    None
                }
            }
        },
    )
    .fuse()
    .boxed()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use futures::TryStreamExt;
    use tokio::sync::{Notify, mpsc};
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;
    use crate::expr::Reference;
    use crate::metadata_columns::{RESERVED_COL_NAME_FILE, RESERVED_FIELD_ID_FILE};
    use crate::metrics::ScanMetrics;
    use crate::scan::{FileScanTask, FileScanTaskStream, TableScan};
    use crate::spec::{Datum, ManifestListWriter};
    use crate::test_utils::scan::TableTestFixture;

    #[derive(Debug)]
    struct RecordingReporter(mpsc::UnboundedSender<ScanReport>);

    #[async_trait::async_trait]
    impl MetricsReporter for RecordingReporter {
        async fn report(&self, report: &MetricsReport) {
            let MetricsReport::Scan(report) = report;
            self.0.send(report.clone()).unwrap();
        }
    }

    fn recording_reporter() -> (Arc<RecordingReporter>, mpsc::UnboundedReceiver<ScanReport>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Arc::new(RecordingReporter(tx)), rx)
    }

    async fn next_report(rx: &mut mpsc::UnboundedReceiver<ScanReport>) -> Option<ScanReport> {
        tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("report delivery")
    }

    async fn plan(scan: &TableScan, cow: bool) -> Result<FileScanTaskStream> {
        if cow {
            Ok(scan
                .plan_cow_rewrite_files()
                .await?
                .map_ok(|file| file.scan_task)
                .boxed())
        } else {
            scan.plan_files().await
        }
    }

    async fn consume(scan: &TableScan, cow: bool) -> Vec<FileScanTask> {
        let mut stream = plan(scan, cow).await.unwrap();
        let tasks = tokio::time::timeout(Duration::from_secs(10), stream.by_ref().try_collect())
            .await
            .unwrap()
            .unwrap();
        assert!(stream.next().await.is_none());
        tasks
    }

    #[tokio::test]
    async fn test_completed_reports_are_operation_scoped() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_promoted_partition_with_dv().await;
        let filter = Reference::new("x").less_than(Datum::long(200));
        for (cow, custom_metadata) in [(false, false), (false, true), (true, false), (true, true)] {
            let (reporter, mut reports) = recording_reporter();
            let table = crate::table::Table::builder()
                .identifier(fixture.table.identifier().clone())
                .metadata(fixture.table.metadata_ref())
                .file_io(fixture.table.file_io().clone())
                .runtime(fixture.table.runtime().clone())
                .metrics_reporter(reporter)
                .build()
                .unwrap();
            let mut builder = table
                .scan()
                .with_case_sensitive(false)
                .with_filter(filter.clone())
                .select(["X", RESERVED_COL_NAME_FILE, RESERVED_COL_NAME_PARTITION]);
            if custom_metadata {
                builder = builder
                    .with_report_metadata("query-id".to_owned(), "first".to_owned())
                    .with_report_metadata("query-id", "last")
                    .with_report_metadata("engine-name", "my-engine")
                    .with_report_metadata("iceberg-version", "custom-version");
            }
            let scan = builder.build().unwrap();
            let (left, right) = tokio::join!(consume(&scan, cow), consume(&scan, cow));
            assert_eq!((left.len(), right.len()), (1, 1));
            assert_eq!(consume(&scan, cow).await.len(), 1);
            let mut expected = ScanReport {
                table_ident: table.identifier().clone(),
                snapshot_id: scan.snapshot().unwrap().snapshot_id(),
                schema_id: table.metadata().current_schema().schema_id(),
                filter: Some(filter.clone()),
                projected_field_ids: vec![1, RESERVED_FIELD_ID_FILE, RESERVED_FIELD_ID_PARTITION],
                projected_field_names: vec![
                    "x".into(),
                    RESERVED_COL_NAME_FILE.into(),
                    RESERVED_COL_NAME_PARTITION.into(),
                ],
                metadata: HashMap::from([(
                    "iceberg-version".to_owned(),
                    concat!("Apache Iceberg Rust ", env!("CARGO_PKG_VERSION")).to_owned(),
                )]),
                metrics: ScanMetrics {
                    total_planning_duration: Duration::ZERO,
                    total_data_manifests: 1,
                    total_delete_manifests: 1,
                    skipped_data_manifests: 0,
                    skipped_delete_manifests: 0,
                    scanned_data_manifests: 1,
                    scanned_delete_manifests: 1,
                    result_data_files: 1,
                    skipped_data_files: 0,
                    total_file_size_in_bytes: 100,
                    result_delete_files: 1,
                    skipped_delete_files: 0,
                    total_delete_file_size_in_bytes: 40,
                    indexed_delete_files: 1,
                    equality_delete_files: 0,
                    positional_delete_files: 0,
                    dvs: 1,
                },
            };
            if custom_metadata {
                expected.metadata.extend([
                    ("query-id".to_owned(), "last".to_owned()),
                    ("engine-name".to_owned(), "my-engine".to_owned()),
                    ("iceberg-version".to_owned(), "custom-version".to_owned()),
                ]);
            }
            for _ in 0..3 {
                let mut actual = next_report(&mut reports).await.unwrap();
                actual.metrics.total_planning_duration = Duration::ZERO;
                assert_eq!(actual, expected);
            }
            drop(scan);
            drop(table);
            assert!(next_report(&mut reports).await.is_none());
        }
    }

    #[tokio::test]
    async fn test_empty_snapshot_reports_use_the_selected_schema() {
        let fixture = TableTestFixture::new_with_column_added_after_last_write();
        let snapshot = fixture.table.metadata().current_snapshot().unwrap();
        let output = fixture
            .table
            .file_io()
            .new_output(snapshot.manifest_list())
            .unwrap()
            .writer()
            .await
            .unwrap();
        ManifestListWriter::v2(
            output,
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
        )
        .close()
        .await
        .unwrap();
        for cow in [false, true] {
            for time_travel in [false, true] {
                let (reporter, mut reports) = recording_reporter();
                let table = fixture.table.clone().with_metrics_reporter(reporter);
                let mut builder = table.scan();
                let schema = if time_travel {
                    builder = builder.snapshot_id(snapshot.snapshot_id());
                    snapshot.schema(table.metadata()).unwrap()
                } else {
                    table.metadata().current_schema().clone()
                };
                let scan = builder.build().unwrap();
                assert!(consume(&scan, cow).await.is_empty());
                let report = next_report(&mut reports).await.unwrap();
                let expected = (schema.schema_id(), !time_travel, !time_travel, 0, 0);
                assert_eq!(
                    (
                        report.schema_id,
                        report.projected_field_ids.contains(&100),
                        report
                            .projected_field_names
                            .iter()
                            .any(|name| name == "added"),
                        report.metrics.total_data_manifests,
                        report.metrics.result_data_files
                    ),
                    expected
                );
            }
        }
    }

    #[tokio::test]
    async fn test_stream_errors_preserve_remaining_items_without_reporting() {
        let fixture = TableTestFixture::new();
        let scan = fixture.table.scan().build().unwrap();
        let (reporter, mut reports) = recording_reporter();
        let tasks = futures::stream::iter([
            Ok(1),
            Err(crate::Error::new(crate::ErrorKind::DataInvalid, "injected")),
            Ok(2),
        ])
        .boxed();
        let stream = report_on_completion(
            tasks,
            Arc::default(),
            scan.table_ident.clone(),
            scan.plan_context.as_ref().unwrap(),
            reporter,
            scan.runtime.clone(),
            Instant::now(),
        );
        let actual: Vec<_> = stream
            .map(|result| result.map_err(|error| error.kind()))
            .collect()
            .await;
        assert_eq!(actual, [Ok(1), Err(crate::ErrorKind::DataInvalid), Ok(2)]);
        assert!(next_report(&mut reports).await.is_none());
    }

    #[derive(Debug, Default)]
    struct ReportScope;
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ReportScope {}

    #[derive(Debug)]
    struct BlockingReporter {
        recording: RecordingReporter,
        started: Notify,
        released: Notify,
    }

    #[async_trait::async_trait]
    impl MetricsReporter for BlockingReporter {
        async fn report(&self, report: &MetricsReport) {
            assert!(tracing::dispatcher::get_default(|dispatcher| dispatcher
                .downcast_ref::<ReportScope>()
                .is_some()));
            self.started.notify_one();
            self.released.notified().await;
            self.recording.report(report).await;
            panic!("injected reporter panic");
        }
    }

    #[tokio::test]
    async fn test_report_override_is_detached_and_preserves_tracing_context() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;
        let (inherited, mut inherited_reports) = recording_reporter();
        let (tx, mut reports) = mpsc::unbounded_channel();
        let reporter = Arc::new(BlockingReporter {
            recording: RecordingReporter(tx),
            started: Notify::new(),
            released: Notify::new(),
        });
        let table = fixture.table.clone().with_metrics_reporter(inherited);
        let scan = table
            .scan()
            .with_metrics_reporter(reporter.clone())
            .build()
            .unwrap();
        let subscriber = tracing_subscriber::registry().with(ReportScope);
        let consumed = async {
            let mut stream = scan.plan_files().await.unwrap();
            let tasks: Vec<_> = stream.by_ref().take(2).try_collect().await.unwrap();
            let before_pause = Instant::now();
            tokio::time::sleep(Duration::from_millis(10)).await;
            let elapsed = before_pause.elapsed();
            assert!(reports.try_recv().is_err());
            assert!(stream.next().await.is_none());
            assert!(stream.next().await.is_none());
            assert_eq!(tasks.len(), 2);
            elapsed
        }
        .with_subscriber(subscriber);
        let elapsed = tokio::time::timeout(Duration::from_secs(10), consumed)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), reporter.started.notified())
            .await
            .unwrap();
        assert!(reports.try_recv().is_err());
        reporter.released.notify_one();
        assert!(
            next_report(&mut reports)
                .await
                .unwrap()
                .metrics()
                .total_planning_duration()
                >= elapsed
        );
        drop(scan);
        drop(table);
        assert!(next_report(&mut inherited_reports).await.is_none());
    }

    #[tokio::test]
    async fn test_incomplete_and_snapshotless_scans_do_not_report() {
        for cow in [false, true] {
            for termination in [
                "snapshotless",
                "setup-error",
                "stream-error",
                "drop-empty",
                "drop-partial",
            ] {
                let mut fixture = if termination == "snapshotless" {
                    TableTestFixture::new_empty()
                } else {
                    TableTestFixture::new()
                };
                if !matches!(termination, "snapshotless" | "setup-error") {
                    fixture.setup_manifest_files().await;
                }
                let (reporter, mut reports) = recording_reporter();
                let scan = fixture
                    .table
                    .clone()
                    .with_metrics_reporter(reporter)
                    .scan()
                    .build()
                    .unwrap();
                if termination == "stream-error" {
                    let list = scan
                        .plan_context
                        .as_ref()
                        .unwrap()
                        .get_manifest_list()
                        .await
                        .unwrap();
                    fixture
                        .table
                        .file_io()
                        .delete(&list.entries()[0].manifest_path)
                        .await
                        .unwrap();
                }
                let planning = tokio::time::timeout(Duration::from_secs(10), plan(&scan, cow))
                    .await
                    .unwrap();
                if termination == "setup-error" {
                    assert!(planning.is_err());
                } else {
                    let mut stream = planning.unwrap();
                    match termination {
                        "snapshotless" => assert!(stream.next().await.is_none()),
                        "stream-error" => {
                            assert!(
                                tokio::time::timeout(Duration::from_secs(10), stream.next())
                                    .await
                                    .unwrap()
                                    .unwrap()
                                    .is_err()
                            );
                            assert!(stream.next().await.is_none());
                        }
                        "drop-partial" => {
                            stream.next().await.unwrap().unwrap();
                        }
                        _ => {}
                    }
                    drop(stream);
                }
                drop(scan);
                assert!(
                    next_report(&mut reports).await.is_none(),
                    "{termination}, cow={cow}"
                );
            }
        }
    }
}
