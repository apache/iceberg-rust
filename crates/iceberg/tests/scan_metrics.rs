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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use futures::TryStreamExt;
use iceberg::expr::{Predicate, Reference};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::metrics::{MetricsReport, MetricsReporter, ScanReport};
use iceberg::spec::{DataFileFormat, Datum, NestedField, PrimitiveType, Schema};
use iceberg::table::Table;
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, MemoryCatalog, NamespaceIdent, TableCreation, TableIdent};
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;
use tokio::sync::Notify;

#[derive(Debug, Default)]
struct RetainingReporter {
    reports: Mutex<Vec<MetricsReport>>,
    delivered: Notify,
}

#[async_trait::async_trait]
impl MetricsReporter for RetainingReporter {
    async fn report(&self, report: MetricsReport) {
        self.reports.lock().unwrap().push(report);
        self.delivered.notify_one();
    }
}

impl RetainingReporter {
    async fn take_report(&self) -> Box<ScanReport> {
        tokio::time::timeout(Duration::from_secs(10), self.delivered.notified())
            .await
            .expect("report delivery");
        let reports = std::mem::take(&mut *self.reports.lock().unwrap());
        let [MetricsReport::Scan(report)] = <[_; 1]>::try_from(reports).unwrap() else {
            panic!("expected one scan report");
        };
        report
    }
}

async fn physical_table() -> (TempDir, MemoryCatalog, Table, u64) {
    let dir = TempDir::new().unwrap();
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "metrics",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_owned(),
                dir.path().to_str().unwrap().to_owned(),
            )]),
        )
        .await
        .unwrap();
    let namespace = NamespaceIdent::new("metrics".to_owned());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", PrimitiveType::Int).into(),
            NestedField::required(2, "customer", PrimitiveType::String).into(),
            NestedField::optional(3, "event_time", PrimitiveType::Timestamp).into(),
        ])
        .build()
        .unwrap();
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("events".to_owned())
                .schema(schema)
                .build(),
        )
        .await
        .unwrap();
    let arrow_schema = Arc::new(
        arrow_schema::Schema::try_from(table.metadata().current_schema().as_ref()).unwrap(),
    );
    let writer = DataFileWriterBuilder::new(RollingFileWriterBuilder::new_with_default_file_size(
        ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        ),
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata()).unwrap(),
        DefaultFileNameGenerator::new("metrics".to_owned(), None, DataFileFormat::Parquet),
    ));
    let mut files = Vec::new();
    for (ids, customers, times) in [
        (
            vec![11, 22, 33],
            vec!["secret-customer-id", "other", "secret-customer-id"],
            vec![0, 1, 0],
        ),
        (vec![44], vec!["other"], vec![2]),
    ] {
        let mut file_writer = writer.build(None).await.unwrap();
        file_writer
            .write(
                RecordBatch::try_new(arrow_schema.clone(), vec![
                    Arc::new(Int32Array::from(ids)),
                    Arc::new(StringArray::from(customers)),
                    Arc::new(TimestampMicrosecondArray::from(times)),
                ])
                .unwrap(),
            )
            .await
            .unwrap();
        files.extend(file_writer.close().await.unwrap());
    }
    assert_eq!(files.len(), 2);
    let selected_file_bytes = files[0].file_size_in_bytes();
    let tx = Transaction::new(&table);
    let table = tx
        .fast_append()
        .add_data_files(files)
        .apply(tx)
        .unwrap()
        .commit(&catalog)
        .await
        .unwrap();
    (dir, catalog, table, selected_file_bytes)
}

fn assert_report(
    report: &ScanReport,
    ident: &TableIdent,
    snapshot_id: i64,
    filter: Predicate,
    fields: [(i32, &str); 2],
    file_bytes: u64,
) {
    assert_eq!(
        (
            report.table_ident(),
            report.snapshot_id(),
            report.schema_id(),
            report.projected_field_ids(),
            report.projected_field_names(),
            report.filter()
        ),
        (
            ident,
            snapshot_id,
            0,
            fields.map(|(id, _)| id).as_slice(),
            fields.map(|(_, name)| name.to_owned()).as_slice(),
            &filter
        ),
    );
    let metrics = report.metrics();
    assert_eq!(
        [
            ("total_data_manifests", metrics.total_data_manifests()),
            ("total_delete_manifests", metrics.total_delete_manifests()),
            ("scanned_data_manifests", metrics.scanned_data_manifests()),
            (
                "scanned_delete_manifests",
                metrics.scanned_delete_manifests()
            ),
            ("skipped_data_manifests", metrics.skipped_data_manifests()),
            (
                "skipped_delete_manifests",
                metrics.skipped_delete_manifests()
            ),
            ("result_data_files", metrics.result_data_files()),
            ("result_delete_files", metrics.result_delete_files()),
            ("skipped_data_files", metrics.skipped_data_files()),
            ("skipped_delete_files", metrics.skipped_delete_files()),
            ("indexed_delete_files", metrics.indexed_delete_files()),
            ("equality_delete_files", metrics.equality_delete_files()),
            ("positional_delete_files", metrics.positional_delete_files()),
            ("dvs", metrics.dvs()),
            (
                "total_file_size_in_bytes",
                metrics.total_file_size_in_bytes()
            ),
            (
                "total_delete_file_size_in_bytes",
                metrics.total_delete_file_size_in_bytes()
            ),
        ],
        [
            ("total_data_manifests", 1),
            ("total_delete_manifests", 0),
            ("scanned_data_manifests", 1),
            ("scanned_delete_manifests", 0),
            ("skipped_data_manifests", 0),
            ("skipped_delete_manifests", 0),
            ("result_data_files", 1),
            ("result_delete_files", 0),
            ("skipped_data_files", 1),
            ("skipped_delete_files", 0),
            ("indexed_delete_files", 0),
            ("equality_delete_files", 0),
            ("positional_delete_files", 0),
            ("dvs", 0),
            ("total_file_size_in_bytes", file_bytes),
            ("total_delete_file_size_in_bytes", 0),
        ]
    );
}

#[tokio::test]
async fn test_public_report_sanitization_preserves_row_filtering_and_ownership() {
    for scan_override in [false, true] {
        let (_dir, catalog, table, file_bytes) = physical_table().await;
        let ident = table.identifier().clone();
        let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();
        let reporter = Arc::new(RetainingReporter::default());
        let table = if scan_override {
            table
        } else {
            table.with_metrics_reporter(reporter.clone())
        };
        let builder = table
            .scan()
            .select(["id"])
            .with_filter(Reference::new("customer").equal_to(Datum::string("secret-customer-id")));
        let builder = if scan_override {
            builder.with_metrics_reporter(reporter.clone())
        } else {
            builder
        };
        let scan = builder.build().unwrap();
        let batches: Vec<RecordBatch> = scan.to_arrow().await.unwrap().try_collect().await.unwrap();
        let mut ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                assert_eq!(batch.num_columns(), 1);
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![11, 33]);
        drop(scan);
        drop(table);
        drop(catalog);
        let report = reporter.take_report().await;
        assert_report(
            &report,
            &ident,
            snapshot_id,
            Reference::new("customer").equal_to(Datum::string("(hash-759e890b)")),
            [(1, "id"), (2, "customer")],
            file_bytes,
        );
        assert!(!format!("{report:?}").contains("secret-customer-id"));
    }
}

#[tokio::test]
async fn test_report_binds_numeric_timestamp_to_selected_snapshot_schema() {
    let (_dir, catalog, table, file_bytes) = physical_table().await;
    let ident = table.identifier().clone();
    let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();
    let tx = Transaction::new(&table);
    let table = tx
        .update_schema()
        .delete_column("event_time")
        .apply(tx)
        .unwrap()
        .commit(&catalog)
        .await
        .unwrap();
    let tx = Transaction::new(&table);
    let table = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "event_time",
            PrimitiveType::Long.into(),
        ))
        .apply(tx)
        .unwrap()
        .commit(&catalog)
        .await
        .unwrap();
    assert_eq!(
        table
            .metadata()
            .current_schema()
            .field_by_name("event_time")
            .unwrap()
            .field_type
            .as_ref(),
        &PrimitiveType::Long.into()
    );
    let reporter = Arc::new(RetainingReporter::default());
    let scan = table
        .scan()
        .snapshot_id(snapshot_id)
        .select(["id"])
        .with_metrics_reporter(reporter.clone())
        .with_filter(Reference::new("event_time").equal_to(Datum::long(0)))
        .build()
        .unwrap();
    scan.plan_files()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    drop(scan);
    drop(table);
    drop(catalog);
    let report = reporter.take_report().await;
    assert_report(
        &report,
        &ident,
        snapshot_id,
        Reference::new("event_time").equal_to(Datum::string("(timestamp)")),
        [(1, "id"), (3, "event_time")],
        file_bytes,
    );
}
