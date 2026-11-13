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
use std::time::Duration;

use futures::TryStreamExt;
use iceberg::io::FileIO;
use iceberg::metrics::{MetricsReport, MetricsReporter};
use iceberg::table::StaticTable;
use iceberg::{Error, ErrorKind, Result, TableIdent};
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Default)]
struct InMemoryReporter {
    reports: Mutex<Vec<MetricsReport>>,
    reported: Notify,
}

#[async_trait::async_trait]
impl MetricsReporter for InMemoryReporter {
    async fn report(&self, report: MetricsReport) {
        self.reports.lock().await.push(report);
        self.reported.notify_one();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let metadata = std::env::args().nth(1).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "usage: scan-metrics <local metadata.json>",
        )
    })?;
    let reporter = Arc::new(InMemoryReporter::default());
    let table = StaticTable::from_metadata_file(
        &metadata,
        TableIdent::from_strs(["example", "table"])?,
        FileIO::new_with_fs(),
    )
    .await?
    .into_table()
    .with_metrics_reporter(reporter.clone());

    let scan = table.scan().build()?;
    let tasks: Vec<_> = scan.plan_files().await?.try_collect().await?;
    println!("Planned {} file tasks", tasks.len());
    // EOF does not wait for reporting. Keep the runtime alive until our consumer signals.
    if scan.snapshot().is_some() {
        tokio::time::timeout(Duration::from_secs(10), reporter.reported.notified())
            .await
            .map_err(|error| {
                Error::new(ErrorKind::Unexpected, "Timed out waiting for scan metrics")
                    .with_source(error)
            })?;
    }
    for report in reporter.reports.lock().await.iter() {
        if let MetricsReport::Scan(scan) = report {
            println!(
                "{} snapshot {}: {} data files, {} delete attachments, {:?}",
                scan.table_ident(),
                scan.snapshot_id(),
                scan.metrics().result_data_files(),
                scan.metrics().result_delete_files(),
                scan.total_planning_duration(),
            );
        }
    }
    Ok(())
}
