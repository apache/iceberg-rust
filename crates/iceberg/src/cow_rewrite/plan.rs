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

use futures::TryStreamExt;

use crate::scan::FileScanTask;
use crate::spec::DataFile;

/// A data file selected for COW rewrite.
#[derive(Debug, Clone)]
pub struct CowRewriteFile {
    /// Original data file from the manifest entry.
    pub old_data_file: DataFile,
    /// Full-read task for visible rows in the original file.
    pub scan_task: FileScanTask,
}

/// Plan data files that may need copy-on-write rewrite.
pub async fn plan_cow_rewrite_files(
    table: &crate::table::Table,
    predicate: Option<crate::expr::Predicate>,
    snapshot_id: Option<i64>,
    case_sensitive: bool,
) -> crate::Result<Vec<CowRewriteFile>> {
    let mut scan_builder = table
        .scan()
        .select_all()
        .with_case_sensitive(case_sensitive);

    if let Some(predicate) = predicate {
        scan_builder = scan_builder.with_filter(predicate);
    }

    if let Some(snapshot_id) = snapshot_id {
        scan_builder = scan_builder.snapshot_id(snapshot_id);
    }

    scan_builder
        .build()?
        .plan_cow_rewrite_files()
        .await?
        .try_collect()
        .await
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;

    use crate::Result;
    use crate::expr::Predicate;
    use crate::scan::tests::TableTestFixture;

    #[tokio::test]
    async fn cow_planner_returns_old_file_and_full_read_task() -> Result<()> {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let mut files =
            super::plan_cow_rewrite_files(&fixture.table, Some(Predicate::AlwaysTrue), None, true)
                .await?;

        assert_eq!(files.len(), 2);

        files.sort_by_key(|file| file.old_data_file.file_path().to_string());
        assert_eq!(
            files[0].old_data_file.file_path(),
            format!("{}/1.parquet", fixture.table_location)
        );
        assert_eq!(
            files[1].old_data_file.file_path(),
            format!("{}/3.parquet", fixture.table_location)
        );

        for file in files {
            assert_eq!(
                file.old_data_file.file_path(),
                file.scan_task.data_file_path()
            );
            assert!(file.scan_task.predicate.is_none());
            assert_eq!(
                Some(file.old_data_file.record_count()),
                file.scan_task.record_count
            );
            assert_eq!(0, file.scan_task.start);
            assert_eq!(
                file.old_data_file.file_size_in_bytes(),
                file.scan_task.length
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn cow_planner_preserves_delete_files() -> Result<()> {
        let mut fixture = TableTestFixture::new();
        fixture.setup_deadlock_manifests().await;

        let scan = fixture
            .table
            .scan()
            .select_all()
            .with_concurrency_limit(1)
            .build()?;

        let files = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            scan.plan_cow_rewrite_files()
                .await?
                .try_collect::<Vec<_>>()
                .await
        })
        .await
        .expect("COW planning should not deadlock")?;

        assert_eq!(files.len(), 10);
        for file in files {
            assert_eq!(file.scan_task.deletes.len(), 1);
            assert_eq!(
                file.scan_task.deletes[0].file_path,
                format!("{}/del.parquet", fixture.table_location)
            );
        }

        Ok(())
    }
}
