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

use super::{ManifestsTable, SnapshotsTable};
use crate::table::Table;

/// Metadata table is used to inspect a table's history, snapshots, and other metadata as a table.
///
/// References:
/// - <https://github.com/apache/iceberg/blob/ac865e334e143dfd9e33011d8cf710b46d91f1e5/core/src/main/java/org/apache/iceberg/MetadataTableType.java#L23-L39>
/// - <https://iceberg.apache.org/docs/latest/spark-queries/#querying-with-sql>
/// - <https://py.iceberg.apache.org/api/#inspecting-tables>
#[derive(Debug)]
pub struct MetadataTable<'a>(&'a Table);

impl<'a> MetadataTable<'a> {
    /// Creates a new metadata scan.
    pub fn new(table: &'a Table) -> Self {
        Self(table)
    }

    /// Get the snapshots table.
    pub fn snapshots(&self) -> SnapshotsTable {
        SnapshotsTable::new(self.0)
    }

    /// Get the manifests table.
    pub fn manifests(&self) -> ManifestsTable {
        ManifestsTable::new(self.0)
    }
}
#[allow(missing_docs)]
#[cfg(test)]
pub mod tests {
    use expect_test::Expect;
    use futures::TryStreamExt;
    use itertools::Itertools;

    use crate::scan::ArrowRecordBatchStream;

    /// Snapshot testing to check the resulting record batch.
    ///
    /// - `expected_schema/data`: put `expect![[""]]` as a placeholder,
    ///   and then run test with `UPDATE_EXPECT=1 cargo test` to automatically update the result,
    ///   or use rust-analyzer (see [video](https://github.com/rust-analyzer/expect-test)).
    ///   Check the doc of [`expect_test`] for more details.
    /// - `ignore_check_columns`: Some columns are not stable, so we can skip them.
    /// - `sort_column`: The order of the data might be non-deterministic, so we can sort it by a column.
    pub async fn check_record_batches(
        batch_stream: ArrowRecordBatchStream,
        expected_schema: Expect,
        expected_data: Expect,
        ignore_check_columns: &[&str],
        sort_column: Option<&str>,
    ) {
        let record_batches = batch_stream.try_collect::<Vec<_>>().await.unwrap();
        assert!(!record_batches.is_empty(), "Empty record batches");

        // Combine record batches using the first batch's schema
        let first_batch = record_batches.first().unwrap();
        let record_batch =
            arrow_select::concat::concat_batches(&first_batch.schema(), &record_batches).unwrap();

        let mut columns = record_batch.columns().to_vec();
        if let Some(sort_column) = sort_column {
            let column = record_batch.column_by_name(sort_column).unwrap();
            let indices = arrow_ord::sort::sort_to_indices(column, None, None).unwrap();
            columns = columns
                .iter()
                .map(|column| arrow_select::take::take(column.as_ref(), &indices, None).unwrap())
                .collect_vec();
        }

        expected_schema.assert_eq(&format!(
            "{}",
            record_batch.schema().fields().iter().format(",\n")
        ));
        expected_data.assert_eq(&format!(
            "{}",
            record_batch
                .schema()
                .fields()
                .iter()
                .zip_eq(columns)
                .map(|(field, column)| {
                    if ignore_check_columns.contains(&field.name().as_str()) {
                        format!("{}: (skipped)", field.name())
                    } else {
                        format!("{}: {:?}", field.name(), column)
                    }
                })
                .format(",\n")
        ));
    }
}
