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

use super::files::{ContentFilter, files_schema, scan_all_files};
use crate::Result;
use crate::scan::ArrowRecordBatchStream;
use crate::table::Table;

/// All files metadata table.
///
/// Shows all data and delete files from all unique manifests across all snapshots.
pub struct AllFilesTable<'a> {
    table: &'a Table,
}

/// All data files metadata table.
///
/// Shows only data files from all unique manifests across all snapshots.
pub struct AllDataFilesTable<'a> {
    table: &'a Table,
}

/// All delete files metadata table.
///
/// Shows only delete files from all unique manifests across all snapshots.
pub struct AllDeleteFilesTable<'a> {
    table: &'a Table,
}

impl<'a> AllFilesTable<'a> {
    /// Create a new AllFiles table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the iceberg schema of the all files table.
    pub fn schema(&self) -> crate::spec::Schema {
        files_schema(self.table.metadata().default_partition_type())
    }

    /// Scans the all files table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        scan_all_files(self.table, ContentFilter::All).await
    }
}

impl<'a> AllDataFilesTable<'a> {
    /// Create a new AllDataFiles table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the iceberg schema of the all data files table.
    pub fn schema(&self) -> crate::spec::Schema {
        files_schema(self.table.metadata().default_partition_type())
    }

    /// Scans the all data files table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        scan_all_files(self.table, ContentFilter::DataOnly).await
    }
}

impl<'a> AllDeleteFilesTable<'a> {
    /// Create a new AllDeleteFiles table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the iceberg schema of the all delete files table.
    pub fn schema(&self) -> crate::spec::Schema {
        files_schema(self.table.metadata().default_partition_type())
    }

    /// Scans the all delete files table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        scan_all_files(self.table, ContentFilter::DeletesOnly).await
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect;
    use futures::TryStreamExt;

    use crate::scan::tests::TableTestFixture;
    use crate::test_utils::check_record_batches;

    #[tokio::test]
    async fn test_all_files_table() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_all_snapshot_manifest_files().await;

        let batch_stream = fixture.table.inspect().all_files().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "content": Int32, metadata: {"PARQUET:field_id": "134"} },
                Field { "file_path": Utf8, metadata: {"PARQUET:field_id": "100"} },
                Field { "file_format": Utf8, metadata: {"PARQUET:field_id": "101"} },
                Field { "partition": Struct("x": Int64, metadata: {"PARQUET:field_id": "1000"}), metadata: {"PARQUET:field_id": "102"} },
                Field { "record_count": Int64, metadata: {"PARQUET:field_id": "103"} },
                Field { "file_size_in_bytes": Int64, metadata: {"PARQUET:field_id": "104"} },
                Field { "column_sizes": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "201"}, "value": Int64, metadata: {"PARQUET:field_id": "202"}), unsorted), metadata: {"PARQUET:field_id": "108"} },
                Field { "value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "203"}, "value": Int64, metadata: {"PARQUET:field_id": "204"}), unsorted), metadata: {"PARQUET:field_id": "109"} },
                Field { "null_value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "205"}, "value": Int64, metadata: {"PARQUET:field_id": "206"}), unsorted), metadata: {"PARQUET:field_id": "110"} },
                Field { "nan_value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "207"}, "value": Int64, metadata: {"PARQUET:field_id": "208"}), unsorted), metadata: {"PARQUET:field_id": "137"} },
                Field { "lower_bounds": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "209"}, "value": LargeBinary, metadata: {"PARQUET:field_id": "210"}), unsorted), metadata: {"PARQUET:field_id": "125"} },
                Field { "upper_bounds": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "211"}, "value": LargeBinary, metadata: {"PARQUET:field_id": "212"}), unsorted), metadata: {"PARQUET:field_id": "128"} },
                Field { "key_metadata": nullable LargeBinary, metadata: {"PARQUET:field_id": "131"} },
                Field { "split_offsets": nullable List(Int64, field: 'element', metadata: {"PARQUET:field_id": "213"}), metadata: {"PARQUET:field_id": "132"} },
                Field { "equality_ids": nullable List(Int32, field: 'element', metadata: {"PARQUET:field_id": "214"}), metadata: {"PARQUET:field_id": "135"} },
                Field { "sort_order_id": nullable Int32, metadata: {"PARQUET:field_id": "140"} },
                Field { "spec_id": nullable Int32, metadata: {"PARQUET:field_id": "141"} }"#]],
            expect![[r#"
                content: PrimitiveArray<Int32>
                [
                  0,
                  0,
                ],
                file_path: (skipped),
                file_format: StringArray
                [
                  "parquet",
                  "parquet",
                ],
                partition: StructArray
                -- validity:
                [
                  valid,
                  valid,
                ]
                [
                -- child 0: "x" (Int64)
                PrimitiveArray<Int64>
                [
                  100,
                  300,
                ]
                ],
                record_count: PrimitiveArray<Int64>
                [
                  1,
                  1,
                ],
                file_size_in_bytes: PrimitiveArray<Int64>
                [
                  100,
                  100,
                ],
                column_sizes: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                value_counts: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                null_value_counts: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                nan_value_counts: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                lower_bounds: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                ],
                upper_bounds: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                ],
                key_metadata: LargeBinaryArray
                [
                  null,
                  null,
                ],
                split_offsets: ListArray
                [
                  null,
                  null,
                ],
                equality_ids: ListArray
                [
                  null,
                  null,
                ],
                sort_order_id: PrimitiveArray<Int32>
                [
                  null,
                  null,
                ],
                spec_id: PrimitiveArray<Int32>
                [
                  0,
                  0,
                ]"#]],
            &["file_path"],
            Some("file_path"),
        );
    }

    #[tokio::test]
    async fn test_all_data_files_table() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_all_snapshot_manifest_files().await;

        let batch_stream = fixture
            .table
            .inspect()
            .all_data_files()
            .scan()
            .await
            .unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "content": Int32, metadata: {"PARQUET:field_id": "134"} },
                Field { "file_path": Utf8, metadata: {"PARQUET:field_id": "100"} },
                Field { "file_format": Utf8, metadata: {"PARQUET:field_id": "101"} },
                Field { "partition": Struct("x": Int64, metadata: {"PARQUET:field_id": "1000"}), metadata: {"PARQUET:field_id": "102"} },
                Field { "record_count": Int64, metadata: {"PARQUET:field_id": "103"} },
                Field { "file_size_in_bytes": Int64, metadata: {"PARQUET:field_id": "104"} },
                Field { "column_sizes": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "201"}, "value": Int64, metadata: {"PARQUET:field_id": "202"}), unsorted), metadata: {"PARQUET:field_id": "108"} },
                Field { "value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "203"}, "value": Int64, metadata: {"PARQUET:field_id": "204"}), unsorted), metadata: {"PARQUET:field_id": "109"} },
                Field { "null_value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "205"}, "value": Int64, metadata: {"PARQUET:field_id": "206"}), unsorted), metadata: {"PARQUET:field_id": "110"} },
                Field { "nan_value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "207"}, "value": Int64, metadata: {"PARQUET:field_id": "208"}), unsorted), metadata: {"PARQUET:field_id": "137"} },
                Field { "lower_bounds": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "209"}, "value": LargeBinary, metadata: {"PARQUET:field_id": "210"}), unsorted), metadata: {"PARQUET:field_id": "125"} },
                Field { "upper_bounds": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "211"}, "value": LargeBinary, metadata: {"PARQUET:field_id": "212"}), unsorted), metadata: {"PARQUET:field_id": "128"} },
                Field { "key_metadata": nullable LargeBinary, metadata: {"PARQUET:field_id": "131"} },
                Field { "split_offsets": nullable List(Int64, field: 'element', metadata: {"PARQUET:field_id": "213"}), metadata: {"PARQUET:field_id": "132"} },
                Field { "equality_ids": nullable List(Int32, field: 'element', metadata: {"PARQUET:field_id": "214"}), metadata: {"PARQUET:field_id": "135"} },
                Field { "sort_order_id": nullable Int32, metadata: {"PARQUET:field_id": "140"} },
                Field { "spec_id": nullable Int32, metadata: {"PARQUET:field_id": "141"} }"#]],
            expect![[r#"
                content: PrimitiveArray<Int32>
                [
                  0,
                  0,
                ],
                file_path: (skipped),
                file_format: StringArray
                [
                  "parquet",
                  "parquet",
                ],
                partition: StructArray
                -- validity:
                [
                  valid,
                  valid,
                ]
                [
                -- child 0: "x" (Int64)
                PrimitiveArray<Int64>
                [
                  100,
                  300,
                ]
                ],
                record_count: PrimitiveArray<Int64>
                [
                  1,
                  1,
                ],
                file_size_in_bytes: PrimitiveArray<Int64>
                [
                  100,
                  100,
                ],
                column_sizes: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                value_counts: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                null_value_counts: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                nan_value_counts: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                lower_bounds: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                ],
                upper_bounds: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                ],
                key_metadata: LargeBinaryArray
                [
                  null,
                  null,
                ],
                split_offsets: ListArray
                [
                  null,
                  null,
                ],
                equality_ids: ListArray
                [
                  null,
                  null,
                ],
                sort_order_id: PrimitiveArray<Int32>
                [
                  null,
                  null,
                ],
                spec_id: PrimitiveArray<Int32>
                [
                  0,
                  0,
                ]"#]],
            &["file_path"],
            Some("file_path"),
        );
    }

    #[tokio::test]
    async fn test_all_delete_files_table_empty() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_all_snapshot_manifest_files().await;

        let batch_stream = fixture
            .table
            .inspect()
            .all_delete_files()
            .scan()
            .await
            .unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 0);
    }
}
