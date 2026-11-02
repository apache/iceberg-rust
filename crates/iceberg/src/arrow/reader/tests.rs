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

use std::time::Duration;

use futures::{TryStreamExt, stream};

use super::{ArrowReaderBuilder, ArrowReaderOptions, ParquetReadOptions};
use crate::io::FileIO;
use crate::test_utils::test_runtime;
use crate::util::available_parallelism;

#[tokio::test]
async fn deserialized_zero_concurrency_completes_reads() {
    let options: ArrowReaderOptions =
        serde_json::from_str(r#"{"data_file_concurrency_limit":0}"#).unwrap();
    let reader = ArrowReaderBuilder::new(FileIO::new_with_memory(), test_runtime())
        .with_options(options)
        .build();
    assert_eq!(reader.concurrency_limit_data_files, 1);
    let batches = tokio::time::timeout(
        Duration::from_secs(1),
        reader
            .read(Box::pin(stream::empty()))
            .unwrap()
            .stream()
            .try_collect::<Vec<_>>(),
    )
    .await
    .expect("zero concurrency must not stall the task stream")
    .unwrap();
    assert!(batches.is_empty());
}

#[test]
fn default_options_restore_local_reader_defaults() {
    let reader = ArrowReaderBuilder::new(FileIO::new_with_memory(), test_runtime())
        .with_options(
            ArrowReaderOptions::builder()
                .with_batch_size(1024)
                .with_data_file_concurrency_limit(3)
                .with_row_group_filtering_enabled(false)
                .with_row_selection_enabled(true)
                .with_bloom_filter_enabled(true)
                .with_parquet_options(
                    ParquetReadOptions::builder()
                        .with_range_fetch_concurrency(99)
                        .build(),
                )
                .build(),
        )
        .with_options(ArrowReaderOptions::default())
        .build();
    assert_eq!(
        (
            reader.batch_size,
            reader.concurrency_limit_data_files,
            reader.row_group_filtering_enabled,
            reader.row_selection_enabled,
            reader.bloom_filter_enabled,
            reader.parquet_read_options
        ),
        (
            None,
            available_parallelism().get(),
            true,
            false,
            false,
            ParquetReadOptions::default()
        ),
    );
}

#[test]
fn explicit_options_configure_reader_settings() {
    let parquet = ParquetReadOptions::builder()
        .with_metadata_size_hint(4096)
        .with_range_coalesce_bytes(8192)
        .with_range_fetch_concurrency(2)
        .build();
    let options = ArrowReaderOptions::builder()
        .with_batch_size(17)
        .with_data_file_concurrency_limit(3)
        .with_row_group_filtering_enabled(false)
        .with_row_selection_enabled(true)
        .with_bloom_filter_enabled(true)
        .with_parquet_options(parquet.clone())
        .build();
    let reader = ArrowReaderBuilder::new(FileIO::new_with_memory(), test_runtime())
        .with_options(options)
        .build();
    assert_eq!(
        (
            reader.batch_size,
            reader.concurrency_limit_data_files,
            reader.row_group_filtering_enabled,
            reader.row_selection_enabled,
            reader.bloom_filter_enabled,
            reader.parquet_read_options
        ),
        (Some(17), 3, false, true, true, parquet),
    );
}
