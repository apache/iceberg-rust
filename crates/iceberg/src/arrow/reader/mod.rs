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

//! Parquet file data reader

use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::io::FileIO;
use crate::runtime::Runtime;
use crate::util::available_parallelism;

/// Default gap between byte ranges below which they are coalesced into a
/// single request. Matches object_store's `OBJECT_STORE_COALESCE_DEFAULT`.
const DEFAULT_RANGE_COALESCE_BYTES: u64 = 1024 * 1024;

/// Default maximum number of coalesced byte ranges fetched concurrently.
/// Matches object_store's `OBJECT_STORE_COALESCE_PARALLEL`.
const DEFAULT_RANGE_FETCH_CONCURRENCY: usize = 10;

/// Default number of bytes to prefetch when parsing Parquet footer metadata.
/// Matches DataFusion's default `ParquetOptions::metadata_size_hint`.
const DEFAULT_METADATA_SIZE_HINT: usize = 512 * 1024;

mod file_reader;
mod options;
mod pipeline;
mod positional_deletes;
mod predicate_visitor;
mod projection;
mod row_filter;
mod row_lineage;
pub use file_reader::ArrowFileReader;
pub use options::{ArrowReaderOptions, ParquetReadOptions};
use predicate_visitor::{CollectFieldIdVisitor, PredicateConverter};
use projection::{
    add_fallback_field_ids_to_arrow_schema, apply_name_mapping_to_arrow_schema,
    find_leaf_by_field_id,
};

/// Builder to create ArrowReader
pub struct ArrowReaderBuilder {
    options: ArrowReaderOptions,
    file_io: FileIO,
    runtime: Runtime,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub fn new(file_io: FileIO, runtime: Runtime) -> Self {
        ArrowReaderBuilder {
            options: ArrowReaderOptions::default(),
            file_io,
            runtime,
        }
    }

    /// Replaces all reader settings while preserving local I/O and runtime resources.
    pub fn with_options(mut self, options: ArrowReaderOptions) -> Self {
        self.options = options;
        self
    }

    /// Replaces Parquet I/O settings while preserving other reader settings.
    pub fn with_parquet_options(mut self, options: ParquetReadOptions) -> Self {
        self.options = self.options.with_parquet_options(options);
        self
    }

    /// Build the ArrowReader.
    pub fn build(self) -> ArrowReader {
        let options = self
            .options
            .with_data_file_concurrency_limit_if_unset(available_parallelism().get());
        let concurrency_limit_data_files = options
            .data_file_concurrency_limit()
            .expect("reader concurrency was set above")
            .max(1);
        ArrowReader {
            options,
            file_io: self.file_io.clone(),
            delete_file_loader: CachingDeleteFileLoader::new(
                self.file_io.clone(),
                concurrency_limit_data_files,
                self.runtime.clone(),
            ),
            runtime: self.runtime.clone(),
        }
    }
}

/// Reads data from Parquet files
#[derive(Clone)]
pub struct ArrowReader {
    options: ArrowReaderOptions,
    file_io: FileIO,
    delete_file_loader: CachingDeleteFileLoader,

    runtime: Runtime,
}

#[cfg(test)]
mod tests {
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
            reader.options,
            ArrowReaderOptions::builder()
                .with_data_file_concurrency_limit(available_parallelism().get())
                .build()
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
            .with_parquet_options(parquet)
            .build();
        let reader = ArrowReaderBuilder::new(FileIO::new_with_memory(), test_runtime())
            .with_options(options.clone())
            .build();
        assert_eq!(reader.options, options);
    }
}
