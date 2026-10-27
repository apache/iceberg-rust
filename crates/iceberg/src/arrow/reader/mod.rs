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
pub use options::{
    ArrowReaderOptions, ArrowReaderOptionsBuilder, ParquetReadOptions, ParquetReadOptionsBuilder,
};
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

    /// Sets the max number of in flight data files that are being fetched
    pub fn with_data_file_concurrency_limit(mut self, val: usize) -> Self {
        self.options.set_data_file_concurrency_limit(Some(val));
        self
    }

    /// Sets the desired size of batches in the response
    /// to something other than the default
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.options.set_batch_size(Some(batch_size));
        self
    }

    /// Determines whether to enable row group filtering.
    pub fn with_row_group_filtering_enabled(mut self, row_group_filtering_enabled: bool) -> Self {
        self.options
            .set_row_group_filtering_enabled(row_group_filtering_enabled);
        self
    }

    /// Determines whether to enable row selection.
    pub fn with_row_selection_enabled(mut self, row_selection_enabled: bool) -> Self {
        self.options
            .set_row_selection_enabled(row_selection_enabled);
        self
    }

    /// Determines whether to enable bloom filter-based row group filtering.
    ///
    /// When enabled, if a read is performed with an equality or IN predicate,
    /// the bloom filter for relevant columns in each row group is read and
    /// checked. Row groups where the bloom filter proves the value is absent
    /// are skipped entirely.
    ///
    /// Defaults to disabled. Each bloom filter is a separate read, and they are
    /// issued serially — one round trip per relevant column per row group, before
    /// any data is read. TODO(#3191)
    pub fn with_bloom_filter_enabled(mut self, bloom_filter_enabled: bool) -> Self {
        self.options.set_bloom_filter_enabled(bloom_filter_enabled);
        self
    }

    /// Provide a hint as to the number of bytes to prefetch for parsing the Parquet metadata
    ///
    /// This hint can help reduce the number of fetch requests. For more details see the
    /// [ParquetMetaDataReader documentation](https://docs.rs/parquet/latest/parquet/file/metadata/struct.ParquetMetaDataReader.html#method.with_prefetch_hint).
    pub fn with_metadata_size_hint(mut self, metadata_size_hint: usize) -> Self {
        self.options
            .parquet_options_mut()
            .set_metadata_size_hint(Some(metadata_size_hint));
        self
    }

    /// Sets the gap threshold for merging nearby byte ranges into a single request.
    /// Ranges with gaps smaller than this value will be coalesced.
    ///
    /// Defaults to 1 MiB, matching object_store's OBJECT_STORE_COALESCE_DEFAULT.
    pub fn with_range_coalesce_bytes(mut self, range_coalesce_bytes: u64) -> Self {
        self.options
            .parquet_options_mut()
            .set_range_coalesce_bytes(range_coalesce_bytes);
        self
    }

    /// Sets the maximum number of merged byte ranges to fetch concurrently.
    ///
    /// Defaults to 10, matching object_store's OBJECT_STORE_COALESCE_PARALLEL.
    pub fn with_range_fetch_concurrency(mut self, range_fetch_concurrency: usize) -> Self {
        self.options
            .parquet_options_mut()
            .set_range_fetch_concurrency(range_fetch_concurrency);
        self
    }

    /// Replaces all reader settings while preserving local I/O and runtime resources.
    pub fn with_options(mut self, options: ArrowReaderOptions) -> Self {
        self.options = options;
        self
    }

    /// Replaces Parquet I/O settings while preserving other reader settings.
    pub fn with_parquet_options(mut self, options: ParquetReadOptions) -> Self {
        self.options.set_parquet_options(options);
        self
    }

    /// Build the ArrowReader.
    pub fn build(self) -> ArrowReader {
        let concurrency_limit_data_files = self
            .options
            .data_file_concurrency_limit()
            .unwrap_or_else(|| available_parallelism().get());
        ArrowReader {
            batch_size: self.options.batch_size(),
            file_io: self.file_io.clone(),
            delete_file_loader: CachingDeleteFileLoader::new(
                self.file_io.clone(),
                concurrency_limit_data_files,
                self.runtime.clone(),
            ),
            concurrency_limit_data_files,
            row_group_filtering_enabled: self.options.row_group_filtering_enabled(),
            row_selection_enabled: self.options.row_selection_enabled(),
            bloom_filter_enabled: self.options.bloom_filter_enabled(),
            parquet_read_options: *self.options.parquet_options(),
        }
    }
}

/// Reads data from Parquet files
#[derive(Clone)]
pub struct ArrowReader {
    batch_size: Option<usize>,
    file_io: FileIO,
    delete_file_loader: CachingDeleteFileLoader,

    /// the maximum number of data files that can be fetched at the same time
    concurrency_limit_data_files: usize,

    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
    bloom_filter_enabled: bool,
    parquet_read_options: ParquetReadOptions,
}
