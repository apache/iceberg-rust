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
        self.options.set_parquet_options(options);
        self
    }

    /// Build the ArrowReader.
    pub fn build(self) -> ArrowReader {
        let concurrency_limit_data_files = self
            .options
            .data_file_concurrency_limit()
            .unwrap_or_else(|| available_parallelism().get())
            .max(1);
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
            parquet_read_options: self.options.parquet_options().clone(),
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
