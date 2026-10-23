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

//! Tunables for Parquet file I/O used by `ArrowReader`.

use typed_builder::TypedBuilder;

use super::{
    DEFAULT_METADATA_SIZE_HINT, DEFAULT_RANGE_COALESCE_BYTES, DEFAULT_RANGE_FETCH_CONCURRENCY,
};

/// Options for tuning Parquet file I/O.
#[derive(Clone, Copy, Debug, TypedBuilder)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub(crate) struct ParquetReadOptions {
    /// Number of bytes to prefetch for parsing the Parquet metadata.
    ///
    /// This hint can help reduce the number of fetch requests. For more details see the
    /// [ParquetMetaDataReader documentation](https://docs.rs/parquet/latest/parquet/file/metadata/struct.ParquetMetaDataReader.html#method.with_prefetch_hint).
    ///
    /// Defaults to 512 KiB, matching DataFusion's default `ParquetOptions::metadata_size_hint`.
    #[builder(default = Some(DEFAULT_METADATA_SIZE_HINT))]
    pub(crate) metadata_size_hint: Option<usize>,
    /// Gap threshold for merging nearby byte ranges into a single request.
    /// Ranges with gaps smaller than this value will be coalesced.
    ///
    /// Defaults to 1 MiB, matching object_store's `OBJECT_STORE_COALESCE_DEFAULT`.
    #[builder(default = DEFAULT_RANGE_COALESCE_BYTES)]
    pub(crate) range_coalesce_bytes: u64,
    /// Maximum number of merged byte ranges to fetch concurrently.
    ///
    /// Defaults to 10, matching object_store's `OBJECT_STORE_COALESCE_PARALLEL`.
    #[builder(default = DEFAULT_RANGE_FETCH_CONCURRENCY)]
    pub(crate) range_fetch_concurrency: usize,
    /// Whether to preload the column index when reading Parquet metadata.
    #[builder(default = true)]
    pub(crate) preload_column_index: bool,
    /// Whether to preload the offset index when reading Parquet metadata.
    #[builder(default = true)]
    pub(crate) preload_offset_index: bool,
    /// Whether to preload the page index when reading Parquet metadata.
    #[builder(default = false)]
    pub(crate) preload_page_index: bool,
}

impl ParquetReadOptions {
    pub(crate) fn metadata_size_hint(&self) -> Option<usize> {
        self.metadata_size_hint
    }

    pub(crate) fn range_coalesce_bytes(&self) -> u64 {
        self.range_coalesce_bytes
    }

    pub(crate) fn range_fetch_concurrency(&self) -> usize {
        self.range_fetch_concurrency
    }

    pub(crate) fn preload_column_index(&self) -> bool {
        self.preload_column_index
    }

    pub(crate) fn preload_offset_index(&self) -> bool {
        self.preload_offset_index
    }

    pub(crate) fn preload_page_index(&self) -> bool {
        self.preload_page_index
    }
}
