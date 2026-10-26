<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Apache Iceberg Official Native Rust Implementation

[![crates.io](https://img.shields.io/crates/v/iceberg.svg)](https://crates.io/crates/iceberg)
[![docs.rs](https://img.shields.io/docsrs/iceberg.svg)](https://docs.rs/iceberg/latest/iceberg/)

This crate contains the official Native Rust implementation of [Apache Iceberg](https://rust.iceberg.apache.org/).

See the [API documentation](https://docs.rs/iceberg/latest) for examples and the full API.

## Usage

```rust
use std::collections::HashMap;
use std::sync::Arc;

use futures::TryStreamExt;
use iceberg::io::MemoryStorageFactory;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::{Catalog, CatalogBuilder, Result, TableIdent};

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to a catalog with a memory storage factory.
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(MemoryStorageFactory))
        .load(
            "my_catalog",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), "/tmp/warehouse".to_string())]),
        )
        .await?;
    // Load table from catalog.
    let table = catalog
        .load_table(&TableIdent::from_strs(["hello", "world"])?)
        .await?;
    // Build table scan.
    let stream = table
        .scan()
        .select(["name", "id"])
        .build()?
        .to_arrow(Default::default())
        .await?;

    // Consume this stream like arrow record batch stream.
    let _data: Vec<_> = stream.try_collect().await?;
    Ok(())
}
```

### Reader settings

Pass reader settings separately from file planning:

```rust
let options = iceberg::arrow::ArrowReaderOptions::builder()
    .with_batch_size(Some(1024))
    .with_row_selection_enabled(true)
    .with_parquet_options(
        iceberg::arrow::ParquetReadOptions::builder()
            .with_metadata_size_hint(Some(512 * 1024))
            .build(),
    )
    .build();
let stream = table.scan().build()?.to_arrow(options).await?;
```

When upgrading, move reader-setting calls from `TableScanBuilder` to
`ArrowReaderOptions::builder()` and pass the result to `to_arrow(options)`.
Use `to_arrow(Default::default())` for default settings. Scan
`with_concurrency_limit()` controls manifest planning only; set
`with_data_file_concurrency_limit(Some(limit))` on the reader options to also
limit data-file reads. An unset data-file limit uses execution-host parallelism.

Locally constructed readers accept the same options through
`ArrowReaderBuilder::with_options(options)`, including Parquet I/O tuning.
This replaces the complete configuration; subsequent convenience setters override
individual settings. `with_parquet_options()` replaces only the nested Parquet
configuration.

## Storage Backends

For storage backend support (S3, GCS, local filesystem, etc.), use the [`iceberg-storage-opendal`](https://crates.io/crates/iceberg-storage-opendal) crate. See its [README](../storage/opendal/README.md) for available backends and feature flags.
