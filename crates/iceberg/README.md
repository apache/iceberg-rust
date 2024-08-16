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
use futures::TryStreamExt;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::{Catalog, Result, TableIdent};
use iceberg_catalog_memory::MemoryCatalog;

#[tokio::main]
async fn main() -> Result<()> {
    // Build your file IO.
    let file_io = FileIOBuilder::new("memory").build()?;
    // Connect to a catalog.
    let catalog = MemoryCatalog::new(file_io, None);
    // Load table from catalog.
    let table = catalog
        .load_table(&TableIdent::from_strs(["hello", "world"])?)
        .await?;
    // Build table scan.
    let stream = table
        .scan()
        .select(["name", "id"])
        .build()?
        .to_arrow()
        .await?;

    // Consume this stream like arrow record batch stream.
    let _data: Vec<_> = stream.try_collect().await?;
    Ok(())
}
```
