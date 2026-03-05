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
        .to_arrow()
        .await?;

    // Consume this stream like arrow record batch stream.
    let _data: Vec<_> = stream.try_collect().await?;
    Ok(())
}
```

## IO Support

Iceberg Rust provides various storage backends through feature flags. Here are the currently supported storage backends:

| Storage Backend      | Feature Flag     | Status         | Description                                   |
| -------------------- | ---------------- | -------------- | --------------------------------------------- |
| Memory               | `opendal-memory` | ✅ Stable       | In-memory storage for testing and development |
| Local Filesystem     | `opendal-fs`     | ✅ Stable       | Local filesystem storage                      |
| Amazon S3            | `opendal-s3`     | ✅ Stable       | Amazon S3 storage                             |
| Google Cloud Storage | `opendal-gcs`    | ✅ Stable       | Google Cloud Storage                          |
| Alibaba Cloud OSS    | `opendal-oss`    | 🧪 Experimental | Alibaba Cloud Object Storage Service          |
| Azure Datalake       | `opendal-azdls`  | 🧪 Experimental | Azure Datalake Storage v2                     |

You can enable all stable storage backends at once using the `opendal-all` feature flag. 

> Note that `opendal-oss` and `opendal-azdls` are currently experimental and not included in `opendal-all`.

Example usage in `Cargo.toml`:

```toml
[dependencies]
iceberg = { version = "x.y.z", features = ["opendal-s3", "opendal-fs"] }
```
