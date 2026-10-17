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

# iceberg-storage-opendal

OpenDAL-based storage backend implementations for [Apache Iceberg Rust](https://rust.iceberg.apache.org/).

## Supported Storage Backends

| Storage Backend      | Feature Flag     | Status          | Description                                   |
| -------------------- | ---------------- | --------------- | --------------------------------------------- |
| Memory               | `opendal-memory` | ✅ Stable       | In-memory storage for testing and development |
| Local Filesystem     | `opendal-fs`     | ✅ Stable       | Local filesystem storage                      |
| Amazon S3            | `opendal-s3`     | ✅ Stable       | Amazon S3 storage                             |
| Google Cloud Storage | `opendal-gcs`    | ✅ Stable       | Google Cloud Storage                          |
| Hugging Face         | `opendal-hf`     | ✅ Stable       | Hugging Face buckets and repositories         |
| Alibaba Cloud OSS    | `opendal-oss`    | 🧪 Experimental | Alibaba Cloud Object Storage Service          |
| Azure Datalake       | `opendal-azdls`  | 🧪 Experimental | Azure Datalake Storage v2                     |
| HDFS                 | `opendal-hdfs-native` | 🧪 Experimental | HDFS via the native Rust client (`hdfs-native`) |

You can enable all stable storage backends at once using the `opendal-all` feature flag.

> Note that `opendal-oss` and `opendal-azdls` are currently experimental and not included in `opendal-all`.

> `opendal-hdfs-native` is experimental and not included in `opendal-all`; see [HDFS](#hdfs).

## Usage

Add the crate to your `Cargo.toml` with the feature flags for the backends you need:

```toml
[dependencies]
iceberg = { version = "x.y.z" }
iceberg-storage-opendal = { version = "x.y.z", features = ["opendal-s3"] }
iceberg-catalog-rest = { version = "x.y.z" }
```

Then pass an `OpenDalStorageFactory` to your catalog builder:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::{RestCatalogBuilder, REST_CATALOG_PROP_URI};
use iceberg_storage_opendal::OpenDalStorageFactory;

#[tokio::main]
async fn main() -> iceberg::Result<()> {
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load(
            "my_catalog",
            HashMap::from([
                (REST_CATALOG_PROP_URI.to_string(), "http://localhost:8181".to_string()),
            ]),
        )
        .await?;

    let table = catalog
        .load_table(&TableIdent::from_strs(["my_namespace", "my_table"])?)
        .await?;

    let scan = table.scan().select_all().build()?;
    let stream = scan.to_arrow().await?;

    Ok(())
}
```

## HDFS

The `opendal-hdfs-native` feature adds an HDFS backend built on
[`hdfs-native`](https://github.com/Kimahriman/hdfs-native), a native Rust client that needs no
JVM. Enable it on its own, since it is not part of `opendal-all`:

```toml
iceberg-storage-opendal = { version = "x.y.z", features = ["opendal-hdfs-native"] }
```

Then pass `OpenDalStorageFactory::HdfsNative` to your catalog builder, or use
`OpenDalResolvingStorageFactory`, which routes `hdfs://` paths to it. The backend reads these
properties:

| Property | Description |
| --- | --- |
| `hdfs.name-node` | NameNode `host:port` for authority-less paths (`hdfs:///warehouse/...`); a comma-separated list enables HA failover |
| `hdfs.name-node.<nameservice>` | The NameNodes of a nameservice used as a path authority (`hdfs://nameservice1/warehouse/...`), comma-separated |
| `hdfs.host`, `hdfs.port` | PyIceberg's keys for authority-less paths; the port defaults to `8020` |
| `hadoop.*` | Forwarded to the HDFS client with the prefix stripped, e.g. `hadoop.dfs.client.failover.max.attempts` |

A path with a `host:port` authority is always used as is. For a table under
`hdfs://nameservice1/warehouse` on an HA cluster:

```text
hdfs.name-node.nameservice1 = nn1.example.com:8020,nn2.example.com:8020
```

The client also loads `core-site.xml` and `hdfs-site.xml` from `$HADOOP_CONF_DIR`, else
`$HADOOP_HOME/etc/hadoop`, and takes the user from `HADOOP_USER_NAME`. Kerberos is used when
`hadoop.security.authentication` is `kerberos` there, with the default credential cache
(`kinit`); `libgssapi_krb5` is loaded at runtime. PyIceberg's `hdfs.user` and
`hdfs.kerberos_ticket` are ignored with a warning.

Unlike Hadoop, a portless authority is never resolved from `$HADOOP_CONF_DIR` or dialed on
port 8020: declare it with `hdfs.name-node.<nameservice>`.
