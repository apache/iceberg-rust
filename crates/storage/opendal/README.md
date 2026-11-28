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

The default features enable Memory, Local Filesystem, and S3. `opendal-all` also enables GCS,
Hugging Face, OSS, and Azure Data Lake Storage. Enable `opendal-hdfs-native` separately for HDFS.

## Usage

Add the crate to your `Cargo.toml` with the feature flags for the backends you need:

```toml
[dependencies]
iceberg = { version = "x.y.z" }
iceberg-storage-opendal = { version = "x.y.z", features = ["opendal-s3"] }
iceberg-catalog-rest = { version = "x.y.z" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Then pass an `OpenDalStorageFactory` to your catalog builder:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::CLIENT_REGION;
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
                (CLIENT_REGION.to_string(), "us-east-1".to_string()),
            ]),
        )
        .await?;

    let table = catalog
        .load_table(&TableIdent::from_strs(["my_namespace", "my_table"])?)
        .await?;

    let scan = table.scan().select_all().build()?;
    let stream = scan.to_arrow(Default::default()).await?;

    Ok(())
}
```

## Storage configuration

Pass these settings as string properties to `FileIOBuilder::with_props` or in the
property map passed to `RestCatalogBuilder::load`. Use `OpenDalResolvingStorageFactory`
when table locations use more than one backend. Supply credentials through configuration
or the backend's credential provider and keep location URIs free of credentials.

### Shared settings

| Property | Behavior |
| --- | --- |
| `opendal.io-timeout-ms` | Positive timeout for each reader, writer, lister, or deleter I/O call, per attempt. Default `10000` ms. Control operations such as `stat` use OpenDAL's separate 60-second timeout. |
| `opendal.retry.num-retries` | Maximum retry count. Set to `0` to disable retries. |
| `opendal.retry.min-wait-ms` | Positive minimum retry delay in milliseconds. |
| `opendal.retry.max-wait-ms` | Positive maximum retry delay in milliseconds. |

Omitted retry settings use OpenDAL's defaults.

### Amazon S3

Enable `opendal-s3` and use `OpenDalStorageFactory::S3` for locations such as
`s3://warehouse/tables/data.parquet`. The resolver also accepts `s3a://` and `s3n://`.

| Property | Behavior |
| --- | --- |
| `client.region`, `s3.region` | S3 region. `client.region` takes precedence. When both are absent, OpenDAL checks `AWS_REGION` and then `AWS_DEFAULT_REGION`. A region is required. |
| `s3.endpoint` | Optional service URL. With configuration loading enabled, OpenDAL falls back to `AWS_ENDPOINT_URL`, `AWS_ENDPOINT`, and `AWS_S3_ENDPOINT`, in that order, before constructing the AWS endpoint from the region. |
| `s3.path-style-access` | Set to `true` for path-style URLs. The default is virtual-hosted style. |
| `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token` | Static credentials. Supply both key properties and the session token when using temporary credentials. |
| `client.assume-role.arn`, `client.assume-role.external-id`, `client.assume-role.session-name` | IAM role to assume with the resolved source credentials, with optional external ID and session name. |
| `s3.disable-config-load` | Set to `true` to skip the environment credential provider, AWS profile loading, and environment endpoint discovery. Region lookup remains available. |
| `s3.disable-ec2-metadata` | Set to `true` to skip EC2 instance metadata in the default credential chain. |
| `s3.allow-anonymous` | Set to `true` to send unsigned requests to a public bucket or test service. |
| `s3.sse.type` | Server-side encryption mode. Accepted values are `none`, `s3`, `kms`, and `custom`. |
| `s3.sse.key`, `s3.sse.md5` | For `kms`, `s3.sse.key` is an optional KMS key ID. For `custom`, supply the Base64-encoded customer key and its Base64-encoded MD5 digest. |
| `s3.multipart.part-size-bytes` | Upload part size in bytes, from `5242880` (5 MiB) to `2147483647`. Default `33554432` (32 MiB). |
| `s3.retry.num-retries`, `s3.retry.min-wait-ms`, `s3.retry.max-wait-ms` | Override the corresponding `opendal.retry.*` setting for S3, per property. |

Explicit keys take priority over the default AWS credential chain, which can use
environment credentials, AWS profile files, workload identity, container credentials,
and EC2 metadata. A `CustomAwsCredentialLoader` replaces that chain and any configured
assume-role provider. Configure a custom loader in the process doing the I/O because
the loader cannot be serialized.

### Google Cloud Storage

Enable `opendal-gcs` and use `OpenDalStorageFactory::Gcs` for locations such as
`gs://warehouse/tables/data.parquet`. The resolver also accepts `gcs://`.

| Property | Behavior |
| --- | --- |
| `gcs.service.host` | Optional service URL. Default `https://storage.googleapis.com`. |
| `gcs.credentials-json` | Base64-encoded JSON credentials. |
| `gcs.oauth2.token` | OAuth2 access token. Takes priority over JSON credentials. |
| `gcs.disable-config-load` | Set to `true` to skip `GOOGLE_APPLICATION_CREDENTIALS` and the well-known Application Default Credentials file. |
| `gcs.disable-vm-metadata` | Set to `true` to skip the VM metadata credential provider. |
| `gcs.allow-anonymous` | Set to `true` to send unsigned requests. |
| `gcs.no-auth` | Presence of this key enables unsigned requests and disables config loading and VM metadata. Remove the key to restore normal authentication. |

Without explicit credentials, the default chain tries `GOOGLE_APPLICATION_CREDENTIALS`,
the well-known Application Default Credentials file, and VM metadata, in that order.
The OpenDAL backend consumes the properties above. `gcs.project-id` and `gcs.user-project`
have no effect here.

### Alibaba Cloud OSS

Enable `opendal-oss` and use `OpenDalStorageFactory::Oss` for locations such as
`oss://warehouse/tables/data.parquet`.

| Property | Behavior |
| --- | --- |
| `oss.endpoint` | Required regional service endpoint. Supply the service hostname without the bucket prefix. The bucket comes from the location. |
| `oss.access-key-id`, `oss.access-key-secret` | Explicit key pair. Both properties are needed to use this credential source. |

The credential chain tries the explicit key pair, then environment credentials such as
`ALIBABA_CLOUD_ACCESS_KEY_ID` and `ALIBABA_CLOUD_ACCESS_KEY_SECRET`, then Alibaba Cloud
OIDC role credentials and ECS RAM role credentials. Set a temporary credential's
security token in the environment with the corresponding environment key pair.

### Azure Data Lake Storage

Enable `opendal-azdls` and use `OpenDalStorageFactory::Azdls`. A location such as
`abfss://warehouse@account.dfs.core.windows.net/tables/data.parquet` supplies the
filesystem, account, and endpoint suffix. `abfs` and `abfss` require a `dfs` host,
while `wasb` and `wasbs` require a `blob` host. The backend derives its `dfs` endpoint
from the location and uses HTTPS for `abfss` and `wasbs`.

| Property | Behavior |
| --- | --- |
| `adls.account-name` | Expected account name. It must match the account in the location. Shared Key authentication with a custom endpoint suffix requires this property. |
| `adls.account-key` | Shared account key. When `adls.account-name` is omitted, OpenDAL infers the account from endpoints with the `core.windows.net`, `core.usgovcloudapi.net`, or `core.chinacloudapi.cn` suffix. |
| `adls.sas-token` | Shared access signature. Takes priority over the shared account key. |
| `adls.tenant-id`, `adls.client-id`, `adls.client-secret` | Service principal settings for Microsoft Entra ID. |
| `adls.authority-host` | Optional Entra ID authority host for the service principal. |
| `adls.connection-string` | Supplying this property returns `FeatureUnsupported`. |

Supply Azure environment credentials through the corresponding `adls.*` properties.
The OpenDAL signer builds its Azure configuration from these properties and the location.

### Hugging Face

Enable `opendal-hf` and use `OpenDalStorageFactory::Hf`. Include the repository type in
the URI, such as `hf://datasets/organization/warehouse@main/tables/data.parquet`.
The supported types are `datasets`, `models`, `spaces`, and `buckets`.

| Property | Behavior |
| --- | --- |
| `hf.endpoint` | Hub base URL. Takes priority over `HF_ENDPOINT`. Default `https://huggingface.co`. |
| `hf.revision` | Default branch, tag, or commit for repository locations. A revision in the URI takes priority. Default `main`. |
| `hf.token` | Explicit Hub credential. Takes priority over implicit discovery. |

Implicit discovery tries `HF_TOKEN` and then the credential file. `HF_TOKEN_PATH`
sets that file's path. Otherwise the client looks for `token` in `HF_HOME`,
`$XDG_CACHE_HOME/huggingface`, or `~/.cache/huggingface`, in that order of directory
selection. A nonempty `HF_HUB_DISABLE_IMPLICIT_TOKEN` skips implicit discovery.
Public repositories can be read anonymously. Writes and deletes require a credential
with permission for the target repository or bucket.

### HDFS

Enable `opendal-hdfs-native` and use `OpenDalStorageFactory::HdfsNative`.
A location with a port, such as `hdfs://nn.example:8020/warehouse/data.parquet`,
selects that NameNode directly.

| Property | Behavior |
| --- | --- |
| `hdfs.name-node.<nameservice>` | Comma-separated NameNode endpoints for a portless authority. For example, set `hdfs.name-node.warehouse` to `nn1.example:8020,nn2.example:8020` for `hdfs://warehouse/tables/data.parquet`. |
| `hdfs.name-node` | NameNode endpoint or comma-separated failover list for locations with no authority, such as `hdfs:///warehouse/data.parquet`. |
| `hadoop.fs.defaultFS` | Fallback endpoint for authority-less locations when `hdfs.name-node` is unset. Use an HDFS host and port. |
| `hdfs.host`, `hdfs.port` | Legacy fallback for authority-less locations. The port defaults to `8020` when a host is supplied. An explicit `hadoop.fs.defaultFS` takes priority. |
| `hadoop.*` | Forward a Hadoop configuration property with the prefix stripped. Explicit `hadoop.dfs.ha.namenodes.<nameservice>` and `hadoop.dfs.namenode.rpc-address.<nameservice>.<id>` settings take priority over the generated nameservice entries. |

The native client loads `core-site.xml` and `hdfs-site.xml` from `HADOOP_CONF_DIR`,
falling back to `$HADOOP_HOME/etc/hadoop`.
Declare portless authorities through the properties above so the Iceberg resolver can
select the NameNodes. For simple authentication the client reads `HADOOP_USER_NAME`.
For Kerberos it uses the local credential cache. The properties `hdfs.user` and
`hdfs.kerberos_ticket` are ignored with a warning.
