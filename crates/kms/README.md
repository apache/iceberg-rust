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

# Apache Iceberg KMS

This crate provides key management service integrations for Apache Iceberg
client-side encryption. Provider implementations are enabled through feature
flags.

| Provider                   | Feature flag |
| -------------------------- | ------------ |
| AWS Key Management Service | `aws`        |

Add the crate with the feature for the provider you need:

```toml
[dependencies]
iceberg-kms = { version = "x.y.z", features = ["aws"] }
```

`ResolvingKmsClientFactory` can select an enabled provider from the
`encryption.kms-type` catalog property. Built-in values follow Iceberg Java:
`aws`, `azure`, and `gcp`. Currently, only `aws` has a Rust implementation.

The built-in mappings are fixed. Applications using a custom KMS provider can
supply their own implementation of `KmsClientFactory` directly to the catalog
builder. This replaces Java's reflection-based `encryption.kms-impl` extension
point.

See the [API documentation](https://docs.rs/iceberg-kms/latest) for
provider-specific configuration and usage.
