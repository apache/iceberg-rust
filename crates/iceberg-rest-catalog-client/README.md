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

# Rust API client for openapi

Defines the specification for the first version of the REST Catalog API. Implementations should ideally support both Iceberg table specs v1 and v2, with priority given to v2.

## Installation

Put the package under your project folder in a directory named `openapi` and add the following to `Cargo.toml` under `[dependencies]`:

```
openapi = { path = "./openapi" }
```

## Contributing

### Generate Code

To regenerate the client, run the following command:

```shell
export API_VERSION=1.3.1

curl "https://raw.githubusercontent.com/apache/iceberg/apache-iceberg-${API_VERSION}/open-api/rest-catalog-open-api.yaml" -o rest-catalog-open-api.yaml
openapi-generator generate \
    -g rust \
    -i rest-catalog-open-api.yaml \
    -o .
```

NOTES:

- `API_VERSION` should be the latest released version of iceberg.
- `PACKAGE_VERSION` should be changed based on need.
- `openapi-generator` doesn't respect `HTTPS_PROXY`, users behind a proxy should use `curl` to download specs locally instead.

### Post-Generation Actions

After generation, please take following actions to make sure code is compilable:

- Code format: `cargo fmt`
- Fix licenses: `license-eye header fix`
