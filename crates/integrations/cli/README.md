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


# Introduction

Iceberg CLI (`iceberg-cli`) is a small command line utility that runs SQL queries against tables,
which is backed by the DataFusion engine.

## Supported Catalog Types

The CLI supports the following catalog types:

1. **REST Catalog**: Connect to an Iceberg REST catalog service
2. **S3Tables Catalog**: Connect to AWS S3Tables service

## Configuration

The CLI uses a configuration file (typically `~/.icebergrc`) to define catalogs. Here's how to configure different catalog types:

### REST Catalog Configuration

```toml
[[catalogs]]
name = "local_rest_catalog"
type = "rest"

[catalogs.config]
uri = "http://localhost:8181"
warehouse = "file:///tmp/iceberg_warehouse"

[catalogs.config.props]
aws_region = "<region>"
```

### S3Tables Catalog Configuration

```toml
[[catalogs]]
name = "s3tables_catalog"
type = "s3tables"

[catalogs.config]
endpoint_url = "https://s3tables.<region>.amazonaws.com"
table_bucket_arn = "arn:aws:s3:<region>:<account>:bucket/<bucket_name>"

[catalogs.config.props]
aws_region = "<region>"
"s3.region" = "<region>"
"s3.access-key-id" = "<access-key-id>"
"s3.secret-access-key" = "<secret-access-key>"
"s3.session-token" = "<session-token>"
profile_name = "<profile>"
```

## Usage

Once configured, you can use the CLI to query tables from your catalogs:

```bash
# Start the CLI
cargo run --package iceberg-cli --bin iceberg-cli

# List tables
SHOW TABLES;

# Query a table
SELECT * FROM my_catalog.my_namespace.my_table LIMIT 10;
```

