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

This crate contains a suite of [sqllogictest](https://crates.io/crates/sqllogictest) tests that are used to validate [iceberg-rust](https://github.com/apache/iceberg-rust).

## Running the tests

Just run the following command:

```bash
cargo test
```

## Sql Engines

The tests are run against the following sql engines:

* [Apache datafusion](https://crates.io/crates/datafusion)
* [Apache spark](https://github.com/apache/spark)

## Catalog Configuration

This crate now supports dynamic catalog configuration in test schedules. You can configure different types of catalogs (memory, rest, glue, sql, etc.) and share them across multiple engines.

### Configuration Format

```toml
# Define catalogs
[catalogs.memory_catalog]
type = "memory"
warehouse = "memory://warehouse"

[catalogs.rest_catalog]
type = "rest"
uri = "http://localhost:8181"
warehouse = "s3://my-bucket/warehouse"
credential = "client_credentials"
token = "xxx"

[catalogs.sql_catalog]
type = "sql"
uri = "postgresql://user:pass@localhost/iceberg"
warehouse = "s3://my-bucket/warehouse"
sql_bind_style = "DollarNumeric"

# Define engines with optional catalog references
[engines.datafusion1]
type = "datafusion"
catalog = "memory_catalog"  # Reference to a catalog

[engines.datafusion2]
type = "datafusion"
catalog = "rest_catalog"    # Multiple engines can share the same catalog

[engines.datafusion3]
type = "datafusion"
# No catalog specified - uses default MemoryCatalog

# Define test steps
[[steps]]
engine = "datafusion1"
slt = "path/to/test1.slt"

[[steps]]
engine = "datafusion2"
slt = "path/to/test2.slt"
```

### Supported Catalog Types

- `memory`: In-memory catalog for testing
- `rest`: REST catalog for integration testing
- `glue`: AWS Glue catalog
- `sql`: SQL database catalog
- `hms`: Hive Metastore catalog
- `s3tables`: S3 Tables catalog

### Key Features

- **Lazy Loading**: Catalogs are created only when first referenced
- **Sharing**: Multiple engines can share the same catalog instance
- **Backward Compatibility**: Existing schedules continue to work without modification
- **Type Safety**: Catalog configurations are validated at parse time