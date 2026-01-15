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

# DataFusion Integration

The `iceberg-datafusion` crate provides integration between Apache Iceberg and [DataFusion](https://datafusion.apache.org/), enabling SQL queries on Iceberg tables.

## Features

- **SQL DDL/DML**: `CREATE TABLE`, `INSERT INTO`, `SELECT`
- **Query Optimization**: Projection, filter, and LIMIT pushdown
- **Metadata Tables**: Query snapshots and manifests
- **Partitioned Tables**: Automatic partition routing for writes

## Dependencies

Add the following to your `Cargo.toml`:

```toml
[dependencies]
iceberg = "0.8"
iceberg-datafusion = "0.8"
datafusion = "51"
tokio = { version = "1", features = ["full"] }
```

## Catalog-Based Access

The recommended way to use Iceberg with DataFusion is through `IcebergCatalogProvider`, which integrates an Iceberg catalog with DataFusion's catalog system.

### Setup

```rust,no_run,noplayground
{{#rustdoc_include ../../crates/examples/src/datafusion_integration.rs:catalog_setup}}
```

### Creating Tables

Once the catalog is registered, you can create tables using SQL:

```rust,no_run,noplayground
{{#rustdoc_include ../../crates/examples/src/datafusion_integration.rs:create_table}}
```

Supported column types include:
- `INT`, `BIGINT` - Integer types
- `FLOAT`, `DOUBLE` - Floating point types
- `STRING` - String/text type
- `BOOLEAN` - Boolean type
- `DATE`, `TIMESTAMP` - Date/time types

> **Note**: `CREATE TABLE AS SELECT` is not currently supported. Create the table first, then use `INSERT INTO`.

### Inserting Data

```rust,no_run,noplayground
{{#rustdoc_include ../../crates/examples/src/datafusion_integration.rs:insert_data}}
```

For nested structs, use the `named_struct()` function:

```sql
INSERT INTO catalog.namespace.table
SELECT
    1 as id,
    named_struct('street', '123 Main St', 'city', 'NYC') as address
```

### Querying Data

```rust,no_run,noplayground
{{#rustdoc_include ../../crates/examples/src/datafusion_integration.rs:query_data}}
```

## Metadata Tables

Iceberg metadata tables can be queried using the `$` syntax (following Flink convention):

```rust,no_run,noplayground
{{#rustdoc_include ../../crates/examples/src/datafusion_integration.rs:metadata_tables}}
```

Available metadata tables:
- `table$snapshots` - Table snapshot history
- `table$manifests` - Manifest file information

## File-Based Access (External Tables)

For reading existing Iceberg tables without a catalog, use `IcebergTableProviderFactory`:

```rust,no_run,noplayground
{{#rustdoc_include ../../crates/examples/src/datafusion_integration.rs:external_table_setup}}
```

Then create external tables via SQL:

```sql
CREATE EXTERNAL TABLE my_table
STORED AS ICEBERG
LOCATION '/path/to/iceberg/metadata/v1.metadata.json';

SELECT * FROM my_table;
```

> **Note**: External tables are read-only. For write operations, use `IcebergCatalogProvider`.

## Table Provider Types

### IcebergTableProvider

- Backed by an Iceberg catalog
- Automatically refreshes metadata on each operation
- Supports both read and write operations
- Use when you need the latest table state or write capability

### IcebergStaticTableProvider

- Fixed table snapshot at construction time
- No catalog round-trips (better performance)
- Read-only
- Use for time-travel queries or when consistency within a query is important

## Partitioned Tables

### Creating Partitioned Tables

Partitioned tables must be created using the Iceberg catalog API (not SQL):

```rust,no_run
use iceberg::spec::{Transform, UnboundPartitionSpec};

let partition_spec = UnboundPartitionSpec::builder()
    .with_spec_id(0)
    .add_partition_field(column_id, "partition_column", Transform::Identity)?
    .build();
```

Supported partition transforms:
- `Identity` - Partition by exact value
- `Year`, `Month`, `Day`, `Hour` - Time-based partitioning
- `Bucket(n)` - Hash partitioning into n buckets
- `Truncate(width)` - String/number truncation

### Writing to Partitioned Tables

When inserting into a partitioned table, data is automatically routed to the correct partition directories:

```sql
INSERT INTO catalog.namespace.partitioned_table VALUES
    (1, 'electronics', 'laptop'),
    (2, 'books', 'novel');
-- Data files will be created under:
--   data/category=electronics/
--   data/category=books/
```

### Write Modes

Two write modes are available for partitioned tables:

| Mode | Property Value | Description |
|------|---------------|-------------|
| **Fanout** (default) | `true` | Handles unsorted data, maintains open writers for all partitions |
| **Clustered** | `false` | Requires sorted input, more memory efficient |

Configure via table property:
```
write.datafusion.fanout.enabled = true
```

## Query Optimization

The DataFusion integration supports several query optimizations:

- **Projection Pushdown**: Only reads columns referenced in the query
- **Filter Pushdown**: Prunes data files using manifest statistics
- **LIMIT Pushdown**: Reduces the amount of data scanned

These optimizations are applied automatically by the query planner.

## Configuration Options

| Property | Default | Description |
|----------|---------|-------------|
| `write.datafusion.fanout.enabled` | `true` | Use FanoutWriter (true) or ClusteredWriter (false) for partitioned writes |
| `write.target-file-size-bytes` | `536870912` (512MB) | Target size for data files |
| `write.format.default` | `parquet` | Default file format for new data files |

## Current Limitations

- `CREATE TABLE AS SELECT` is not supported
- Metadata tables are limited to `$snapshots` and `$manifests`
- `ALTER TABLE` and `DROP TABLE` via SQL are not supported (use catalog API)
- Schema evolution through SQL is not supported

## Running the Example

A complete example is available in the repository:

```bash
cargo run -p iceberg-examples --example datafusion-integration
```
