// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Example demonstrating DataFusion integration with Apache Iceberg.
//!
//! This example shows how to:
//! - Set up an Iceberg catalog with DataFusion
//! - Create tables using SQL
//! - Insert and query data
//! - Query metadata tables

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_datafusion::{IcebergCatalogProvider, IcebergTableProviderFactory};
use tempfile::TempDir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the warehouse
    let temp_dir = TempDir::new()?;
    let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

    // ANCHOR: catalog_setup
    // Create an in-memory Iceberg catalog
    let iceberg_catalog = MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path)]),
        )
        .await?;

    // Create a namespace for our tables
    let namespace = NamespaceIdent::new("demo".to_string());
    iceberg_catalog
        .create_namespace(&namespace, HashMap::new())
        .await?;

    // Create the IcebergCatalogProvider and register it with DataFusion
    let catalog_provider =
        Arc::new(IcebergCatalogProvider::try_new(Arc::new(iceberg_catalog)).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("iceberg", catalog_provider);
    // ANCHOR_END: catalog_setup

    // ANCHOR: create_table
    // Create a table using SQL
    ctx.sql(
        "CREATE TABLE iceberg.demo.users (
            id INT NOT NULL,
            name STRING NOT NULL,
            email STRING
        )",
    )
    .await?;

    println!("Table 'users' created successfully.");
    // ANCHOR_END: create_table

    // ANCHOR: insert_data
    // Insert data into the table
    let result = ctx
        .sql(
            "INSERT INTO iceberg.demo.users VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com'),
            (3, 'Charlie', NULL)",
        )
        .await?
        .collect()
        .await?;

    // The result contains the number of rows inserted
    println!("Inserted {} rows.", result[0].num_rows());
    // ANCHOR_END: insert_data

    // ANCHOR: query_data
    // Query the data with filtering
    println!("\nQuerying users with email:");
    let df = ctx
        .sql("SELECT id, name, email FROM iceberg.demo.users WHERE email IS NOT NULL")
        .await?;

    df.show().await?;

    // Query with projection (only specific columns)
    println!("\nQuerying only names:");
    let df = ctx
        .sql("SELECT name FROM iceberg.demo.users ORDER BY id")
        .await?;

    df.show().await?;
    // ANCHOR_END: query_data

    // ANCHOR: metadata_tables
    // Query the snapshots metadata table
    println!("\nTable snapshots:");
    let df = ctx
        .sql("SELECT snapshot_id, operation FROM iceberg.demo.users$snapshots")
        .await?;

    df.show().await?;

    // Query the manifests metadata table
    println!("\nTable manifests:");
    let df = ctx
        .sql("SELECT path, added_data_files_count FROM iceberg.demo.users$manifests")
        .await?;

    df.show().await?;
    // ANCHOR_END: metadata_tables

    println!("\nDataFusion integration example completed successfully!");

    Ok(())
}

// ANCHOR: external_table_setup
/// Example of setting up IcebergTableProviderFactory for external tables.
///
/// This allows reading existing Iceberg tables via `CREATE EXTERNAL TABLE` syntax.
#[allow(dead_code)]
async fn setup_external_table_support() -> SessionContext {
    // Create a session state with the Iceberg table factory registered
    let mut state = SessionStateBuilder::new().with_default_features().build();

    // Register the IcebergTableProviderFactory to handle "ICEBERG" file type
    state.table_factories_mut().insert(
        "ICEBERG".to_string(),
        Arc::new(IcebergTableProviderFactory::new()),
    );

    SessionContext::new_with_state(state)
}
// ANCHOR_END: external_table_setup

// ANCHOR: external_table_query
/// Example SQL for creating and querying an external Iceberg table.
///
/// ```sql
/// -- Create an external table from an existing Iceberg metadata file
/// CREATE EXTERNAL TABLE my_table
/// STORED AS ICEBERG
/// LOCATION '/path/to/iceberg/metadata/v1.metadata.json';
///
/// -- Query the external table
/// SELECT * FROM my_table WHERE column > 100;
/// ```
#[allow(dead_code)]
fn external_table_sql_example() {}
// ANCHOR_END: external_table_query
