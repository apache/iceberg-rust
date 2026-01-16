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
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_datafusion::IcebergCatalogProvider;
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
    ctx.register_catalog("my_catalog", catalog_provider);
    // ANCHOR_END: catalog_setup

    // ANCHOR: create_table
    // Create a table using SQL
    ctx.sql(
        "CREATE TABLE my_catalog.demo.users (
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
            "INSERT INTO my_catalog.demo.users VALUES
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
        .sql("SELECT id, name, email FROM my_catalog.demo.users WHERE email IS NOT NULL")
        .await?;

    df.show().await?;

    // Query with projection (only specific columns)
    println!("\nQuerying only names:");
    let df = ctx
        .sql("SELECT name FROM my_catalog.demo.users ORDER BY id")
        .await?;

    df.show().await?;
    // ANCHOR_END: query_data

    // ANCHOR: metadata_tables
    // Query the snapshots metadata table
    println!("\nTable snapshots:");
    let df = ctx
        .sql("SELECT snapshot_id, operation FROM my_catalog.demo.users$snapshots")
        .await?;

    df.show().await?;

    // Query the manifests metadata table
    println!("\nTable manifests:");
    let df = ctx
        .sql("SELECT path, added_data_files_count FROM my_catalog.demo.users$manifests")
        .await?;

    df.show().await?;
    // ANCHOR_END: metadata_tables

    println!("\nDataFusion integration example completed successfully!");

    Ok(())
}
