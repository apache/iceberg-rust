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

//! Example demonstrating incremental reads with DataFusion.
//!
//! Incremental reads allow you to scan only the data that was added between
//! two snapshots. This is useful for:
//! - Change data capture (CDC) pipelines
//! - Incremental data processing
//! - Efficiently reading only new data since last checkpoint
//!
//! # Prerequisites
//!
//! This example requires a running iceberg-rest catalog on port 8181 with
//! a table that has multiple snapshots. You can set this up using the official
//! [quickstart documentation](https://iceberg.apache.org/spark-quickstart/).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};
use iceberg_datafusion::IcebergStaticTableProvider;

static REST_URI: &str = "http://localhost:8181";
static NAMESPACE: &str = "default";
static TABLE_NAME: &str = "incremental_test";

/// This example demonstrates how to perform incremental reads using DataFusion.
///
/// Incremental reads scan only the data files that were added between two snapshots,
/// which is much more efficient than scanning the entire table when you only need
/// the new data.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create the REST iceberg catalog
    let catalog = RestCatalogBuilder::default()
        .load(
            "rest",
            HashMap::from([(REST_CATALOG_PROP_URI.to_string(), REST_URI.to_string())]),
        )
        .await?;

    // Load the table
    let table_ident = TableIdent::from_strs([NAMESPACE, TABLE_NAME])?;
    let table = catalog.load_table(&table_ident).await?;

    // Get available snapshots
    let snapshots: Vec<_> = table.metadata().snapshots().collect();
    println!("Table has {} snapshots:", snapshots.len());
    for snapshot in &snapshots {
        println!(
            "  - Snapshot {}: {:?}",
            snapshot.snapshot_id(),
            snapshot.summary().operation
        );
    }

    if snapshots.len() < 2 {
        println!("\nNeed at least 2 snapshots for incremental read demo.");
        println!("Try inserting some data into the table to create more snapshots.");
        return Ok(());
    }

    // Get the first and last snapshot IDs
    let from_snapshot_id = snapshots[0].snapshot_id();
    let to_snapshot_id = snapshots[snapshots.len() - 1].snapshot_id();

    println!(
        "\nPerforming incremental read from snapshot {} to {}",
        from_snapshot_id, to_snapshot_id
    );

    // ANCHOR: incremental_read
    // Create a DataFusion session
    let ctx = SessionContext::new();

    // Method 1: Scan changes between two specific snapshots (exclusive from)
    // This returns only data added AFTER from_snapshot_id up to and including to_snapshot_id
    let provider = IcebergStaticTableProvider::try_new_incremental(
        table.clone(),
        from_snapshot_id,
        to_snapshot_id,
    )
    .await?;

    ctx.register_table("incremental_changes", Arc::new(provider))?;

    // Query the incremental changes
    let df = ctx
        .sql("SELECT * FROM incremental_changes LIMIT 10")
        .await?;
    println!("\nIncremental changes (first 10 rows):");
    df.show().await?;
    // ANCHOR_END: incremental_read

    // ANCHOR: appends_after
    // Method 2: Scan all appends after a specific snapshot up to current
    // Useful for "give me all new data since my last checkpoint"
    let provider =
        IcebergStaticTableProvider::try_new_appends_after(table.clone(), from_snapshot_id).await?;

    ctx.register_table("new_data", Arc::new(provider))?;

    let df = ctx.sql("SELECT COUNT(*) as new_rows FROM new_data").await?;
    println!("\nNew rows since snapshot {}:", from_snapshot_id);
    df.show().await?;
    // ANCHOR_END: appends_after

    // ANCHOR: incremental_inclusive
    // Method 3: Inclusive incremental read (includes the from_snapshot)
    let provider = IcebergStaticTableProvider::try_new_incremental_inclusive(
        table.clone(),
        from_snapshot_id,
        to_snapshot_id,
    )
    .await?;

    ctx.register_table("inclusive_changes", Arc::new(provider))?;

    let df = ctx
        .sql("SELECT COUNT(*) as total_rows FROM inclusive_changes")
        .await?;
    println!("\nRows including from_snapshot:");
    df.show().await?;
    // ANCHOR_END: incremental_inclusive

    // ANCHOR: with_filters
    // You can combine incremental reads with filters and projections
    let provider =
        IcebergStaticTableProvider::try_new_appends_after(table.clone(), from_snapshot_id).await?;

    ctx.register_table("filtered_changes", Arc::new(provider))?;

    // Example: Get only specific columns with a filter
    // (adjust column names based on your actual table schema)
    let df = ctx.sql("SELECT * FROM filtered_changes LIMIT 5").await?;
    println!("\nFiltered incremental data:");
    df.show().await?;
    // ANCHOR_END: with_filters

    println!("\nIncremental read example completed successfully!");

    Ok(())
}
