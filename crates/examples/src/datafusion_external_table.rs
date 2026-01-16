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

//! Example demonstrating external Iceberg table access via DataFusion.
//!
//! This example shows how to use `IcebergTableProviderFactory` to read
//! existing Iceberg tables via `CREATE EXTERNAL TABLE` syntax.
//!
//! Note: External tables are read-only. For write operations, use
//! `IcebergCatalogProvider` instead (see `datafusion_integration.rs`).

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use iceberg_datafusion::IcebergTableProviderFactory;

// ANCHOR: external_table_setup
/// Set up a DataFusion session with IcebergTableProviderFactory registered.
///
/// This allows reading existing Iceberg tables via `CREATE EXTERNAL TABLE` syntax.
fn setup_external_table_support() -> SessionContext {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = setup_external_table_support();

    // ANCHOR: external_table_query
    // Example SQL for creating and querying an external Iceberg table:
    //
    // CREATE EXTERNAL TABLE my_table
    // STORED AS ICEBERG
    // LOCATION '/path/to/iceberg/metadata/v1.metadata.json';
    //
    // SELECT * FROM my_table WHERE column > 100;
    // ANCHOR_END: external_table_query

    println!("External table support configured.");
    println!("Use CREATE EXTERNAL TABLE ... STORED AS ICEBERG to read existing tables.");
    println!();
    println!("Example:");
    println!("  CREATE EXTERNAL TABLE my_table");
    println!("  STORED AS ICEBERG");
    println!("  LOCATION '/path/to/iceberg/metadata/v1.metadata.json';");

    // This example requires an actual Iceberg table to query.
    // For a complete working example with table creation, see datafusion_integration.rs

    // Verify the session is configured correctly
    let tables = ctx.catalog_names();
    println!("\nRegistered catalogs: {tables:?}");

    Ok(())
}
