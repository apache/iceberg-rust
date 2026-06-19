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

//! Real-data, cross-engine validation of V3 deletion-vector writes.
//!
//! Writes a `deletion-vector-v1` to a REAL Iceberg table via a REST catalog
//! (Polaris), so an INDEPENDENT engine (Doris / Spark / DuckDB) can read the
//! table back and confirm the deletes were applied. This is the gate before
//! opening the upstream RowDelta MoR DV-write PR (#2203).
//!
//! Prereqs: a clean V3 table with one data file and NO pre-existing deletion
//! vector (create it via Doris/Spark/DuckDB first), and a port-forward to
//! Polaris. Run from a host with S3 access for the warehouse bucket.
//!
//! ```bash
//! kubectl port-forward svc/polaris -n pulse-data 8181:8181 &
//! POLARIS_URI=http://localhost:8181/api/catalog \
//! POLARIS_CREDENTIAL="$(kubectl get secret -n pulse-compute polaris-svc-spark \
//!     -o jsonpath='{.data.client-id}' | base64 -d):$(kubectl get secret -n \
//!     pulse-compute polaris-svc-spark -o jsonpath='{.data.client-secret}' | base64 -d)" \
//! POLARIS_WAREHOUSE=bronze_dbnew \
//! DV_NAMESPACE=zz_compactbench DV_TABLE=zz_rust_dv_test DV_DELETE_COUNT=3 \
//!   cargo run -p iceberg-catalog-rest --example pulse_dv_realdata
//! ```
//!
//! Then verify in Doris: `SELECT COUNT(*) FROM bronze_dbnew.zz_compactbench.zz_rust_dv_test;`
//! should drop by `DV_DELETE_COUNT`.

use std::collections::HashMap;

use iceberg::delete_vector::DeleteVector;
use iceberg::spec::DataContentType;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let uri = std::env::var("POLARIS_URI")?;
    let credential = std::env::var("POLARIS_CREDENTIAL")?;
    let warehouse = std::env::var("POLARIS_WAREHOUSE").unwrap_or_else(|_| "bronze_dbnew".into());
    let namespace = std::env::var("DV_NAMESPACE").unwrap_or_else(|_| "zz_compactbench".into());
    let table_name = std::env::var("DV_TABLE").unwrap_or_else(|_| "zz_rust_dv_test".into());
    let delete_count: u64 = std::env::var("DV_DELETE_COUNT")
        .unwrap_or_else(|_| "3".into())
        .parse()?;

    // --- connect to Polaris (REST + OAuth2; S3 creds vended by the catalog) ---
    let mut props = HashMap::new();
    props.insert("uri".to_string(), uri.clone());
    props.insert("warehouse".to_string(), warehouse);
    props.insert("credential".to_string(), credential);
    props.insert("scope".to_string(), "PRINCIPAL_ROLE:ALL".to_string());
    props.insert(
        "oauth2-server-uri".to_string(),
        format!("{}/v1/oauth/tokens", uri.trim_end_matches('/')),
    );
    let catalog = RestCatalogBuilder::default().load("polaris", props).await?;

    let ident = TableIdent::from_strs([namespace.as_str(), table_name.as_str()])?;
    let table = catalog.load_table(&ident).await?;
    println!(
        "loaded {ident:?} (format_version={:?})",
        table.metadata().format_version()
    );

    // --- find a live Data file in the current snapshot ---
    let snapshot = table
        .metadata()
        .current_snapshot()
        .ok_or("table has no current snapshot")?;
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await?;
    let mut chosen = None;
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        for entry in manifest.entries() {
            if entry.is_alive() && entry.data_file().content_type() == DataContentType::Data {
                chosen = Some(entry.data_file().clone());
                break;
            }
        }
        if chosen.is_some() {
            break;
        }
    }
    let data_file = chosen.ok_or("no live data file found in the table")?;
    let referenced = data_file.file_path().to_string();
    let total_rows = data_file.record_count();
    println!("target data file: {referenced} ({total_rows} rows)");
    if delete_count > total_rows {
        return Err(format!("DV_DELETE_COUNT {delete_count} > rows in file {total_rows}").into());
    }

    // --- build a DV deleting the first `delete_count` positions ---
    let mut dv = DeleteVector::default();
    for pos in 0..delete_count {
        dv.insert(pos);
    }

    // --- write the DV to a Puffin file and build the PositionDeletes DataFile ---
    let dv_location = format!(
        "{}/data/rust-dv-{}.puffin",
        table.metadata().location().trim_end_matches('/'),
        Uuid::now_v7()
    );
    let dv_data_file = dv
        .write_to_puffin_file(
            table.file_io(),
            dv_location.clone(),
            referenced.clone(),
            data_file.partition().clone(),
            table.metadata().default_partition_spec_id(),
        )
        .await?;
    println!("wrote deletion vector puffin: {dv_location}");

    // --- commit via RowDelta -> content=Deletes manifest + Operation::Delete ---
    let tx = Transaction::new(&table);
    let action = tx.row_delta().add_delete_files(vec![dv_data_file]);
    let tx = action.apply(tx)?;
    let updated = tx.commit(&catalog).await?;

    println!(
        "COMMITTED. new snapshot_id={:?}. Now verify with an independent engine \
         (Doris/Spark): COUNT(*) should be (previous count - {delete_count}).",
        updated.metadata().current_snapshot_id()
    );
    Ok(())
}
