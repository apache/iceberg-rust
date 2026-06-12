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

//! End-to-end distributed Iceberg write/read test.
//!
//! Like the other iceberg-rust integration tests, this runs against the shared
//! docker fixture (an Iceberg REST catalog + MinIO). The `make test` target
//! brings the fixture up automatically; to run it on its own:
//!
//! ```bash
//! cd iceberg-rust && make docker-up
//! cargo test -p iceberg-ballista --test distributed_read_write
//! ```
//!
//! The endpoints can be overridden with the `ICEBERG_REST_URI` and
//! `ICEBERG_S3_ENDPOINT` environment variables.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{AsArray, Int64Array, RecordBatch};
use arrow::datatypes::Int32Type;
use ballista::datafusion::execution::{SessionState, SessionStateBuilder};
use ballista::datafusion::prelude::{SessionConfig, SessionContext};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_executor::new_standalone_executor_from_state;
use ballista_scheduler::standalone::new_standalone_scheduler_from_state;
use iceberg_ballista::{
    IcebergCatalogConfig, register_iceberg_catalog, register_iceberg_codecs,
    register_iceberg_table,
};
use iceberg::spec::{
    NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionField,
    UnboundPartitionSpec,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;
use tokio::sync::Mutex;

/// Serializes the catalog-mutating tests in this binary. The REST fixture is
/// backed by in-memory SQLite, which rejects concurrent commits with
/// `SQLITE_BUSY`. Each test still exercises parallelism *internally* (multiple
/// write tasks / executors); this only stops the two test cases from committing
/// to the catalog at the same time, so the suite is robust under a parallel test
/// harness (`cargo test`, `nextest`) without relying on `--test-threads=1`.
static CATALOG_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Table name unique per run, so reruns don't collide in the shared catalog.
fn unique_table_name(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{prefix}_{millis}")
}

/// Session state with the Iceberg codecs installed.
fn iceberg_session_state(config: SessionConfig) -> SessionState {
    SessionStateBuilder::new()
        .with_config(register_iceberg_codecs(config))
        .with_default_features()
        .build()
}

/// Runs a SQL statement to completion and returns its batches.
async fn run_sql(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .expect("plan sql")
        .collect()
        .await
        .expect("run sql")
}

/// Extracts the single `i64` value of the first column (a COUNT result).
fn single_i64(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .find_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(0))
        })
        .expect("i64 column")
}

/// Flattens the first column of every batch into a `Vec<i32>`.
fn i32_values(batches: &[RecordBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int32Type>().values().to_vec())
        .collect()
}

fn catalog_props() -> HashMap<String, String> {
    let rest_uri =
        std::env::var("ICEBERG_REST_URI").unwrap_or_else(|_| "http://localhost:8181".to_string());
    let s3_endpoint = std::env::var("ICEBERG_S3_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:9000".to_string());
    HashMap::from([
        ("uri".to_string(), rest_uri),
        ("s3.endpoint".to_string(), s3_endpoint),
        ("s3.access-key-id".to_string(), "admin".to_string()),
        ("s3.secret-access-key".to_string(), "password".to_string()),
        ("s3.region".to_string(), "us-east-1".to_string()),
        ("s3.path-style-access".to_string(), "true".to_string()),
    ])
}

async fn build_rest_catalog(props: &HashMap<String, String>) -> impl Catalog + use<> {
    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", props.clone())
        .await
        .expect("build rest catalog")
}

/// Ensures the shared test namespace exists, tolerating a concurrent creator
/// (tests run in parallel and share this namespace).
async fn ensure_namespace(catalog: &impl Catalog) -> NamespaceIdent {
    let namespace = NamespaceIdent::new("ballista_it".to_string());
    if !catalog.namespace_exists(&namespace).await.unwrap()
        && let Err(e) = catalog.create_namespace(&namespace, HashMap::new()).await
        && !catalog.namespace_exists(&namespace).await.unwrap()
    {
        panic!("create namespace: {e}");
    }
    namespace
}

async fn create_table(props: &HashMap<String, String>, table_name: &str) -> NamespaceIdent {
    let catalog = build_rest_catalog(props).await;
    let namespace = ensure_namespace(&catalog).await;

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();
    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(schema)
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");

    namespace
}

/// Creates a table partitioned by `region` (identity). A distributed INSERT then
/// fans the rows out to one writer per region — exercising the partition-value
/// expression (`PartitionExpr`) serialization across the cluster.
async fn create_partitioned_table(
    props: &HashMap<String, String>,
    table_name: &str,
) -> NamespaceIdent {
    let catalog = build_rest_catalog(props).await;
    let namespace = ensure_namespace(&catalog).await;

    // Optional (nullable) fields so the schema matches the nullable columns a
    // `VALUES` source produces; the partitioned-write path checks nullability.
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "region", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();
    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        // The REST catalog requires an explicit partition field-id; the in-memory
        // catalog used elsewhere assigns one automatically, but REST does not.
        .add_partition_fields([UnboundPartitionField {
            source_id: 2,
            field_id: Some(1000),
            name: "region".to_string(),
            transform: Transform::Identity,
        }])
        .unwrap()
        .build();
    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create partitioned table");

    namespace
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_insert_and_read() {
    let _ = env_logger::builder().is_test(true).try_init();
    let _catalog_guard = CATALOG_GUARD.lock().await;

    let props = catalog_props();
    let table_name = unique_table_name("events");
    let namespace = create_table(&props, &table_name).await;

    let state = iceberg_session_state(
        SessionConfig::new_with_ballista()
            .with_target_partitions(2)
            .with_ballista_standalone_parallelism(2),
    );
    let ctx = SessionContext::standalone_with_state(state)
        .await
        .expect("start standalone ballista");

    let catalog_config = IcebergCatalogConfig::new("rest", "rest", props.clone());
    register_iceberg_table(&ctx, "events", catalog_config, namespace, table_name.clone())
        .await
        .expect("register iceberg table");

    run_sql(
        &ctx,
        "INSERT INTO events VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')",
    )
    .await;

    let count = single_i64(&run_sql(&ctx, "SELECT count(*) AS n FROM events").await);
    assert_eq!(count, 3, "expected 3 rows after distributed insert");

    let rows = run_sql(&ctx, "SELECT id, name FROM events ORDER BY id").await;
    assert_eq!(i32_values(&rows), vec![1, 2, 3]);

    // Catalog-level registration: mount the whole Iceberg catalog and read the
    // same table as `<catalog>.<namespace>.<table>`. The providers built through
    // the catalog carry the config too, so this distributed read exercises the
    // catalog/schema config-threading path end to end.
    register_iceberg_catalog(&ctx, "ice", IcebergCatalogConfig::new("rest", "rest", props))
        .await
        .expect("register iceberg catalog");
    let count = single_i64(
        &run_sql(
            &ctx,
            &format!("SELECT count(*) AS n FROM ice.ballista_it.{table_name}"),
        )
        .await,
    );
    assert_eq!(count, 3, "catalog-qualified distributed read");
}

/// Distributed correctness on a real multi-executor cluster, writing a
/// **partitioned** table.
///
/// Where [`distributed_insert_and_read`] uses standalone Ballista (one in-process
/// executor) and an unpartitioned table, this stands up a single scheduler with
/// **several in-process executors** and writes a table partitioned by `region`. A
/// partitioned write injects a partition-value expression (`PartitionExpr`) into
/// the physical plan, so this exercises that expression's serialization through
/// the codec on top of the plan-node serialization — and fans the rows out to one
/// writer per region across the executors.
///
/// The assertions target the correctness properties of a distributed, multi-writer
/// write:
///   1. the write commits exactly **one** atomic snapshot (not one per task),
///   2. the parallel writers contributed multiple data files (one per region), and
///   3. every input row lands exactly once (no loss, no duplication).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parallel_multi_executor_insert_commits_all_rows() {
    let _ = env_logger::builder().is_test(true).try_init();
    let _catalog_guard = CATALOG_GUARD.lock().await;

    const N_EXECUTORS: usize = 2;
    const SLOTS_PER_EXECUTOR: usize = 2;
    const WRITE_PARTITIONS: usize = 8;
    const REGIONS: [&str; 4] = ["a", "b", "c", "d"];
    // REGIONS.len() * 3.
    const TOTAL_ROWS: i32 = 12;

    let props = catalog_props();
    let table_name = unique_table_name("parallel_events");
    let namespace = create_partitioned_table(&props, &table_name).await;

    // --- Bring up one scheduler + N executors in-process (real multi-executor) ---
    let state = iceberg_session_state(
        SessionConfig::new_with_ballista().with_target_partitions(WRITE_PARTITIONS),
    );

    let scheduler_addr = new_standalone_scheduler_from_state(&state)
        .await
        .expect("start scheduler");
    let scheduler_url = format!("http://localhost:{}", scheduler_addr.port());

    let scheduler_client = loop {
        match SchedulerGrpcClient::connect(scheduler_url.clone()).await {
            Ok(client) => break client,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    };

    // Each executor is a separate service; the scheduler load-balances across
    // them via pull-based scheduling.
    for _ in 0..N_EXECUTORS {
        new_standalone_executor_from_state(scheduler_client.clone(), SLOTS_PER_EXECUTOR, &state)
            .await
            .expect("start executor");
    }

    let ctx = SessionContext::remote_with_state(&scheduler_url, state)
        .await
        .expect("connect to scheduler");

    let catalog_config = IcebergCatalogConfig::new("rest", "rest", props.clone());
    register_iceberg_table(
        &ctx,
        "target",
        catalog_config,
        namespace.clone(),
        table_name.clone(),
    )
    .await
    .expect("register iceberg table");

    // --- Distributed, partitioned INSERT across the multi-executor cluster ---
    // A serializable VALUES source whose rows span every region; the partitioned
    // write fans them out to one writer per region across the executors, which is
    // the path that injects PartitionExpr into the physical plan. (A registered
    // MemTable would not work here — it's a custom TableProvider that Ballista
    // cannot serialize into the logical plan.)
    let values = (0..TOTAL_ROWS)
        .map(|i| {
            let region = REGIONS[i as usize % REGIONS.len()];
            format!("({}, '{region}')", i + 1)
        })
        .collect::<Vec<_>>()
        .join(", ");
    run_sql(&ctx, &format!("INSERT INTO target VALUES {values}")).await;

    // (1) The distributed write committed exactly once (a single atomic
    // snapshot), not one commit per task. Checked straight from the catalog so
    // it isolates write correctness from the distributed read-back below.
    let catalog = build_rest_catalog(&props).await;
    let table = catalog
        .load_table(&TableIdent::new(namespace.clone(), table_name.clone()))
        .await
        .expect("load committed table");
    assert_eq!(
        table.metadata().snapshots().count(),
        1,
        "the distributed write must produce exactly one atomic commit"
    );

    // (2) The parallel writers each produced a data file (one per region), all
    // coalesced into that single commit.
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("current snapshot");
    let mut data_files = 0usize;
    let manifest_list = table
        .manifest_list_reader(snapshot)
        .load()
        .await
        .expect("load manifest list");
    for entry in manifest_list.entries() {
        let manifest = entry
            .load_manifest(table.file_io())
            .await
            .expect("load manifest");
        data_files += manifest.entries().len();
    }
    assert!(
        data_files >= 2,
        "partitioned write should produce multiple data files, got {data_files}"
    );

    // (3) Every row landed exactly once.
    let count = single_i64(&run_sql(&ctx, "SELECT count(*) AS n FROM target").await);
    assert_eq!(count as i32, TOTAL_ROWS, "row count after parallel insert");

    let ids = i32_values(&run_sql(&ctx, "SELECT id FROM target ORDER BY id").await);
    assert_eq!(
        ids,
        (1..=TOTAL_ROWS).collect::<Vec<_>>(),
        "exact id set after parallel insert (no lost or duplicated rows)"
    );

    // (4) Predicate pushdown survives serialization: a WHERE clause is pushed into
    // the distributed scan (and re-applied above it), and the result is correct.
    let half = TOTAL_ROWS / 2;
    let filtered_ids = i32_values(
        &run_sql(
            &ctx,
            &format!("SELECT id FROM target WHERE id <= {half} ORDER BY id"),
        )
        .await,
    );
    assert_eq!(
        filtered_ids,
        (1..=half).collect::<Vec<_>>(),
        "predicate-filtered distributed read"
    );
}
