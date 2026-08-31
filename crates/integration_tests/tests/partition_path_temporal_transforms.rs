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

//! Regression test for the partition path format of the temporal transforms.
//!
//! The unit tests in `spec::transform` assert against constants transcribed from
//! `TransformUtil` in the Java reference implementation, so they confirm the Rust
//! code matches a *reading* of Java rather than Java itself. This test closes that
//! loop: it renders a partition path from the partition values Java recorded in the
//! manifest, then asserts that path appears as a directory component of the data
//! file path Java chose for those same values.

use std::collections::HashSet;
use std::sync::Arc;

use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_integration_tests::get_test_fixture;
use iceberg_storage_opendal::OpenDalStorageFactory;

/// Provisioned by `dev/spark/provision.py`, partitioned by
/// `years(year_ts), months(month_ts), days(day_ts), hours(hour_ts)`.
const TABLE: &str = "test_temporal_partition_paths";

/// The path each provisioned row is expected to land in, one per rounding case.
///
/// Java's own output is the authority for these strings, and the per-file
/// assertion below checks against it. Restating them here pins down that the
/// fixture still holds the instants this test means to cover, so that a change to
/// `provision.py` cannot leave the test passing vacuously.
const EXPECTED_PATHS: [&str; 3] = [
    // Post-epoch: 2017-06-15T16:00:00.
    "year_ts_year=2017/month_ts_month=2017-06/day_ts_day=2017-06-15/hour_ts_hour=2017-06-15-16",
    // The epoch itself, where every ordinal is 0.
    "year_ts_year=1970/month_ts_month=1970-01/day_ts_day=1970-01-01/hour_ts_hour=1970-01-01-00",
    // Pre-epoch: 1969-12-31T23:00:00, where every ordinal is negative and
    // truncating division would round the wrong way.
    "year_ts_year=1969/month_ts_month=1969-12/day_ts_day=1969-12-31/hour_ts_hour=1969-12-31-23",
];

#[tokio::test]
async fn test_temporal_partition_paths_match_java() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", TABLE]).unwrap())
        .await
        .unwrap();

    let metadata = table.metadata();
    let snapshot = metadata
        .current_snapshot()
        .unwrap_or_else(|| panic!("`{TABLE}` should have a snapshot; is provisioning complete?"));
    let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

    let mut rendered = HashSet::new();
    for manifest_file in manifest_list.entries() {
        let manifest = table.manifest_reader().read(manifest_file).await.unwrap();

        // Render with the spec and schema the manifest was written against rather
        // than the current ones, so that evolving the fixture cannot silently start
        // rendering old files with a spec that never applied to them.
        let spec = metadata
            .partition_spec_by_id(manifest_file.partition_spec_id)
            .expect("manifest should reference a spec the table still holds")
            .clone();
        let schema = metadata
            .schema_by_id(manifest.metadata().schema_id())
            .expect("manifest should reference a schema the table still holds")
            .clone();

        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            let data_file = entry.data_file();
            let path = spec.partition_to_path(data_file.partition(), schema.clone());

            // Java lays a data file down at `<table location>/data/<partition
            // path>/<file name>`, so its own rendering of these same partition
            // values is embedded in the path it wrote.
            assert!(
                data_file.file_path().contains(&format!("/data/{path}/")),
                "Rust rendered the partition path `{path}`, which does not appear \
                 in the path Java wrote for the same partition values: `{}`",
                data_file.file_path()
            );

            rendered.insert(path);
        }
    }

    assert_eq!(
        rendered,
        HashSet::from(EXPECTED_PATHS.map(str::to_string)),
        "the set of partition paths in `{TABLE}` is not the set the fixture provisions"
    );
}
