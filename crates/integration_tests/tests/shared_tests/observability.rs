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

use futures::TryStreamExt;
use iceberg::expr::Reference;
use iceberg::spec::Datum;
use iceberg::{Catalog, Error as IcebergError, TableIdent};
use iceberg_catalog_rest::RestCatalog;
use init_tracing_opentelemetry::tracing_subscriber_ext::TracingGuard;
use metrics_exporter_prometheus::PrometheusBuilder;

use crate::get_shared_containers_no_tracing_sub;

#[tokio::test]
async fn test_observability() -> Result<(), IcebergError> {
    let _guard = configure_o11y_exports();

    let fixture = get_shared_containers_no_tracing_sub();
    let rest_catalog = RestCatalog::new(fixture.catalog_config.clone());

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "nyc_taxi_trips"]).unwrap())
        .await?;

    let predicate = Reference::new("vendor_id").equal_to(Datum::long(1));

    let scan = table.scan().with_filter(predicate).build()?;

    let results = scan.to_arrow().await?.try_collect::<Vec<_>>().await?;
    assert!(!results.is_empty());

    // flush OTel OTLP traces to Jaeger
    drop(_guard);
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    // TODO: confirm traces are present in Jaeger

    // TODO: check metrics are present in Prometheus
    Ok(())
}

fn configure_o11y_exports() -> TracingGuard {
    // RUST_LOG needs to contain otel::tracing=trace for traces to be exported.
    unsafe {
        std::env::set_var(
            "RUST_LOG",
            "info,iceberg=trace,otel::tracing=trace,otel=debug",
        )
    };

    // Set OTEL_SERVICE_NAME to identify the service in Jaeger
    unsafe { std::env::set_var("OTEL_SERVICE_NAME", "test") };

    // Set OTEL_EXPORTER_OTLP_ENDPOINT to export traces to Jaeger container
    unsafe { std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "grpc://localhost:4317") };

    let _guard = init_tracing_opentelemetry::tracing_subscriber_ext::init_subscribers()
        .expect("could not set up tracing-otel");

    PrometheusBuilder::new()
        .install()
        .expect("could not set up prometheus metrics exporter");

    _guard
}
