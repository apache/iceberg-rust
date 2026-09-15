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

//! REST pagination must be transparent to DataFusion catalog and schema discovery.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::CatalogProvider;
use datafusion::prelude::SessionContext;
use expect_test::expect;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{
    FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
    TableMetadataBuilder, Type,
};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};
use iceberg_datafusion::IcebergCatalogProvider;
use mockito::Server;
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn test_rest_page_size_through_datafusion() {
    let mut server = Server::new_async().await;
    let mut mocks = vec![
        server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(
                json!({
                    "defaults": {"rest-page-size": "3"},
                    "overrides": {"rest-page-size": "1"},
                })
                .to_string(),
            )
            .create_async()
            .await,
    ];

    for (token, namespace, next_token) in [("", "ns1", Some("next")), ("next", "ns2", None)] {
        mocks.push(
            server
                .mock("GET", "/v1/namespaces")
                .match_query(format!("pageSize=1&pageToken={token}").as_str())
                .with_status(200)
                .with_body(
                    json!({
                        "namespaces": [[namespace]],
                        "next-page-token": next_token,
                    })
                    .to_string(),
                )
                .create_async()
                .await,
        );
    }

    let temp_dir = TempDir::new().unwrap();
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    let partition_spec = PartitionSpec::builder(schema.clone()).build().unwrap();
    let sort_order = SortOrder::builder().build(&schema).unwrap();
    let metadata = TableMetadataBuilder::new(
        schema,
        partition_spec,
        sort_order,
        temp_dir.path().to_str().unwrap().to_string(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;

    for namespace in ["ns1", "ns2"] {
        let endpoint = format!("/v1/namespaces/{namespace}/tables");
        for (token, table, next_token) in [("", "t1", Some("next")), ("next", "t2", None)] {
            mocks.push(
                server
                    .mock("GET", endpoint.as_str())
                    .match_query(format!("pageSize=1&pageToken={token}").as_str())
                    .with_status(200)
                    .with_body(
                        json!({
                            "identifiers": [{"namespace": [namespace], "name": table}],
                            "next-page-token": next_token,
                        })
                        .to_string(),
                    )
                    .create_async()
                    .await,
            );
            mocks.push(
                server
                    .mock("GET", format!("{endpoint}/{table}").as_str())
                    .match_query(mockito::Matcher::Missing)
                    // Information-schema discovery may reload table metadata.
                    .expect_at_least(1)
                    .with_status(200)
                    .with_body(
                        json!({
                            "metadata-location": temp_dir.path().join("metadata.json"),
                            "metadata": metadata,
                        })
                        .to_string(),
                    )
                    .create_async()
                    .await,
            );
        }
    }

    let catalog = RestCatalogBuilder::default()
        .with_page_size(2)
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "rest",
            HashMap::from([(REST_CATALOG_PROP_URI.to_string(), server.url())]),
        )
        .await
        .unwrap();
    let provider = IcebergCatalogProvider::try_new(Arc::new(catalog))
        .await
        .unwrap();

    let mut schemas = provider.schema_names();
    schemas.sort();
    assert_eq!(schemas, ["ns1", "ns2"]);
    for namespace in &schemas {
        let schema = provider.schema(namespace).unwrap();
        for table in ["t1", "t2"] {
            assert!(schema.table_exist(table));
            assert!(schema.table(table).await.unwrap().is_some());
        }
    }

    let context = SessionContext::new_with_config(
        datafusion::prelude::SessionConfig::new().with_information_schema(true),
    );
    context.register_catalog("rest", Arc::new(provider));
    let batches = context
        .sql(
            "SELECT table_schema, table_name FROM rest.information_schema.tables \
             WHERE table_catalog = 'rest' AND table_name IN ('t1', 't2') \
             ORDER BY table_schema, table_name",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    expect![[r#"
            +--------------+------------+
            | table_schema | table_name |
            +--------------+------------+
            | ns1          | t1         |
            | ns1          | t2         |
            | ns2          | t1         |
            | ns2          | t2         |
            +--------------+------------+"#]]
    .assert_eq(&pretty_format_batches(&batches).unwrap().to_string());

    for mock in mocks {
        mock.assert_async().await;
    }
}
