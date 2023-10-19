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

//! This module contains rest catalog implementation.

use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::header::{self, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Request};
use serde::de::DeserializeOwned;
use typed_builder::TypedBuilder;
use urlencoding::encode;

use iceberg::table::Table;
use iceberg::Result;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};

use self::_serde::{
    CatalogConfig, ErrorModel, ErrorResponse, ListNamespaceResponse, ListTableResponse,
    NamespaceSerde, RenameTableRequest, NO_CONTENT, OK,
};

const ICEBERG_REST_SPEC_VERSION: &str = "1.14";
const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const PATH_V1: &str = "v1";

/// Rest catalog configuration.
#[derive(Debug, TypedBuilder)]
pub struct RestCatalogConfig {
    uri: String,
    #[builder(default, setter(strip_option))]
    warehouse: Option<String>,

    #[builder(default)]
    props: HashMap<String, String>,
}

impl RestCatalogConfig {
    fn config_endpoint(&self) -> String {
        [&self.uri, PATH_V1, "config"].join("/")
    }

    fn namespaces_endpoint(&self) -> String {
        [&self.uri, PATH_V1, "namespaces"].join("/")
    }

    fn namespace_endpoint(&self, ns: &NamespaceIdent) -> String {
        [&self.uri, PATH_V1, "namespaces", &ns.encode_in_url()].join("/")
    }

    fn tables_endpoint(&self, ns: &NamespaceIdent) -> String {
        [
            &self.uri,
            PATH_V1,
            "namespaces",
            &ns.encode_in_url(),
            "tables",
        ]
        .join("/")
    }

    fn rename_table_endpoint(&self) -> String {
        [&self.uri, PATH_V1, "tables", "rename"].join("/")
    }

    fn table_endpoint(&self, table: &TableIdent) -> String {
        [
            &self.uri,
            PATH_V1,
            "namespaces",
            &table.namespace.encode_in_url(),
            "tables",
            encode(&table.name).as_ref(),
        ]
        .join("/")
    }

    fn try_create_rest_client(&self) -> Result<HttpClient> {
        //TODO: We will add oauth, ssl config, sigv4 later
        let headers = HeaderMap::from_iter([
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                HeaderName::from_static("x-client-version"),
                HeaderValue::from_static(ICEBERG_REST_SPEC_VERSION),
            ),
            (
                header::USER_AGENT,
                HeaderValue::from_str(&format!("iceberg-rs/{}", CARGO_PKG_VERSION)).unwrap(),
            ),
        ]);

        Ok(HttpClient(
            Client::builder().default_headers(headers).build()?,
        ))
    }
}

struct HttpClient(Client);

impl HttpClient {
    async fn query<
        R: DeserializeOwned,
        E: DeserializeOwned + Into<Error>,
        const SUCCESS_CODE: u16,
    >(
        &self,
        request: Request,
    ) -> Result<R> {
        let resp = self.0.execute(request).await?;

        if resp.status().as_u16() == SUCCESS_CODE {
            let text = resp.bytes().await?;
            Ok(serde_json::from_slice::<R>(&text).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to parse response from rest catalog server!",
                )
                .with_context("json", String::from_utf8_lossy(&text))
                .with_source(e)
            })?)
        } else {
            let text = resp.bytes().await?;
            let e = serde_json::from_slice::<E>(&text).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to parse response from rest catalog server!",
                )
                .with_context("json", String::from_utf8_lossy(&text))
                .with_source(e)
            })?;
            Err(e.into())
        }
    }

    async fn execute<E: DeserializeOwned + Into<Error>, const SUCCESS_CODE: u16>(
        &self,
        request: Request,
    ) -> Result<()> {
        let resp = self.0.execute(request).await?;

        if resp.status().as_u16() == SUCCESS_CODE {
            Ok(())
        } else {
            let text = resp.bytes().await?;
            let e = serde_json::from_slice::<E>(&text).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to parse response from rest catalog server!",
                )
                .with_context("json", String::from_utf8_lossy(&text))
                .with_source(e)
            })?;
            Err(e.into())
        }
    }
}

/// Rest catalog implementation.
pub struct RestCatalog {
    config: RestCatalogConfig,
    client: HttpClient,
}

#[async_trait]
impl Catalog for RestCatalog {
    /// List namespaces from table.
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let mut request = self.client.0.get(self.config.namespaces_endpoint());
        if let Some(ns) = parent {
            request = request.query(&[("parent", ns.encode_in_url())]);
        }

        let resp = self
            .client
            .query::<ListNamespaceResponse, ErrorModel, OK>(request.build()?)
            .await?;

        resp.namespaces
            .into_iter()
            .map(NamespaceIdent::from_vec)
            .collect::<Result<Vec<NamespaceIdent>>>()
    }

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let request = self
            .client
            .0
            .post(self.config.namespaces_endpoint())
            .json(&NamespaceSerde {
                namespace: namespace.as_ref().clone(),
                properties: Some(properties),
            })
            .build()?;

        let resp = self
            .client
            .query::<NamespaceSerde, ErrorModel, OK>(request)
            .await?;

        Namespace::try_from(resp)
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let request = self
            .client
            .0
            .get(self.config.namespace_endpoint(namespace))
            .build()?;

        let resp = self
            .client
            .query::<NamespaceSerde, ErrorModel, OK>(request)
            .await?;
        Namespace::try_from(resp)
    }

    /// Update a namespace inside the catalog.
    ///
    /// # Behavior
    ///
    /// The properties must be the full set of namespace.
    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Updating namespace not supported yet!",
        ))
    }

    async fn namespace_exists(&self, ns: &NamespaceIdent) -> Result<bool> {
        let request = self
            .client
            .0
            .head(self.config.namespace_endpoint(ns))
            .build()?;

        self.client
            .execute::<ErrorModel, NO_CONTENT>(request)
            .await
            .map(|_| true)
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let request = self
            .client
            .0
            .delete(self.config.namespace_endpoint(namespace))
            .build()?;

        self.client.execute::<ErrorModel, NO_CONTENT>(request).await
    }

    /// List tables from namespace.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let request = self
            .client
            .0
            .get(self.config.tables_endpoint(namespace))
            .build()?;

        let resp = self
            .client
            .query::<ListTableResponse, ErrorModel, OK>(request)
            .await?;

        Ok(resp.identifiers)
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Creating table not supported yet!",
        ))
    }

    /// Load table from the catalog.
    async fn load_table(&self, _table: &TableIdent) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Creating table not supported yet!",
        ))
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let request = self
            .client
            .0
            .delete(self.config.table_endpoint(table))
            .build()?;

        self.client.execute::<ErrorModel, NO_CONTENT>(request).await
    }

    /// Check if a table exists in the catalog.
    async fn stat_table(&self, table: &TableIdent) -> Result<bool> {
        let request = self
            .client
            .0
            .head(self.config.table_endpoint(table))
            .build()?;

        self.client
            .execute::<ErrorModel, NO_CONTENT>(request)
            .await
            .map(|_| true)
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        let request = self
            .client
            .0
            .post(self.config.rename_table_endpoint())
            .json(&RenameTableRequest {
                source: src.clone(),
                destination: dest.clone(),
            })
            .build()?;

        self.client.execute::<ErrorModel, NO_CONTENT>(request).await
    }

    /// Update a table to the catalog.
    async fn update_table(&self, _table: &TableIdent, _commit: TableCommit) -> Result<Table> {
        todo!()
    }

    /// Update multiple tables to the catalog as an atomic operation.
    async fn update_tables(&self, _tables: &[(TableIdent, TableCommit)]) -> Result<()> {
        todo!()
    }
}

impl RestCatalog {
    /// Creates a rest catalog from config.
    pub async fn new(config: RestCatalogConfig) -> Result<Self> {
        let mut catalog = Self {
            client: config.try_create_rest_client()?,
            config,
        };

        catalog.update_config().await?;
        catalog.client = catalog.config.try_create_rest_client()?;

        Ok(catalog)
    }

    async fn update_config(&mut self) -> Result<()> {
        let mut request = self.client.0.get(self.config.config_endpoint());

        if let Some(warehouse_location) = &self.config.warehouse {
            request = request.query(&[("warehouse", warehouse_location)]);
        }

        let config = self
            .client
            .query::<CatalogConfig, ErrorResponse, OK>(request.build()?)
            .await?;

        let mut props = config.defaults;
        props.extend(self.config.props.clone());
        props.extend(config.overrides);

        self.config.props = props;

        Ok(())
    }
}

/// Requests and responses for rest api.
mod _serde {
    use std::collections::HashMap;

    use serde_derive::{Deserialize, Serialize};

    use iceberg::{Error, ErrorKind, Namespace, TableIdent};

    pub(super) const OK: u16 = 200u16;
    pub(super) const NO_CONTENT: u16 = 204u16;
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub(super) struct CatalogConfig {
        pub(super) overrides: HashMap<String, String>,
        pub(super) defaults: HashMap<String, String>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(super) struct ErrorResponse {
        error: ErrorModel,
    }

    impl From<ErrorResponse> for Error {
        fn from(resp: ErrorResponse) -> Error {
            resp.error.into()
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(super) struct ErrorModel {
        pub(super) message: String,
        pub(super) r#type: String,
        pub(super) code: u16,
        pub(super) stack: Option<Vec<String>>,
    }

    impl From<ErrorModel> for Error {
        fn from(value: ErrorModel) -> Self {
            let mut error = Error::new(ErrorKind::DataInvalid, value.message)
                .with_context("type", value.r#type)
                .with_context("code", format!("{}", value.code));

            if let Some(stack) = value.stack {
                error = error.with_context("stack", stack.join("\n"));
            }

            error
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(super) struct OAuthError {
        pub(super) error: String,
        pub(super) error_description: Option<String>,
        pub(super) error_uri: Option<String>,
    }

    impl From<OAuthError> for Error {
        fn from(value: OAuthError) -> Self {
            let mut error = Error::new(
                ErrorKind::DataInvalid,
                format!("OAuthError: {}", value.error),
            );

            if let Some(desc) = value.error_description {
                error = error.with_context("description", desc);
            }

            if let Some(uri) = value.error_uri {
                error = error.with_context("uri", uri);
            }

            error
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(super) struct NamespaceSerde {
        pub(super) namespace: Vec<String>,
        pub(super) properties: Option<HashMap<String, String>>,
    }

    impl TryFrom<NamespaceSerde> for super::Namespace {
        type Error = Error;
        fn try_from(value: NamespaceSerde) -> std::result::Result<Self, Self::Error> {
            Ok(super::Namespace::with_properties(
                super::NamespaceIdent::from_vec(value.namespace)?,
                value.properties.unwrap_or_default(),
            ))
        }
    }

    impl From<&Namespace> for NamespaceSerde {
        fn from(value: &Namespace) -> Self {
            Self {
                namespace: value.name().as_ref().clone(),
                properties: Some(value.properties().clone()),
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(super) struct ListNamespaceResponse {
        pub(super) namespaces: Vec<Vec<String>>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(super) struct UpdateNamespacePropsRequest {
        removals: Option<Vec<String>>,
        updates: Option<HashMap<String, String>>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(super) struct UpdateNamespacePropsResponse {
        updated: Vec<String>,
        removed: Vec<String>,
        missing: Option<Vec<String>>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(super) struct ListTableResponse {
        pub(super) identifiers: Vec<TableIdent>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(super) struct RenameTableRequest {
        pub(super) source: TableIdent,
        pub(super) destination: TableIdent,
    }
}

#[cfg(test)]
mod tests {
    use mockito::{Mock, Server, ServerGuard};

    use super::*;

    #[tokio::test]
    async fn test_update_config() {
        let mut server = Server::new_async().await;

        let config_mock = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(
                r#"{
                "overrides": {
                    "warehouse": "s3://iceberg-catalog"
                },
                "defaults": {}
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build())
            .await
            .unwrap();

        assert_eq!(
            catalog.config.props.get("warehouse"),
            Some(&"s3://iceberg-catalog".to_string())
        );

        config_mock.assert_async().await;
    }

    async fn create_config_mock(server: &mut ServerGuard) -> Mock {
        server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(
                r#"{
                "overrides": {
                    "warehouse": "s3://iceberg-catalog"
                },
                "defaults": {}
            }"#,
            )
            .create_async()
            .await
    }

    #[tokio::test]
    async fn test_list_namespace() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let list_ns_mock = server
            .mock("GET", "/v1/namespaces")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns1", "ns11"],
                    ["ns2"]
                ]
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build())
            .await
            .unwrap();

        let namespaces = catalog.list_namespaces(None).await.unwrap();

        let expected_ns = vec![
            NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns2".to_string()]).unwrap(),
        ];

        assert_eq!(expected_ns, namespaces);

        config_mock.assert_async().await;
        list_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_create_namespace() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let create_ns_mock = server
            .mock("POST", "/v1/namespaces")
            .with_body(
                r#"{
                "namespace": [ "ns1", "ns11"],
                "properties" : {
                    "key1": "value1"
                }
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build())
            .await
            .unwrap();

        let namespaces = catalog
            .create_namespace(
                &NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap(),
                HashMap::from([("key1".to_string(), "value1".to_string())]),
            )
            .await
            .unwrap();

        let expected_ns = Namespace::with_properties(
            NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap(),
            HashMap::from([("key1".to_string(), "value1".to_string())]),
        );

        assert_eq!(expected_ns, namespaces);

        config_mock.assert_async().await;
        create_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_namespace() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let get_ns_mock = server
            .mock("GET", "/v1/namespaces/ns1")
            .with_body(
                r#"{
                "namespace": [ "ns1"],
                "properties" : {
                    "key1": "value1"
                }
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build())
            .await
            .unwrap();

        let namespaces = catalog
            .get_namespace(&NamespaceIdent::new("ns1".to_string()))
            .await
            .unwrap();

        let expected_ns = Namespace::with_properties(
            NamespaceIdent::new("ns1".to_string()),
            HashMap::from([("key1".to_string(), "value1".to_string())]),
        );

        assert_eq!(expected_ns, namespaces);

        config_mock.assert_async().await;
        get_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn check_namespace_exists() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let get_ns_mock = server
            .mock("HEAD", "/v1/namespaces/ns1")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build())
            .await
            .unwrap();

        assert!(catalog
            .namespace_exists(&NamespaceIdent::new("ns1".to_string()))
            .await
            .unwrap());

        config_mock.assert_async().await;
        get_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_drop_namespace() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let drop_ns_mock = server
            .mock("DELETE", "/v1/namespaces/ns1")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build())
            .await
            .unwrap();

        catalog
            .drop_namespace(&NamespaceIdent::new("ns1".to_string()))
            .await
            .unwrap();

        config_mock.assert_async().await;
        drop_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_tables() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let list_tables_mock = server
            .mock("GET", "/v1/namespaces/ns1/tables")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table1"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "table2"
                    }
                ]
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build())
            .await
            .unwrap();

        let tables = catalog
            .list_tables(&NamespaceIdent::new("ns1".to_string()))
            .await
            .unwrap();

        let expected_tables = vec![
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table1".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table2".to_string()),
        ];

        assert_eq!(tables, expected_tables);

        config_mock.assert_async().await;
        list_tables_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_drop_tables() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let delete_table_mock = server
            .mock("DELETE", "/v1/namespaces/ns1/tables/table1")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build())
            .await
            .unwrap();

        catalog
            .drop_table(&TableIdent::new(
                NamespaceIdent::new("ns1".to_string()),
                "table1".to_string(),
            ))
            .await
            .unwrap();

        config_mock.assert_async().await;
        delete_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_check_table_exists() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let check_table_exists_mock = server
            .mock("HEAD", "/v1/namespaces/ns1/tables/table1")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build())
            .await
            .unwrap();

        assert!(catalog
            .stat_table(&TableIdent::new(
                NamespaceIdent::new("ns1".to_string()),
                "table1".to_string(),
            ))
            .await
            .unwrap());

        config_mock.assert_async().await;
        check_table_exists_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_rename_table() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let rename_table_mock = server
            .mock("POST", "/v1/tables/rename")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build())
            .await
            .unwrap();

        catalog
            .rename_table(
                &TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table1".to_string()),
                &TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table2".to_string()),
            )
            .await
            .unwrap();

        config_mock.assert_async().await;
        rename_table_mock.assert_async().await;
    }
}
