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
use std::str::FromStr;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use itertools::Itertools;
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, {self},
};
use reqwest::{Method, StatusCode, Url};
use tokio::sync::OnceCell;
use typed_builder::TypedBuilder;

use crate::client::HttpClient;
use crate::types::{
    CatalogConfig, CommitTableRequest, CommitTableResponse, CreateTableRequest, ErrorResponse,
    ListNamespaceResponse, ListTableResponse, LoadTableResponse, NamespaceSerde,
    RenameTableRequest, NO_CONTENT, OK,
};

const ICEBERG_REST_SPEC_VERSION: &str = "0.14.1";
const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const PATH_V1: &str = "v1";

/// Rest catalog configuration.
#[derive(Clone, Debug, TypedBuilder)]
pub struct RestCatalogConfig {
    uri: String,
    #[builder(default, setter(strip_option))]
    warehouse: Option<String>,

    #[builder(default)]
    props: HashMap<String, String>,
}

impl RestCatalogConfig {
    fn url_prefixed(&self, parts: &[&str]) -> String {
        [&self.uri, PATH_V1]
            .into_iter()
            .chain(self.props.get("prefix").map(|s| &**s))
            .chain(parts.iter().cloned())
            .join("/")
    }

    fn config_endpoint(&self) -> String {
        [&self.uri, PATH_V1, "config"].join("/")
    }

    pub(crate) fn get_token_endpoint(&self) -> String {
        if let Some(oauth2_uri) = self.props.get("oauth2-server-uri") {
            oauth2_uri.to_string()
        } else if let Some(auth_url) = self.props.get("rest.authorization-url") {
            log::warn!(
                "'rest.authorization-url' is deprecated and will be removed in version 0.4.0. \
                 Please use 'oauth2-server-uri' instead."
            );
            auth_url.to_string()
        } else {
            [&self.uri, PATH_V1, "oauth", "tokens"].join("/")
        }
    }

    fn namespaces_endpoint(&self) -> String {
        self.url_prefixed(&["namespaces"])
    }

    fn namespace_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string()])
    }

    fn tables_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string(), "tables"])
    }

    fn rename_table_endpoint(&self) -> String {
        self.url_prefixed(&["tables", "rename"])
    }

    fn table_endpoint(&self, table: &TableIdent) -> String {
        self.url_prefixed(&[
            "namespaces",
            &table.namespace.to_url_string(),
            "tables",
            &table.name,
        ])
    }

    /// Get the token from the config.
    ///
    /// Client will use `token` to send requests if exists.
    pub(crate) fn token(&self) -> Option<String> {
        self.props.get("token").cloned()
    }

    /// Get the credentials from the config. Client will use `credential`
    /// to fetch a new token if exists.
    ///
    /// ## Output
    ///
    /// - `None`: No credential is set.
    /// - `Some(None, client_secret)`: No client_id is set, use client_secret directly.
    /// - `Some(Some(client_id), client_secret)`: Both client_id and client_secret are set.
    pub(crate) fn credential(&self) -> Option<(Option<String>, String)> {
        let cred = self.props.get("credential")?;

        match cred.split_once(':') {
            Some((client_id, client_secret)) => {
                Some((Some(client_id.to_string()), client_secret.to_string()))
            }
            None => Some((None, cred.to_string())),
        }
    }

    /// Get the extra headers from config.
    ///
    /// We will include:
    ///
    /// - `content-type`
    /// - `x-client-version`
    /// - `user-agnet`
    /// - all headers specified by `header.xxx` in props.
    pub(crate) fn extra_headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::from_iter([
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

        for (key, value) in self
            .props
            .iter()
            .filter(|(k, _)| k.starts_with("header."))
            // The unwrap here is same since we are filtering the keys
            .map(|(k, v)| (k.strip_prefix("header.").unwrap(), v))
        {
            headers.insert(
                HeaderName::from_str(key).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid header name: {key}"),
                    )
                    .with_source(e)
                })?,
                HeaderValue::from_str(value).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid header value: {value}"),
                    )
                    .with_source(e)
                })?,
            );
        }

        Ok(headers)
    }

    /// Get the optional oauth headers from the config.
    pub(crate) fn extra_oauth_params(&self) -> HashMap<String, String> {
        let mut params = HashMap::new();

        if let Some(scope) = self.props.get("scope") {
            params.insert("scope".to_string(), scope.to_string());
        } else {
            params.insert("scope".to_string(), "catalog".to_string());
        }

        let optional_params = ["audience", "resource"];
        for param_name in optional_params {
            if let Some(value) = self.props.get(param_name) {
                params.insert(param_name.to_string(), value.to_string());
            }
        }
        params
    }

    /// Merge the config with the given config fetched from rest server.
    pub(crate) fn merge_with_config(mut self, mut config: CatalogConfig) -> Self {
        if let Some(uri) = config.overrides.remove("uri") {
            self.uri = uri;
        }

        let mut props = config.defaults;
        props.extend(self.props);
        props.extend(config.overrides);

        self.props = props;
        self
    }
}

#[derive(Debug)]
struct RestContext {
    client: HttpClient,

    /// Runtime config is fetched from rest server and stored here.
    ///
    /// It's could be different from the user config.
    config: RestCatalogConfig,
}

impl RestContext {}

/// Rest catalog implementation.
#[derive(Debug)]
pub struct RestCatalog {
    /// User config is stored as-is and never be changed.
    ///
    /// It's could be different from the config fetched from the server and used at runtime.
    user_config: RestCatalogConfig,
    ctx: OnceCell<RestContext>,
}

impl RestCatalog {
    /// Creates a rest catalog from config.
    pub fn new(config: RestCatalogConfig) -> Self {
        Self {
            user_config: config,
            ctx: OnceCell::new(),
        }
    }

    /// Get the context from the catalog.
    async fn context(&self) -> Result<&RestContext> {
        self.ctx
            .get_or_try_init(|| async {
                let catalog_config = RestCatalog::load_config(&self.user_config).await?;
                let config = self.user_config.clone().merge_with_config(catalog_config);
                let client = HttpClient::new(&config)?;

                Ok(RestContext { config, client })
            })
            .await
    }

    /// Load the runtime config from the server by user_config.
    ///
    /// It's required for a rest catalog to update it's config after creation.
    async fn load_config(user_config: &RestCatalogConfig) -> Result<CatalogConfig> {
        let client = HttpClient::new(user_config)?;

        let mut request = client.request(Method::GET, user_config.config_endpoint());

        if let Some(warehouse_location) = &user_config.warehouse {
            request = request.query(&[("warehouse", warehouse_location)]);
        }

        let config = client
            .query::<CatalogConfig, ErrorResponse, OK>(request.build()?)
            .await?;
        Ok(config)
    }

    async fn load_file_io(
        &self,
        metadata_location: Option<&str>,
        extra_config: Option<HashMap<String, String>>,
    ) -> Result<FileIO> {
        let mut props = self.context().await?.config.props.clone();
        if let Some(config) = extra_config {
            props.extend(config);
        }

        // If the warehouse is a logical identifier instead of a URL we don't want
        // to raise an exception
        let warehouse_path = match self.context().await?.config.warehouse.as_deref() {
            Some(url) if Url::parse(url).is_ok() => Some(url),
            Some(_) => None,
            None => None,
        };

        let file_io = match warehouse_path.or(metadata_location) {
            Some(url) => FileIO::from_path(url)?.with_props(props).build()?,
            None => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Unable to load file io, neither warehouse nor metadata location is set!",
                ))?
            }
        };

        Ok(file_io)
    }
}

#[async_trait]
impl Catalog for RestCatalog {
    /// List namespaces from table.
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let mut request = self.context().await?.client.request(
            Method::GET,
            self.context().await?.config.namespaces_endpoint(),
        );
        if let Some(ns) = parent {
            request = request.query(&[("parent", ns.to_url_string())]);
        }

        let resp = self
            .context()
            .await?
            .client
            .query::<ListNamespaceResponse, ErrorResponse, OK>(request.build()?)
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
            .context()
            .await?
            .client
            .request(
                Method::POST,
                self.context().await?.config.namespaces_endpoint(),
            )
            .json(&NamespaceSerde {
                namespace: namespace.as_ref().clone(),
                properties: Some(properties),
            })
            .build()?;

        let resp = self
            .context()
            .await?
            .client
            .query::<NamespaceSerde, ErrorResponse, OK>(request)
            .await?;

        Namespace::try_from(resp)
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let request = self
            .context()
            .await?
            .client
            .request(
                Method::GET,
                self.context().await?.config.namespace_endpoint(namespace),
            )
            .build()?;

        let resp = self
            .context()
            .await?
            .client
            .query::<NamespaceSerde, ErrorResponse, OK>(request)
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
            .context()
            .await?
            .client
            .request(
                Method::HEAD,
                self.context().await?.config.namespace_endpoint(ns),
            )
            .build()?;

        self.context()
            .await?
            .client
            .do_execute::<bool, ErrorResponse>(request, |resp| match resp.status() {
                StatusCode::NO_CONTENT => Some(true),
                StatusCode::NOT_FOUND => Some(false),
                _ => None,
            })
            .await
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let request = self
            .context()
            .await?
            .client
            .request(
                Method::DELETE,
                self.context().await?.config.namespace_endpoint(namespace),
            )
            .build()?;

        self.context()
            .await?
            .client
            .execute::<ErrorResponse, NO_CONTENT>(request)
            .await
    }

    /// List tables from namespace.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let request = self
            .context()
            .await?
            .client
            .request(
                Method::GET,
                self.context().await?.config.tables_endpoint(namespace),
            )
            .build()?;

        let resp = self
            .context()
            .await?
            .client
            .query::<ListTableResponse, ErrorResponse, OK>(request)
            .await?;

        Ok(resp.identifiers)
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());

        let request = self
            .context()
            .await?
            .client
            .request(
                Method::POST,
                self.context().await?.config.tables_endpoint(namespace),
            )
            .json(&CreateTableRequest {
                name: creation.name,
                location: creation.location,
                schema: creation.schema,
                partition_spec: creation.partition_spec,
                write_order: creation.sort_order,
                // We don't support stage create yet.
                stage_create: Some(false),
                properties: if creation.properties.is_empty() {
                    None
                } else {
                    Some(creation.properties)
                },
            })
            .build()?;

        let resp = self
            .context()
            .await?
            .client
            .query::<LoadTableResponse, ErrorResponse, OK>(request)
            .await?;

        let file_io = self
            .load_file_io(resp.metadata_location.as_deref(), resp.config)
            .await?;

        Table::builder()
            .identifier(table_ident)
            .file_io(file_io)
            .metadata(resp.metadata)
            .metadata_location(resp.metadata_location.ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Metadata location missing in create table response!",
                )
            })?)
            .build()
    }

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        let request = self
            .context()
            .await?
            .client
            .request(
                Method::GET,
                self.context().await?.config.table_endpoint(table),
            )
            .build()?;

        let resp = self
            .context()
            .await?
            .client
            .query::<LoadTableResponse, ErrorResponse, OK>(request)
            .await?;

        let file_io = self
            .load_file_io(resp.metadata_location.as_deref(), resp.config)
            .await?;

        let table_builder = Table::builder()
            .identifier(table.clone())
            .file_io(file_io)
            .metadata(resp.metadata);

        if let Some(metadata_location) = resp.metadata_location {
            table_builder.metadata_location(metadata_location).build()
        } else {
            table_builder.build()
        }
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let request = self
            .context()
            .await?
            .client
            .request(
                Method::DELETE,
                self.context().await?.config.table_endpoint(table),
            )
            .build()?;

        self.context()
            .await?
            .client
            .execute::<ErrorResponse, NO_CONTENT>(request)
            .await
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        let request = self
            .context()
            .await?
            .client
            .request(
                Method::HEAD,
                self.context().await?.config.table_endpoint(table),
            )
            .build()?;

        self.context()
            .await?
            .client
            .do_execute::<bool, ErrorResponse>(request, |resp| match resp.status() {
                StatusCode::NO_CONTENT => Some(true),
                StatusCode::NOT_FOUND => Some(false),
                _ => None,
            })
            .await
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        let request = self
            .context()
            .await?
            .client
            .request(
                Method::POST,
                self.context().await?.config.rename_table_endpoint(),
            )
            .json(&RenameTableRequest {
                source: src.clone(),
                destination: dest.clone(),
            })
            .build()?;

        self.context()
            .await?
            .client
            .execute::<ErrorResponse, NO_CONTENT>(request)
            .await
    }

    /// Update table.
    async fn update_table(&self, mut commit: TableCommit) -> Result<Table> {
        let request = self
            .context()
            .await?
            .client
            .request(
                Method::POST,
                self.context()
                    .await?
                    .config
                    .table_endpoint(commit.identifier()),
            )
            .json(&CommitTableRequest {
                identifier: commit.identifier().clone(),
                requirements: commit.take_requirements(),
                updates: commit.take_updates(),
            })
            .build()?;

        let resp = self
            .context()
            .await?
            .client
            .query::<CommitTableResponse, ErrorResponse, OK>(request)
            .await?;

        let file_io = self
            .load_file_io(Some(&resp.metadata_location), None)
            .await?;
        Table::builder()
            .identifier(commit.identifier().clone())
            .file_io(file_io)
            .metadata(resp.metadata)
            .metadata_location(resp.metadata_location)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use iceberg::spec::{
        FormatVersion, NestedField, NullOrder, Operation, PrimitiveType, Schema, Snapshot,
        SnapshotLog, SortDirection, SortField, SortOrder, Summary, Transform, Type,
        UnboundPartitionField, UnboundPartitionSpec,
    };
    use iceberg::transaction::Transaction;
    use mockito::{Mock, Server, ServerGuard};
    use serde_json::json;
    use uuid::uuid;

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

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        assert_eq!(
            catalog
                .context()
                .await
                .unwrap()
                .config
                .props
                .get("warehouse"),
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

    async fn create_oauth_mock(server: &mut ServerGuard) -> Mock {
        create_oauth_mock_with_path(server, "/v1/oauth/tokens").await
    }

    async fn create_oauth_mock_with_path(server: &mut ServerGuard, path: &str) -> Mock {
        server
            .mock("POST", path)
            .with_status(200)
            .with_body(
                r#"{
                "access_token": "ey000000000000",
                "token_type": "Bearer",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "expires_in": 86400
                }"#,
            )
            .expect(2)
            .create_async()
            .await
    }

    #[tokio::test]
    async fn test_oauth() {
        let mut server = Server::new_async().await;
        let oauth_mock = create_oauth_mock(&mut server).await;
        let config_mock = create_config_mock(&mut server).await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
        );

        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));
    }

    #[tokio::test]
    async fn test_oauth_with_optional_param() {
        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());
        props.insert("scope".to_string(), "custom_scope".to_string());
        props.insert("audience".to_string(), "custom_audience".to_string());
        props.insert("resource".to_string(), "custom_resource".to_string());

        let mut server = Server::new_async().await;
        let oauth_mock = server
            .mock("POST", "/v1/oauth/tokens")
            .match_body(mockito::Matcher::Regex("scope=custom_scope".to_string()))
            .match_body(mockito::Matcher::Regex(
                "audience=custom_audience".to_string(),
            ))
            .match_body(mockito::Matcher::Regex(
                "resource=custom_resource".to_string(),
            ))
            .with_status(200)
            .with_body(
                r#"{
                "access_token": "ey000000000000",
                "token_type": "Bearer",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "expires_in": 86400
                }"#,
            )
            .expect(2)
            .create_async()
            .await;

        let config_mock = create_config_mock(&mut server).await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
        );

        let token = catalog.context().await.unwrap().client.token().await;

        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));
    }

    #[tokio::test]
    async fn test_http_headers() {
        let server = Server::new_async().await;
        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let config = RestCatalogConfig::builder()
            .uri(server.url())
            .props(props)
            .build();
        let headers: HeaderMap = config.extra_headers().unwrap();

        let expected_headers = HeaderMap::from_iter([
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
        assert_eq!(headers, expected_headers);
    }

    #[tokio::test]
    async fn test_http_headers_with_custom_headers() {
        let server = Server::new_async().await;
        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());
        props.insert(
            "header.content-type".to_string(),
            "application/yaml".to_string(),
        );
        props.insert(
            "header.customized-header".to_string(),
            "some/value".to_string(),
        );

        let config = RestCatalogConfig::builder()
            .uri(server.url())
            .props(props)
            .build();
        let headers: HeaderMap = config.extra_headers().unwrap();

        let expected_headers = HeaderMap::from_iter([
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/yaml"),
            ),
            (
                HeaderName::from_static("x-client-version"),
                HeaderValue::from_static(ICEBERG_REST_SPEC_VERSION),
            ),
            (
                header::USER_AGENT,
                HeaderValue::from_str(&format!("iceberg-rs/{}", CARGO_PKG_VERSION)).unwrap(),
            ),
            (
                HeaderName::from_static("customized-header"),
                HeaderValue::from_static("some/value"),
            ),
        ]);
        assert_eq!(headers, expected_headers);
    }

    #[tokio::test]
    async fn test_oauth_with_deprecated_auth_url() {
        let mut server = Server::new_async().await;
        let config_mock = create_config_mock(&mut server).await;

        let mut auth_server = Server::new_async().await;
        let auth_server_path = "/some/path";
        let oauth_mock = create_oauth_mock_with_path(&mut auth_server, auth_server_path).await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());
        props.insert(
            "rest.authorization-url".to_string(),
            format!("{}{}", auth_server.url(), auth_server_path).to_string(),
        );

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
        );

        let token = catalog.context().await.unwrap().client.token().await;

        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));
    }

    #[tokio::test]
    async fn test_oauth_with_oauth2_server_uri() {
        let mut server = Server::new_async().await;
        let config_mock = create_config_mock(&mut server).await;

        let mut auth_server = Server::new_async().await;
        let auth_server_path = "/some/path";
        let oauth_mock = create_oauth_mock_with_path(&mut auth_server, auth_server_path).await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());
        props.insert(
            "oauth2-server-uri".to_string(),
            format!("{}{}", auth_server.url(), auth_server_path).to_string(),
        );

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
        );

        let token = catalog.context().await.unwrap().client.token().await;

        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));
    }

    #[tokio::test]
    async fn test_config_override() {
        let mut server = Server::new_async().await;
        let mut redirect_server = Server::new_async().await;
        let new_uri = redirect_server.url();

        let config_mock = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(
                json!(
                    {
                        "overrides": {
                            "uri": new_uri,
                            "warehouse": "s3://iceberg-catalog",
                            "prefix": "ice/warehouses/my"
                        },
                        "defaults": {},
                    }
                )
                .to_string(),
            )
            .create_async()
            .await;

        let list_ns_mock = redirect_server
            .mock("GET", "/v1/ice/warehouses/my/namespaces")
            .with_body(
                r#"{
                    "namespaces": []
                }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let _namespaces = catalog.list_namespaces(None).await.unwrap();

        config_mock.assert_async().await;
        list_ns_mock.assert_async().await;
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

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

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

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

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

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

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

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

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

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

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

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

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

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

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

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        assert!(catalog
            .table_exists(&TableIdent::new(
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

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

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

    #[tokio::test]
    async fn test_load_table() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let rename_table_mock = server
            .mock("GET", "/v1/namespaces/ns1/tables/test1")
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "load_table_response.json"
            ))
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("ns1".to_string()),
                "test1".to_string(),
            ))
            .await
            .unwrap();

        assert_eq!(
            &TableIdent::from_strs(vec!["ns1", "test1"]).unwrap(),
            table.identifier()
        );
        assert_eq!("s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json", table.metadata_location().unwrap());
        assert_eq!(FormatVersion::V1, table.metadata().format_version());
        assert_eq!("s3://warehouse/database/table", table.metadata().location());
        assert_eq!(
            uuid!("b55d9dda-6561-423a-8bfc-787980ce421f"),
            table.metadata().uuid()
        );
        assert_eq!(
            Utc.timestamp_millis_opt(1646787054459).unwrap(),
            table.metadata().last_updated_timestamp().unwrap()
        );
        assert_eq!(
            vec![&Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])
                    .build()
                    .unwrap()
            )],
            table.metadata().schemas_iter().collect::<Vec<_>>()
        );
        assert_eq!(
            &HashMap::from([
                ("owner".to_string(), "bryan".to_string()),
                (
                    "write.metadata.compression-codec".to_string(),
                    "gzip".to_string()
                )
            ]),
            table.metadata().properties()
        );
        assert_eq!(vec![&Arc::new(Snapshot::builder()
            .with_snapshot_id(3497810964824022504)
            .with_timestamp_ms(1646787054459)
            .with_manifest_list("s3://warehouse/database/table/metadata/snap-3497810964824022504-1-c4f68204-666b-4e50-a9df-b10c34bf6b82.avro")
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_summary(Summary {
                operation: Operation::Append,
                other: HashMap::from_iter([
                    ("spark.app.id", "local-1646787004168"),
                    ("added-data-files", "1"),
                    ("added-records", "1"),
                    ("added-files-size", "697"),
                    ("changed-partition-count", "1"),
                    ("total-records", "1"),
                    ("total-files-size", "697"),
                    ("total-data-files", "1"),
                    ("total-delete-files", "0"),
                    ("total-position-deletes", "0"),
                    ("total-equality-deletes", "0")
                ].iter().map(|p| (p.0.to_string(), p.1.to_string()))),
            }).build()
        )], table.metadata().snapshots().collect::<Vec<_>>());
        assert_eq!(
            &[SnapshotLog {
                timestamp_ms: 1646787054459,
                snapshot_id: 3497810964824022504,
            }],
            table.metadata().history()
        );
        assert_eq!(
            vec![&Arc::new(SortOrder {
                order_id: 0,
                fields: vec![],
            })],
            table.metadata().sort_orders_iter().collect::<Vec<_>>()
        );

        config_mock.assert_async().await;
        rename_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_load_table_404() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let rename_table_mock = server
            .mock("GET", "/v1/namespaces/ns1/tables/test1")
            .with_status(404)
            .with_body(r#"
{
    "error": {
        "message": "Table does not exist: ns1.test1 in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
        "type": "NoSuchNamespaceErrorException",
        "code": 404
    }
}
            "#)
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("ns1".to_string()),
                "test1".to_string(),
            ))
            .await;

        assert!(table.is_err());
        assert!(table
            .err()
            .unwrap()
            .message()
            .contains("Table does not exist"));

        config_mock.assert_async().await;
        rename_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_create_table() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let create_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables")
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "create_table_response.json"
            ))
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let table_creation = TableCreation::builder()
            .name("test1".to_string())
            .schema(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean))
                            .into(),
                    ])
                    .with_schema_id(1)
                    .with_identifier_field_ids(vec![2])
                    .build()
                    .unwrap(),
            )
            .properties(HashMap::from([("owner".to_string(), "testx".to_string())]))
            .partition_spec(
                UnboundPartitionSpec::builder()
                    .add_partition_fields(vec![UnboundPartitionField::builder()
                        .source_id(1)
                        .transform(Transform::Truncate(3))
                        .name("id".to_string())
                        .build()])
                    .unwrap()
                    .build(),
            )
            .sort_order(
                SortOrder::builder()
                    .with_sort_field(
                        SortField::builder()
                            .source_id(2)
                            .transform(Transform::Identity)
                            .direction(SortDirection::Ascending)
                            .null_order(NullOrder::First)
                            .build(),
                    )
                    .build_unbound()
                    .unwrap(),
            )
            .build();

        let table = catalog
            .create_table(&NamespaceIdent::from_strs(["ns1"]).unwrap(), table_creation)
            .await
            .unwrap();

        assert_eq!(
            &TableIdent::from_strs(vec!["ns1", "test1"]).unwrap(),
            table.identifier()
        );
        assert_eq!(
            "s3://warehouse/database/table/metadata.json",
            table.metadata_location().unwrap()
        );
        assert_eq!(FormatVersion::V1, table.metadata().format_version());
        assert_eq!("s3://warehouse/database/table", table.metadata().location());
        assert_eq!(
            uuid!("bf289591-dcc0-4234-ad4f-5c3eed811a29"),
            table.metadata().uuid()
        );
        assert_eq!(
            1657810967051,
            table
                .metadata()
                .last_updated_timestamp()
                .unwrap()
                .timestamp_millis()
        );
        assert_eq!(
            vec![&Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean))
                            .into(),
                    ])
                    .with_schema_id(0)
                    .with_identifier_field_ids(vec![2])
                    .build()
                    .unwrap()
            )],
            table.metadata().schemas_iter().collect::<Vec<_>>()
        );
        assert_eq!(
            &HashMap::from([
                (
                    "write.delete.parquet.compression-codec".to_string(),
                    "zstd".to_string()
                ),
                (
                    "write.metadata.compression-codec".to_string(),
                    "gzip".to_string()
                ),
                (
                    "write.summary.partition-limit".to_string(),
                    "100".to_string()
                ),
                (
                    "write.parquet.compression-codec".to_string(),
                    "zstd".to_string()
                ),
            ]),
            table.metadata().properties()
        );
        assert!(table.metadata().current_snapshot().is_none());
        assert!(table.metadata().history().is_empty());
        assert_eq!(
            vec![&Arc::new(SortOrder {
                order_id: 0,
                fields: vec![],
            })],
            table.metadata().sort_orders_iter().collect::<Vec<_>>()
        );

        config_mock.assert_async().await;
        create_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_create_table_409() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let create_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables")
            .with_status(409)
            .with_body(r#"
{
    "error": {
        "message": "Table already exists: ns1.test1 in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
        "type": "AlreadyExistsException",
        "code": 409
    }
}
            "#)
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let table_creation = TableCreation::builder()
            .name("test1".to_string())
            .schema(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean))
                            .into(),
                    ])
                    .with_schema_id(1)
                    .with_identifier_field_ids(vec![2])
                    .build()
                    .unwrap(),
            )
            .properties(HashMap::from([("owner".to_string(), "testx".to_string())]))
            .build();

        let table_result = catalog
            .create_table(&NamespaceIdent::from_strs(["ns1"]).unwrap(), table_creation)
            .await;

        assert!(table_result.is_err());
        assert!(table_result
            .err()
            .unwrap()
            .message()
            .contains("Table already exists"));

        config_mock.assert_async().await;
        create_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_update_table() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let update_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1")
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "update_table_response.json"
            ))
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let table1 = {
            let file = File::open(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "create_table_response.json"
            ))
            .unwrap();
            let reader = BufReader::new(file);
            let resp = serde_json::from_reader::<_, LoadTableResponse>(reader).unwrap();

            Table::builder()
                .metadata(resp.metadata)
                .metadata_location(resp.metadata_location.unwrap())
                .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
                .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
                .build()
                .unwrap()
        };

        let table = Transaction::new(&table1)
            .upgrade_table_version(FormatVersion::V2)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        assert_eq!(
            &TableIdent::from_strs(vec!["ns1", "test1"]).unwrap(),
            table.identifier()
        );
        assert_eq!(
            "s3://warehouse/database/table/metadata.json",
            table.metadata_location().unwrap()
        );
        assert_eq!(FormatVersion::V2, table.metadata().format_version());
        assert_eq!("s3://warehouse/database/table", table.metadata().location());
        assert_eq!(
            uuid!("bf289591-dcc0-4234-ad4f-5c3eed811a29"),
            table.metadata().uuid()
        );
        assert_eq!(
            1657810967051,
            table
                .metadata()
                .last_updated_timestamp()
                .unwrap()
                .timestamp_millis()
        );
        assert_eq!(
            vec![&Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean))
                            .into(),
                    ])
                    .with_schema_id(0)
                    .with_identifier_field_ids(vec![2])
                    .build()
                    .unwrap()
            )],
            table.metadata().schemas_iter().collect::<Vec<_>>()
        );
        assert_eq!(
            &HashMap::from([
                (
                    "write.delete.parquet.compression-codec".to_string(),
                    "zstd".to_string()
                ),
                (
                    "write.metadata.compression-codec".to_string(),
                    "gzip".to_string()
                ),
                (
                    "write.summary.partition-limit".to_string(),
                    "100".to_string()
                ),
                (
                    "write.parquet.compression-codec".to_string(),
                    "zstd".to_string()
                ),
            ]),
            table.metadata().properties()
        );
        assert!(table.metadata().current_snapshot().is_none());
        assert!(table.metadata().history().is_empty());
        assert_eq!(
            vec![&Arc::new(SortOrder {
                order_id: 0,
                fields: vec![],
            })],
            table.metadata().sort_orders_iter().collect::<Vec<_>>()
        );

        config_mock.assert_async().await;
        update_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_update_table_404() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let update_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1")
            .with_status(404)
            .with_body(
                r#"
{
    "error": {
        "message": "The given table does not exist",
        "type": "NoSuchTableException",
        "code": 404
    }
}
            "#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build());

        let table1 = {
            let file = File::open(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "create_table_response.json"
            ))
            .unwrap();
            let reader = BufReader::new(file);
            let resp = serde_json::from_reader::<_, LoadTableResponse>(reader).unwrap();

            Table::builder()
                .metadata(resp.metadata)
                .metadata_location(resp.metadata_location.unwrap())
                .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
                .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
                .build()
                .unwrap()
        };

        let table_result = Transaction::new(&table1)
            .upgrade_table_version(FormatVersion::V2)
            .unwrap()
            .commit(&catalog)
            .await;

        assert!(table_result.is_err());
        assert!(table_result
            .err()
            .unwrap()
            .message()
            .contains("The given table does not exist"));

        config_mock.assert_async().await;
        update_table_mock.assert_async().await;
    }
}
