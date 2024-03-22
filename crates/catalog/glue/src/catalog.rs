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

//! Iceberg Glue Catalog implementation.

#![allow(unused)]

use async_trait::async_trait;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use std::{collections::HashMap, fmt::Debug};

use typed_builder::TypedBuilder;

use crate::error::from_aws_error;

#[derive(Debug, TypedBuilder)]
/// Glue Catalog configuration
pub struct GlueCatalogConfig {
    #[builder(default, setter(strip_option))]
    endpoint_url: Option<String>,
    #[builder(default)]
    props: HashMap<String, String>,
}

struct GlueClient(aws_sdk_glue::Client);

/// Glue Catalog
pub struct GlueCatalog {
    config: GlueCatalogConfig,
    client: GlueClient,
}

impl Debug for GlueCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlueCatalog")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl GlueCatalog {
    /// Create a new glue catalog
    pub async fn new(config: GlueCatalogConfig) -> Self {
        let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");

        let sdk_config = match &config.endpoint_url {
            None => aws_config::defaults(BehaviorVersion::latest())
                .region(region_provider)
                .test_credentials(),
            Some(url) => aws_config::defaults(BehaviorVersion::latest())
                .region(region_provider)
                .endpoint_url(url)
                .test_credentials(),
        };

        let sdk_config = sdk_config.load().await;

        let client = aws_sdk_glue::Client::new(&sdk_config);

        GlueCatalog {
            config,
            client: GlueClient(client),
        }
    }
}

#[async_trait]
impl Catalog for GlueCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let dbs = if parent.is_some() {
            return Ok(vec![]);
        } else {
            self.client
                .0
                .get_databases()
                .send()
                .await
                .map_err(from_aws_error)?
        };

        Ok(dbs
            .database_list()
            .into_iter()
            .map(|v| NamespaceIdent::new(v.name().to_string()))
            .collect())
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        todo!()
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        todo!()
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        todo!()
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        todo!()
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        todo!()
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        todo!()
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> Result<Table> {
        todo!()
    }

    async fn load_table(&self, _table: &TableIdent) -> Result<Table> {
        todo!()
    }

    async fn drop_table(&self, _table: &TableIdent) -> Result<()> {
        todo!()
    }

    async fn table_exists(&self, _table: &TableIdent) -> Result<bool> {
        todo!()
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        todo!()
    }

    async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
        todo!()
    }
}

#[cfg(test)]
mod tests {}
