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

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use std::{collections::HashMap, fmt::Debug};

use typed_builder::TypedBuilder;

use crate::error::from_sdk_error;
use crate::utils::{
    convert_to_database, convert_to_namespace, create_sdk_config, validate_namespace,
};
use crate::with_catalog_id;

#[derive(Debug, TypedBuilder)]
/// Glue Catalog configuration
pub struct GlueCatalogConfig {
    #[builder(default, setter(strip_option))]
    uri: Option<String>,
    #[builder(default, setter(strip_option))]
    catalog_id: Option<String>,
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
        let sdk_config = create_sdk_config(&config.props, config.uri.as_ref()).await;

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
        if parent.is_some() {
            return Ok(vec![]);
        }

        let mut database_list: Vec<NamespaceIdent> = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let builder = match &next_token {
                Some(token) => self.client.0.get_databases().next_token(token),
                None => self.client.0.get_databases(),
            };
            let builder = with_catalog_id!(builder, self.config);
            let resp = builder.send().await.map_err(from_sdk_error)?;

            let dbs: Vec<NamespaceIdent> = resp
                .database_list()
                .iter()
                .map(|db| NamespaceIdent::new(db.name().to_string()))
                .collect();

            database_list.extend(dbs);

            next_token = resp.next_token().map(ToOwned::to_owned);
            if next_token.is_none() {
                break;
            }
        }

        Ok(database_list)
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let db_input = convert_to_database(namespace, &properties)?;

        let builder = self.client.0.create_database().database_input(db_input);
        let builder = with_catalog_id!(builder, self.config);

        builder.send().await.map_err(from_sdk_error)?;

        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let db_name = validate_namespace(namespace)?;

        let builder = self.client.0.get_database().name(&db_name);
        let builder = with_catalog_id!(builder, self.config);

        let resp = builder.send().await.map_err(from_sdk_error)?;

        match resp.database() {
            Some(db) => {
                let namespace = convert_to_namespace(db);
                Ok(namespace)
            }
            None => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Database with name: {} does not exist", db_name),
            )),
        }
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let db_name = validate_namespace(namespace)?;

        let builder = self.client.0.get_database().name(&db_name);
        let builder = with_catalog_id!(builder, self.config);

        let resp = builder.send().await;

        match resp {
            Ok(_) => Ok(true),
            Err(err) => {
                if err
                    .as_service_error()
                    .map(|e| e.is_entity_not_found_exception())
                    == Some(true)
                {
                    return Ok(false);
                }
                Err(from_sdk_error(err))
            }
        }
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let db_name = validate_namespace(namespace)?;
        let db_input = convert_to_database(namespace, &properties)?;

        let builder = self
            .client
            .0
            .update_database()
            .name(&db_name)
            .database_input(db_input);
        let builder = with_catalog_id!(builder, self.config);

        builder.send().await.map_err(from_sdk_error)?;

        Ok(())
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let db_name = validate_namespace(namespace)?;
        let table_list = self.list_tables(namespace).await?;

        if !table_list.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Database with name: {} is not empty", &db_name),
            ));
        }

        let builder = self.client.0.delete_database().name(db_name);
        let builder = with_catalog_id!(builder, self.config);

        builder.send().await.map_err(from_sdk_error)?;

        Ok(())
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let db_name = validate_namespace(namespace)?;
        let mut table_list: Vec<TableIdent> = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let builder = match &next_token {
                Some(token) => self
                    .client
                    .0
                    .get_tables()
                    .database_name(&db_name)
                    .next_token(token),
                None => self.client.0.get_tables().database_name(&db_name),
            };
            let builder = with_catalog_id!(builder, self.config);
            let resp = builder.send().await.map_err(from_sdk_error)?;

            let tables: Vec<_> = resp
                .table_list()
                .iter()
                .map(|tbl| TableIdent::new(namespace.clone(), tbl.name().to_string()))
                .collect();

            table_list.extend(tables);

            next_token = resp.next_token().map(ToOwned::to_owned);
            if next_token.is_none() {
                break;
            }
        }

        Ok(table_list)
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
