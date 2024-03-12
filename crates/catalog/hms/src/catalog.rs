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

use super::utils::*;
use async_trait::async_trait;
use hive_metastore::ThriftHiveMetastoreClient;
use hive_metastore::ThriftHiveMetastoreClientBuilder;
use hive_metastore::ThriftHiveMetastoreGetDatabaseException;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::net::ToSocketAddrs;
use typed_builder::TypedBuilder;
use volo_thrift::ResponseError;

/// Which variant of the thrift transport to communicate with HMS
/// See: <https://github.com/apache/thrift/blob/master/doc/specs/thrift-rpc.md#framed-vs-unframed-transport>
#[derive(Debug, Default)]
pub enum HmsThriftTransport {
    /// Use the framed transport
    Framed,
    /// Use the buffered transport (default)
    #[default]
    Buffered,
}

/// Hive metastore Catalog configuration.
#[derive(Debug, TypedBuilder)]
pub struct HmsCatalogConfig {
    address: String,
    thrift_transport: HmsThriftTransport,
}

struct HmsClient(ThriftHiveMetastoreClient);

/// Hive metastore Catalog.
pub struct HmsCatalog {
    config: HmsCatalogConfig,
    client: HmsClient,
}

impl Debug for HmsCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HmsCatalog")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl HmsCatalog {
    /// Create a new hms catalog.
    pub fn new(config: HmsCatalogConfig) -> Result<Self> {
        let address = config
            .address
            .as_str()
            .to_socket_addrs()
            .map_err(from_io_error)?
            .next()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("invalid address: {}", config.address),
                )
            })?;

        let builder = ThriftHiveMetastoreClientBuilder::new("hms").address(address);

        let client = match &config.thrift_transport {
            HmsThriftTransport::Framed => builder
                .make_codec(volo_thrift::codec::default::DefaultMakeCodec::framed())
                .build(),
            HmsThriftTransport::Buffered => builder
                .make_codec(volo_thrift::codec::default::DefaultMakeCodec::buffered())
                .build(),
        };

        Ok(Self {
            config,
            client: HmsClient(client),
        })
    }
}

#[async_trait]
impl Catalog for HmsCatalog {
    /// HMS doesn't support nested namespaces.
    ///
    /// We will return empty list if parent is some.
    ///
    /// Align with java implementation: <https://github.com/apache/iceberg/blob/9bd62f79f8cd973c39d14e89163cb1c707470ed2/hive-metastore/src/main/java/org/apache/iceberg/hive/HiveCatalog.java#L305C26-L330>
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let dbs = if parent.is_some() {
            return Ok(vec![]);
        } else {
            self.client
                .0
                .get_all_databases()
                .await
                .map_err(from_thrift_error)?
        };

        Ok(dbs
            .into_iter()
            .map(|v| NamespaceIdent::new(v.into()))
            .collect())
    }

    /// Creates a new namespace with the given identifier and properties.
    ///
    /// Attempts to create a namespace defined by the `namespace`
    /// parameter and configured with the specified `properties`.
    ///
    /// This function can return an error in the following situations:
    ///
    /// - If `hive.metastore.database.owner-type` is specified without  
    /// `hive.metastore.database.owner`,
    /// - Errors from `validate_namespace` if the namespace identifier does not
    /// meet validation criteria.
    /// - Errors from `convert_to_database` if the properties cannot be  
    /// successfully converted into a database configuration.
    /// - Errors from the underlying database creation process, converted using
    /// `from_thrift_error`.
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let database = convert_to_database(namespace, &properties)?;

        self.client
            .0
            .create_database(database)
            .await
            .map_err(from_thrift_error)?;

        Ok(Namespace::new(namespace.clone()))
    }

    /// Retrieves a namespace by its identifier.
    ///
    /// Validates the given namespace identifier and then queries the
    /// underlying database client to fetch the corresponding namespace data.
    /// Constructs a `Namespace` object with the retrieved data and returns it.
    ///
    /// This function can return an error in any of the following situations:
    /// - If the provided namespace identifier fails validation checks
    /// - If there is an error querying the database, returned by
    /// `from_thrift_error`.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let name = validate_namespace(namespace)?;

        let db = self
            .client
            .0
            .get_database(name.clone().into())
            .await
            .map_err(from_thrift_error)?;

        let ns = convert_to_namespace(&db)?;

        Ok(ns)
    }

    /// Checks if a namespace exists within the Hive Metastore.
    ///
    /// Validates the namespace identifier by querying the Hive Metastore
    /// to determine if the specified namespace (database) exists.
    ///
    /// # Returns
    /// A `Result<bool>` indicating the outcome of the check:
    /// - `Ok(true)` if the namespace exists.
    /// - `Ok(false)` if the namespace does not exist, identified by a specific
    /// `UserException` variant.
    /// - `Err(...)` if an error occurs during validation or the Hive Metastore
    /// query, with the error encapsulating the issue.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let name = validate_namespace(namespace)?;

        let resp = self.client.0.get_database(name.clone().into()).await;

        match resp {
            Ok(_) => Ok(true),
            Err(err) => {
                if let ResponseError::UserException(ThriftHiveMetastoreGetDatabaseException::O1(
                    _,
                )) = &err
                {
                    Ok(false)
                } else {
                    Err(from_thrift_error(err))
                }
            }
        }
    }

    /// Asynchronously updates properties of an existing namespace.
    ///
    /// Converts the given namespace identifier and properties into a database
    /// representation and then attempts to update the corresponding namespace  
    /// in the Hive Metastore.
    ///
    /// # Returns
    /// Returns `Ok(())` if the namespace update is successful. If the
    /// namespace cannot be updated due to missing information or an error
    /// during the update process, an `Err(...)` is returned.
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let db = convert_to_database(namespace, &properties)?;

        let name = match &db.name {
            Some(name) => name,
            None => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Database name must be specified",
                ))
            }
        };

        self.client
            .0
            .alter_database(name.clone(), db)
            .await
            .map_err(from_thrift_error)?;

        Ok(())
    }

    /// Asynchronously drops a namespace from the Hive Metastore.
    ///
    /// # Returns
    /// A `Result<()>` indicating the outcome:
    /// - `Ok(())` signifies successful namespace deletion.
    /// - `Err(...)` signifies failure to drop the namespace due to validation  
    /// errors, connectivity issues, or Hive Metastore constraints.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let name = validate_namespace(namespace)?;

        self.client
            .0
            .drop_database(name.into(), false, false)
            .await
            .map_err(from_thrift_error)?;

        Ok(())
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let name = validate_namespace(namespace)?;

        let tables = self
            .client
            .0
            .get_all_tables(name.clone().into())
            .await
            .map_err(from_thrift_error)?;

        let tables = tables
            .iter()
            .map(|table| TableIdent::new(namespace.clone(), table.to_string()))
            .collect();

        Ok(tables)
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
