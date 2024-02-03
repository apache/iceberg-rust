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
use hive_metastore::{TThriftHiveMetastoreSyncClient, ThriftHiveMetastoreSyncClient};
use iceberg::table::Table;
use iceberg::{Catalog, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{
    ReadHalf, TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, WriteHalf,
};
use typed_builder::TypedBuilder;

/// Hive metastore Catalog configuration.
#[derive(Debug, TypedBuilder)]
pub struct HmsCatalogConfig {
    address: String,
}

/// TODO: We only support binary protocol for now.
type HmsClientType = ThriftHiveMetastoreSyncClient<
    TBinaryInputProtocol<TBufferedReadTransport<ReadHalf<thrift::transport::TTcpChannel>>>,
    TBinaryOutputProtocol<TBufferedWriteTransport<WriteHalf<thrift::transport::TTcpChannel>>>,
>;

/// # TODO
///
/// we are using the same connection everytime, we should support connection
/// pool in the future.
struct HmsClient(Arc<Mutex<HmsClientType>>);

impl HmsClient {
    fn call<T>(&self, f: impl FnOnce(&mut HmsClientType) -> thrift::Result<T>) -> Result<T> {
        let mut client = self.0.lock().unwrap();
        f(&mut client).map_err(from_thrift_error)
    }
}

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
        let mut channel = thrift::transport::TTcpChannel::new();
        channel
            .open(config.address.as_str())
            .map_err(from_thrift_error)?;
        let (i_chan, o_chan) = channel.split().map_err(from_thrift_error)?;
        let i_chan = TBufferedReadTransport::new(i_chan);
        let o_chan = TBufferedWriteTransport::new(o_chan);
        let i_proto = TBinaryInputProtocol::new(i_chan, true);
        let o_proto = TBinaryOutputProtocol::new(o_chan, true);
        let client = ThriftHiveMetastoreSyncClient::new(i_proto, o_proto);
        Ok(Self {
            config,
            client: HmsClient(Arc::new(Mutex::new(client))),
        })
    }
}

/// Refer to <https://github.com/apache/iceberg/blob/main/hive-metastore/src/main/java/org/apache/iceberg/hive/HiveCatalog.java> for implementation details.
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
            self.client.call(|client| client.get_all_databases())?
        };

        Ok(dbs.into_iter().map(NamespaceIdent::new).collect())
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        todo!()
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> Result<Namespace> {
        todo!()
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> Result<bool> {
        todo!()
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        todo!()
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> Result<()> {
        todo!()
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
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

    async fn stat_table(&self, _table: &TableIdent) -> Result<bool> {
        todo!()
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        todo!()
    }

    async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
        todo!()
    }
}
