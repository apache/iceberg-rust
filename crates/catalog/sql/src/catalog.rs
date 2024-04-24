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

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::{AsyncReadExt, AsyncWriteExt};
use sqlx::any::AnyPoolOptions;
use sqlx::{
    any::{install_default_drivers, AnyRow},
    AnyPool, Row,
};
use std::collections::HashMap;

use iceberg::{
    io::FileIO,
    spec::{TableMetadata, TableMetadataBuilder},
    table::Table,
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use std::time::Duration;
use typed_builder::TypedBuilder;
use uuid::Uuid;

use crate::error::from_sqlx_error;

/// Sql catalog config
#[derive(Debug, TypedBuilder)]
pub struct SqlCatalogConfig {
    url: String,
    name: String,
    warehouse: String,
    #[builder(default)]
    props: HashMap<String, String>,
}

#[derive(Debug)]
/// Sql catalog implementation.
pub struct SqlCatalog {
    name: String,
    connection: AnyPool,
    storage: FileIO,
    cache: Arc<DashMap<TableIdent, (String, TableMetadata)>>,
}

impl SqlCatalog {
    /// Create new sql catalog instance
    pub async fn new(config: SqlCatalogConfig) -> Result<Self> {
        install_default_drivers();
        let max_connections: u32 = config
            .props
            .get("pool.max-connections")
            .map(|v| v.parse().unwrap())
            .unwrap_or(10);
        let idle_timeout: u64 = config
            .props
            .get("pool.idle-timeout")
            .map(|v| v.parse().unwrap())
            .unwrap_or(10);
        let test_before_acquire: bool = config
            .props
            .get("pool.test-before-acquire")
            .map(|v| v.parse().unwrap())
            .unwrap_or(true);

        let pool = AnyPoolOptions::new()
            .max_connections(max_connections)
            .idle_timeout(Duration::from_secs(idle_timeout))
            .test_before_acquire(test_before_acquire)
            .connect(&config.url)
            .await
            .map_err(from_sqlx_error)?;

        sqlx::query(
            "create table if not exists iceberg_tables (
                            catalog_name varchar(255) not null,
                            table_namespace varchar(255) not null,
                            table_name varchar(255) not null,
                            metadata_location varchar(255),
                            previous_metadata_location varchar(255),
                            primary key (catalog_name, table_namespace, table_name)
                        );",
        )
        .execute(&pool)
        .await
        .map_err(from_sqlx_error)?;

        sqlx::query(
            "create table if not exists iceberg_namespace_properties (
                            catalog_name varchar(255) not null,
                            namespace varchar(255) not null,
                            property_key varchar(255),
                            property_value varchar(255),
                            primary key (catalog_name, namespace, property_key)
                        );",
        )
        .execute(&pool)
        .await
        .map_err(from_sqlx_error)?;

        let file_io = FileIO::from_path(&config.warehouse)?
            .with_props(&config.props)
            .build()?;

        Ok(SqlCatalog {
            name: config.name.to_owned(),
            connection: pool,
            storage: file_io,
            cache: Arc::new(DashMap::new()),
        })
    }
}

#[derive(Debug)]
struct TableRef {
    table_namespace: String,
    table_name: String,
    metadata_location: String,
    _previous_metadata_location: Option<String>,
}

fn query_map(row: &AnyRow) -> std::result::Result<TableRef, sqlx::Error> {
    Ok(TableRef {
        table_namespace: row.try_get(0)?,
        table_name: row.try_get(1)?,
        metadata_location: row.try_get(2)?,
        _previous_metadata_location: row.try_get::<String, _>(3).map(Some).or_else(|err| {
            if let sqlx::Error::ColumnDecode {
                index: _,
                source: _,
            } = err
            {
                Ok(None)
            } else {
                Err(err)
            }
        })?,
    })
}

#[async_trait]
impl Catalog for SqlCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let name = self.name.clone();
        let rows = sqlx::query(
            "select distinct table_namespace from iceberg_tables where catalog_name = ?;",
        )
        .bind(&name)
        .fetch_all(&self.connection)
        .await
        .map_err(from_sqlx_error)?;
        let iter = rows.iter().map(|row| row.try_get::<String, _>(0));

        Ok(iter
            .map(|x| {
                x.and_then(|y| {
                    NamespaceIdent::from_vec(
                        y.split('.').map(ToString::to_string).collect::<Vec<_>>(),
                    )
                    .map_err(|err| sqlx::Error::Decode(Box::new(err)))
                })
            })
            .collect::<std::result::Result<_, sqlx::Error>>()
            .map_err(from_sqlx_error)?)
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

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let name = self.name.clone();
        let namespace = namespace.encode_in_url();
        let rows = sqlx::query("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = ? and table_namespace = ?;").bind(&name).bind(&namespace).fetch_all(&self.connection).await.map_err(from_sqlx_error)?;
        let iter = rows.iter().map(query_map);

        Ok(iter
            .map(|x| {
                x.and_then(|y| {
                    let namespace = NamespaceIdent::from_vec(
                        y.table_namespace
                            .split('.')
                            .map(ToString::to_string)
                            .collect::<Vec<_>>(),
                    )
                    .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
                    Ok(TableIdent::new(namespace, y.table_name))
                })
            })
            .collect::<std::result::Result<_, sqlx::Error>>()
            .map_err(from_sqlx_error)?)
    }

    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool> {
        let catalog_name = self.name.clone();
        let namespace = identifier.namespace().encode_in_url();
        let name = identifier.name().to_string();
        let rows = sqlx::query("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = ? and table_namespace = ? and table_name = ?;")
            .bind(&catalog_name)
            .bind(&namespace)
            .bind(&name)
            .fetch_all(&self.connection)
            .await
            .map_err(from_sqlx_error)?;
        let mut iter = rows.iter().map(query_map);

        Ok(iter.next().is_some())
    }

    async fn drop_table(&self, _identifier: &TableIdent) -> Result<()> {
        todo!()
    }

    async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
        let metadata_location = {
            let catalog_name = self.name.clone();
            let namespace = identifier.namespace().encode_in_url();
            let name = identifier.name().to_string();
            let row = sqlx::query("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = ? and table_namespace = ? and table_name = ?;").bind(&catalog_name).bind(&namespace).bind(&name).fetch_one(&self.connection).await.map_err(from_sqlx_error)?;
            let row = query_map(&row).map_err(from_sqlx_error)?;

            row.metadata_location
        };
        let file = self.storage.new_input(&metadata_location)?;

        let mut json = String::new();
        file.reader().await?.read_to_string(&mut json).await?;

        let metadata: TableMetadata = serde_json::from_str(&json)?;

        self.cache
            .insert(identifier.clone(), (metadata_location, metadata.clone()));

        let table = Table::builder()
            .file_io(self.storage.clone())
            .identifier(identifier.clone())
            .metadata(metadata)
            .build();

        Ok(table)
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let location = creation.location.as_ref().ok_or(Error::new(
            ErrorKind::DataInvalid,
            "Table creation with the Sql catalog requires a location.",
        ))?;
        let name = creation.name.clone();

        let uuid = Uuid::new_v4();
        let metadata_location =
            location.clone() + "/metadata/" + "0-" + &uuid.to_string() + ".metadata.json";

        let metadata = TableMetadataBuilder::from_table_creation(creation)?.build()?;

        let file = self.storage.new_output(&metadata_location)?;
        file.writer()
            .await?
            .write_all(&serde_json::to_vec(&metadata)?)
            .await?;
        {
            let catalog_name = self.name.clone();
            let namespace = namespace.encode_in_url();
            let name = name.clone();
            let metadata_location = metadata_location.to_string();

            sqlx::query("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values (?, ?, ?, ?);")
                .bind(&catalog_name)
                .bind(&namespace)
                .bind(&name)
                .bind(&metadata_location)
                .execute(&self.connection)
                .await
                .map_err(from_sqlx_error)?;
        }

        Ok(Table::builder()
            .file_io(self.storage.clone())
            .metadata_location(metadata_location)
            .identifier(TableIdent::new(namespace.clone(), name))
            .metadata(metadata)
            .build())
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        todo!()
    }

    async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
        todo!()
    }
}

#[cfg(test)]
pub mod tests {
    use iceberg::{
        spec::{NestedField, PrimitiveType, Schema, Type},
        Catalog, NamespaceIdent, TableCreation, TableIdent,
    };
    use tempfile::TempDir;

    use crate::{SqlCatalog, SqlCatalogConfig};
    use sqlx::migrate::MigrateDatabase;

    #[tokio::test]
    async fn test_create_update_drop_table() {
        let dir = TempDir::with_prefix("sql-test").unwrap();
        let warehouse_root = dir.path().to_str().unwrap();

        //name of the database should be part of the url. usually for sqllite it creates or opens one if (.db found)
        let sql_lite_url = "sqlite://iceberg";

        if !sqlx::Sqlite::database_exists(sql_lite_url).await.unwrap() {
            sqlx::Sqlite::create_database(sql_lite_url).await.unwrap();
        }

        let config = SqlCatalogConfig::builder()
            .url(sql_lite_url.to_string())
            .name("iceberg".to_string())
            .warehouse(warehouse_root.to_owned())
            .build();

        let catalog = SqlCatalog::new(config).await.unwrap();

        let namespace = NamespaceIdent::new("test".to_owned());

        let identifier = TableIdent::new(namespace.clone(), "table1".to_owned());

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(1, "one", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(2, "two", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let creation = TableCreation::builder()
            .name("table1".to_owned())
            .location(warehouse_root.to_owned() + "/warehouse/table1")
            .schema(schema)
            .build();

        catalog.create_table(&namespace, creation).await.unwrap();

        let exists = catalog
            .table_exists(&identifier)
            .await
            .expect("Table doesn't exist");
        assert!(exists);

        let tables = catalog
            .list_tables(&namespace)
            .await
            .expect("Failed to list Tables");
        assert_eq!(tables[0].name(), "table1".to_owned());

        let namespaces = catalog
            .list_namespaces(None)
            .await
            .expect("Failed to list namespaces");
        assert_eq!(namespaces[0].encode_in_url(), "test");

        //load table points to a /var location - check why

        let table = catalog.load_table(&identifier).await.unwrap();

        assert!(table.metadata().location().ends_with("/warehouse/table1"));

        //tear down the database and tables
        sqlx::Sqlite::drop_database(sql_lite_url).await.unwrap();
    }
}
