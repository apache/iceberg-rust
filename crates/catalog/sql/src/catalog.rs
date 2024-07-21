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

use async_trait::async_trait;
use sqlx::{
    any::{install_default_drivers, AnyPoolOptions, AnyRow},
    Any, AnyPool, Column, Execute, Row, TypeInfo,
};
use std::borrow::Cow;
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

static CATALOG_TABLE_VIEW_NAME: &str = "iceberg_tables";
static CATALOG_NAME: &str = "catalog_name";
static TABLE_NAME: &str = "table_name";
static TABLE_NAMESPACE: &str = "table_namespace";
static METADATA_LOCATION_PROP: &str = "metadata_location";
static PREVIOUS_METADATA_LOCATION_PROP: &str = "previous_metadata_location";
static RECORD_TYPE: &str = "iceberg_type";
static TABLE_RECORD_TYPE: &str = "TABLE";

static NAMESPACE_PROPERTIES_TABLE_NAME: &str = "iceberg_namespace_properties";
static NAMESPACE_NAME: &str = "namespace";
static NAMESPACE_PROPERTY_KEY: &str = "property_key";
static NAMESPACE_PROPERTY_VALUE: &str = "property_value";

static MAX_CONNECTIONS: u32 = 10;
static IDLE_TIMEOUT: u64 = 10;
static TEST_BEFORE_ACQUIRE: bool = true;

/// Sql catalog config
#[derive(Debug, TypedBuilder)]
pub struct SqlCatalogConfig {
    uri: String,
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
    fileio: FileIO,
    backend: DatabaseType,
}

#[derive(Debug, PartialEq)]
enum DatabaseType {
    PostgreSQL,
    MySQL,
    SQLite,
}

impl SqlCatalog {
    /// Create new sql catalog instance
    pub async fn new(config: SqlCatalogConfig) -> Result<Self> {
        install_default_drivers();
        let max_connections: u32 = config
            .props
            .get("pool.max-connections")
            .map(|v| v.parse().unwrap())
            .unwrap_or(MAX_CONNECTIONS);
        let idle_timeout: u64 = config
            .props
            .get("pool.idle-timeout")
            .map(|v| v.parse().unwrap())
            .unwrap_or(IDLE_TIMEOUT);
        let test_before_acquire: bool = config
            .props
            .get("pool.test-before-acquire")
            .map(|v| v.parse().unwrap())
            .unwrap_or(TEST_BEFORE_ACQUIRE);

        let pool = AnyPoolOptions::new()
            .max_connections(max_connections)
            .idle_timeout(Duration::from_secs(idle_timeout))
            .test_before_acquire(test_before_acquire)
            .connect(&config.uri)
            .await
            .map_err(from_sqlx_error)?;

        let conn = pool.acquire().await.map_err(from_sqlx_error)?;

        let db_type = match conn.backend_name() {
            "PostgreSQL" => DatabaseType::PostgreSQL,
            "MySQL" => DatabaseType::MySQL,
            "SQLite" => DatabaseType::SQLite,
            _ => DatabaseType::SQLite,
        };

        sqlx::query(
            &("create table if not exists ".to_string()
                + CATALOG_TABLE_VIEW_NAME
                + " ("
                + CATALOG_NAME
                + " varchar(255) not null,"
                + TABLE_NAMESPACE
                + " varchar(255) not null,"
                + TABLE_NAME
                + " varchar(255) not null,"
                + METADATA_LOCATION_PROP
                + " varchar(255),"
                + PREVIOUS_METADATA_LOCATION_PROP
                + " varchar(255),"
                + RECORD_TYPE
                + " varchar(5), primary key ("
                + CATALOG_NAME
                + ", "
                + TABLE_NAMESPACE
                + ", "
                + TABLE_NAME
                + ")
                );"),
        )
        .execute(&pool)
        .await
        .map_err(from_sqlx_error)?;

        sqlx::query(
            &("create table if not exists ".to_owned()
                + NAMESPACE_PROPERTIES_TABLE_NAME
                + " ( "
                + CATALOG_NAME
                + " varchar(255) not null, "
                + NAMESPACE_NAME
                + " varchar(255) not null, "
                + NAMESPACE_PROPERTY_KEY
                + " varchar(255),  "
                + NAMESPACE_PROPERTY_VALUE
                + " varchar(255), primary key ("
                + CATALOG_NAME
                + ", "
                + NAMESPACE_NAME
                + ", "
                + NAMESPACE_PROPERTY_KEY
                + ") );"),
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
            fileio: file_io,
            backend: db_type,
        })
    }
    /// handle postgres doing things differently
    pub async fn execute_statement(
        &self,
        query: &String,
        args: Vec<Option<&String>>,
    ) -> Result<Vec<AnyRow>> {
        let query_with_placeholders: Cow<str> = if self.backend == DatabaseType::PostgreSQL {
            let mut query = query.clone();
            for i in 0..args.len() {
                query = query.replacen("?", &format!("${}", i + 1), 1);
            }
            Cow::Owned(query)
        } else {
            Cow::Borrowed(&query)
        };

        let mut sqlx_query = sqlx::query(&query_with_placeholders);
        for arg in args {
            sqlx_query = sqlx_query.bind(arg);
        }

        sqlx_query
            .fetch_all(&self.connection)
            .await
            .map_err(from_sqlx_error)
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

#[derive(Debug)]
struct NamespaceRef {
    namespace_name: String,
    namespace_prop_key: String,
    namespace_prop_value: String,
}

fn query_map_namespace(row: &AnyRow) -> std::result::Result<NamespaceRef, sqlx::Error> {
    Ok(NamespaceRef {
        namespace_name: row.try_get(0)?,
        namespace_prop_key: row.try_get(1)?,
        namespace_prop_value: row.try_get(2)?,
    })
}

#[async_trait]
impl Catalog for SqlCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let name = &self.name;
        let base_query = format!(
            "select distinct {} from {} where {} = ?",
            NAMESPACE_NAME, NAMESPACE_PROPERTIES_TABLE_NAME, CATALOG_NAME
        );

        let rows = match parent {
            None => {
                self.execute_statement(&base_query, vec![Some(&name.to_string())])
                    .await?
            }
            Some(parent) => {
                self.execute_statement(
                    &(base_query + " and " + TABLE_NAMESPACE + " like ?%"),
                    vec![Some(&name.to_string()), Some(&parent.join("."))],
                )
                .await?
            }
        };

        let iter = rows.iter().map(|row| row.try_get::<String, _>(0));

        Ok(iter
            .map(|x| {
                x.and_then(|y| {
                    NamespaceIdent::from_strs(y.split('.'))
                        .map_err(|err| sqlx::Error::Decode(Box::new(err)))
                })
            })
            .collect::<std::result::Result<_, sqlx::Error>>()
            .map_err(from_sqlx_error)?)
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        {
            let catalog_name = self.name.clone();
            let namespace = namespace.encode_in_url();

            let query_string = format!(
                "insert into {} ({}, {}, {}, {}) values (?, ?, ?, ?);",
                NAMESPACE_PROPERTIES_TABLE_NAME,
                CATALOG_NAME,
                NAMESPACE_NAME,
                NAMESPACE_PROPERTY_KEY,
                NAMESPACE_PROPERTY_VALUE
            );

            self.execute_statement(
                &query_string,
                vec![
                    Some(&catalog_name),
                    Some(&namespace),
                    None::<&String>,
                    None::<&String>,
                ],
            )
            .await?;
            for (key, value) in properties.iter() {
                self.execute_statement(
                    &query_string,
                    vec![
                        Some(&catalog_name),
                        Some(&namespace),
                        Some(&key),
                        Some(&value),
                    ],
                )
                .await?;
            }
        }

        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let catalog_name = self.name.clone();
        let rows = self
            .execute_statement(
                &format!(
                    "select {}, {}, {} from {} where {} = ? and {} = ?",
                    NAMESPACE_NAME,
                    NAMESPACE_PROPERTY_KEY,
                    NAMESPACE_PROPERTY_VALUE,
                    NAMESPACE_PROPERTIES_TABLE_NAME,
                    CATALOG_NAME,
                    NAMESPACE_NAME
                ),
                vec![Some(&catalog_name), Some(&namespace.join("."))],
            )
            .await?;

        let properties: HashMap<String, String> = rows
            .iter()
            .filter_map(|row| query_map_namespace(row).ok())
            .map(|ns| (ns.namespace_prop_key, ns.namespace_prop_value))
            .collect();

        Ok(Namespace::with_properties(namespace.clone(), properties))
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
        let namespace = namespace.join(".");
        let rows = self
            .execute_statement(
                &format!("select {}, {}, {}, {} from {} where {} = ? and {} = ? and ({} = '{}' or {} is null)", 
                    TABLE_NAMESPACE,
                    TABLE_NAME,
                    METADATA_LOCATION_PROP,
                    PREVIOUS_METADATA_LOCATION_PROP,
                    CATALOG_TABLE_VIEW_NAME,
                    CATALOG_NAME,
                    TABLE_NAMESPACE,
                    RECORD_TYPE,
                    TABLE_RECORD_TYPE,
                    RECORD_TYPE),
                vec![Some(&name), Some(&namespace)],
            )
            .await?;

        let iter = rows.iter().map(query_map);

        Ok(iter
            .map(|x| {
                x.and_then(|y| {
                    let namespace = NamespaceIdent::from_strs(y.table_namespace.split('.'))
                        .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
                    Ok(TableIdent::new(namespace, y.table_name))
                })
            })
            .collect::<std::result::Result<_, sqlx::Error>>()
            .map_err(from_sqlx_error)?)
    }

    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool> {
        let catalog_name = self.name.clone();
        let namespace = identifier.namespace().join(".");
        let name = identifier.name().to_string();
        let rows = self
            .execute_statement(
                &format!("select {}, {}, {}, {} from {} where {} = ? and {} = ? and {} = ? and ({} = '{}' or {} is null);", 
                    TABLE_NAMESPACE,
                    TABLE_NAME,
                    METADATA_LOCATION_PROP,
                    PREVIOUS_METADATA_LOCATION_PROP,
                    CATALOG_TABLE_VIEW_NAME,
                    CATALOG_NAME,
                    TABLE_NAMESPACE,
                    TABLE_NAME,
                    RECORD_TYPE,
                    TABLE_RECORD_TYPE,
                    RECORD_TYPE),
                vec![Some(&catalog_name), Some(&namespace), Some(&name)],
            )
            .await?;
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
            let row = self
                .execute_statement(
                    &format!("select {}, {}, {}, {} FROM {} where {} = ? and {} = ? and {} = ? and ({} = '{}' or {} is null)", 
                        TABLE_NAMESPACE,
                        TABLE_NAME,
                        METADATA_LOCATION_PROP,
                        PREVIOUS_METADATA_LOCATION_PROP,
                        CATALOG_TABLE_VIEW_NAME,
                        CATALOG_NAME,
                        TABLE_NAMESPACE,
                        TABLE_NAME,
                        RECORD_TYPE,
                        TABLE_RECORD_TYPE,
                        RECORD_TYPE
                    ),
                    vec![Some(&catalog_name), Some(&namespace), Some(&name)],
                )
                .await?;
            assert_eq!(row.len(), 1, "expected only one row from load_table query");
            let row = query_map(&row[0]).map_err(from_sqlx_error)?;
            row.metadata_location
        };
        let file = self.fileio.new_input(&metadata_location)?;
        let metadata_content = file.read().await?;
        let metadata = serde_json::from_slice::<TableMetadata>(&metadata_content)?;

        let table = Table::builder()
            .file_io(self.fileio.clone())
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

        let file = self.fileio.new_output(&metadata_location)?;

        file.write(serde_json::to_vec(&metadata)?.into()).await?;

        {
            let catalog_name = self.name.clone();
            let namespace = namespace.encode_in_url();
            let name = name.clone();
            let metadata_location = metadata_location.to_string();

            self.execute_statement(
                &format!(
                    "insert into {} ({}, {}, {}, {}) values (?, ?, ?, ?)",
                    CATALOG_TABLE_VIEW_NAME,
                    CATALOG_NAME,
                    TABLE_NAMESPACE,
                    TABLE_NAME,
                    METADATA_LOCATION_PROP
                ),
                vec![
                    Some(&catalog_name),
                    Some(&namespace),
                    Some(&name),
                    Some(&metadata_location),
                ],
            )
            .await?;
        }

        Ok(Table::builder()
            .file_io(self.fileio.clone())
            .metadata_location(metadata_location)
            .identifier(TableIdent::new(namespace.clone(), name))
            .metadata(metadata)
            .build())
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        let source_namespace = &src.namespace.encode_in_url();
        let source_table = &src.name;

        let destination_namespace = &dest.namespace.encode_in_url();
        let destination_table = &dest.name;

        let src_table_exist = self.table_exists(src).await;
        let dst_table_exist = self.table_exists(dest).await;

        let _pre_rename_check = match src_table_exist {
            Ok(res) => {
                if res {
                    match dst_table_exist {
                        Ok(dst_res) => {
                            if dst_res {
                                Err(Error::new(
                                    ErrorKind::Unexpected,
                                    "failed to rename table as destination already exists",
                                ))
                            } else {
                                Ok(())
                            }
                        }
                        Err(_) => Err(Error::new(ErrorKind::Unexpected, "failed to rename table")),
                    }
                } else {
                    Err(Error::new(
                        ErrorKind::Unexpected,
                        "failed to rename table as source does not exist",
                    ))
                }
            }
            Err(_) => Err(Error::new(ErrorKind::Unexpected, "failed to rename table")),
        }?;

        let query = format!(
            "update {} set {} = ?, {} = ? where {} = ? and {} = ?",
            CATALOG_TABLE_VIEW_NAME, TABLE_NAMESPACE, TABLE_NAME, TABLE_NAMESPACE, TABLE_NAME
        );

        self.execute_statement(
            &query,
            vec![
                Some(destination_namespace),
                Some(destination_table),
                Some(source_namespace),
                Some(source_table),
            ],
        )
        .await?;

        let src_table_exist = self.table_exists(src).await;
        let dst_table_exist = self.table_exists(dest).await;

        match src_table_exist {
            Ok(src_res) => match dst_table_exist {
                Ok(dst_res) => {
                    if !src_res && dst_res {
                        Ok(())
                    } else {
                        Err(Error::new(ErrorKind::Unexpected, "failed to rename table"))
                    }
                }
                Err(_) => Err(Error::new(ErrorKind::Unexpected, "failed to rename table")),
            },
            Err(_) => Err(Error::new(ErrorKind::Unexpected, "failed to rename table")),
        }
    }

    async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
        todo!()
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

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
        let sql_lite_uri = "sqlite://iceberg";

        if !sqlx::Sqlite::database_exists(sql_lite_uri).await.unwrap() {
            sqlx::Sqlite::create_database(sql_lite_uri).await.unwrap();
        }

        let config = SqlCatalogConfig::builder()
            .uri(sql_lite_uri.to_string())
            .name("iceberg".to_string())
            .warehouse(warehouse_root.to_owned())
            .build();

        let catalog = SqlCatalog::new(config).await.unwrap();

        let namespace = NamespaceIdent::new("test".to_owned());

        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

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
        sqlx::Sqlite::drop_database(sql_lite_uri).await.unwrap();
    }
}
