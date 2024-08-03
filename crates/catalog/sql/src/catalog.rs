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

use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use sqlx::any::{install_default_drivers, AnyPoolOptions, AnyRow};
use sqlx::{AnyPool, Row};
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
    warehouse_location: Option<String>,
    file_io: FileIO,
    #[builder(default)]
    props: HashMap<String, String>,
}

#[derive(Debug)]
/// Sql catalog implementation.
pub struct SqlCatalog {
    name: String,
    connection: AnyPool,
    warehouse_location: Option<String>,
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
            &format!("create table if not exists {} ({} varchar(255) not null, {} varchar(255) not null, {} varchar(255) not null, {} varchar(255), {} varchar(255), {} varchar(5), primary key ({}, {}, {}))", 
            CATALOG_TABLE_VIEW_NAME,
            CATALOG_NAME,
            TABLE_NAMESPACE,
            TABLE_NAME,
            METADATA_LOCATION_PROP,
            PREVIOUS_METADATA_LOCATION_PROP,
            RECORD_TYPE,
            CATALOG_NAME,
            TABLE_NAMESPACE,
            TABLE_NAME),
        )
        .execute(&pool)
        .await
        .map_err(from_sqlx_error)?;

        sqlx::query(
            &format!("create table if not exists {} ({} varchar(255) not null, {} varchar(255) not null, {} varchar(255), {} varchar(255), primary key ({}, {}, {}))",
            NAMESPACE_PROPERTIES_TABLE_NAME,
            CATALOG_NAME,
            NAMESPACE_NAME,
            NAMESPACE_PROPERTY_KEY,
            NAMESPACE_PROPERTY_VALUE,
            CATALOG_NAME,
            NAMESPACE_NAME,
            NAMESPACE_PROPERTY_KEY)

        )
        .execute(&pool)
        .await
        .map_err(from_sqlx_error)?;

        Ok(SqlCatalog {
            name: config.name.to_owned(),
            connection: pool,
            warehouse_location: config.warehouse_location,
            fileio: config.file_io,
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
            Cow::Borrowed(query)
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
        table_namespace: row.try_get(TABLE_NAMESPACE)?,
        table_name: row.try_get(TABLE_NAME)?,
        metadata_location: row.try_get(METADATA_LOCATION_PROP)?,
        _previous_metadata_location: row
            .try_get::<String, _>(PREVIOUS_METADATA_LOCATION_PROP)
            .map(Some)
            .or_else(|err| {
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
struct NamespacePropRef {
    namespace_prop_key: String,
    namespace_prop_value: String,
}

fn query_map_namespace(row: &AnyRow) -> std::result::Result<NamespacePropRef, sqlx::Error> {
    Ok(NamespacePropRef {
        namespace_prop_key: row.try_get(NAMESPACE_PROPERTY_KEY)?,
        namespace_prop_value: row.try_get(NAMESPACE_PROPERTY_VALUE)?,
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
            let namespace = namespace.to_url_string();

            let query_string = format!(
                "insert into {} ({}, {}, {}, {}) values (?, ?, ?, ?);",
                NAMESPACE_PROPERTIES_TABLE_NAME,
                CATALOG_NAME,
                NAMESPACE_NAME,
                NAMESPACE_PROPERTY_KEY,
                NAMESPACE_PROPERTY_VALUE
            );

            if properties.is_empty() {
                self.execute_statement(&query_string, vec![
                    Some(&catalog_name),
                    Some(&namespace),
                    None::<&String>,
                    None::<&String>,
                ])
                .await?;
            } else {
                for (key, value) in properties.iter() {
                    self.execute_statement(&query_string, vec![
                        Some(&catalog_name),
                        Some(&namespace),
                        Some(key),
                        Some(value),
                    ])
                    .await?;
                }
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

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let catalog_name = self.name.clone();
        let namespace = namespace.join(".");
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
                vec![Some(&catalog_name), Some(&namespace)],
            )
            .await?;
        let mut iter = rows.iter().map(query_map_namespace);

        Ok(iter.next().is_some())
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let catalog_name = self.name.clone();
        let namespace_name = namespace.join(".");
        let exists = self.namespace_exists(namespace).await?;
        if !exists {
            Err(Error::new(
                ErrorKind::Unexpected,
                "cannot update namespace that does not exist",
            ))
        } else {
            let current_nsp = self.get_namespace(namespace).await?;
            let current_props = current_nsp.properties();

            for (key, value) in properties {
                if current_props.contains_key(&key) {
                    self.execute_statement(
                        &format!(
                            "update {} set {} = ?, {} = ? where {} = ? and {} = ? and {} = ?",
                            NAMESPACE_PROPERTIES_TABLE_NAME,
                            NAMESPACE_PROPERTY_KEY,
                            NAMESPACE_PROPERTY_VALUE,
                            NAMESPACE_PROPERTY_KEY,
                            CATALOG_NAME,
                            NAMESPACE_NAME
                        ),
                        vec![
                            Some(&key),
                            Some(&value),
                            Some(&key),
                            Some(&catalog_name),
                            Some(&namespace_name),
                        ],
                    )
                    .await?;
                } else {
                    self.execute_statement(
                        &format!(
                            "insert into {} ({}, {}, {}, {}) values (?, ?, ?, ?)",
                            NAMESPACE_PROPERTIES_TABLE_NAME,
                            CATALOG_NAME,
                            NAMESPACE_NAME,
                            NAMESPACE_PROPERTY_KEY,
                            NAMESPACE_PROPERTY_VALUE
                        ),
                        vec![
                            Some(&catalog_name),
                            Some(&namespace_name),
                            Some(&key),
                            Some(&value),
                        ],
                    )
                    .await?;
                }
            }

            Ok(())
        }
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let existence = self.namespace_exists(namespace).await?;
        if existence {
            if let Ok(tbls) = self.list_tables(namespace).await {
                if !tbls.is_empty() {
                    Err(Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "unable to drop namespace as it contains {} tables",
                            tbls.len()
                        ),
                    ))
                } else {
                    self.execute_statement(
                        &format!(
                            "delete from {} where {} = ? and {} = ?",
                            NAMESPACE_PROPERTIES_TABLE_NAME, CATALOG_NAME, NAMESPACE_NAME
                        ),
                        vec![Some(&self.name.clone()), Some(&namespace.join("."))],
                    )
                    .await?;

                    Ok(())
                }
            } else {
                Err(Error::new(
                    ErrorKind::Unexpected,
                    "unable to drop namespace",
                ))
            }
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "unable to drop namespace as it does not exist",
            ))
        }
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

    async fn drop_table(&self, identifier: &TableIdent) -> Result<()> {
        let catalog_name = self.name.clone();
        let namespace = identifier.namespace().to_url_string();
        let name = identifier.name.to_string();

        self.execute_statement(
            &format!(
                "delete from {} where {} = ? and {} = ? and {} = ?",
                CATALOG_TABLE_VIEW_NAME, CATALOG_NAME, TABLE_NAMESPACE, TABLE_NAME
            ),
            vec![Some(&catalog_name), Some(&namespace), Some(&name)],
        )
        .await?;

        let table_existence = self.table_exists(identifier).await;

        match table_existence {
            Ok(false) => Ok(()),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                "drop table was not successful",
            )),
        }
    }

    async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
        let metadata_location = {
            let catalog_name = self.name.clone();
            let namespace = identifier.namespace().to_url_string();
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
            let namespace = namespace.to_url_string();
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
        let source_namespace = &src.namespace.to_url_string();
        let source_table = &src.name;

        let destination_namespace = &dest.namespace.to_url_string();
        let destination_table = &dest.name;

        let src_table_exist = self.table_exists(src).await;
        let dst_table_exist = self.table_exists(dest).await;

        match (src_table_exist, dst_table_exist) {
            (Ok(true), Ok(false)) => Ok(()),
            (_, Ok(true)) => Err(Error::new(
                ErrorKind::Unexpected,
                "failed to rename table as destination already exists",
            )),
            (Ok(false), _) => Err(Error::new(
                ErrorKind::Unexpected,
                "failed to rename table as source does not exist",
            )),
            _ => Err(Error::new(ErrorKind::Unexpected, "failed to rename table")),
        }?;

        let query = format!(
            "update {} set {} = ?, {} = ? where {} = ? and {} = ?",
            CATALOG_TABLE_VIEW_NAME, TABLE_NAMESPACE, TABLE_NAME, TABLE_NAMESPACE, TABLE_NAME
        );

        self.execute_statement(&query, vec![
            Some(destination_namespace),
            Some(destination_table),
            Some(source_namespace),
            Some(source_table),
        ])
        .await?;

        let src_table_exist = self.table_exists(src).await;
        let dst_table_exist = self.table_exists(dest).await;

        match (src_table_exist, dst_table_exist) {
            (Ok(false), Ok(true)) => Ok(()),
            _ => Err(Error::new(ErrorKind::Unexpected, "failed to rename table")),
        }
    }

    async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::hash::Hash;
    use std::iter::FromIterator;

    use iceberg::io::FileIOBuilder;
    use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Type};
    use iceberg::{Catalog, Namespace, NamespaceIdent, TableCreation, TableIdent};
    use itertools::Itertools;
    use regex::Regex;
    use sqlx::migrate::MigrateDatabase;
    use tempfile::TempDir;

    use super::*;
    use crate::{SqlCatalog, SqlCatalogConfig};

    fn temp_path() -> String {
        let temp_dir = TempDir::new().unwrap();
        temp_dir.path().to_str().unwrap().to_string()
    }

    async fn new_sql_catalog() -> impl Catalog {
        new_sql_catalog_with_warehouse_location(None).await
    }

    async fn new_sql_catalog_with_warehouse_location(
        warehouse_location: Option<String>,
    ) -> impl Catalog {
        let sql_lite_uri = format!("sqlite:{}", temp_path());
        sqlx::Sqlite::create_database(&sql_lite_uri).await.unwrap();

        let config = SqlCatalogConfig::builder()
            .uri(sql_lite_uri.to_string())
            .name("iceberg".to_string())
            .warehouse_location(warehouse_location)
            .file_io(FileIOBuilder::new_fs_io().build().unwrap())
            .build();

        SqlCatalog::new(config).await.unwrap()
    }

    async fn create_namespace<C: Catalog>(catalog: &C, namespace_ident: &NamespaceIdent) {
        let _ = catalog
            .create_namespace(namespace_ident, HashMap::new())
            .await
            .unwrap();
    }

    async fn create_namespaces<C: Catalog>(catalog: &C, namespace_idents: &Vec<&NamespaceIdent>) {
        for namespace_ident in namespace_idents {
            let _ = create_namespace(catalog, namespace_ident).await;
        }
    }

    fn to_set<T: std::cmp::Eq + Hash>(vec: Vec<T>) -> HashSet<T> {
        HashSet::from_iter(vec)
    }

    fn simple_table_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![NestedField::required(
                1,
                "foo",
                Type::Primitive(PrimitiveType::Int),
            )
            .into()])
            .build()
            .unwrap()
    }

    async fn create_table<C: Catalog>(catalog: &C, table_ident: &TableIdent) {
        let _ = catalog
            .create_table(
                &table_ident.namespace,
                TableCreation::builder()
                    .name(table_ident.name().into())
                    .schema(simple_table_schema())
                    .location(temp_path())
                    .build(),
            )
            .await
            .unwrap();
    }

    async fn create_tables<C: Catalog>(catalog: &C, table_idents: Vec<&TableIdent>) {
        for table_ident in table_idents {
            create_table(catalog, table_ident).await;
        }
    }

    fn assert_table_eq(table: &Table, expected_table_ident: &TableIdent, expected_schema: &Schema) {
        assert_eq!(table.identifier(), expected_table_ident);

        let metadata = table.metadata();

        assert_eq!(metadata.current_schema().as_ref(), expected_schema);

        let expected_partition_spec = PartitionSpec::builder()
            .with_spec_id(0)
            .with_fields(vec![])
            .build()
            .unwrap();

        assert_eq!(
            metadata
                .partition_specs_iter()
                .map(|p| p.as_ref())
                .collect_vec(),
            vec![&expected_partition_spec]
        );

        let expected_sorted_order = SortOrder::builder()
            .with_order_id(0)
            .with_fields(vec![])
            .build(expected_schema.clone())
            .unwrap();

        assert_eq!(
            metadata
                .sort_orders_iter()
                .map(|s| s.as_ref())
                .collect_vec(),
            vec![&expected_sorted_order]
        );

        assert_eq!(metadata.properties(), &HashMap::new());

        assert!(!table.readonly());
    }

    const UUID_REGEX_STR: &str = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    fn assert_table_metadata_location_matches(table: &Table, regex_str: &str) {
        let actual = table.metadata_location().unwrap().to_string();
        let regex = Regex::new(regex_str).unwrap();
        assert!(regex.is_match(&actual))
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_empty_vector() {
        let catalog = new_sql_catalog().await;

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_single_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![
            namespace_ident
        ]);
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_multiple_namespaces() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]).await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1, namespace_ident_2])
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_only_top_level_namespaces() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![
            &namespace_ident_1,
            &namespace_ident_2,
            &namespace_ident_3,
        ])
        .await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1, namespace_ident_3])
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_no_namespaces_under_parent() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]).await;

        assert_eq!(
            catalog
                .list_namespaces(Some(&namespace_ident_1))
                .await
                .unwrap(),
            vec![]
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_namespace_under_parent() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::new("c".into());
        create_namespaces(&catalog, &vec![
            &namespace_ident_1,
            &namespace_ident_2,
            &namespace_ident_3,
        ])
        .await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1.clone(), namespace_ident_3])
        );

        assert_eq!(
            catalog
                .list_namespaces(Some(&namespace_ident_1))
                .await
                .unwrap(),
            vec![NamespaceIdent::new("b".into())]
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_multiple_namespaces_under_parent() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_1 = NamespaceIdent::new("a".to_string());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "a"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_4 = NamespaceIdent::from_strs(vec!["a", "c"]).unwrap();
        let namespace_ident_5 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![
            &namespace_ident_1,
            &namespace_ident_2,
            &namespace_ident_3,
            &namespace_ident_4,
            &namespace_ident_5,
        ])
        .await;

        assert_eq!(
            to_set(
                catalog
                    .list_namespaces(Some(&namespace_ident_1))
                    .await
                    .unwrap()
            ),
            to_set(vec![
                NamespaceIdent::new("a".into()),
                NamespaceIdent::new("b".into()),
                NamespaceIdent::new("c".into()),
            ])
        );
    }

    #[tokio::test]
    async fn test_namespace_exists_returns_false() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert!(!catalog
            .namespace_exists(&NamespaceIdent::new("b".into()))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_namespace_exists_returns_true() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert!(catalog.namespace_exists(&namespace_ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_namespace_with_empty_properties() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("a".into());

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .unwrap(),
            Namespace::new(namespace_ident.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_namespace_with_properties() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("abc".into());

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("k".into(), "v".into());

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, properties.clone())
                .await
                .unwrap(),
            Namespace::with_properties(namespace_ident.clone(), properties.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, properties)
        );
    }

    #[tokio::test]
    async fn test_create_namespace_throws_error_if_namespace_already_exists() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => Cannot create namespace {:?}. Namespace already exists.",
                &namespace_ident
            )
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_nested_namespace() {
        let catalog = new_sql_catalog().await;
        let parent_namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &parent_namespace_ident).await;

        let child_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&child_namespace_ident, HashMap::new())
                .await
                .unwrap(),
            Namespace::new(child_namespace_ident.clone())
        );

        assert_eq!(
            catalog.get_namespace(&child_namespace_ident).await.unwrap(),
            Namespace::with_properties(child_namespace_ident, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_deeply_nested_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident_a_b_c, HashMap::new())
                .await
                .unwrap(),
            Namespace::new(namespace_ident_a_b_c.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b_c).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b_c, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_nested_namespace_throws_error_if_top_level_namespace_doesnt_exist() {
        let catalog = new_sql_catalog().await;

        let nested_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&nested_namespace_ident, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                NamespaceIdent::new("a".into())
            )
        );

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_create_deeply_nested_namespace_throws_error_if_intermediate_namespace_doesnt_exist(
    ) {
        let catalog = new_sql_catalog().await;

        let namespace_ident_a = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident_a).await;

        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident_a_b_c, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                NamespaceIdent::from_strs(vec!["a", "b"]).unwrap()
            )
        );

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![
            namespace_ident_a.clone()
        ]);

        assert_eq!(
            catalog
                .list_namespaces(Some(&namespace_ident_a))
                .await
                .unwrap(),
            vec![]
        );
    }

    #[tokio::test]
    async fn test_get_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("abc".into());

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("k".into(), "v".into());
        let _ = catalog
            .create_namespace(&namespace_ident, properties.clone())
            .await
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, properties)
        )
    }

    #[tokio::test]
    async fn test_get_nested_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_get_deeply_nested_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(&catalog, &vec![
            &namespace_ident_a,
            &namespace_ident_a_b,
            &namespace_ident_a_b_c,
        ])
        .await;

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b_c).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b_c, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_get_namespace_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_sql_catalog().await;
        create_namespace(&catalog, &NamespaceIdent::new("a".into())).await;

        let non_existent_namespace_ident = NamespaceIdent::new("b".into());
        assert_eq!(
            catalog
                .get_namespace(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_namespace_ident
            )
        )
    }

    #[tokio::test]
    async fn test_update_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident).await;

        let mut new_properties: HashMap<String, String> = HashMap::new();
        new_properties.insert("k".into(), "v".into());

        catalog
            .update_namespace(&namespace_ident, new_properties.clone())
            .await
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, new_properties)
        )
    }

    #[tokio::test]
    async fn test_update_nested_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let mut new_properties = HashMap::new();
        new_properties.insert("k".into(), "v".into());

        catalog
            .update_namespace(&namespace_ident_a_b, new_properties.clone())
            .await
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b, new_properties)
        );
    }

    #[tokio::test]
    async fn test_update_deeply_nested_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(&catalog, &vec![
            &namespace_ident_a,
            &namespace_ident_a_b,
            &namespace_ident_a_b_c,
        ])
        .await;

        let mut new_properties = HashMap::new();
        new_properties.insert("k".into(), "v".into());

        catalog
            .update_namespace(&namespace_ident_a_b_c, new_properties.clone())
            .await
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b_c).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b_c, new_properties)
        );
    }

    #[tokio::test]
    async fn test_update_namespace_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_sql_catalog().await;
        create_namespace(&catalog, &NamespaceIdent::new("abc".into())).await;

        let non_existent_namespace_ident = NamespaceIdent::new("def".into());
        assert_eq!(
            catalog
                .update_namespace(&non_existent_namespace_ident, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_namespace_ident
            )
        )
    }

    #[tokio::test]
    async fn test_drop_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident).await;

        catalog.drop_namespace(&namespace_ident).await.unwrap();

        assert!(!catalog.namespace_exists(&namespace_ident).await.unwrap())
    }

    #[tokio::test]
    async fn test_drop_nested_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        catalog.drop_namespace(&namespace_ident_a_b).await.unwrap();

        assert!(!catalog
            .namespace_exists(&namespace_ident_a_b)
            .await
            .unwrap());

        assert!(catalog.namespace_exists(&namespace_ident_a).await.unwrap());
    }

    #[tokio::test]
    async fn test_drop_deeply_nested_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(&catalog, &vec![
            &namespace_ident_a,
            &namespace_ident_a_b,
            &namespace_ident_a_b_c,
        ])
        .await;

        catalog
            .drop_namespace(&namespace_ident_a_b_c)
            .await
            .unwrap();

        assert!(!catalog
            .namespace_exists(&namespace_ident_a_b_c)
            .await
            .unwrap());

        assert!(catalog
            .namespace_exists(&namespace_ident_a_b)
            .await
            .unwrap());

        assert!(catalog.namespace_exists(&namespace_ident_a).await.unwrap());
    }

    #[tokio::test]
    async fn test_drop_namespace_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_sql_catalog().await;

        let non_existent_namespace_ident = NamespaceIdent::new("abc".into());
        assert_eq!(
            catalog
                .drop_namespace(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_namespace_ident
            )
        )
    }

    #[tokio::test]
    async fn test_drop_namespace_throws_error_if_nested_namespace_doesnt_exist() {
        let catalog = new_sql_catalog().await;
        create_namespace(&catalog, &NamespaceIdent::new("a".into())).await;

        let non_existent_namespace_ident =
            NamespaceIdent::from_vec(vec!["a".into(), "b".into()]).unwrap();
        assert_eq!(
            catalog
                .drop_namespace(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_namespace_ident
            )
        )
    }

    #[tokio::test]
    async fn test_dropping_a_namespace_also_drops_namespaces_nested_under_that_one() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        catalog.drop_namespace(&namespace_ident_a).await.unwrap();

        assert!(!catalog.namespace_exists(&namespace_ident_a).await.unwrap());

        assert!(!catalog
            .namespace_exists(&namespace_ident_a_b)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_create_table_with_location() {
        let tmp_dir = TempDir::new().unwrap();
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_name = "abc";
        let location = tmp_dir.path().to_str().unwrap().to_string();
        let table_creation = TableCreation::builder()
            .name(table_name.into())
            .location(location.clone())
            .schema(simple_table_schema())
            .build();

        let expected_table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());

        assert_table_eq(
            &catalog
                .create_table(&namespace_ident, table_creation)
                .await
                .unwrap(),
            &expected_table_ident,
            &simple_table_schema(),
        );

        let table = catalog.load_table(&expected_table_ident).await.unwrap();

        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());

        assert!(table
            .metadata_location()
            .unwrap()
            .to_string()
            .starts_with(&location))
    }

    #[tokio::test]
    async fn test_create_table_falls_back_to_namespace_location_if_table_location_is_missing() {
        let warehouse_location = temp_path();
        let catalog =
            new_sql_catalog_with_warehouse_location(Some(warehouse_location.clone())).await;

        let namespace_ident = NamespaceIdent::new("a".into());
        let mut namespace_properties = HashMap::new();
        let namespace_location = temp_path();
        namespace_properties.insert(LOCATION.to_string(), namespace_location.to_string());
        catalog
            .create_namespace(&namespace_ident, namespace_properties)
            .await
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex = format!(
            "^{}/tbl1/metadata/0-{}.metadata.json$",
            namespace_location, UUID_REGEX_STR,
        );

        let table = catalog
            .create_table(
                &namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .await
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);

        let table = catalog.load_table(&expected_table_ident).await.unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);
    }

    #[tokio::test]
    async fn test_create_table_in_nested_namespace_falls_back_to_nested_namespace_location_if_table_location_is_missing(
    ) {
        let warehouse_location = temp_path();
        let catalog =
            new_sql_catalog_with_warehouse_location(Some(warehouse_location.clone())).await;

        let namespace_ident = NamespaceIdent::new("a".into());
        let mut namespace_properties = HashMap::new();
        let namespace_location = temp_path();
        namespace_properties.insert(LOCATION.to_string(), namespace_location.to_string());
        catalog
            .create_namespace(&namespace_ident, namespace_properties)
            .await
            .unwrap();

        let nested_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let mut nested_namespace_properties = HashMap::new();
        let nested_namespace_location = temp_path();
        nested_namespace_properties
            .insert(LOCATION.to_string(), nested_namespace_location.to_string());
        catalog
            .create_namespace(&nested_namespace_ident, nested_namespace_properties)
            .await
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident =
            TableIdent::new(nested_namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex = format!(
            "^{}/tbl1/metadata/0-{}.metadata.json$",
            nested_namespace_location, UUID_REGEX_STR,
        );

        let table = catalog
            .create_table(
                &nested_namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .await
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);

        let table = catalog.load_table(&expected_table_ident).await.unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);
    }

    #[tokio::test]
    async fn test_create_table_falls_back_to_warehouse_location_if_both_table_location_and_namespace_location_are_missing(
    ) {
        let warehouse_location = temp_path();
        let catalog =
            new_sql_catalog_with_warehouse_location(Some(warehouse_location.clone())).await;

        let namespace_ident = NamespaceIdent::new("a".into());
        // note: no location specified in namespace_properties
        let namespace_properties = HashMap::new();
        catalog
            .create_namespace(&namespace_ident, namespace_properties)
            .await
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex = format!(
            "^{}/a/tbl1/metadata/0-{}.metadata.json$",
            warehouse_location, UUID_REGEX_STR
        );

        let table = catalog
            .create_table(
                &namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .await
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);

        let table = catalog.load_table(&expected_table_ident).await.unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);
    }

    #[tokio::test]
    async fn test_create_table_in_nested_namespace_falls_back_to_warehouse_location_if_both_table_location_and_namespace_location_are_missing(
    ) {
        let warehouse_location = temp_path();
        let catalog =
            new_sql_catalog_with_warehouse_location(Some(warehouse_location.clone())).await;

        let namespace_ident = NamespaceIdent::new("a".into());
        catalog
            // note: no location specified in namespace_properties
            .create_namespace(&namespace_ident, HashMap::new())
            .await
            .unwrap();

        let nested_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        catalog
            // note: no location specified in namespace_properties
            .create_namespace(&nested_namespace_ident, HashMap::new())
            .await
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident =
            TableIdent::new(nested_namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex = format!(
            "^{}/a/b/tbl1/metadata/0-{}.metadata.json$",
            warehouse_location, UUID_REGEX_STR
        );

        let table = catalog
            .create_table(
                &nested_namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .await
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);

        let table = catalog.load_table(&expected_table_ident).await.unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);
    }

    #[tokio::test]
    async fn test_create_table_throws_error_if_table_location_and_namespace_location_and_warehouse_location_are_missing(
    ) {
        let catalog = new_sql_catalog_with_warehouse_location(None).await;

        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_name = "tbl1";
        let expected_table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());

        assert_eq!(
            catalog
                .create_table(
                    &namespace_ident,
                    TableCreation::builder()
                        .name(table_name.into())
                        .schema(simple_table_schema())
                        .build(),
                )
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => Cannot create table {:?}. No default path is set, please specify a location when creating a table.",
                &expected_table_ident
            )
        )
    }

    #[tokio::test]
    async fn test_create_table_throws_error_if_table_with_same_name_already_exists() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_name = "tbl1";
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        create_table(&catalog, &table_ident).await;

        let tmp_dir = TempDir::new().unwrap();
        let location = tmp_dir.path().to_str().unwrap().to_string();

        assert_eq!(
            catalog
                .create_table(
                    &namespace_ident,
                    TableCreation::builder()
                        .name(table_name.into())
                        .schema(simple_table_schema())
                        .location(location)
                        .build()
                )
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => Cannot create table {:?}. Table already exists.",
                &table_ident
            )
        );
    }

    #[tokio::test]
    async fn test_list_tables_returns_empty_vector() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert_eq!(catalog.list_tables(&namespace_ident).await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_list_tables_returns_a_single_table() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        assert_eq!(catalog.list_tables(&namespace_ident).await.unwrap(), vec![
            table_ident
        ]);
    }

    #[tokio::test]
    async fn test_list_tables_returns_multiple_tables() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident_1 = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let table_ident_2 = TableIdent::new(namespace_ident.clone(), "tbl2".into());
        let _ = create_tables(&catalog, vec![&table_ident_1, &table_ident_2]).await;

        assert_eq!(
            to_set(catalog.list_tables(&namespace_ident).await.unwrap()),
            to_set(vec![table_ident_1, table_ident_2])
        );
    }

    #[tokio::test]
    async fn test_list_tables_returns_tables_from_correct_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_1 = NamespaceIdent::new("n1".into());
        let namespace_ident_2 = NamespaceIdent::new("n2".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]).await;

        let table_ident_1 = TableIdent::new(namespace_ident_1.clone(), "tbl1".into());
        let table_ident_2 = TableIdent::new(namespace_ident_1.clone(), "tbl2".into());
        let table_ident_3 = TableIdent::new(namespace_ident_2.clone(), "tbl1".into());
        let _ = create_tables(&catalog, vec![
            &table_ident_1,
            &table_ident_2,
            &table_ident_3,
        ])
        .await;

        assert_eq!(
            to_set(catalog.list_tables(&namespace_ident_1).await.unwrap()),
            to_set(vec![table_ident_1, table_ident_2])
        );

        assert_eq!(
            to_set(catalog.list_tables(&namespace_ident_2).await.unwrap()),
            to_set(vec![table_ident_3])
        );
    }

    #[tokio::test]
    async fn test_list_tables_returns_table_under_nested_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        assert_eq!(
            catalog.list_tables(&namespace_ident_a_b).await.unwrap(),
            vec![table_ident]
        );
    }

    #[tokio::test]
    async fn test_list_tables_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_sql_catalog().await;

        let non_existent_namespace_ident = NamespaceIdent::new("n1".into());

        assert_eq!(
            catalog
                .list_tables(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_namespace_ident
            ),
        );
    }

    #[tokio::test]
    async fn test_drop_table() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        catalog.drop_table(&table_ident).await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_table_drops_table_under_nested_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        catalog.drop_table(&table_ident).await.unwrap();

        assert_eq!(
            catalog.list_tables(&namespace_ident_a_b).await.unwrap(),
            vec![]
        );
    }

    #[tokio::test]
    async fn test_drop_table_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_sql_catalog().await;

        let non_existent_namespace_ident = NamespaceIdent::new("n1".into());
        let non_existent_table_ident =
            TableIdent::new(non_existent_namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .drop_table(&non_existent_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_namespace_ident
            ),
        );
    }

    #[tokio::test]
    async fn test_drop_table_throws_error_if_table_doesnt_exist() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;

        let non_existent_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .drop_table(&non_existent_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such table: {:?}",
                non_existent_table_ident
            ),
        );
    }

    #[tokio::test]
    async fn test_table_exists_returns_true() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        assert!(catalog.table_exists(&table_ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_table_exists_returns_false() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let non_existent_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());

        assert!(!catalog
            .table_exists(&non_existent_table_ident)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_table_exists_under_nested_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        assert!(catalog.table_exists(&table_ident).await.unwrap());

        let non_existent_table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl2".into());
        assert!(!catalog
            .table_exists(&non_existent_table_ident)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_table_exists_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_sql_catalog().await;

        let non_existent_namespace_ident = NamespaceIdent::new("n1".into());
        let non_existent_table_ident =
            TableIdent::new(non_existent_namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .table_exists(&non_existent_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_namespace_ident
            ),
        );
    }

    #[tokio::test]
    async fn test_rename_table_in_same_namespace() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());
        create_table(&catalog, &src_table_ident).await;

        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .await
            .unwrap();

        assert_eq!(catalog.list_tables(&namespace_ident).await.unwrap(), vec![
            dst_table_ident
        ],);
    }

    #[tokio::test]
    async fn test_rename_table_across_namespaces() {
        let catalog = new_sql_catalog().await;
        let src_namespace_ident = NamespaceIdent::new("a".into());
        let dst_namespace_ident = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&src_namespace_ident, &dst_namespace_ident]).await;
        let src_table_ident = TableIdent::new(src_namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(dst_namespace_ident.clone(), "tbl2".into());
        create_table(&catalog, &src_table_ident).await;

        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .await
            .unwrap();

        assert_eq!(
            catalog.list_tables(&src_namespace_ident).await.unwrap(),
            vec![],
        );

        assert_eq!(
            catalog.list_tables(&dst_namespace_ident).await.unwrap(),
            vec![dst_table_ident],
        );
    }

    #[tokio::test]
    async fn test_rename_table_src_table_is_same_as_dst_table() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl".into());
        create_table(&catalog, &table_ident).await;

        catalog
            .rename_table(&table_ident, &table_ident)
            .await
            .unwrap();

        assert_eq!(catalog.list_tables(&namespace_ident).await.unwrap(), vec![
            table_ident
        ],);
    }

    #[tokio::test]
    async fn test_rename_table_across_nested_namespaces() {
        let catalog = new_sql_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(&catalog, &vec![
            &namespace_ident_a,
            &namespace_ident_a_b,
            &namespace_ident_a_b_c,
        ])
        .await;

        let src_table_ident = TableIdent::new(namespace_ident_a_b_c.clone(), "tbl1".into());
        create_tables(&catalog, vec![&src_table_ident]).await;

        let dst_table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .await
            .unwrap();

        assert!(!catalog.table_exists(&src_table_ident).await.unwrap());

        assert!(catalog.table_exists(&dst_table_ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_src_namespace_doesnt_exist() {
        let catalog = new_sql_catalog().await;

        let non_existent_src_namespace_ident = NamespaceIdent::new("n1".into());
        let src_table_ident =
            TableIdent::new(non_existent_src_namespace_ident.clone(), "tbl1".into());

        let dst_namespace_ident = NamespaceIdent::new("n2".into());
        create_namespace(&catalog, &dst_namespace_ident).await;
        let dst_table_ident = TableIdent::new(dst_namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_src_namespace_ident
            ),
        );
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_dst_namespace_doesnt_exist() {
        let catalog = new_sql_catalog().await;
        let src_namespace_ident = NamespaceIdent::new("n1".into());
        let src_table_ident = TableIdent::new(src_namespace_ident.clone(), "tbl1".into());
        create_namespace(&catalog, &src_namespace_ident).await;
        create_table(&catalog, &src_table_ident).await;

        let non_existent_dst_namespace_ident = NamespaceIdent::new("n2".into());
        let dst_table_ident =
            TableIdent::new(non_existent_dst_namespace_ident.clone(), "tbl1".into());
        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => No such namespace: {:?}",
                non_existent_dst_namespace_ident
            ),
        );
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_src_table_doesnt_exist() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());

        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("Unexpected => No such table: {:?}", src_table_ident),
        );
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_dst_table_already_exists() {
        let catalog = new_sql_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());
        create_tables(&catalog, vec![&src_table_ident, &dst_table_ident]).await;

        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => Cannot create table {:? }. Table already exists.",
                &dst_table_ident
            ),
        );
    }
}
