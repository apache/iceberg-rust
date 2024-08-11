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

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent,
};
use sqlx::any::{install_default_drivers, AnyPoolOptions, AnyQueryResult, AnyRow};
use sqlx::{Any, AnyPool, Row, Transaction};
use typed_builder::TypedBuilder;

use crate::error::{from_sqlx_error, no_such_namespace_err};

static CATALOG_TABLE_NAME: &str = "iceberg_tables";
static CATALOG_FIELD_CATALOG_NAME: &str = "catalog_name";
static CATALOG_FIELD_TABLE_NAME: &str = "table_name";
static CATALOG_FIELD_TABLE_NAMESPACE: &str = "table_namespace";
static CATALOG_FIELD_METADATA_LOCATION_PROP: &str = "metadata_location";
static CATALOG_FIELD_PREVIOUS_METADATA_LOCATION_PROP: &str = "previous_metadata_location";
static CATALOG_FIELD_RECORD_TYPE: &str = "iceberg_type";

static NAMESPACE_TABLE_NAME: &str = "iceberg_namespace_properties";
static NAMESPACE_FIELD_NAME: &str = "namespace";
static NAMESPACE_FIELD_PROPERTY_KEY: &str = "property_key";
static NAMESPACE_FIELD_PROPERTY_VALUE: &str = "property_value";

static MAX_CONNECTIONS: u32 = 10; // Default the SQL pool to 10 connections if not provided
static IDLE_TIMEOUT: u64 = 10; // Default the maximum idle timeout per connection to 10s before it is closed
static TEST_BEFORE_ACQUIRE: bool = true; // Default the health-check of each connection to enabled prior to returning

/// Sql catalog config
#[derive(Debug, TypedBuilder)]
pub struct SqlCatalogConfig {
    uri: String,
    name: String,
    warehouse_location: String,
    file_io: FileIO,
    sql_bind_style: SqlBindStyle,
    #[builder(default)]
    props: HashMap<String, String>,
}

#[derive(Debug)]
/// Sql catalog implementation.
pub struct SqlCatalog {
    name: String,
    connection: AnyPool,
    _warehouse_location: String,
    _fileio: FileIO,
    sql_bind_style: SqlBindStyle,
}

#[derive(Debug, PartialEq)]
/// Set the SQL parameter bind style to either $1..$N (Postgres style) or ? (SQLite/MySQL/MariaDB)
pub enum SqlBindStyle {
    /// DollarNumeric uses parameters of the form `$1..$N``, which is the Postgres style
    DollarNumeric,
    /// QMark uses parameters of the form `?` which is the style for other dialects (SQLite/MySQL/MariaDB)
    QMark,
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

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {CATALOG_TABLE_NAME} (
                {CATALOG_FIELD_CATALOG_NAME} VARCHAR(255) NOT NULL,
                {CATALOG_FIELD_TABLE_NAMESPACE} VARCHAR(255) NOT NULL,
                {CATALOG_FIELD_TABLE_NAME} VARCHAR(255) NOT NULL,
                {CATALOG_FIELD_METADATA_LOCATION_PROP} VARCHAR(1000),
                {CATALOG_FIELD_PREVIOUS_METADATA_LOCATION_PROP} VARCHAR(1000),
                {CATALOG_FIELD_RECORD_TYPE} VARCHAR(5),
                PRIMARY KEY ({CATALOG_FIELD_CATALOG_NAME}, {CATALOG_FIELD_TABLE_NAMESPACE}, {CATALOG_FIELD_TABLE_NAME}))"
        ))
        .execute(&pool)
        .await
        .map_err(from_sqlx_error)?;

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {NAMESPACE_TABLE_NAME} (
                {CATALOG_FIELD_CATALOG_NAME} VARCHAR(255) NOT NULL,
                {NAMESPACE_FIELD_NAME} VARCHAR(255) NOT NULL,
                {NAMESPACE_FIELD_PROPERTY_KEY} VARCHAR(255),
                {NAMESPACE_FIELD_PROPERTY_VALUE} VARCHAR(1000),
                PRIMARY KEY ({CATALOG_FIELD_CATALOG_NAME}, {NAMESPACE_FIELD_NAME}, {NAMESPACE_FIELD_PROPERTY_KEY}))"
        ))
        .execute(&pool)
        .await
        .map_err(from_sqlx_error)?;

        Ok(SqlCatalog {
            name: config.name.to_owned(),
            connection: pool,
            _warehouse_location: config.warehouse_location,
            _fileio: config.file_io,
            sql_bind_style: config.sql_bind_style,
        })
    }

    /// SQLX Any does not implement PostgresSQL bindings, so we have to do this.
    pub fn build_query(&self, query: &str) -> String {
        match self.sql_bind_style {
            SqlBindStyle::DollarNumeric => {
                let mut query = query.to_owned();
                let mut i = 1;
                while let Some(pos) = query.find('?') {
                    query.replace_range(pos..pos + 1, &format!("${}", i));
                    i += 1;
                }
                query
            }
            _ => query.to_owned(),
        }
    }

    /// Fetch a vec of AnyRows from a given query
    pub async fn fetch_rows(&self, query: &str, args: Vec<Option<&String>>) -> Result<Vec<AnyRow>> {
        let query_with_placeholders = self.build_query(query);

        let mut sqlx_query = sqlx::query(&query_with_placeholders);
        for arg in args {
            sqlx_query = sqlx_query.bind(arg);
        }

        sqlx_query
            .fetch_all(&self.connection)
            .await
            .map_err(from_sqlx_error)
    }
    /// Execute statements in a transaction, provided or not
    pub async fn execute(
        &self,
        query: &str,
        args: Vec<Option<&String>>,
        transaction: Option<&mut Transaction<'_, Any>>,
    ) -> Result<AnyQueryResult> {
        let query_with_placeholders = self.build_query(query);

        let mut sqlx_query = sqlx::query(&query_with_placeholders);
        for arg in args {
            sqlx_query = sqlx_query.bind(arg);
        }

        match transaction {
            Some(t) => sqlx_query.execute(&mut **t).await.map_err(from_sqlx_error),
            None => {
                let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;
                let result = sqlx_query.execute(&mut *tx).await.map_err(from_sqlx_error);
                let _ = tx.commit().await.map_err(from_sqlx_error);
                result
            }
        }
    }
}

#[async_trait]
impl Catalog for SqlCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let table_namespaces_stmt = format!(
            "SELECT {CATALOG_FIELD_TABLE_NAMESPACE}
             FROM {CATALOG_TABLE_NAME}
             WHERE {CATALOG_FIELD_CATALOG_NAME} = ?"
        );
        let namespaces_stmt = format!(
            "SELECT {NAMESPACE_FIELD_NAME}
             FROM {NAMESPACE_TABLE_NAME}
             WHERE {CATALOG_FIELD_CATALOG_NAME} = ?"
        );

        match parent {
            Some(parent) => match self.namespace_exists(parent).await? {
                true => {
                    let parent_str = parent.join(".");
                    let parent_table_namespaces_stmt = format!(
                        "{table_namespaces_stmt} AND {CATALOG_FIELD_TABLE_NAMESPACE} LIKE CONCAT(?, '%')"
                    );
                    let parent_namespaces_stmt =
                        format!("{namespaces_stmt} AND {NAMESPACE_FIELD_NAME} LIKE CONCAT(?, '%')");

                    let namespace_rows = self
                        .fetch_rows(
                            &format!(
                                "{parent_namespaces_stmt} UNION {parent_table_namespaces_stmt}"
                            ),
                            vec![
                                Some(&self.name),
                                Some(&parent_str),
                                Some(&self.name),
                                Some(&parent_str),
                            ],
                        )
                        .await?;

                    Ok(namespace_rows
                        .iter()
                        .filter_map(|r| {
                            let nsp = r.try_get::<String, _>(0).ok();
                            nsp.and_then(|n| {
                                if n == parent_str {
                                    // Filter out itself
                                    None
                                } else {
                                    NamespaceIdent::from_strs(n.split(".")).ok()
                                }
                            })
                        })
                        .collect())
                }
                false => no_such_namespace_err(parent),
            },
            None => {
                let namespace_rows = self
                    .fetch_rows(
                        &format!("{namespaces_stmt} UNION {table_namespaces_stmt}"),
                        vec![Some(&self.name), Some(&self.name)],
                    )
                    .await?;

                Ok(namespace_rows
                    .iter()
                    .filter_map(|r| {
                        let nsp = r.try_get::<String, _>(0).ok();
                        nsp.and_then(|n| {
                            // for each row, split a.b.c into a, b, c levels
                            let mut levels = n.split(".").collect::<Vec<&str>>();
                            if !levels.is_empty() {
                                // only return first-level idents
                                let first_level = levels.drain(..1).collect::<Vec<&str>>();
                                NamespaceIdent::from_strs(first_level).ok()
                            } else {
                                None
                            }
                        })
                    })
                    .collect::<HashSet<NamespaceIdent>>()
                    .into_iter()
                    .collect::<Vec<NamespaceIdent>>())
            }
        }
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let exists = self.namespace_exists(namespace).await?;

        if exists {
            return Err(Error::new(
                iceberg::ErrorKind::Unexpected,
                format!("Namespace {:?} already exists", namespace),
            ));
        }

        for i in 1..namespace.len() {
            let parent_namespace = NamespaceIdent::from_vec(namespace[..i].to_vec())?;
            let parent_exists = self.namespace_exists(&parent_namespace).await?;
            if !parent_exists {
                return no_such_namespace_err(&parent_namespace);
            }
        }

        let namespace_str = namespace.join(".");
        let insert = format!(
            "INSERT INTO {NAMESPACE_TABLE_NAME} ({CATALOG_FIELD_CATALOG_NAME}, {NAMESPACE_FIELD_NAME}, {NAMESPACE_FIELD_PROPERTY_KEY}, {NAMESPACE_FIELD_PROPERTY_VALUE})
             VALUES (?, ?, ?, ?)");
        if !properties.is_empty() {
            let mut query_args = vec![];
            let mut properties_insert = insert.clone();
            for (index, (key, value)) in properties.iter().enumerate() {
                query_args.extend(
                    [
                        Some(&self.name),
                        Some(&namespace_str),
                        Some(key),
                        Some(value),
                    ]
                    .iter(),
                );
                if index > 0 {
                    properties_insert = format!("{properties_insert}, (?, ?, ?, ?)");
                }
            }

            self.execute(&properties_insert, query_args, None).await?;

            Ok(Namespace::with_properties(namespace.clone(), properties))
        } else {
            // set a default property of exists = true
            // up for debate if this is worthwhile
            self.execute(
                &insert,
                vec![
                    Some(&self.name),
                    Some(&namespace_str),
                    Some(&"exists".to_string()),
                    Some(&"true".to_string()),
                ],
                None,
            )
            .await?;
            Ok(Namespace::with_properties(namespace.clone(), properties))
        }
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let exists = self.namespace_exists(namespace).await?;
        if exists {
            let namespace_props = self
                .fetch_rows(
                    &format!(
                        "SELECT
                            {NAMESPACE_FIELD_NAME},
                            {NAMESPACE_FIELD_PROPERTY_KEY},
                            {NAMESPACE_FIELD_PROPERTY_VALUE}
                            FROM {NAMESPACE_TABLE_NAME}
                            WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                            AND {NAMESPACE_FIELD_NAME} = ?"
                    ),
                    vec![Some(&self.name), Some(&namespace.join("."))],
                )
                .await?;

            let properties: HashMap<String, String> = namespace_props
                .iter()
                .filter_map(|r| {
                    let key = r.try_get(NAMESPACE_FIELD_PROPERTY_KEY).ok();
                    let value = r.try_get(NAMESPACE_FIELD_PROPERTY_VALUE).ok();
                    match (key, value) {
                        (Some(k), Some(v)) => Some((k, v)),
                        _ => None,
                    }
                })
                .collect();

            Ok(Namespace::with_properties(namespace.clone(), properties))
        } else {
            no_such_namespace_err(namespace)
        }
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let namespace_str = namespace.join(".");

        let table_namespaces = self
            .fetch_rows(
                &format!(
                    "SELECT 1 FROM {CATALOG_TABLE_NAME}
                     WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                     LIMIT 1"
                ),
                vec![Some(&self.name), Some(&namespace_str)],
            )
            .await?;

        if !table_namespaces.is_empty() {
            Ok(true)
        } else {
            let namespaces = self
                .fetch_rows(
                    &format!(
                        "SELECT 1 FROM {NAMESPACE_TABLE_NAME}
                         WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                          AND {NAMESPACE_FIELD_NAME} = ?
                         LIMIT 1"
                    ),
                    vec![Some(&self.name), Some(&namespace_str)],
                )
                .await?;
            if !namespaces.is_empty() {
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let exists = self.namespace_exists(namespace).await?;
        if exists {
            let existing_properties = self.get_namespace(namespace).await?.properties().clone();
            let namespace_str = namespace.join(".");

            let mut updates = vec![];
            let mut inserts = vec![];

            for (key, value) in properties.iter() {
                if existing_properties.contains_key(key) {
                    if existing_properties.get(key) != Some(value) {
                        updates.push((key, value));
                    }
                } else {
                    inserts.push((key, value));
                }
            }

            let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;
            let update_stmt = format!(
                "UPDATE {NAMESPACE_TABLE_NAME} SET {NAMESPACE_FIELD_PROPERTY_VALUE} = ?
                 WHERE {CATALOG_FIELD_CATALOG_NAME} = ? 
                 AND {NAMESPACE_FIELD_NAME} = ?
                 AND {NAMESPACE_FIELD_PROPERTY_KEY} = ?"
            );

            let insert_stmt = format!(
                "INSERT INTO {NAMESPACE_TABLE_NAME} ({CATALOG_FIELD_CATALOG_NAME}, {NAMESPACE_FIELD_NAME}, {NAMESPACE_FIELD_PROPERTY_KEY}, {NAMESPACE_FIELD_PROPERTY_VALUE})
                 VALUES (?, ?, ?, ?)"
            );

            for (key, value) in updates {
                self.execute(
                    &update_stmt,
                    vec![
                        Some(value),
                        Some(&self.name),
                        Some(&namespace_str),
                        Some(key),
                    ],
                    Some(&mut tx),
                )
                .await?;
            }

            for (key, value) in inserts {
                self.execute(
                    &insert_stmt,
                    vec![
                        Some(&self.name),
                        Some(&namespace_str),
                        Some(key),
                        Some(value),
                    ],
                    Some(&mut tx),
                )
                .await?;
            }

            let _ = tx.commit().await.map_err(from_sqlx_error)?;

            Ok(())
        } else {
            no_such_namespace_err(namespace)
        }
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let exists = self.namespace_exists(namespace).await?;
        if exists {
            // TODO: check that the namespace is empty
            self.execute(
                &format!("DELETE FROM {NAMESPACE_TABLE_NAME} WHERE {NAMESPACE_FIELD_NAME} = ?"),
                vec![Some(&namespace.join("."))],
                None,
            )
            .await?;

            Ok(())
        } else {
            no_such_namespace_err(namespace)
        }
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        todo!()
    }

    async fn table_exists(&self, _identifier: &TableIdent) -> Result<bool> {
        todo!()
    }

    async fn drop_table(&self, _identifier: &TableIdent) -> Result<()> {
        todo!()
    }

    async fn load_table(&self, _identifier: &TableIdent) -> Result<Table> {
        todo!()
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> Result<Table> {
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
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::hash::Hash;

    use iceberg::io::FileIOBuilder;
    use iceberg::{Catalog, Namespace, NamespaceIdent};
    use sqlx::migrate::MigrateDatabase;
    use tempfile::TempDir;

    use crate::{SqlBindStyle, SqlCatalog, SqlCatalogConfig};

    fn temp_path() -> String {
        let temp_dir = TempDir::new().unwrap();
        temp_dir.path().to_str().unwrap().to_string()
    }

    fn to_set<T: std::cmp::Eq + Hash>(vec: Vec<T>) -> HashSet<T> {
        HashSet::from_iter(vec)
    }

    fn default_properties() -> HashMap<String, String> {
        HashMap::from([("exists".to_string(), "true".to_string())])
    }

    async fn new_sql_catalog(warehouse_location: String) -> impl Catalog {
        let sql_lite_uri = format!("sqlite:{}", temp_path());
        sqlx::Sqlite::create_database(&sql_lite_uri).await.unwrap();

        let config = SqlCatalogConfig::builder()
            .uri(sql_lite_uri.to_string())
            .name("iceberg".to_string())
            .warehouse_location(warehouse_location)
            .file_io(FileIOBuilder::new_fs_io().build().unwrap())
            .sql_bind_style(SqlBindStyle::QMark)
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

    #[tokio::test]
    async fn test_initialized() {
        let warehouse_loc = temp_path();
        new_sql_catalog(warehouse_loc.clone()).await;
        // catalog instantiation should not fail even if tables exist
        new_sql_catalog(warehouse_loc.clone()).await;
        new_sql_catalog(warehouse_loc.clone()).await;
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_empty_vector() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_multiple_namespaces() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
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
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
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
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
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
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
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
            vec![NamespaceIdent::from_strs(vec!["a", "b"]).unwrap()]
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_multiple_namespaces_under_parent() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
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
                NamespaceIdent::from_strs(vec!["a", "a"]).unwrap(),
                NamespaceIdent::from_strs(vec!["a", "b"]).unwrap(),
                NamespaceIdent::from_strs(vec!["a", "c"]).unwrap(),
            ])
        );
    }

    #[tokio::test]
    async fn test_namespace_exists_returns_false() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert!(!catalog
            .namespace_exists(&NamespaceIdent::new("b".into()))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_namespace_exists_returns_true() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert!(catalog.namespace_exists(&namespace_ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_namespace_with_properties() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
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
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "Unexpected => Namespace {:?} already exists",
                &namespace_ident
            )
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, default_properties())
        );
    }

    #[tokio::test]
    async fn test_create_nested_namespace() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
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
            Namespace::with_properties(child_namespace_ident, default_properties())
        );
    }

    #[tokio::test]
    async fn test_create_deeply_nested_namespace() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
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
            Namespace::with_properties(namespace_ident_a_b_c, default_properties())
        );
    }

    #[tokio::test]
    async fn test_create_nested_namespace_throws_error_if_top_level_namespace_doesnt_exist() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;

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
    async fn test_drop_namespace() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident).await;

        catalog.drop_namespace(&namespace_ident).await.unwrap();

        assert!(!catalog.namespace_exists(&namespace_ident).await.unwrap())
    }

    #[tokio::test]
    async fn test_drop_nested_namespace() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
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
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
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
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;

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
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
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
    #[ignore = "Java/Python do not drop nested namespaces?"]
    async fn test_dropping_a_namespace_also_drops_namespaces_nested_under_that_one() {
        let warehouse_loc = temp_path();
        let catalog = new_sql_catalog(warehouse_loc).await;
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
}
