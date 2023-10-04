use std::sync::Arc;

use async_trait::async_trait;
use futures::lock::Mutex;
use opendal::Operator;
use rusqlite::{Connection, Row};
use uuid::Uuid;

use crate::{spec::TableMetadata, table::Table, Catalog, Error, NamespaceIdent, TableIdent};

/// In memory catalog
pub struct MemoryCatalog {
    name: String,
    connection: Arc<Mutex<Connection>>,
    operator: Operator,
}

impl MemoryCatalog {
    /// create new in memory catalog
    pub fn new(name: &str, operator: Operator) -> Result<Self, anyhow::Error> {
        let connection = Connection::open_in_memory()?;
        connection.execute(
            "create table iceberg_tables (
                catalog_name varchar(255) not null,
                table_namespace varchar(255) not null,
                table_name varchar(255) not null,
                metadata_location varchar(255) not null,
                previous_metadata_location varchar(255),
                primary key (catalog_name, table_namespace, table_name)
            );",
            (),
        )?;
        Ok(MemoryCatalog {
            name: name.to_owned(),
            connection: Arc::new(Mutex::new(connection)),
            operator,
        })
    }
}

struct TableRef {
    table_namespace: String,
    table_name: String,
    metadata_location: String,
    _previous_metadata_location: Option<String>,
}

fn query_map<'a>(row: &Row<'a>) -> Result<TableRef, rusqlite::Error> {
    Ok(TableRef {
        table_namespace: row.get(0)?,
        table_name: row.get(1)?,
        metadata_location: row.get(2)?,
        _previous_metadata_location: row.get(3)?,
    })
}

#[async_trait]
impl Catalog for MemoryCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&crate::NamespaceIdent>,
    ) -> crate::Result<Vec<crate::NamespaceIdent>> {
        unimplemented!()
    }
    async fn create_namespace(
        &self,
        _namespace: &crate::NamespaceIdent,
        _properties: std::collections::HashMap<String, String>,
    ) -> crate::Result<crate::Namespace> {
        unimplemented!()
    }
    async fn get_namespace(
        &self,
        _namespace: &crate::NamespaceIdent,
    ) -> crate::Result<crate::Namespace> {
        unimplemented!()
    }
    async fn update_namespace(
        &self,
        _namespace: &crate::NamespaceIdent,
        _properties: std::collections::HashMap<String, String>,
    ) -> crate::Result<()> {
        unimplemented!()
    }
    async fn drop_namespace(&self, _namespace: &crate::NamespaceIdent) -> crate::Result<()> {
        unimplemented!()
    }
    async fn list_tables(
        &self,
        namespace: &crate::NamespaceIdent,
    ) -> crate::Result<Vec<crate::TableIdent>> {
        let connection = self.connection.lock().await;
        let mut stmt = connection.prepare("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = ?1 and table_namespace = ?2")?;
        let iter = stmt.query_map([&self.name, &namespace.0.join(".")], query_map)?;

        Ok(iter
            .map(|x| {
                x.and_then(|y| {
                    let namespace = NamespaceIdent::from_vec(
                        y.table_namespace.split(".").map(|x| x.to_owned()).collect(),
                    );
                    Ok(TableIdent::new(namespace, y.table_name))
                })
            })
            .collect::<Result<_, rusqlite::Error>>()?)
    }
    async fn create_table(
        &self,
        namespace: &crate::NamespaceIdent,
        creation: crate::TableCreation,
    ) -> crate::Result<crate::table::Table> {
        // Placeholder for real table metadata
        let metadata = "{}";
        let path = creation.location.clone()
            + "/metadata/0-"
            + &Uuid::new_v4().to_string()
            + ".metadata.json";
        self.operator.write(&path, metadata.as_bytes()).await?;
        {
            let connection = self.connection.lock().await;
            connection.execute("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values (?1, ?2, ?3, ?4)", (self.name.clone(),&namespace.0.join("."),creation.name.to_string(), path))?;
        }
        self.load_table(&TableIdent::new((*namespace).clone(), creation.name))
            .await
    }
    async fn load_table(&self, table: &crate::TableIdent) -> crate::Result<crate::table::Table> {
        let path = {
            let connection = self.connection.lock().await;
            let mut stmt = connection.prepare("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = ?1 and table_namespace = ?2 and table_name = ?3")?;
            let mut iter = stmt.query_map(
                [&self.name, &table.namespace().0.join("."), table.name()],
                query_map,
            )?;

            iter.next()
                .ok_or(Error::new(
                    crate::ErrorKind::DataInvalid,
                    "No table entry found in catalog",
                ))??
                .metadata_location
        };
        let bytes = self.operator.read(&path).await?;
        let metadata: TableMetadata = serde_json::from_str(std::str::from_utf8(&bytes)?)?;
        Ok(Table {
            metadata_location: path.to_string(),
            metadata,
        })
    }
    async fn drop_table(&self, table: &crate::TableIdent) -> crate::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute("delete from iceberg_tables where catalog_name = ?1 and table_namespace = ?2 and table_name = ?3", (self.name.clone(),&table.namespace().0.join("."),table.name().to_string()))?;
        Ok(())
    }
    async fn stat_table(&self, table: &crate::TableIdent) -> crate::Result<bool> {
        let connection = self.connection.lock().await;
        let mut stmt = connection.prepare("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = ?1 and table_namespace = ?2 and table_name = ?3")?;
        let mut iter = stmt.query_map(
            [&self.name, &table.namespace().0.join("."), table.name()],
            query_map,
        )?;

        Ok(iter.next().is_some())
    }
    async fn rename_table(
        &self,
        _src: &crate::TableIdent,
        _dest: &crate::TableIdent,
    ) -> crate::Result<()> {
        unimplemented!()
    }
    async fn update_table(
        &self,
        table: &crate::TableIdent,
        commit: crate::TableCommit,
    ) -> crate::Result<crate::table::Table> {
        // Placeholder for previous metadata location
        let previous_metadata_location = "";
        // Placeholder for table metadata
        let table_metadata = "{}";
        // Placeholder for real table location
        let table_location = "";
        let table_version = "";
        let metadata_location = table_location.to_string()
            + "/metadata/"
            + table_version
            + "-"
            + &Uuid::new_v4().to_string()
            + ".metadata.json";
        // Update table metadata
        for _update in commit.updates {}
        // Write updated table metadata
        self.operator
            .write(&metadata_location, table_metadata.as_bytes())
            .await?;
        {
            let connection = self.connection.lock().await;
            connection.execute("update iceberg_tables set metadata_location = ?4, previous_metadata_location = ?5 where catalog_name = ?1 and table_namespace = ?2 and table_name = ?3", (self.name.clone(),&table.namespace().0.join("."),table.name().to_string(), metadata_location, previous_metadata_location))?;
        }
        self.load_table(&table).await
    }
    async fn update_tables(
        &self,
        _tables: &[(crate::TableIdent, crate::TableCommit)],
    ) -> crate::Result<()> {
        unimplemented!()
    }
}
