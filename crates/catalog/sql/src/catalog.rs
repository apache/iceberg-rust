use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::lock::Mutex;
use sqlx::{
    any::{install_default_drivers, AnyConnectOptions, AnyRow},
    AnyConnection, ConnectOptions, Connection, Row,
};
use std::collections::HashMap;

use iceberg::{
    spec::TableMetadata, table::Table, Catalog, Error, ErrorKind, Namespace, NamespaceIdent,
    Result, TableCommit, TableCreation, TableIdent,
};
use opendal::Operator;
use uuid::Uuid;

#[derive(Debug)]
/// Sql catalog implementation.
pub struct SqlCatalog {
    name: String,
    connection: Arc<Mutex<AnyConnection>>,
    operator: Operator,
    cache: Arc<DashMap<TableIdent, (String, TableMetadata)>>,
}

// impl SqlCatalog {
//     pub async fn new(url: &str, name: &str, operator: Operator) -> Result<Self> {
//         install_default_drivers();

//         let mut connection =
//             AnyConnectOptions::connect(&AnyConnectOptions::from_url(&url.try_into()?)?).await?;

//         connection
//             .transaction(|txn| {
//                 Box::pin(async move {
//                     sqlx::query(
//                         "create table if not exists iceberg_tables (
//                                 catalog_name text not null,
//                                 table_namespace text not null,
//                                 table_name text not null,
//                                 metadata_location text not null,
//                                 previous_metadata_location text,
//                                 primary key (catalog_name, table_namespace, table_name)
//                             );",
//                     )
//                     .execute(&mut **txn)
//                     .await
//                 })
//             })
//             .await?;

//         connection
//             .transaction(|txn| {
//                 Box::pin(async move {
//                     sqlx::query(
//                         "create table if not exists iceberg_namespace_properties (
//                                 catalog_name text not null,
//                                 namespace text not null,
//                                 property_key text,
//                                 property_value text,
//                                 primary key (catalog_name, namespace, property_key)
//                             );",
//                     )
//                     .execute(&mut **txn)
//                     .await
//                 })
//             })
//             .await?;

//         Ok(SqlCatalog {
//             name: name.to_owned(),
//             connection: Arc::new(Mutex::new(connection)),
//             operator,
//             cache: Arc::new(DashMap::new()),
//         })
//     }
// }

// #[derive(Debug)]
// struct TableRef {
//     table_namespace: String,
//     table_name: String,
//     metadata_location: String,
//     _previous_metadata_location: Option<String>,
// }

// fn query_map(row: &AnyRow) -> std::result::Result<TableRef, sqlx::Error> {
//     Ok(TableRef {
//         table_namespace: row.try_get(0)?,
//         table_name: row.try_get(1)?,
//         metadata_location: row.try_get(2)?,
//         _previous_metadata_location: row.try_get::<String, _>(3).map(Some).or_else(|err| {
//             if let sqlx::Error::ColumnDecode {
//                 index: _,
//                 source: _,
//             } = err
//             {
//                 Ok(None)
//             } else {
//                 Err(err)
//             }
//         })?,
//     })
// }

#[async_trait]
impl Catalog for SqlCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        // let mut connection = self.connection.lock().await;
        // let rows = connection.transaction(|txn|{
        //     let name = self.name.clone();
        //     Box::pin(async move {
        //     sqlx::query(&format!("select distinct table_namespace from iceberg_tables where catalog_name = '{}';",&name)).fetch_all(&mut **txn).await
        // })}).await.map_err(Error::from)?;
        // let iter = rows.iter().map(|row| row.try_get::<String, _>(0));

        // Ok(iter
        //     .map(|x| {
        //         x.and_then(|y| {
        //             Namespace::try_new(&y.split('.').map(ToString::to_string).collect::<Vec<_>>())
        //                 .map_err(|err| sqlx::Error::Decode(Box::new(err)))
        //         })
        //     })
        //     .collect::<Result<_, sqlx::Error>>()
        //     .map_err(Error::from)?)
        todo!()
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
        // let mut connection = self.connection.lock().await;
        // let rows = connection.transaction(|txn|{
        //     let name = self.name.clone();
        //     let namespace = namespace.to_string();
        //     Box::pin(async move {
        //     sqlx::query(&format!("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = '{}' and table_namespace = '{}';",&name, &namespace)).fetch_all(&mut **txn).await
        // })}).await.map_err(Error::from)?;
        // let iter = rows.iter().map(query_map);
        todo!()

        // Ok(iter
        //     .map(|x| {
        //         x.and_then(|y| {
        //             TableIdent::parse(&(y.table_namespace.to_string() + "." + &y.table_name))
        //                 .map_err(|err| sqlx::Error::Decode(Box::new(err)))
        //         })
        //     })
        //     .collect::<Result<_, sqlx::Error>>()
        //     .map_err(Error::from)?)
    }

    async fn stat_table(&self, identifier: &TableIdent) -> Result<bool> {
        // let mut connection = self.connection.lock().await;
        // let rows = connection.transaction(|txn|{
        //     let catalog_name = self.name.clone();
        //     let namespace = identifier.namespace().to_string();
        //     let name = identifier.name().to_string();
        //     Box::pin(async move {
        //     sqlx::query(&format!("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
        //         &namespace,
        //         &name)).fetch_all(&mut **txn).await
        // })}).await.map_err(Error::from)?;
        // let mut iter = rows.iter().map(query_map);

        // Ok(iter.next().is_some())
        todo!()
    }

    async fn drop_table(&self, identifier: &TableIdent) -> Result<()> {
        // let mut connection = self.connection.lock().await;
        // connection.transaction(|txn|{
        //     let catalog_name = self.name.clone();
        //     let namespace = identifier.namespace().to_string();
        //     let name = identifier.name().to_string();
        //     Box::pin(async move {
        //     sqlx::query(&format!("delete from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
        //         &namespace,
        //         &name)).execute(&mut **txn).await
        // })}).await.map_err(Error::from)?;
        Ok(())
    }

    async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
        // let path = {
        //     let mut connection = self.connection.lock().await;
        //     let row = connection.transaction(|txn|{
        //     let catalog_name = self.name.clone();
        //     let namespace = identifier.namespace().to_string();
        //     let name = identifier.name().to_string();
        //         Box::pin(async move {
        //     sqlx::query(&format!("select table_namespace, table_name, metadata_location, previous_metadata_location from iceberg_tables where catalog_name = '{}' and table_namespace = '{}' and table_name = '{}';",&catalog_name,
        //             &namespace,
        //             &name)).fetch_one(&mut **txn).await
        // })}).await.map_err(Error::from)?;
        //     let row = query_map(&row).map_err(Error::from)?;

        //     row.metadata_location
        // };
        todo!()
        // let bytes = &self
        //     .operator
        //     .get(&strip_prefix(&path).as_str().into())
        //     .await?
        //     .bytes()
        //     .await?;
        // let metadata: TabularMetadata = serde_json::from_str(std::str::from_utf8(bytes)?)?;
        // self.cache
        //     .insert(identifier.clone(), (path.clone(), metadata.clone()));
        // match metadata {
        //     TabularMetadata::Table(metadata) => Ok(Tabular::Table(
        //         Table::new(identifier.clone(), self.clone(), metadata).await?,
        //     )),
        //     TabularMetadata::View(metadata) => Ok(Tabular::View(
        //         View::new(identifier.clone(), self.clone(), metadata).await?,
        //     )),
        //     TabularMetadata::MaterializedView(metadata) => Ok(Tabular::MaterializedView(
        //         MaterializedView::new(identifier.clone(), self.clone(), metadata).await?,
        //     )),
        // }
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        // Create metadata
        // let location = metadata.location.to_string();

        // let uuid = Uuid::new_v4();
        // let version = &metadata.last_sequence_number;
        // let metadata_json = serde_json::to_string(&metadata)?;
        // let metadata_location = location
        //     + "/metadata/"
        //     + &version.to_string()
        //     + "-"
        //     + &uuid.to_string()
        //     + ".metadata.json";
        // operator
        //     .put(
        //         &strip_prefix(&metadata_location).into(),
        //         metadata_json.into(),
        //     )
        //     .await?;
        // {
        //     let mut connection = self.connection.lock().await;
        //     connection.transaction(|txn|{
        //         let catalog_name = self.name.clone();
        //         let namespace = identifier.namespace().to_string();
        //         let name = identifier.name().to_string();
        //         let metadata_location = metadata_location.to_string();
        //         Box::pin(async move {
        //     sqlx::query(&format!("insert into iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) values ('{}', '{}', '{}', '{}');",catalog_name,namespace,name, metadata_location)).execute(&mut **txn).await
        // })}).await.map_err(Error::from)?;
        // }
        // self.clone()
        //     .load_tabular(&identifier)
        //     .await
        todo!()
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        todo!()
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        todo!()
    }
}

// #[cfg(test)]
// pub mod tests {
//     use iceberg_rust::{
//         catalog::{identifier::TableIdent, namespace::Namespace, Catalog},
//         spec::{
//             schema::Schema,
//             types::{PrimitiveType, StructField, StructType, Type},
//         },
//         table::table_builder::TableBuilder,
//     };
//     use operator::{memory::InMemory, ObjectStore};
//     use std::sync::Arc;

//     use crate::SqlCatalog;

//     #[tokio::test]
//     async fn test_create_update_drop_table() {
//         let operator: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
//         let catalog: Arc<dyn Catalog> = Arc::new(
//             SqlCatalog::new("sqlite://", "test", operator)
//                 .await
//                 .unwrap(),
//         );
//         let identifier = TableIdent::parse("load_table.table3").unwrap();
//         let schema = Schema::builder()
//             .with_schema_id(1)
//             .with_identifier_field_ids(vec![1, 2])
//             .with_fields(
//                 StructType::builder()
//                     .with_struct_field(StructField {
//                         id: 1,
//                         name: "one".to_string(),
//                         required: false,
//                         field_type: Type::Primitive(PrimitiveType::String),
//                         doc: None,
//                     })
//                     .with_struct_field(StructField {
//                         id: 2,
//                         name: "two".to_string(),
//                         required: false,
//                         field_type: Type::Primitive(PrimitiveType::String),
//                         doc: None,
//                     })
//                     .build()
//                     .unwrap(),
//             )
//             .build()
//             .unwrap();

//         let mut builder = TableBuilder::new(&identifier, catalog.clone())
//             .expect("Failed to create table builder.");
//         builder
//             .location("/")
//             .with_schema((1, schema))
//             .current_schema_id(1);
//         let mut table = builder.build().await.expect("Failed to create table.");

//         let exists = Arc::clone(&catalog)
//             .table_exists(&identifier)
//             .await
//             .expect("Table doesn't exist");
//         assert!(exists);

//         let tables = catalog
//             .clone()
//             .list_tables(
//                 &Namespace::try_new(&["load_table".to_owned()])
//                     .expect("Failed to create namespace"),
//             )
//             .await
//             .expect("Failed to list Tables");
//         assert_eq!(tables[0].to_string(), "load_table.table3".to_owned());

//         let namespaces = catalog
//             .clone()
//             .list_namespaces(None)
//             .await
//             .expect("Failed to list namespaces");
//         assert_eq!(namespaces[0].to_string(), "load_table");

//         let transaction = table.new_transaction(None);
//         transaction.commit().await.expect("Transaction failed.");

//         catalog
//             .drop_table(&identifier)
//             .await
//             .expect("Failed to drop table.");

//         let exists = Arc::clone(&catalog)
//             .table_exists(&identifier)
//             .await
//             .expect("Table exists failed");
//         assert!(!exists);
//     }
// }
