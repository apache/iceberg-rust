use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    // Create catalog
    let config = RestCatalogConfig::builder()
        .uri("http://localhost:8080".to_string())
        .build();

    let catalog = RestCatalog::new(config).await.unwrap();

    // ANCHOR: create_table
    let table_id = TableIdent::from_strs(["default", "t1"]).unwrap();

    let table_schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .build()
        .unwrap();

    // Create table
    let table_creation = TableCreation::builder()
        .name(table_id.name.clone())
        .schema(table_schema.clone())
        .properties(HashMap::from([("owner".to_string(), "testx".to_string())]))
        .build();

    let table = catalog
        .create_table(&table_id.namespace, table_creation)
        .await
        .unwrap();

    println!("Table created: {:?}", table.metadata());
    // ANCHOR_END: create_table

    // ANCHOR: load_table
    let table2 = catalog
        .load_table(&TableIdent::from_strs(["default", "t2"]).unwrap())
        .await
        .unwrap();
    println!("{:?}", table2.metadata());
    // ANCHOR_END: load_table
}
