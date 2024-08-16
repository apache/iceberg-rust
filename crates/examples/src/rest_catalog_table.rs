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

use std::collections::HashMap;

use iceberg::spec::{NestedField, PartitionSpecBuilder, PrimitiveType, Schema, Transform, Type};
use iceberg::{Catalog, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

mod utils;

fn get_table_creation(table_id: &TableIdent) -> TableCreation {
    let table_schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "a", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "b", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "c", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .build()
        .unwrap();

    let p = PartitionSpecBuilder::new(&table_schema)
        .add_partition_field("a", "bucket_a", Transform::Bucket(16))
        .unwrap()
        .add_partition_field("b", "b", Transform::Identity)
        .unwrap()
        .build()
        .unwrap();

    TableCreation::builder()
        .name(table_id.name.clone())
        .schema(table_schema.clone())
        .partition_spec(p)
        .properties(HashMap::from([("owner".to_string(), "testx".to_string())]))
        .build()
}

#[tokio::main]
async fn main() {
    let catalog: RestCatalog = utils::get_rest_catalog();

    let table_id = TableIdent::from_strs(["ns1", "t1"]).unwrap();
    catalog.drop_table(&table_id).await.unwrap();

    let table_creation = get_table_creation(&table_id);
    let table = catalog
        .create_table(&table_id.namespace, table_creation)
        .await
        .unwrap();
    println!("Table created: {:?}", table.metadata());

    let table2 = catalog.load_table(&table_id).await.unwrap();
    println!("{:?}", table2.metadata());
}
