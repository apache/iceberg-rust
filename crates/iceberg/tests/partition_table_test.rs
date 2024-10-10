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

mod tests {
    use std::collections::HashMap;

    use iceberg::io::FileIOBuilder;
    use iceberg::spec::{
        NestedField, PartitionSpecBuilder, PrimitiveType, Schema, Transform, Type,
    };
    use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
    use iceberg_catalog_memory::MemoryCatalog;
    use tempfile::TempDir;

    fn temp_path() -> String {
        let temp_dir = TempDir::new().unwrap();
        temp_dir.path().to_str().unwrap().to_string()
    }

    fn get_iceberg_catalog() -> MemoryCatalog {
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        MemoryCatalog::new(file_io, Some(temp_path()))
    }

    #[tokio::test]
    async fn test_partition_table() {
        let catalog = get_iceberg_catalog();

        let namespace_id = NamespaceIdent::from_vec(vec!["ns1".to_string()]).unwrap();
        catalog
            .create_namespace(&namespace_id, HashMap::new())
            .await
            .unwrap();

        let table_id = TableIdent::from_strs(["ns1", "t1"]).unwrap();
        let table_schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "a", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "b", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "c", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .with_schema_id(1)
            .build()
            .unwrap();
        let p = PartitionSpecBuilder::new(&table_schema)
            .add_partition_field("a", "bucket_a", Transform::Bucket(16))
            .unwrap()
            .add_partition_field("b", "b", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        // Create table
        let table_creation = TableCreation::builder()
            .name(table_id.name.clone())
            .schema(table_schema.clone())
            .partition_spec(p)
            .properties(HashMap::from([("owner".to_string(), "testx".to_string())]))
            .build();

        catalog
            .create_table(&table_id.namespace, table_creation)
            .await
            .unwrap();
        let table = catalog.load_table(&table_id).await.unwrap();
        let partition_fields = table.metadata().default_partition_spec().unwrap().fields();
        assert_eq!(
            2,
            partition_fields.len(),
            "There should be 2 partition fields"
        );

        let partition_field_0 = &partition_fields[0];
        assert_eq!("bucket_a", partition_field_0.name);
        assert_eq!(1, partition_field_0.source_id);
        assert_eq!(Transform::Bucket(16), partition_field_0.transform);

        let partition_field_1 = &partition_fields[1];
        assert_eq!("b", partition_field_1.name);
        assert_eq!(2, partition_field_1.source_id);
        assert_eq!(Transform::Identity, partition_field_1.transform);
    }
}
