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

//! Integration tests for Iceberg Datafusion with Hive Metastore.

use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use datafusion::arrow::array::{Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::execution::context::SessionContext;
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use expect_test::expect;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, StructType, Type};
use iceberg::test_utils::check_record_batches;
use iceberg::{Catalog, CatalogBuilder, MemoryCatalog, NamespaceIdent, Result, TableCreation};
use iceberg_datafusion::IcebergCatalogProvider;
use tempfile::TempDir;

fn temp_path() -> String {
    let temp_dir = TempDir::new().unwrap();
    temp_dir.path().to_str().unwrap().to_string()
}

async fn get_iceberg_catalog() -> MemoryCatalog {
    MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), temp_path())]),
        )
        .await
        .unwrap()
}

fn get_struct_type() -> StructType {
    StructType::new(vec![
        NestedField::required(4, "s_foo1", Type::Primitive(PrimitiveType::Int)).into(),
        NestedField::required(5, "s_foo2", Type::Primitive(PrimitiveType::String)).into(),
    ])
}

async fn set_test_namespace(catalog: &MemoryCatalog, namespace: &NamespaceIdent) -> Result<()> {
    let properties = HashMap::new();

    catalog.create_namespace(namespace, properties).await?;

    Ok(())
}

fn get_table_creation(
    location: impl ToString,
    name: impl ToString,
    schema: Option<Schema>,
) -> Result<TableCreation> {
    let schema = match schema {
        None => Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "foo1", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "foo2", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?,
        Some(schema) => schema,
    };

    let creation = TableCreation::builder()
        .location(location.to_string())
        .name(name.to_string())
        .properties(HashMap::new())
        .schema(schema)
        .build();

    Ok(creation)
}

#[tokio::test]
async fn test_provider_plan_stream_schema() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_provider_get_table_schema".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "my_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let provider = ctx.catalog("catalog").unwrap();
    let schema = provider.schema("test_provider_get_table_schema").unwrap();

    let table = schema.table("my_table").await.unwrap().unwrap();
    let table_schema = table.schema();

    let expected = [("foo1", &DataType::Int32), ("foo2", &DataType::Utf8)];

    for (field, exp) in table_schema.fields().iter().zip(expected.iter()) {
        assert_eq!(field.name(), exp.0);
        assert_eq!(field.data_type(), exp.1);
        assert!(!field.is_nullable())
    }

    let df = ctx
        .sql("select foo2 from catalog.test_provider_get_table_schema.my_table")
        .await
        .unwrap();

    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await.unwrap();
    let stream = plan.execute(1, task_ctx).unwrap();

    // Ensure both the plan and the stream conform to the same schema
    assert_eq!(plan.schema(), stream.schema());
    assert_eq!(
        stream.schema().as_ref(),
        &ArrowSchema::new(vec![
            Field::new("foo2", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )]))
        ]),
    );

    Ok(())
}

#[tokio::test]
async fn test_provider_list_table_names() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_provider_list_table_names".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "my_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let provider = ctx.catalog("catalog").unwrap();
    let schema = provider.schema("test_provider_list_table_names").unwrap();

    let result = schema.table_names();

    expect![[r#"
        [
            "my_table",
            "my_table$snapshots",
            "my_table$manifests",
        ]
    "#]]
    .assert_debug_eq(&result);

    Ok(())
}

#[tokio::test]
async fn test_provider_list_schema_names() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_provider_list_schema_names".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let provider = ctx.catalog("catalog").unwrap();

    let expected = ["test_provider_list_schema_names"];
    let result = provider.schema_names();

    assert!(
        expected
            .iter()
            .all(|item| result.contains(&item.to_string()))
    );
    Ok(())
}

#[tokio::test]
async fn test_table_projection() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("ns".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo1", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "foo2", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "foo3", Type::Struct(get_struct_type())).into(),
        ])
        .build()?;
    let creation = get_table_creation(temp_path(), "t1", Some(schema))?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);
    let table_df = ctx.table("catalog.ns.t1").await.unwrap();

    let records = table_df
        .clone()
        .explain(false, false)
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(1, records.len());
    let record = &records[0];
    // the first column is plan_type, the second column plan string.
    let s = record
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(2, s.len());
    // the first row is logical_plan, the second row is physical_plan
    assert!(s.value(1).contains("projection:[foo1,foo2,foo3]"));

    // datafusion doesn't support query foo3.s_foo1, use foo3 instead
    let records = table_df
        .select_columns(&["foo1", "foo3"])
        .unwrap()
        .explain(false, false)
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(1, records.len());
    let record = &records[0];
    let s = record
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(2, s.len());
    assert!(
        s.value(1)
            .contains("IcebergTableScan projection:[foo1,foo3]")
    );

    Ok(())
}

#[tokio::test]
async fn test_table_predict_pushdown() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("ns".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;
    let creation = get_table_creation(temp_path(), "t1", Some(schema))?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);
    let records = ctx
        .sql("select * from catalog.ns.t1 where (foo > 1 and length(bar) = 1 ) or bar is null")
        .await
        .unwrap()
        .explain(false, false)
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(1, records.len());
    let record = &records[0];
    // the first column is plan_type, the second column plan string.
    let s = record
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(2, s.len());
    // the first row is logical_plan, the second row is physical_plan
    let expected = "predicate:[(foo > 1) OR (bar IS NULL)]";
    assert!(s.value(1).trim().contains(expected));
    Ok(())
}

#[tokio::test]
async fn test_metadata_table() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("ns".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;
    let creation = get_table_creation(temp_path(), "t1", Some(schema))?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);
    let snapshots = ctx
        .sql("select * from catalog.ns.t1$snapshots")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    check_record_batches(
        snapshots,
        expect![[r#"
            Field { name: "committed_at", data_type: Timestamp(Microsecond, Some("+00:00")), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1"} },
            Field { name: "snapshot_id", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "2"} },
            Field { name: "parent_id", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "3"} },
            Field { name: "operation", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "4"} },
            Field { name: "manifest_list", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "5"} },
            Field { name: "summary", data_type: Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "7"} }, Field { name: "value", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "8"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "6"} }"#]],
        expect![[r#"
            committed_at: PrimitiveArray<Timestamp(Microsecond, Some("+00:00"))>
            [
            ],
            snapshot_id: PrimitiveArray<Int64>
            [
            ],
            parent_id: PrimitiveArray<Int64>
            [
            ],
            operation: StringArray
            [
            ],
            manifest_list: StringArray
            [
            ],
            summary: MapArray
            [
            ]"#]],
        &[],
        None,
    );

    let manifests = ctx
        .sql("select * from catalog.ns.t1$manifests")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    check_record_batches(
        manifests,
        expect![[r#"
            Field { name: "content", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "14"} },
            Field { name: "path", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1"} },
            Field { name: "length", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "2"} },
            Field { name: "partition_spec_id", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "3"} },
            Field { name: "added_snapshot_id", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "4"} },
            Field { name: "added_data_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "5"} },
            Field { name: "existing_data_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "6"} },
            Field { name: "deleted_data_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "7"} },
            Field { name: "added_delete_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "15"} },
            Field { name: "existing_delete_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "16"} },
            Field { name: "deleted_delete_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "17"} },
            Field { name: "partition_summaries", data_type: List(Field { name: "item", data_type: Struct([Field { name: "contains_null", data_type: Boolean, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "10"} }, Field { name: "contains_nan", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "11"} }, Field { name: "lower_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "12"} }, Field { name: "upper_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "13"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "9"} }), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "8"} }"#]],
        expect![[r#"
            content: PrimitiveArray<Int32>
            [
            ],
            path: StringArray
            [
            ],
            length: PrimitiveArray<Int64>
            [
            ],
            partition_spec_id: PrimitiveArray<Int32>
            [
            ],
            added_snapshot_id: PrimitiveArray<Int64>
            [
            ],
            added_data_files_count: PrimitiveArray<Int32>
            [
            ],
            existing_data_files_count: PrimitiveArray<Int32>
            [
            ],
            deleted_data_files_count: PrimitiveArray<Int32>
            [
            ],
            added_delete_files_count: PrimitiveArray<Int32>
            [
            ],
            existing_delete_files_count: PrimitiveArray<Int32>
            [
            ],
            deleted_delete_files_count: PrimitiveArray<Int32>
            [
            ],
            partition_summaries: ListArray
            [
            ]"#]],
        &[],
        None,
    );

    Ok(())
}

#[tokio::test]
async fn test_insert_into() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_insert_into".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "my_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Verify table schema
    let provider = ctx.catalog("catalog").unwrap();
    let schema = provider.schema("test_insert_into").unwrap();
    let table = schema.table("my_table").await.unwrap().unwrap();
    let table_schema = table.schema();

    let expected = [("foo1", &DataType::Int32), ("foo2", &DataType::Utf8)];
    for (field, exp) in table_schema.fields().iter().zip(expected.iter()) {
        assert_eq!(field.name(), exp.0);
        assert_eq!(field.data_type(), exp.1);
        assert!(!field.is_nullable())
    }

    // Insert data into the table
    let df = ctx
        .sql("INSERT INTO catalog.test_insert_into.my_table VALUES (1, 'alan'), (2, 'turing')")
        .await
        .unwrap();

    // Verify the insert operation result
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert!(
        batch.num_rows() == 1 && batch.num_columns() == 1,
        "Results should only have one row and one column that has the number of rows inserted"
    );
    // Verify the number of rows inserted
    let rows_inserted = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(rows_inserted.value(0), 2);

    // Refresh context to avoid getting stale table
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);
    ctx.register_catalog("catalog", catalog);

    // Query the table to verify the inserted data
    let df = ctx
        .sql("SELECT * FROM catalog.test_insert_into.my_table")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();

    // Use check_record_batches to verify the data
    check_record_batches(
        batches,
        expect![[r#"
            Field { name: "foo1", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1"} },
            Field { name: "foo2", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "2"} }"#]],
        expect![[r#"
            foo1: PrimitiveArray<Int32>
            [
              1,
              2,
            ],
            foo2: StringArray
            [
              "alan",
              "turing",
            ]"#]],
        &[],
        Some("foo1"),
    );

    Ok(())
}

fn get_nested_struct_type() -> StructType {
    // Create a nested struct type with:
    // - address: STRUCT<street: STRING, city: STRING, zip: INT>
    // - contact: STRUCT<email: STRING, phone: STRING>
    StructType::new(vec![
        NestedField::optional(
            10,
            "address",
            Type::Struct(StructType::new(vec![
                NestedField::required(11, "street", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(12, "city", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(13, "zip", Type::Primitive(PrimitiveType::Int)).into(),
            ])),
        )
        .into(),
        NestedField::optional(
            20,
            "contact",
            Type::Struct(StructType::new(vec![
                NestedField::optional(21, "email", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(22, "phone", Type::Primitive(PrimitiveType::String)).into(),
            ])),
        )
        .into(),
    ])
}

#[tokio::test]
async fn test_insert_into_nested() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_insert_nested".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;
    let table_name = "nested_table";

    // Create a schema with nested fields
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "profile", Type::Struct(get_nested_struct_type())).into(),
        ])
        .build()?;

    // Create the table with the nested schema
    let creation = get_table_creation(temp_path(), table_name, Some(schema))?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Verify table schema
    let provider = ctx.catalog("catalog").unwrap();
    let schema = provider.schema("test_insert_nested").unwrap();
    let table = schema.table("nested_table").await.unwrap().unwrap();
    let table_schema = table.schema();

    // Verify the schema has the expected structure
    assert_eq!(table_schema.fields().len(), 3);
    assert_eq!(table_schema.field(0).name(), "id");
    assert_eq!(table_schema.field(1).name(), "name");
    assert_eq!(table_schema.field(2).name(), "profile");
    assert!(matches!(
        table_schema.field(2).data_type(),
        DataType::Struct(_)
    ));

    // In DataFusion, we need to use named_struct to create struct values
    // Insert data with nested structs
    let insert_sql = r#"
    INSERT INTO catalog.test_insert_nested.nested_table
    SELECT 
        1 as id, 
        'Alice' as name,
        named_struct(
            'address', named_struct(
                'street', '123 Main St',
                'city', 'San Francisco',
                'zip', 94105
            ),
            'contact', named_struct(
                'email', 'alice@example.com',
                'phone', '555-1234'
            )
        ) as profile
    UNION ALL
    SELECT 
        2 as id, 
        'Bob' as name,
        named_struct(
            'address', named_struct(
                'street', '456 Market St',
                'city', 'San Jose',
                'zip', 95113
            ),
            'contact', named_struct(
                'email', 'bob@example.com',
                'phone', NULL
            )
        ) as profile
    "#;

    // Execute the insert
    let df = ctx.sql(insert_sql).await.unwrap();
    let batches = df.collect().await.unwrap();

    // Verify the insert operation result
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert!(batch.num_rows() == 1 && batch.num_columns() == 1);

    // Verify the number of rows inserted
    let rows_inserted = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(rows_inserted.value(0), 2);

    // Refresh context to avoid getting stale table
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);
    ctx.register_catalog("catalog", catalog);

    // Query the table to verify the inserted data
    let df = ctx
        .sql("SELECT * FROM catalog.test_insert_nested.nested_table ORDER BY id")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();

    // Use check_record_batches to verify the data
    check_record_batches(
        batches,
        expect![[r#"
            Field { name: "id", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1"} },
            Field { name: "name", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "2"} },
            Field { name: "profile", data_type: Struct([Field { name: "address", data_type: Struct([Field { name: "street", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "6"} }, Field { name: "city", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "7"} }, Field { name: "zip", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "8"} }]), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "4"} }, Field { name: "contact", data_type: Struct([Field { name: "email", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "9"} }, Field { name: "phone", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "10"} }]), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "5"} }]), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "3"} }"#]],
        expect![[r#"
            id: PrimitiveArray<Int32>
            [
              1,
              2,
            ],
            name: StringArray
            [
              "Alice",
              "Bob",
            ],
            profile: StructArray
            -- validity:
            [
              valid,
              valid,
            ]
            [
            -- child 0: "address" (Struct([Field { name: "street", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "6"} }, Field { name: "city", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "7"} }, Field { name: "zip", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "8"} }]))
            StructArray
            -- validity:
            [
              valid,
              valid,
            ]
            [
            -- child 0: "street" (Utf8)
            StringArray
            [
              "123 Main St",
              "456 Market St",
            ]
            -- child 1: "city" (Utf8)
            StringArray
            [
              "San Francisco",
              "San Jose",
            ]
            -- child 2: "zip" (Int32)
            PrimitiveArray<Int32>
            [
              94105,
              95113,
            ]
            ]
            -- child 1: "contact" (Struct([Field { name: "email", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "9"} }, Field { name: "phone", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "10"} }]))
            StructArray
            -- validity:
            [
              valid,
              valid,
            ]
            [
            -- child 0: "email" (Utf8)
            StringArray
            [
              "alice@example.com",
              "bob@example.com",
            ]
            -- child 1: "phone" (Utf8)
            StringArray
            [
              "555-1234",
              null,
            ]
            ]
            ]"#]],
        &[],
        Some("id"),
    );

    // Query with explicit field access to verify nested data
    let df = ctx
        .sql(
            r#"
            SELECT 
                id, 
                name,
                profile.address.street,
                profile.address.city,
                profile.address.zip,
                profile.contact.email,
                profile.contact.phone
            FROM catalog.test_insert_nested.nested_table 
            ORDER BY id
        "#,
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();

    // Use check_record_batches to verify the flattened data
    check_record_batches(
        batches,
        expect![[r#"
            Field { name: "id", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1"} },
            Field { name: "name", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "2"} },
            Field { name: "catalog.test_insert_nested.nested_table.profile[address][street]", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} },
            Field { name: "catalog.test_insert_nested.nested_table.profile[address][city]", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} },
            Field { name: "catalog.test_insert_nested.nested_table.profile[address][zip]", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} },
            Field { name: "catalog.test_insert_nested.nested_table.profile[contact][email]", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} },
            Field { name: "catalog.test_insert_nested.nested_table.profile[contact][phone]", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }"#]],
        expect![[r#"
            id: PrimitiveArray<Int32>
            [
              1,
              2,
            ],
            name: StringArray
            [
              "Alice",
              "Bob",
            ],
            catalog.test_insert_nested.nested_table.profile[address][street]: StringArray
            [
              "123 Main St",
              "456 Market St",
            ],
            catalog.test_insert_nested.nested_table.profile[address][city]: StringArray
            [
              "San Francisco",
              "San Jose",
            ],
            catalog.test_insert_nested.nested_table.profile[address][zip]: PrimitiveArray<Int32>
            [
              94105,
              95113,
            ],
            catalog.test_insert_nested.nested_table.profile[contact][email]: StringArray
            [
              "alice@example.com",
              "bob@example.com",
            ],
            catalog.test_insert_nested.nested_table.profile[contact][phone]: StringArray
            [
              "555-1234",
              null,
            ]"#]],
        &[],
        Some("id"),
    );

    Ok(())
}
