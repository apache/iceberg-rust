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

use arrow_array::RecordBatch;
use futures::{Stream, TryStreamExt};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::Result;
use crate::spec::{DataFile, DataFileFormat, PartitionKey};
use crate::table::Table;
use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use crate::writer::file_writer::ParquetWriterBuilder;
use crate::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use crate::writer::{IcebergWriter, IcebergWriterBuilder};

/// Writes replacement record batches as Iceberg data files.
pub(crate) async fn write_replacement_batches<S>(
    table: &Table,
    partition_key: Option<PartitionKey>,
    mut batches: S,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let location_generator = DefaultLocationGenerator::new(table.metadata())?;
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("cow-rewrite-{}", Uuid::now_v7()),
        None,
        DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
    );
    let rolling_builder = RollingFileWriterBuilder::new(
        parquet_builder,
        table
            .metadata()
            .table_properties()?
            .write_target_file_size_bytes,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_writer_builder = DataFileWriterBuilder::new(rolling_builder);
    let mut writer = data_writer_builder.build(partition_key).await?;

    let mut wrote_rows = false;
    while let Some(batch) = batches.try_next().await? {
        if batch.num_rows() == 0 {
            continue;
        }
        wrote_rows = true;
        writer.write(batch).await?;
    }

    if !wrote_rows {
        return Ok(vec![]);
    }

    writer.close().await
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use tempfile::TempDir;

    use super::write_replacement_batches;
    use crate::io::LocalFsStorageFactory;
    use crate::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use crate::spec::{
        DataContentType, NestedField, PartitionKey, PrimitiveType, Schema, Struct, Transform, Type,
    };
    use crate::{Catalog, CatalogBuilder, NamespaceIdent, Result, TableCreation};

    #[tokio::test]
    async fn cow_replacement_writer_outputs_data_files() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await?;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("cow_writer".to_string())
                    .schema(schema)
                    .build(),
            )
            .await?;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));
        let batch = RecordBatch::try_new(arrow_schema, vec![Arc::new(Int32Array::from(vec![
            1, 2, 3,
        ])) as ArrayRef])?;

        let data_files =
            write_replacement_batches(&table, None, futures::stream::iter(vec![Ok(batch)])).await?;

        assert_eq!(data_files.len(), 1);
        assert_eq!(data_files[0].record_count(), 3);
        assert_eq!(data_files[0].content_type(), DataContentType::Data);

        Ok(())
    }

    #[tokio::test]
    async fn cow_replacement_writer_skips_empty_input() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await?;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("cow_empty_writer".to_string())
                    .schema(schema)
                    .build(),
            )
            .await?;

        let data_files =
            write_replacement_batches(&table, None, futures::stream::iter(vec![])).await?;

        assert!(data_files.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn cow_replacement_writer_skips_zero_row_batches() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await?;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("cow_zero_row_writer".to_string())
                    .schema(schema)
                    .build(),
            )
            .await?;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));
        let batch =
            RecordBatch::try_new(arrow_schema, vec![
                Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
            ])?;

        let data_files =
            write_replacement_batches(&table, None, futures::stream::iter(vec![Ok(batch)])).await?;

        assert!(data_files.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn cow_replacement_writer_preserves_partition_key() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await?;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()?;
        let partition_spec = crate::spec::PartitionSpec::builder(Arc::new(schema.clone()))
            .add_partition_field("id", "id", Transform::Identity)?
            .build()?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("cow_partitioned_writer".to_string())
                    .schema(schema)
                    .partition_spec(partition_spec.clone().into_unbound())
                    .build(),
            )
            .await?;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));
        let batch = RecordBatch::try_new(arrow_schema, vec![Arc::new(Int32Array::from(vec![
            1, 1, 1,
        ])) as ArrayRef])?;
        let partition_key = PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            table.metadata().current_schema().clone(),
            Struct::from_iter([Some(crate::spec::Literal::int(1))]),
        );

        let data_files = write_replacement_batches(
            &table,
            Some(partition_key),
            futures::stream::iter(vec![Ok(batch)]),
        )
        .await?;

        assert_eq!(data_files.len(), 1);
        assert_eq!(data_files[0].partition_spec_id, 0);
        assert_eq!(
            data_files[0].partition(),
            &Struct::from_iter([Some(crate::spec::Literal::int(1))])
        );

        Ok(())
    }
}
