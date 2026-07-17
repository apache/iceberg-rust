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

use uuid::Uuid;

use crate::Result;
#[cfg(test)]
use crate::spec::DataFile;
use crate::spec::{DataFileFormat, PartitionKey, SchemaRef};
use crate::table::Table;
use crate::writer::IcebergWriterBuilder;
use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use crate::writer::file_writer::ParquetWriterBuilder;
use crate::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;

/// Builds a boxed replacement-data-file writer.
///
/// `write_schema` is the schema the input batches are encoded in. It must match
/// the schema the rows were read in (the planned snapshot's schema), not the
/// table's possibly-evolved current schema; otherwise the parquet writer will
/// reject batches that lack columns added after the source files were written.
///
/// Building the writer is cheap: no physical file is opened until the first
/// batch is written, so it is safe to construct one optimistically and only
/// write to it once a source file is known to have changed.
pub(crate) async fn build_replacement_writer(
    table: &Table,
    write_schema: SchemaRef,
    partition_key: Option<PartitionKey>,
) -> Result<Box<dyn crate::writer::IcebergWriter>> {
    let location_generator = DefaultLocationGenerator::new(table.metadata())?;
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("cow-rewrite-{}", Uuid::now_v7()),
        None,
        DataFileFormat::Parquet,
    );
    let table_props = table.metadata().table_properties()?;
    let parquet_builder = ParquetWriterBuilder::from_table_properties(&table_props, write_schema);
    let rolling_builder = RollingFileWriterBuilder::new(
        parquet_builder,
        table_props.write_target_file_size_bytes,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_writer_builder = DataFileWriterBuilder::new(rolling_builder);
    Ok(Box::new(data_writer_builder.build(partition_key).await?))
}

/// Writes replacement record batches as Iceberg data files.
///
/// This is a convenience wrapper around [`build_replacement_writer`] that
/// consumes a stream end-to-end. Empty input or all-zero-row batches produce no
/// data files.
#[cfg(test)]
pub(crate) async fn write_replacement_batches<S>(
    table: &Table,
    write_schema: SchemaRef,
    partition_key: Option<PartitionKey>,
    mut batches: S,
) -> Result<Vec<DataFile>>
where
    S: futures::Stream<Item = Result<arrow_array::RecordBatch>> + Unpin,
{
    use futures::TryStreamExt as _;

    let mut writer = build_replacement_writer(table, write_schema, partition_key).await?;

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

        let data_files = write_replacement_batches(
            &table,
            table.metadata().current_schema().clone(),
            None,
            futures::stream::iter(vec![Ok(batch)]),
        )
        .await?;

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

        let data_files = write_replacement_batches(
            &table,
            table.metadata().current_schema().clone(),
            None,
            futures::stream::iter(vec![]),
        )
        .await?;

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

        let data_files = write_replacement_batches(
            &table,
            table.metadata().current_schema().clone(),
            None,
            futures::stream::iter(vec![Ok(batch)]),
        )
        .await?;

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
            table.metadata().current_schema().clone(),
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
