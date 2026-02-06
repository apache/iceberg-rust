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
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::{
    Int32Builder, Int64Builder, LargeBinaryBuilder, ListBuilder, MapBuilder, MapFieldNames,
    PrimitiveBuilder, StringBuilder, StructBuilder,
};
use arrow_array::types::Int64Type;
use arrow_schema::{DataType, Field, Fields};
use futures::{StreamExt, stream};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{
    DataContentType, ListType, MapType, NestedField, PrimitiveType, StructType, Type,
};
use crate::table::Table;

/// Files metadata table.
///
/// Shows all data and delete files referenced by the current snapshot.
pub struct FilesTable<'a> {
    table: &'a Table,
}

/// Data files metadata table.
///
/// Shows only data files referenced by the current snapshot.
pub struct DataFilesTable<'a> {
    table: &'a Table,
}

/// Delete files metadata table.
///
/// Shows only delete files referenced by the current snapshot.
pub struct DeleteFilesTable<'a> {
    table: &'a Table,
}

/// Content type filter for file-level metadata tables.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ContentFilter {
    /// All files (data + delete).
    All,
    /// Data files only.
    DataOnly,
    /// Delete files only (position + equality).
    DeletesOnly,
}

impl<'a> FilesTable<'a> {
    /// Create a new Files table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the iceberg schema of the files table.
    pub fn schema(&self) -> crate::spec::Schema {
        files_schema(self.table.metadata().default_partition_type())
    }

    /// Scans the files table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        scan_files(self.table, ContentFilter::All).await
    }
}

impl<'a> DataFilesTable<'a> {
    /// Create a new DataFiles table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the iceberg schema of the data files table.
    pub fn schema(&self) -> crate::spec::Schema {
        files_schema(self.table.metadata().default_partition_type())
    }

    /// Scans the data files table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        scan_files(self.table, ContentFilter::DataOnly).await
    }
}

impl<'a> DeleteFilesTable<'a> {
    /// Create a new DeleteFiles table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the iceberg schema of the delete files table.
    pub fn schema(&self) -> crate::spec::Schema {
        files_schema(self.table.metadata().default_partition_type())
    }

    /// Scans the delete files table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        scan_files(self.table, ContentFilter::DeletesOnly).await
    }
}

/// Build the Iceberg schema for file-level metadata tables.
pub(crate) fn files_schema(partition_type: &StructType) -> crate::spec::Schema {
    let partition_fields: Vec<Arc<NestedField>> = partition_type
        .fields()
        .iter()
        .map(|f| {
            Arc::new(NestedField::optional(
                f.id,
                f.name.as_str(),
                (*f.field_type).clone(),
            ))
        })
        .collect();

    let fields = vec![
        NestedField::required(134, "content", Type::Primitive(PrimitiveType::Int)),
        NestedField::required(100, "file_path", Type::Primitive(PrimitiveType::String)),
        NestedField::required(101, "file_format", Type::Primitive(PrimitiveType::String)),
        NestedField::required(
            102,
            "partition",
            Type::Struct(StructType::new(partition_fields)),
        ),
        NestedField::required(103, "record_count", Type::Primitive(PrimitiveType::Long)),
        NestedField::required(
            104,
            "file_size_in_bytes",
            Type::Primitive(PrimitiveType::Long),
        ),
        NestedField::optional(
            108,
            "column_sizes",
            Type::Map(MapType {
                key_field: Arc::new(NestedField::map_key_element(
                    201,
                    Type::Primitive(PrimitiveType::Int),
                )),
                value_field: Arc::new(NestedField::map_value_element(
                    202,
                    Type::Primitive(PrimitiveType::Long),
                    false,
                )),
            }),
        ),
        NestedField::optional(
            109,
            "value_counts",
            Type::Map(MapType {
                key_field: Arc::new(NestedField::map_key_element(
                    203,
                    Type::Primitive(PrimitiveType::Int),
                )),
                value_field: Arc::new(NestedField::map_value_element(
                    204,
                    Type::Primitive(PrimitiveType::Long),
                    false,
                )),
            }),
        ),
        NestedField::optional(
            110,
            "null_value_counts",
            Type::Map(MapType {
                key_field: Arc::new(NestedField::map_key_element(
                    205,
                    Type::Primitive(PrimitiveType::Int),
                )),
                value_field: Arc::new(NestedField::map_value_element(
                    206,
                    Type::Primitive(PrimitiveType::Long),
                    false,
                )),
            }),
        ),
        NestedField::optional(
            137,
            "nan_value_counts",
            Type::Map(MapType {
                key_field: Arc::new(NestedField::map_key_element(
                    207,
                    Type::Primitive(PrimitiveType::Int),
                )),
                value_field: Arc::new(NestedField::map_value_element(
                    208,
                    Type::Primitive(PrimitiveType::Long),
                    false,
                )),
            }),
        ),
        NestedField::optional(
            125,
            "lower_bounds",
            Type::Map(MapType {
                key_field: Arc::new(NestedField::map_key_element(
                    209,
                    Type::Primitive(PrimitiveType::Int),
                )),
                value_field: Arc::new(NestedField::map_value_element(
                    210,
                    Type::Primitive(PrimitiveType::Binary),
                    false,
                )),
            }),
        ),
        NestedField::optional(
            128,
            "upper_bounds",
            Type::Map(MapType {
                key_field: Arc::new(NestedField::map_key_element(
                    211,
                    Type::Primitive(PrimitiveType::Int),
                )),
                value_field: Arc::new(NestedField::map_value_element(
                    212,
                    Type::Primitive(PrimitiveType::Binary),
                    false,
                )),
            }),
        ),
        NestedField::optional(131, "key_metadata", Type::Primitive(PrimitiveType::Binary)),
        NestedField::optional(
            132,
            "split_offsets",
            Type::List(ListType {
                element_field: Arc::new(NestedField::list_element(
                    213,
                    Type::Primitive(PrimitiveType::Long),
                    false,
                )),
            }),
        ),
        NestedField::optional(
            135,
            "equality_ids",
            Type::List(ListType {
                element_field: Arc::new(NestedField::list_element(
                    214,
                    Type::Primitive(PrimitiveType::Int),
                    false,
                )),
            }),
        ),
        NestedField::optional(140, "sort_order_id", Type::Primitive(PrimitiveType::Int)),
        NestedField::optional(141, "spec_id", Type::Primitive(PrimitiveType::Int)),
    ];
    crate::spec::Schema::builder()
        .with_fields(fields.into_iter().map(|f| f.into()))
        .build()
        .unwrap()
}

/// Shared scan implementation for file-level metadata tables.
async fn scan_files(table: &Table, filter: ContentFilter) -> Result<ArrowRecordBatchStream> {
    let partition_type = table.metadata().default_partition_type();
    let schema = schema_to_arrow_schema(&files_schema(partition_type))?;

    let mut content = Int32Builder::new();
    let mut file_path = StringBuilder::new();
    let mut file_format = StringBuilder::new();
    let mut partition_builder = build_partition_struct_builder(&schema)?;
    let mut record_count = Int64Builder::new();
    let mut file_size_in_bytes = Int64Builder::new();
    let mut column_sizes = new_int_long_map_builder(201, 202);
    let mut value_counts = new_int_long_map_builder(203, 204);
    let mut null_value_counts = new_int_long_map_builder(205, 206);
    let mut nan_value_counts = new_int_long_map_builder(207, 208);
    let mut lower_bounds = new_int_binary_map_builder(209, 210);
    let mut upper_bounds = new_int_binary_map_builder(211, 212);
    let mut key_metadata = LargeBinaryBuilder::new();
    let mut split_offsets = ListBuilder::new(PrimitiveBuilder::<Int64Type>::new()).with_field(
        Arc::new(Field::new("element", DataType::Int64, true).with_metadata(field_id_meta(213))),
    );
    let mut equality_ids = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(
        Field::new("element", DataType::Int32, true).with_metadata(field_id_meta(214)),
    ));
    let mut sort_order_id = Int32Builder::new();
    let mut spec_id = Int32Builder::new();

    if let Some(snapshot) = table.metadata().current_snapshot() {
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await?;

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }

                let data_file = entry.data_file();
                let ct = data_file.content_type();

                match filter {
                    ContentFilter::All => {}
                    ContentFilter::DataOnly => {
                        if ct != DataContentType::Data {
                            continue;
                        }
                    }
                    ContentFilter::DeletesOnly => {
                        if ct == DataContentType::Data {
                            continue;
                        }
                    }
                }

                content.append_value(ct as i32);
                file_path.append_value(data_file.file_path());
                file_format.append_value(data_file.file_format().to_string());
                append_partition_value(
                    &mut partition_builder,
                    data_file.partition(),
                    partition_type,
                );
                record_count.append_value(data_file.record_count() as i64);
                file_size_in_bytes.append_value(data_file.file_size_in_bytes() as i64);

                append_int_long_map(&mut column_sizes, data_file.column_sizes());
                append_int_long_map(&mut value_counts, data_file.value_counts());
                append_int_long_map(&mut null_value_counts, data_file.null_value_counts());
                append_int_long_map(&mut nan_value_counts, data_file.nan_value_counts());
                append_datum_map(&mut lower_bounds, data_file.lower_bounds())?;
                append_datum_map(&mut upper_bounds, data_file.upper_bounds())?;

                match data_file.key_metadata() {
                    Some(km) => key_metadata.append_value(km),
                    None => key_metadata.append_null(),
                }

                match data_file.split_offsets() {
                    Some(offsets) => {
                        let list_builder = split_offsets.values();
                        for offset in offsets {
                            list_builder.append_value(*offset);
                        }
                        split_offsets.append(true);
                    }
                    None => split_offsets.append_null(),
                }

                match data_file.equality_ids() {
                    Some(ids) => {
                        let list_builder = equality_ids.values();
                        for id in ids {
                            list_builder.append_value(id);
                        }
                        equality_ids.append(true);
                    }
                    None => equality_ids.append_null(),
                }

                sort_order_id.append_option(data_file.sort_order_id());
                spec_id.append_value(data_file.partition_spec_id);
            }
        }
    }

    let batch = RecordBatch::try_new(Arc::new(schema), vec![
        Arc::new(content.finish()),
        Arc::new(file_path.finish()),
        Arc::new(file_format.finish()),
        Arc::new(partition_builder.finish()),
        Arc::new(record_count.finish()),
        Arc::new(file_size_in_bytes.finish()),
        Arc::new(column_sizes.finish()),
        Arc::new(value_counts.finish()),
        Arc::new(null_value_counts.finish()),
        Arc::new(nan_value_counts.finish()),
        Arc::new(lower_bounds.finish()),
        Arc::new(upper_bounds.finish()),
        Arc::new(key_metadata.finish()),
        Arc::new(split_offsets.finish()),
        Arc::new(equality_ids.finish()),
        Arc::new(sort_order_id.finish()),
        Arc::new(spec_id.finish()),
    ])?;

    Ok(stream::iter(vec![Ok(batch)]).boxed())
}

fn field_id_meta(id: i32) -> HashMap<String, String> {
    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())])
}

fn new_int_long_map_builder(
    key_field_id: i32,
    value_field_id: i32,
) -> MapBuilder<Int32Builder, Int64Builder> {
    use crate::arrow::DEFAULT_MAP_FIELD_NAME;
    use crate::spec::{MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME};

    MapBuilder::new(
        Some(MapFieldNames {
            entry: DEFAULT_MAP_FIELD_NAME.to_string(),
            key: MAP_KEY_FIELD_NAME.to_string(),
            value: MAP_VALUE_FIELD_NAME.to_string(),
        }),
        Int32Builder::new(),
        Int64Builder::new(),
    )
    .with_keys_field(Arc::new(
        Field::new(MAP_KEY_FIELD_NAME, DataType::Int32, false)
            .with_metadata(field_id_meta(key_field_id)),
    ))
    .with_values_field(Arc::new(
        Field::new(MAP_VALUE_FIELD_NAME, DataType::Int64, true)
            .with_metadata(field_id_meta(value_field_id)),
    ))
}

fn new_int_binary_map_builder(
    key_field_id: i32,
    value_field_id: i32,
) -> MapBuilder<Int32Builder, LargeBinaryBuilder> {
    use crate::arrow::DEFAULT_MAP_FIELD_NAME;
    use crate::spec::{MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME};

    MapBuilder::new(
        Some(MapFieldNames {
            entry: DEFAULT_MAP_FIELD_NAME.to_string(),
            key: MAP_KEY_FIELD_NAME.to_string(),
            value: MAP_VALUE_FIELD_NAME.to_string(),
        }),
        Int32Builder::new(),
        LargeBinaryBuilder::new(),
    )
    .with_keys_field(Arc::new(
        Field::new(MAP_KEY_FIELD_NAME, DataType::Int32, false)
            .with_metadata(field_id_meta(key_field_id)),
    ))
    .with_values_field(Arc::new(
        Field::new(MAP_VALUE_FIELD_NAME, DataType::LargeBinary, true)
            .with_metadata(field_id_meta(value_field_id)),
    ))
}

fn append_int_long_map(
    builder: &mut MapBuilder<Int32Builder, Int64Builder>,
    map: &HashMap<i32, u64>,
) {
    for (&k, &v) in map {
        builder.keys().append_value(k);
        builder.values().append_value(v as i64);
    }
    builder.append(true).unwrap();
}

fn append_datum_map(
    builder: &mut MapBuilder<Int32Builder, LargeBinaryBuilder>,
    map: &HashMap<i32, crate::spec::Datum>,
) -> Result<()> {
    for (&k, v) in map {
        builder.keys().append_value(k);
        builder.values().append_value(v.to_bytes()?);
    }
    builder.append(true).unwrap();
    Ok(())
}

fn build_partition_struct_builder(schema: &arrow_schema::Schema) -> Result<StructBuilder> {
    let partition_fields = match schema.field_with_name("partition")?.data_type() {
        DataType::Struct(fields) => fields.to_vec(),
        _ => unreachable!(),
    };
    Ok(StructBuilder::from_fields(
        Fields::from(partition_fields),
        0,
    ))
}

fn append_partition_value(
    builder: &mut StructBuilder,
    partition: &crate::spec::Struct,
    partition_type: &StructType,
) {
    let fields = partition_type.fields();
    for (i, field) in fields.iter().enumerate() {
        let literal: Option<&crate::spec::Literal> =
            partition.fields().get(i).and_then(|opt| opt.as_ref());
        match &*field.field_type {
            Type::Primitive(PrimitiveType::Long) => {
                let b = builder
                    .field_builder::<PrimitiveBuilder<Int64Type>>(i)
                    .unwrap();
                match literal.and_then(|l| l.as_primitive_literal()) {
                    Some(crate::spec::PrimitiveLiteral::Long(v)) => b.append_value(v),
                    Some(crate::spec::PrimitiveLiteral::Int(v)) => b.append_value(v as i64),
                    _ => b.append_null(),
                }
            }
            Type::Primitive(PrimitiveType::Int) => {
                let b = builder.field_builder::<Int32Builder>(i).unwrap();
                match literal.and_then(|l| l.as_primitive_literal()) {
                    Some(crate::spec::PrimitiveLiteral::Int(v)) => b.append_value(v),
                    _ => b.append_null(),
                }
            }
            Type::Primitive(PrimitiveType::String) => {
                let b = builder.field_builder::<StringBuilder>(i).unwrap();
                match literal.and_then(|l| l.as_primitive_literal()) {
                    Some(crate::spec::PrimitiveLiteral::String(s)) => b.append_value(&s),
                    _ => b.append_null(),
                }
            }
            _ => {
                // For other partition types, we cannot easily append.
                // This is a best-effort approach supporting common types.
            }
        }
    }
    builder.append(true);
}

/// Scan files across all snapshots with manifest deduplication.
pub(crate) async fn scan_all_files(
    table: &Table,
    filter: ContentFilter,
) -> Result<ArrowRecordBatchStream> {
    use std::collections::HashSet;

    let partition_type = table.metadata().default_partition_type();
    let schema = schema_to_arrow_schema(&files_schema(partition_type))?;

    let mut content = Int32Builder::new();
    let mut file_path = StringBuilder::new();
    let mut file_format = StringBuilder::new();
    let mut partition_builder = build_partition_struct_builder(&schema)?;
    let mut record_count = Int64Builder::new();
    let mut file_size_in_bytes = Int64Builder::new();
    let mut column_sizes = new_int_long_map_builder(201, 202);
    let mut value_counts = new_int_long_map_builder(203, 204);
    let mut null_value_counts = new_int_long_map_builder(205, 206);
    let mut nan_value_counts = new_int_long_map_builder(207, 208);
    let mut lower_bounds = new_int_binary_map_builder(209, 210);
    let mut upper_bounds = new_int_binary_map_builder(211, 212);
    let mut key_metadata = LargeBinaryBuilder::new();
    let mut split_offsets = ListBuilder::new(PrimitiveBuilder::<Int64Type>::new()).with_field(
        Arc::new(Field::new("element", DataType::Int64, true).with_metadata(field_id_meta(213))),
    );
    let mut equality_ids = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(
        Field::new("element", DataType::Int32, true).with_metadata(field_id_meta(214)),
    ));
    let mut sort_order_id = Int32Builder::new();
    let mut spec_id = Int32Builder::new();

    let mut seen_manifests = HashSet::new();

    for snapshot in table.metadata().snapshots() {
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await?;

        for manifest_file in manifest_list.entries() {
            if !seen_manifests.insert(manifest_file.manifest_path.clone()) {
                continue;
            }

            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }

                let data_file = entry.data_file();
                let ct = data_file.content_type();

                match filter {
                    ContentFilter::All => {}
                    ContentFilter::DataOnly => {
                        if ct != DataContentType::Data {
                            continue;
                        }
                    }
                    ContentFilter::DeletesOnly => {
                        if ct == DataContentType::Data {
                            continue;
                        }
                    }
                }

                content.append_value(ct as i32);
                file_path.append_value(data_file.file_path());
                file_format.append_value(data_file.file_format().to_string());
                append_partition_value(
                    &mut partition_builder,
                    data_file.partition(),
                    partition_type,
                );
                record_count.append_value(data_file.record_count() as i64);
                file_size_in_bytes.append_value(data_file.file_size_in_bytes() as i64);

                append_int_long_map(&mut column_sizes, data_file.column_sizes());
                append_int_long_map(&mut value_counts, data_file.value_counts());
                append_int_long_map(&mut null_value_counts, data_file.null_value_counts());
                append_int_long_map(&mut nan_value_counts, data_file.nan_value_counts());
                append_datum_map(&mut lower_bounds, data_file.lower_bounds())?;
                append_datum_map(&mut upper_bounds, data_file.upper_bounds())?;

                match data_file.key_metadata() {
                    Some(km) => key_metadata.append_value(km),
                    None => key_metadata.append_null(),
                }

                match data_file.split_offsets() {
                    Some(offsets) => {
                        let list_builder = split_offsets.values();
                        for offset in offsets {
                            list_builder.append_value(*offset);
                        }
                        split_offsets.append(true);
                    }
                    None => split_offsets.append_null(),
                }

                match data_file.equality_ids() {
                    Some(ids) => {
                        let list_builder = equality_ids.values();
                        for id in ids {
                            list_builder.append_value(id);
                        }
                        equality_ids.append(true);
                    }
                    None => equality_ids.append_null(),
                }

                sort_order_id.append_option(data_file.sort_order_id());
                spec_id.append_value(data_file.partition_spec_id);
            }
        }
    }

    let batch = RecordBatch::try_new(Arc::new(schema), vec![
        Arc::new(content.finish()),
        Arc::new(file_path.finish()),
        Arc::new(file_format.finish()),
        Arc::new(partition_builder.finish()),
        Arc::new(record_count.finish()),
        Arc::new(file_size_in_bytes.finish()),
        Arc::new(column_sizes.finish()),
        Arc::new(value_counts.finish()),
        Arc::new(null_value_counts.finish()),
        Arc::new(nan_value_counts.finish()),
        Arc::new(lower_bounds.finish()),
        Arc::new(upper_bounds.finish()),
        Arc::new(key_metadata.finish()),
        Arc::new(split_offsets.finish()),
        Arc::new(equality_ids.finish()),
        Arc::new(sort_order_id.finish()),
        Arc::new(spec_id.finish()),
    ])?;

    Ok(stream::iter(vec![Ok(batch)]).boxed())
}

#[cfg(test)]
mod tests {
    use expect_test::expect;
    use futures::TryStreamExt;

    use crate::scan::tests::TableTestFixture;
    use crate::test_utils::check_record_batches;

    #[tokio::test]
    async fn test_files_table() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let batch_stream = fixture.table.inspect().files().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "content": Int32, metadata: {"PARQUET:field_id": "134"} },
                Field { "file_path": Utf8, metadata: {"PARQUET:field_id": "100"} },
                Field { "file_format": Utf8, metadata: {"PARQUET:field_id": "101"} },
                Field { "partition": Struct("x": Int64, metadata: {"PARQUET:field_id": "1000"}), metadata: {"PARQUET:field_id": "102"} },
                Field { "record_count": Int64, metadata: {"PARQUET:field_id": "103"} },
                Field { "file_size_in_bytes": Int64, metadata: {"PARQUET:field_id": "104"} },
                Field { "column_sizes": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "201"}, "value": Int64, metadata: {"PARQUET:field_id": "202"}), unsorted), metadata: {"PARQUET:field_id": "108"} },
                Field { "value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "203"}, "value": Int64, metadata: {"PARQUET:field_id": "204"}), unsorted), metadata: {"PARQUET:field_id": "109"} },
                Field { "null_value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "205"}, "value": Int64, metadata: {"PARQUET:field_id": "206"}), unsorted), metadata: {"PARQUET:field_id": "110"} },
                Field { "nan_value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "207"}, "value": Int64, metadata: {"PARQUET:field_id": "208"}), unsorted), metadata: {"PARQUET:field_id": "137"} },
                Field { "lower_bounds": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "209"}, "value": LargeBinary, metadata: {"PARQUET:field_id": "210"}), unsorted), metadata: {"PARQUET:field_id": "125"} },
                Field { "upper_bounds": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "211"}, "value": LargeBinary, metadata: {"PARQUET:field_id": "212"}), unsorted), metadata: {"PARQUET:field_id": "128"} },
                Field { "key_metadata": nullable LargeBinary, metadata: {"PARQUET:field_id": "131"} },
                Field { "split_offsets": nullable List(Int64, field: 'element', metadata: {"PARQUET:field_id": "213"}), metadata: {"PARQUET:field_id": "132"} },
                Field { "equality_ids": nullable List(Int32, field: 'element', metadata: {"PARQUET:field_id": "214"}), metadata: {"PARQUET:field_id": "135"} },
                Field { "sort_order_id": nullable Int32, metadata: {"PARQUET:field_id": "140"} },
                Field { "spec_id": nullable Int32, metadata: {"PARQUET:field_id": "141"} }"#]],
            expect![[r#"
                content: PrimitiveArray<Int32>
                [
                  0,
                  0,
                ],
                file_path: (skipped),
                file_format: StringArray
                [
                  "parquet",
                  "parquet",
                ],
                partition: StructArray
                -- validity:
                [
                  valid,
                  valid,
                ]
                [
                -- child 0: "x" (Int64)
                PrimitiveArray<Int64>
                [
                  100,
                  300,
                ]
                ],
                record_count: PrimitiveArray<Int64>
                [
                  1,
                  1,
                ],
                file_size_in_bytes: PrimitiveArray<Int64>
                [
                  100,
                  100,
                ],
                column_sizes: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                value_counts: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                null_value_counts: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                nan_value_counts: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                lower_bounds: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                ],
                upper_bounds: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                ],
                key_metadata: LargeBinaryArray
                [
                  null,
                  null,
                ],
                split_offsets: ListArray
                [
                  null,
                  null,
                ],
                equality_ids: ListArray
                [
                  null,
                  null,
                ],
                sort_order_id: PrimitiveArray<Int32>
                [
                  null,
                  null,
                ],
                spec_id: PrimitiveArray<Int32>
                [
                  0,
                  0,
                ]"#]],
            &["file_path"],
            Some("file_path"),
        );
    }

    #[tokio::test]
    async fn test_data_files_table() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let batch_stream = fixture.table.inspect().data_files().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "content": Int32, metadata: {"PARQUET:field_id": "134"} },
                Field { "file_path": Utf8, metadata: {"PARQUET:field_id": "100"} },
                Field { "file_format": Utf8, metadata: {"PARQUET:field_id": "101"} },
                Field { "partition": Struct("x": Int64, metadata: {"PARQUET:field_id": "1000"}), metadata: {"PARQUET:field_id": "102"} },
                Field { "record_count": Int64, metadata: {"PARQUET:field_id": "103"} },
                Field { "file_size_in_bytes": Int64, metadata: {"PARQUET:field_id": "104"} },
                Field { "column_sizes": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "201"}, "value": Int64, metadata: {"PARQUET:field_id": "202"}), unsorted), metadata: {"PARQUET:field_id": "108"} },
                Field { "value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "203"}, "value": Int64, metadata: {"PARQUET:field_id": "204"}), unsorted), metadata: {"PARQUET:field_id": "109"} },
                Field { "null_value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "205"}, "value": Int64, metadata: {"PARQUET:field_id": "206"}), unsorted), metadata: {"PARQUET:field_id": "110"} },
                Field { "nan_value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "207"}, "value": Int64, metadata: {"PARQUET:field_id": "208"}), unsorted), metadata: {"PARQUET:field_id": "137"} },
                Field { "lower_bounds": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "209"}, "value": LargeBinary, metadata: {"PARQUET:field_id": "210"}), unsorted), metadata: {"PARQUET:field_id": "125"} },
                Field { "upper_bounds": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "211"}, "value": LargeBinary, metadata: {"PARQUET:field_id": "212"}), unsorted), metadata: {"PARQUET:field_id": "128"} },
                Field { "key_metadata": nullable LargeBinary, metadata: {"PARQUET:field_id": "131"} },
                Field { "split_offsets": nullable List(Int64, field: 'element', metadata: {"PARQUET:field_id": "213"}), metadata: {"PARQUET:field_id": "132"} },
                Field { "equality_ids": nullable List(Int32, field: 'element', metadata: {"PARQUET:field_id": "214"}), metadata: {"PARQUET:field_id": "135"} },
                Field { "sort_order_id": nullable Int32, metadata: {"PARQUET:field_id": "140"} },
                Field { "spec_id": nullable Int32, metadata: {"PARQUET:field_id": "141"} }"#]],
            expect![[r#"
                content: PrimitiveArray<Int32>
                [
                  0,
                  0,
                ],
                file_path: (skipped),
                file_format: StringArray
                [
                  "parquet",
                  "parquet",
                ],
                partition: StructArray
                -- validity:
                [
                  valid,
                  valid,
                ]
                [
                -- child 0: "x" (Int64)
                PrimitiveArray<Int64>
                [
                  100,
                  300,
                ]
                ],
                record_count: PrimitiveArray<Int64>
                [
                  1,
                  1,
                ],
                file_size_in_bytes: PrimitiveArray<Int64>
                [
                  100,
                  100,
                ],
                column_sizes: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                value_counts: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                null_value_counts: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                nan_value_counts: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (Int64)
                PrimitiveArray<Int64>
                [
                ]
                ],
                ],
                lower_bounds: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                ],
                upper_bounds: MapArray
                [
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                  StructArray
                -- validity:
                [
                ]
                [
                -- child 0: "key" (Int32)
                PrimitiveArray<Int32>
                [
                ]
                -- child 1: "value" (LargeBinary)
                LargeBinaryArray
                [
                ]
                ],
                ],
                key_metadata: LargeBinaryArray
                [
                  null,
                  null,
                ],
                split_offsets: ListArray
                [
                  null,
                  null,
                ],
                equality_ids: ListArray
                [
                  null,
                  null,
                ],
                sort_order_id: PrimitiveArray<Int32>
                [
                  null,
                  null,
                ],
                spec_id: PrimitiveArray<Int32>
                [
                  0,
                  0,
                ]"#]],
            &["file_path"],
            Some("file_path"),
        );
    }

    #[tokio::test]
    async fn test_delete_files_table_empty() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let batch_stream = fixture.table.inspect().delete_files().scan().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 0);
    }
}
