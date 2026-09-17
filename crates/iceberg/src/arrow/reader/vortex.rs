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

//! Read path for vortex data files, producing the same transformed arrow
//! record batch stream as the parquet read path.
//!
//! Field IDs are restored from the embedded Iceberg schema, or resolved through
//! name mapping / positional fallback for imported files. Filters, deletes,
//! partition constants, and metadata columns use the same Iceberg semantics as
//! Parquet. Vortex's own layouts provide pruning; Parquet page indexes, INT96
//! coercion and modular encryption do not apply here. Byte splits are mapped
//! proportionally to row ranges using the file size and exact row count.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field as ArrowField};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use vortex::VortexSessionDefault;
use vortex::array::VortexSessionExecute;
use vortex::array::buffer::BufferHandle;
use vortex::array::stream::ArrayStream;
use vortex::arrow::ArrowSessionExt;
use vortex::buffer::{Alignment, ByteBuffer};
use vortex::dtype::extension::Matcher;
use vortex::dtype::{DType, DecimalDType, Nullability, PType};
use vortex::error::{VortexResult, vortex_err};
use vortex::expr::{
    Expression, and, eq, get_item, gt, gt_eq, is_not_null, is_null, like, lit, lt, lt_eq, not,
    not_eq, not_like, or, or_collect, root, select,
};
use vortex::extension::datetime::{AnyTemporal, TimeUnit};
use vortex::file::OpenOptionsSessionExt;
use vortex::io::VortexReadAt;
use vortex::layout::scan::split_by::SplitBy;
use vortex::scalar::{DecimalValue, Scalar};
use vortex::scan::selection::Selection;
use vortex::session::VortexSession;

use super::DEFAULT_RANGE_FETCH_CONCURRENCY;
use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::arrow::record_batch_transformer::RecordBatchTransformerBuilder;
use crate::arrow::scan_metrics::CountingFileRead;
use crate::arrow::vortex_util::{VORTEX_SCHEMA_KEY, attach_field_id_metadata};
use crate::arrow::{convert_temporal_value, to_iceberg_error};
use crate::expr::{BoundPredicate, BoundReference, PredicateOperator};
use crate::io::{FileIO, FileRead};
use crate::metadata_columns::{
    RESERVED_COL_NAME_POS, RESERVED_FIELD_ID_FILE, RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    RESERVED_FIELD_ID_PARTITION, RESERVED_FIELD_ID_POS, RESERVED_FIELD_ID_ROW_ID,
    RESERVED_FIELD_ID_SPEC_ID, is_metadata_field,
};
use crate::scan::{ArrowRecordBatchStream, FileScanTask};
use crate::spec::{Datum, NestedField, PrimitiveLiteral, PrimitiveType, Schema, Type};
use crate::{Error, ErrorKind, Result};

/// Reads a [`FileScanTask`] pointing at a vortex data file and returns a
/// stream of arrow record batches matching the task's projected schema.
pub(super) async fn read_vortex_task(
    task: FileScanTask,
    file_io: &FileIO,
    batch_size: Option<usize>,
    delete_file_loader: &CachingDeleteFileLoader,
    bytes_read_counter: Arc<AtomicU64>,
) -> Result<ArrowRecordBatchStream> {
    // Start loading the delete files concurrently with opening the data file.
    let delete_filter_rx = delete_file_loader.load_deletes(task.deletes(), task.schema_ref());

    // The session captures the current tokio runtime handle at construction
    // time, so it is created here, inside the runtime driving the scan.
    let session = VortexSession::default();

    let (vortex_file, file_size) = open_vortex_file(
        task.data_file_path(),
        task.file_size_in_bytes(),
        task.key_metadata(),
        file_io,
        bytes_read_counter,
        &session,
    )
    .await?;
    let row_range = if task.start() == 0 && task.length() == 0 {
        0..vortex_file.row_count()
    } else {
        byte_range_to_rows(
            task.start(),
            task.length(),
            file_size,
            vortex_file.row_count(),
        )
    };
    if row_range.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }
    let file_schema = match vortex_file.metadata_segment(VORTEX_SCHEMA_KEY) {
        Some(bytes) => Arc::new(serde_json::from_slice::<Schema>(bytes.as_slice())?),
        None => {
            let arrow_schema = Arc::new(
                session
                    .arrow()
                    .to_arrow_schema(vortex_file.dtype())
                    .map_err(to_iceberg_error)?,
            );
            let arrow_schema = if let Some(mapping) = task.name_mapping() {
                super::apply_name_mapping_to_arrow_schema(arrow_schema, mapping)?
            } else {
                super::add_fallback_field_ids_to_arrow_schema(&arrow_schema)
            };
            Arc::new(Schema::try_from(arrow_schema.as_ref())?)
        }
    };
    let project_pos = task.project_field_ids().contains(&RESERVED_FIELD_ID_POS);
    let project_row_id = task.project_field_ids().contains(&RESERVED_FIELD_ID_ROW_ID);
    let need_pos = project_pos || (project_row_id && task.first_row_id().is_some());
    let projected_names: Vec<_> = file_schema
        .as_struct()
        .fields()
        .iter()
        .filter(|field| {
            task.project_field_ids().iter().any(|id| {
                *id == field.id
                    || file_schema
                        .name_by_field_id(*id)
                        .is_some_and(|name| name.starts_with(&format!("{}.", field.name)))
            })
        })
        .filter(|field| {
            !is_metadata_field(field.id)
                || (field.id == RESERVED_FIELD_ID_ROW_ID && task.first_row_id().is_some())
                || (field.id == RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
                    && task.first_row_id().is_some())
        })
        .map(|f| f.name.clone())
        .collect();

    let delete_filter = delete_filter_rx.await.unwrap()?;
    let delete_predicate = delete_filter.build_equality_delete_predicate(&task).await?;

    // In addition to the optional predicate supplied in the `FileScanTask`,
    // we also have an optional predicate resulting from equality delete files.
    // If both are present, we logical-AND them together to form a single filter
    // predicate that gets pushed down into the vortex scan.
    let final_predicate = match (task.predicate(), delete_predicate) {
        (None, None) => None,
        (Some(predicate), None) => Some(predicate.clone()),
        (None, Some(predicate)) => Some(predicate),
        (Some(filter_predicate), Some(delete_predicate)) => {
            Some(filter_predicate.clone().and(delete_predicate))
        }
    };
    let filter = final_predicate
        .as_ref()
        .map(|predicate| {
            convert_predicate_to_vortex(predicate, &file_schema, vortex_file.dtype())?
                .bind(vortex_file.dtype())
                .map_err(to_iceberg_error)
        })
        .transpose()?;

    let mut projection = select(
        projected_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        root(),
    );
    // Keep the virtual position separate from user columns with the same name.
    let mut position_name = RESERVED_COL_NAME_POS.to_string();
    while projected_names.contains(&position_name) {
        position_name.push('_');
    }
    if need_pos {
        projection = vortex::expr::merge([
            projection,
            vortex::expr::pack(
                [(
                    position_name.as_str(),
                    vortex::expr::cast(
                        vortex::layout::layouts::row_idx::row_idx(),
                        DType::Primitive(PType::I64, Nullability::NonNullable),
                    ),
                )],
                Nullability::NonNullable,
            ),
        ]);
    }
    let mut scan_builder = vortex_file
        .scan()
        .map_err(to_iceberg_error)?
        .with_projection(
            projection
                .bind(vortex_file.dtype())
                .map_err(to_iceberg_error)?,
        )
        .with_some_filter(filter)
        .with_row_range(row_range);
    if let Some(batch_size) = batch_size {
        scan_builder = scan_builder.with_split_by(SplitBy::RowCount(batch_size));
    }

    // Positional deletes are applied by excluding the deleted row ordinals
    // from the scan. Vortex selections compose with the filter expression.
    if let Some(positional_deletes) = delete_filter.get_delete_vector(&task) {
        scan_builder = scan_builder.with_selection(Selection::ExcludeRoaring(
            positional_deletes.lock().unwrap().iter().collect(),
        ));
    }

    let array_stream = scan_builder.into_array_stream().map_err(to_iceberg_error)?;

    // Mirror the parquet pipeline: adapt file batches to the task schema.
    let mut record_batch_transformer_builder =
        RecordBatchTransformerBuilder::new(task.schema_ref(), task.project_field_ids());
    if task.project_field_ids().contains(&RESERVED_FIELD_ID_FILE) {
        record_batch_transformer_builder = record_batch_transformer_builder.with_constant(
            RESERVED_FIELD_ID_FILE,
            Datum::string(task.data_file_path().to_string()),
        );
    }
    if let (Some(partition_spec), Some(partition_data)) =
        (task.partition_spec().cloned(), task.partition().cloned())
    {
        record_batch_transformer_builder =
            record_batch_transformer_builder.with_partition(partition_spec, partition_data)?;
    }
    if task
        .project_field_ids()
        .contains(&RESERVED_FIELD_ID_SPEC_ID)
    {
        let spec = task
            .partition_spec()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Partition spec is missing"))?;
        record_batch_transformer_builder = record_batch_transformer_builder
            .with_constant(RESERVED_FIELD_ID_SPEC_ID, Datum::int(spec.spec_id()));
    }
    if project_pos {
        record_batch_transformer_builder =
            record_batch_transformer_builder.with_virtual_field(RESERVED_FIELD_ID_POS);
    }
    if project_row_id {
        record_batch_transformer_builder =
            record_batch_transformer_builder.with_virtual_field(RESERVED_FIELD_ID_ROW_ID);
    }
    if task
        .project_field_ids()
        .contains(&RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)
    {
        record_batch_transformer_builder = match (task.first_row_id(), task.data_sequence_number())
        {
            (None, _) => record_batch_transformer_builder
                .with_null_metadata_column(RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)?,
            (Some(_), Some(seq)) => {
                if file_schema
                    .field_by_id(RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)
                    .is_some()
                {
                    record_batch_transformer_builder.with_coalesced_last_updated_seq_column(
                        RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                        Datum::long(seq),
                    )
                } else {
                    record_batch_transformer_builder.with_constant(
                        RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                        Datum::long(seq),
                    )
                }
            }
            (Some(_), None) => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Data file {} has a first_row_id but no data sequence number",
                        task.data_file_path()
                    ),
                ));
            }
        };
    }
    if task
        .project_field_ids()
        .contains(&RESERVED_FIELD_ID_PARTITION)
        && let Some(unified_type) = task.unified_partition_type()
    {
        let (spec, data) = match (task.partition_spec(), task.partition()) {
            (Some(spec), Some(data)) => (Arc::clone(spec), data.clone()),
            _ if unified_type.fields().is_empty() => (
                Arc::new(crate::spec::PartitionSpec::unpartition_spec()),
                crate::spec::Struct::empty(),
            ),
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "cannot build _partition column: unified partition type has fields but the scan task is missing its partition spec or data",
                ));
            }
        };
        record_batch_transformer_builder =
            record_batch_transformer_builder.with_partition_constant(
                crate::arrow::build_partition_constant(unified_type, &spec, &data)?,
            );
    }
    let mut record_batch_transformer = record_batch_transformer_builder.build();

    let arrow_field = session
        .arrow()
        .to_arrow_field("", array_stream.dtype())
        .map_err(to_iceberg_error)?;
    let mut execution_ctx = session.create_execution_ctx();
    let first_row_id = task.first_row_id();
    let task_schema = task.schema_ref();

    let record_batch_stream = array_stream.map(move |chunk| {
        let chunk = chunk.map_err(to_iceberg_error)?;
        let arrow_array = session
            .arrow()
            .execute_arrow(chunk, Some(&arrow_field), &mut execution_ctx)
            .map_err(to_iceberg_error)?;
        let batch = RecordBatch::from(arrow_array.as_struct());
        let mut batch = attach_field_id_metadata(batch, &file_schema)?;
        if need_pos {
            let schema = batch.schema();
            let fields: Vec<_> = schema
                .fields()
                .iter()
                .map(|f| {
                    if f.name() == &position_name {
                        Arc::new(
                            ArrowField::new(RESERVED_COL_NAME_POS, DataType::Int64, false)
                                .with_metadata(std::collections::HashMap::from([(
                                    PARQUET_FIELD_ID_META_KEY.to_string(),
                                    RESERVED_FIELD_ID_POS.to_string(),
                                )])),
                        )
                    } else {
                        f.clone()
                    }
                })
                .collect();
            batch = RecordBatch::try_new(
                Arc::new(arrow_schema::Schema::new(fields)),
                batch.columns().to_vec(),
            )?;
        }
        if project_row_id {
            batch = super::row_lineage::synthesize_row_id_column(batch, first_row_id)?;
        }
        record_batch_transformer.process_record_batch(crate::arrow::evolve_nested_fields(
            batch,
            &task_schema,
            &session,
        )?)
    });

    Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
}

/// Open Vortex files through the same counted FileIO used by data and delete scans.
async fn open_vortex_file(
    path: &str,
    file_size: u64,
    key_metadata: Option<&[u8]>,
    file_io: &FileIO,
    bytes_read_counter: Arc<AtomicU64>,
    session: &VortexSession,
) -> Result<(vortex::file::VortexFile, u64)> {
    if key_metadata.is_some() {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Encrypted Vortex files are not supported",
        ));
    }
    let input_file = file_io.new_input(path)?;
    let file_size = if file_size > 0 {
        file_size
    } else {
        input_file.metadata().await?.size
    };
    let reader = CountingFileRead::new(input_file.reader().await?, bytes_read_counter);
    let source = Arc::new(FileReadVortexSource {
        reader: Arc::new(reader),
        size: file_size,
        uri: Arc::from(path),
    });
    let file = session
        .open_options()
        .with_file_size(file_size)
        .with_include_metadata(true)
        .open(source)
        .await
        .map_err(to_iceberg_error)?;
    Ok((file, file_size))
}

/// Read unfiltered batches for delete files without recursively loading deletes.
pub(crate) async fn vortex_to_batch_stream(
    path: &str,
    file_size: u64,
    key_metadata: Option<&[u8]>,
    file_io: &FileIO,
    bytes_read_counter: Arc<AtomicU64>,
) -> Result<ArrowRecordBatchStream> {
    let session = VortexSession::default();
    let (file, _) = open_vortex_file(
        path,
        file_size,
        key_metadata,
        file_io,
        bytes_read_counter,
        &session,
    )
    .await?;
    let schema = file
        .metadata_segment(VORTEX_SCHEMA_KEY)
        .map(|bytes| serde_json::from_slice::<Schema>(bytes.as_slice()))
        .transpose()?;
    let stream = file
        .scan()
        .map_err(to_iceberg_error)?
        .into_array_stream()
        .map_err(to_iceberg_error)?;
    let target_schema = schema
        .as_ref()
        .map(arrow_schema::Schema::try_from)
        .transpose()?
        .map(Arc::new);
    let field = session
        .arrow()
        .to_arrow_field("", stream.dtype())
        .map_err(to_iceberg_error)?;
    let mut ctx = session.create_execution_ctx();
    Ok(Box::pin(stream.map(move |chunk| {
        let array = session
            .arrow()
            .execute_arrow(chunk.map_err(to_iceberg_error)?, Some(&field), &mut ctx)
            .map_err(to_iceberg_error)?;
        let batch = RecordBatch::from(array.as_struct());
        match &schema {
            Some(schema) => {
                let batch = attach_field_id_metadata(batch, schema)?;
                let batch = crate::arrow::evolve_nested_fields(batch, schema, &session)?;
                let target_schema = target_schema.as_ref().expect("schema converted above");
                let columns = batch
                    .columns()
                    .iter()
                    .zip(target_schema.fields())
                    .map(|(array, field)| arrow_cast::cast(array, field.data_type()))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                Ok(RecordBatch::try_new_with_options(
                    target_schema.clone(),
                    columns,
                    &arrow_array::RecordBatchOptions::new()
                        .with_row_count(Some(batch.num_rows()))
                        .with_match_field_names(false),
                )?)
            }
            None => Ok(batch),
        }
    })))
}

/// A [`VortexReadAt`] source backed by iceberg's [`FileRead`].
struct FileReadVortexSource {
    reader: Arc<dyn FileRead>,
    size: u64,
    uri: Arc<str>,
}

impl VortexReadAt for FileReadVortexSource {
    fn uri(&self) -> Option<&Arc<str>> {
        Some(&self.uri)
    }

    fn concurrency(&self) -> usize {
        DEFAULT_RANGE_FETCH_CONCURRENCY
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        futures::future::ready(Ok(self.size)).boxed()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let reader = Arc::clone(&self.reader);
        async move {
            let bytes = reader
                .read(offset..offset + length as u64)
                .await
                .map_err(|err| vortex_err!("Failed to read from iceberg storage: {err}"))?;
            if bytes.len() != length {
                return Err(vortex_err!(
                    "Short read from iceberg storage: expected {length} bytes, got {}",
                    bytes.len()
                ));
            }
            Ok(BufferHandle::new_host(
                ByteBuffer::from(bytes).aligned(alignment),
            ))
        }
        .boxed()
    }
}

/// Converts an iceberg [`BoundPredicate`] into a vortex filter [`Expression`].
///
/// Vortex applies filters exactly (not just for pruning), matching the row
/// level filtering guarantees of the parquet read path. Like the parquet read
/// path, predicate leaves referencing columns that do not exist in the file
/// evaluate to "keep all rows".
fn convert_predicate_to_vortex(
    predicate: &BoundPredicate,
    schema: &Schema,
    file_dtype: &DType,
) -> Result<Expression> {
    match predicate {
        BoundPredicate::AlwaysTrue => Ok(lit(true)),
        BoundPredicate::AlwaysFalse => Ok(lit(false)),
        BoundPredicate::And(expr) => {
            let [left, right] = expr.inputs();
            Ok(and(
                convert_predicate_to_vortex(left, schema, file_dtype)?,
                convert_predicate_to_vortex(right, schema, file_dtype)?,
            ))
        }
        BoundPredicate::Or(expr) => {
            let [left, right] = expr.inputs();
            Ok(or(
                convert_predicate_to_vortex(left, schema, file_dtype)?,
                convert_predicate_to_vortex(right, schema, file_dtype)?,
            ))
        }
        BoundPredicate::Not(expr) => {
            let [inner] = expr.inputs();
            Ok(not(convert_predicate_to_vortex(inner, schema, file_dtype)?))
        }
        BoundPredicate::Unary(expr) => {
            let Some(column) = column_expr(expr.term(), schema, file_dtype)? else {
                return Ok(lit(true));
            };
            match expr.op() {
                PredicateOperator::IsNull => Ok(is_null(column.expr)),
                PredicateOperator::NotNull => Ok(is_not_null(column.expr)),
                PredicateOperator::IsNan => nan_predicate(column.expr, expr.term().field(), true),
                PredicateOperator::NotNan => nan_predicate(column.expr, expr.term().field(), false),
                op => Err(unsupported_predicate(op)),
            }
        }
        BoundPredicate::Binary(expr) => {
            let Some(column) = column_expr(expr.term(), schema, file_dtype)? else {
                return Ok(lit(true));
            };
            match expr.op() {
                PredicateOperator::StartsWith => {
                    starts_with_predicate(column.expr, expr.literal(), false)
                }
                PredicateOperator::NotStartsWith => {
                    starts_with_predicate(column.expr, expr.literal(), true)
                }
                op => {
                    let literal = lit(datum_to_vortex_scalar(expr.literal(), &column.dtype)?);
                    match op {
                        PredicateOperator::LessThan => Ok(lt(column.expr, literal)),
                        PredicateOperator::LessThanOrEq => Ok(lt_eq(column.expr, literal)),
                        PredicateOperator::GreaterThan => Ok(gt(column.expr, literal)),
                        PredicateOperator::GreaterThanOrEq => Ok(gt_eq(column.expr, literal)),
                        PredicateOperator::Eq => Ok(eq(column.expr, literal)),
                        PredicateOperator::NotEq => Ok(not_eq(column.expr, literal)),
                        op => Err(unsupported_predicate(op)),
                    }
                }
            }
        }
        BoundPredicate::Set(expr) => {
            let Some(column) = column_expr(expr.term(), schema, file_dtype)? else {
                return Ok(lit(true));
            };
            let matches = expr
                .literals()
                .iter()
                .map(|datum| {
                    Ok(eq(
                        column.expr.clone(),
                        lit(datum_to_vortex_scalar(datum, &column.dtype)?),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            match expr.op() {
                PredicateOperator::In => Ok(or_collect(matches).unwrap_or_else(|| lit(false))),
                PredicateOperator::NotIn => Ok(match or_collect(matches) {
                    Some(any_match) => not(any_match),
                    None => lit(true),
                }),
                op => Err(unsupported_predicate(op)),
            }
        }
    }
}

fn unsupported_predicate(op: PredicateOperator) -> Error {
    Error::new(
        ErrorKind::FeatureUnsupported,
        format!("Predicate operator {op} is not yet supported for vortex data files"),
    )
}

/// A column reference resolved against the file: the vortex expression that
/// selects it, plus its dtype in the file.
struct FileColumn {
    expr: Expression,
    dtype: DType,
}

/// Resolves the field referenced by a bound predicate term to a vortex column
/// expression. Nested struct fields become chains of `get_item`.
///
/// Returns `None` when the column (or any struct along its path) does not
/// exist in the file, in which case the predicate leaf must evaluate to
/// "keep all rows", mirroring the parquet read path.
fn column_expr(
    reference: &BoundReference,
    schema: &Schema,
    file_dtype: &DType,
) -> Result<Option<FileColumn>> {
    // Walk the accessor positions through the table schema to build the
    // column's name path.
    let mut names: Vec<String> = Vec::new();
    let mut current_struct = Some(schema.as_struct());
    let Some(file_accessor) = schema.accessor_by_field_id(reference.field().id) else {
        return Ok(None);
    };
    let mut accessor = Some(file_accessor.as_ref());
    while let Some(acc) = accessor {
        let field = current_struct
            .and_then(|struct_type| struct_type.fields().get(acc.position()))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Bound reference {} does not resolve within the table schema",
                        reference.field().name
                    ),
                )
            })?;
        names.push(field.name.clone());
        accessor = acc.inner();
        current_struct = match field.field_type.as_ref() {
            Type::Struct(struct_type) => Some(struct_type),
            _ => None,
        };
    }

    // Resolve the same path in the file's dtype; vortex files are matched by
    // name.
    let mut dtype = file_dtype.clone();
    for name in &names {
        let Some(field_dtype) = dtype
            .as_struct_fields_opt()
            .and_then(|fields| fields.field(name))
        else {
            return Ok(None);
        };
        dtype = field_dtype;
    }

    let mut expr = names
        .iter()
        .fold(root(), |child, name| get_item(name.as_str(), child));
    // Promote the column instead of narrowing the literal: out-of-range literals
    // must remain valid filters after int/float/decimal schema evolution.
    let promoted = match (&dtype, reference.field().field_type.as_ref()) {
        (DType::Primitive(PType::I32, nullability), Type::Primitive(PrimitiveType::Long)) => {
            Some(DType::Primitive(PType::I64, *nullability))
        }
        (DType::Primitive(PType::F32, nullability), Type::Primitive(PrimitiveType::Double)) => {
            Some(DType::Primitive(PType::F64, *nullability))
        }
        (
            DType::Decimal(_, nullability),
            Type::Primitive(PrimitiveType::Decimal { precision, scale }),
        ) => Some(DType::Decimal(
            DecimalDType::new(u8::try_from(*precision)?, i8::try_from(*scale)?),
            *nullability,
        )),
        _ => None,
    };
    if let Some(promoted) = promoted
        && promoted != dtype
    {
        expr = vortex::expr::cast(expr, promoted.clone());
        dtype = promoted;
    }
    Ok(Some(FileColumn { expr, dtype }))
}

/// Builds a NaN check for a float column.
///
/// Vortex compares floats using IEEE-754 total ordering, in which every NaN
/// bit pattern sorts either above `+inf` (positive NaNs) or below `-inf`
/// (negative NaNs), so NaN checks are expressed as comparisons against the
/// infinities. Null values compare to null and are excluded by the filter,
/// matching the three valued logic of the parquet read path.
fn nan_predicate(column: Expression, field: &NestedField, is_nan: bool) -> Result<Expression> {
    let (pos_inf, neg_inf): (Scalar, Scalar) = match field.field_type.as_ref() {
        Type::Primitive(PrimitiveType::Float) => (f32::INFINITY.into(), f32::NEG_INFINITY.into()),
        Type::Primitive(PrimitiveType::Double) => (f64::INFINITY.into(), f64::NEG_INFINITY.into()),
        ty => {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "NaN predicate on non-float field {} of type {ty}",
                    field.name
                ),
            ));
        }
    };
    Ok(if is_nan {
        or(gt(column.clone(), lit(pos_inf)), lt(column, lit(neg_inf)))
    } else {
        and(
            lt_eq(column.clone(), lit(pos_inf)),
            gt_eq(column, lit(neg_inf)),
        )
    })
}

/// Builds a `LIKE` pattern matching values that start with the literal prefix,
/// escaping any SQL LIKE wildcards contained in it.
fn starts_with_predicate(column: Expression, prefix: &Datum, negated: bool) -> Result<Expression> {
    let PrimitiveLiteral::String(prefix) = prefix.literal() else {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "STARTS WITH predicates require a string literal",
        ));
    };
    let escaped = prefix
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    let pattern = format!("{escaped}%");
    Ok(if negated {
        not_like(column, lit(pattern))
    } else {
        like(column, lit(pattern))
    })
}

fn datum_to_vortex_scalar(datum: &Datum, file_dtype: &DType) -> Result<Scalar> {
    match (datum.data_type(), datum.literal()) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(v)) => Ok((*v).into()),
        (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => Ok((*v).into()),
        (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => Ok((*v).into()),
        (PrimitiveType::Float, PrimitiveLiteral::Float(v)) => Ok(v.0.into()),
        (PrimitiveType::Double, PrimitiveLiteral::Double(v)) => Ok(v.0.into()),
        (PrimitiveType::String, PrimitiveLiteral::String(v)) => Ok(v.as_str().into()),
        (PrimitiveType::Binary, PrimitiveLiteral::Binary(v)) => Ok(v.as_slice().into()),
        (PrimitiveType::Uuid, PrimitiveLiteral::UInt128(value)) => {
            let DType::Extension(ext) = file_dtype else {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!("Cannot compare UUID to Vortex column of type {file_dtype}"),
                ));
            };
            if !ext.is::<vortex::extension::uuid::Uuid>() {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!("Cannot compare UUID to Vortex extension {ext}"),
                ));
            }
            let storage = Scalar::fixed_size_list(
                DType::Primitive(PType::U8, Nullability::NonNullable),
                value.to_be_bytes().into_iter().map(Scalar::from).collect(),
                ext.storage_dtype().nullability(),
            );
            Ok(Scalar::extension_ref(ext.clone(), storage))
        }
        (PrimitiveType::Decimal { precision, scale }, PrimitiveLiteral::Int128(v)) => {
            let precision = u8::try_from(*precision).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Decimal precision {precision} out of range"),
                )
                .with_source(err)
            })?;
            let scale = i8::try_from(*scale).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Decimal scale {scale} out of range"),
                )
                .with_source(err)
            })?;
            Ok(Scalar::decimal(
                DecimalValue::I128(*v),
                DecimalDType::new(precision, scale),
                Nullability::NonNullable,
            ))
        }
        (PrimitiveType::Date, PrimitiveLiteral::Int(days)) => {
            temporal_scalar(file_dtype, i64::from(*days), TimeUnit::Days)
        }
        (PrimitiveType::Time, PrimitiveLiteral::Long(micros)) => {
            temporal_scalar(file_dtype, *micros, TimeUnit::Microseconds)
        }
        (PrimitiveType::Timestamp | PrimitiveType::Timestamptz, PrimitiveLiteral::Long(micros)) => {
            temporal_scalar(file_dtype, *micros, TimeUnit::Microseconds)
        }
        (
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs,
            PrimitiveLiteral::Long(nanos),
        ) => temporal_scalar(file_dtype, *nanos, TimeUnit::Nanoseconds),
        (ty, _) => Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!("Predicate literals of type {ty} are not yet supported for vortex data files"),
        )),
    }
}

/// Map byte boundaries using average row size, as in Iceberg Java. Using the
/// same floor operation at each boundary makes adjacent splits disjoint and
/// exhaustive. Widening the product avoids overflow and floating-point rounding.
fn byte_range_to_rows(start: u64, length: u64, file_size: u64, rows: u64) -> std::ops::Range<u64> {
    let row_at = |offset: u64| {
        if offset >= file_size {
            rows
        } else {
            ((u128::from(offset) * u128::from(rows)) / u128::from(file_size)) as u64
        }
    };
    row_at(start)..row_at(start.saturating_add(length))
}

/// Builds a literal scalar for comparing against a temporal column.
///
/// When the file stores the column as a vortex temporal extension type, the
/// literal is converted to the column's exact extension dtype (unit and
/// timezone), since vortex only compares extension values against matching
/// extension dtypes. Plain primitive columns are compared against the raw
/// iceberg value.
fn temporal_scalar(file_dtype: &DType, value: i64, value_unit: TimeUnit) -> Result<Scalar> {
    match file_dtype {
        DType::Extension(ext) => {
            let Some(metadata) = AnyTemporal::try_match(ext) else {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!(
                        "Cannot compare a temporal predicate literal to a column of type {file_dtype}"
                    ),
                ));
            };
            let converted = convert_temporal_value(value, value_unit, metadata.time_unit())?;
            let nullability = ext.storage_dtype().nullability();
            let storage = match ext.storage_dtype() {
                DType::Primitive(PType::I32, _) => Scalar::primitive(
                    i32::try_from(converted).map_err(|err| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Temporal value {converted} overflows the column storage type"),
                        )
                        .with_source(err)
                    })?,
                    nullability,
                ),
                DType::Primitive(PType::I64, _) => Scalar::primitive(converted, nullability),
                dtype => {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        format!("Unsupported temporal storage type {dtype}"),
                    ));
                }
            };
            Ok(Scalar::extension_ref(ext.clone(), storage))
        }
        DType::Primitive(PType::I32, _) => Ok(Scalar::primitive(
            i32::try_from(value).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Temporal value {value} overflows the column storage type"),
                )
                .with_source(err)
            })?,
            Nullability::NonNullable,
        )),
        DType::Primitive(PType::I64, _) => Ok(Scalar::primitive(value, Nullability::NonNullable)),
        dtype => Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!("Cannot compare a temporal predicate literal to a column of type {dtype}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        Array, Date32Array, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray,
        StructArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use futures::TryStreamExt;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use tempfile::TempDir;
    use vortex::VortexSessionDefault;
    use vortex::session::VortexSession;

    use crate::arrow::ArrowReaderBuilder;
    use crate::expr::{Bind, Reference};
    use crate::io::FileIO;
    use crate::runtime::Runtime;
    use crate::scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream};
    use crate::spec::{
        DataContentType, DataFileFormat, Datum, NestedField, PrimitiveType, Schema, SchemaRef, Type,
    };
    use crate::writer::file_writer::{FileWriter, FileWriterBuilder, VortexWriterBuilder};

    fn test_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(3, "score", Type::Primitive(PrimitiveType::Double))
                        .into(),
                    NestedField::optional(4, "ts", Type::Primitive(PrimitiveType::Timestamp))
                        .into(),
                    NestedField::optional(5, "date", Type::Primitive(PrimitiveType::Date)).into(),
                    NestedField::optional(
                        6,
                        "info",
                        Type::Struct(crate::spec::StructType::new(vec![
                            NestedField::optional(
                                7,
                                "points",
                                Type::Primitive(PrimitiveType::Long),
                            )
                            .into(),
                        ])),
                    )
                    .into(),
                    NestedField::optional(
                        8,
                        "price",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 9,
                            scale: 2,
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        )
    }

    fn test_arrow_schema() -> Arc<ArrowSchema> {
        let with_field_id = |field: Field, id: i32| {
            field.with_metadata(std::collections::HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )]))
        };
        Arc::new(ArrowSchema::new(vec![
            with_field_id(Field::new("id", DataType::Int64, false), 1),
            with_field_id(Field::new("name", DataType::Utf8, true), 2),
            with_field_id(Field::new("score", DataType::Float64, true), 3),
            with_field_id(
                Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
                4,
            ),
            with_field_id(Field::new("date", DataType::Date32, true), 5),
            with_field_id(
                Field::new("info", DataType::Struct(info_struct_fields()), true),
                6,
            ),
            with_field_id(Field::new("price", DataType::Decimal128(9, 2), true), 8),
        ]))
    }

    fn info_struct_fields() -> arrow_schema::Fields {
        vec![Arc::new(
            Field::new("points", DataType::Int64, true).with_metadata(
                std::collections::HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "7".to_string(),
                )]),
            ),
        )]
        .into()
    }

    fn test_batch(ids: Vec<i64>, names: Vec<Option<&str>>) -> RecordBatch {
        let scores = ids
            .iter()
            .map(|id| match id % 3 {
                0 => None,
                1 => Some(*id as f64 * 1.5),
                _ => Some(f64::NAN),
            })
            .collect::<Vec<_>>();
        let timestamps = ids
            .iter()
            .map(|id| Some(id * 1_000_000))
            .collect::<Vec<_>>();
        let dates = ids
            .iter()
            .map(|id| Some(*id as i32 * 10))
            .collect::<Vec<_>>();
        let points = Int64Array::from(ids.iter().map(|id| Some(id * 100)).collect::<Vec<_>>());
        let info = StructArray::new(info_struct_fields(), vec![Arc::new(points)], None);
        let prices = Decimal128Array::from(
            ids.iter()
                .map(|id| Some(i128::from(*id) * 250))
                .collect::<Vec<_>>(),
        )
        .with_precision_and_scale(9, 2)
        .unwrap();
        RecordBatch::try_new(test_arrow_schema(), vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(scores)),
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
            Arc::new(Date32Array::from(dates)),
            Arc::new(info),
            Arc::new(prices),
        ])
        .unwrap()
    }

    async fn write_test_file(file_path: &str, file_io: &FileIO) -> crate::spec::DataFile {
        let output = file_io.new_output(file_path).unwrap();
        let mut writer = VortexWriterBuilder::new(test_schema(), VortexSession::default())
            .build(output)
            .await
            .unwrap();
        writer
            .write(&test_batch(vec![1, 2, 3], vec![Some("a"), Some("b"), None]))
            .await
            .unwrap();
        writer
            .write(&test_batch(vec![4, 5], vec![Some("d"), Some("e")]))
            .await
            .unwrap();
        let mut builders = writer.close().await.unwrap();
        assert_eq!(builders.len(), 1);
        builders
            .pop()
            .unwrap()
            .partition_spec_id(0)
            .build()
            .unwrap()
    }

    fn test_scan_task(file_path: &str, schema: SchemaRef, file_size: u64) -> FileScanTask {
        test_scan_task_filtered(file_path, schema, file_size, None, vec![])
    }

    fn test_scan_task_filtered(
        file_path: &str,
        schema: SchemaRef,
        file_size: u64,
        predicate: Option<crate::expr::BoundPredicate>,
        deletes: Vec<FileScanTaskDeleteFile>,
    ) -> FileScanTask {
        FileScanTask::builder()
            .with_file_size_in_bytes(file_size)
            .with_start(0)
            .with_length(file_size)
            .with_data_file_path(file_path.to_string())
            .with_data_file_format(DataFileFormat::Vortex)
            .with_schema(schema.clone())
            .with_project_field_ids(schema.as_struct().fields().iter().map(|f| f.id).collect())
            .with_case_sensitive(false)
            .with_predicate(predicate)
            .with_deletes(deletes)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_vortex_roundtrip() {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/data.vortex", tmp_dir.path().to_str().unwrap());
        let file_io = FileIO::new_with_fs();

        let data_file = write_test_file(&file_path, &file_io).await;
        assert_eq!(data_file.file_format(), DataFileFormat::Vortex);
        assert_eq!(data_file.record_count(), 5);
        assert_eq!(data_file.value_counts()[&1], 5);
        assert_eq!(data_file.null_value_counts()[&1], 0);
        assert_eq!(data_file.null_value_counts()[&2], 1);
        assert_eq!(data_file.nan_value_counts()[&3], 2);

        // Min/max bounds computed from the written batches. NaN values are
        // excluded from float bounds; nested fields carry no bounds.
        assert_eq!(data_file.lower_bounds()[&1], Datum::long(1));
        assert_eq!(data_file.upper_bounds()[&1], Datum::long(5));
        assert_eq!(data_file.lower_bounds()[&2], Datum::string("a"));
        assert_eq!(data_file.upper_bounds()[&2], Datum::string("e"));
        assert_eq!(data_file.lower_bounds()[&3], Datum::double(1.5));
        assert_eq!(data_file.upper_bounds()[&3], Datum::double(6.0));
        assert_eq!(
            data_file.lower_bounds()[&4],
            Datum::timestamp_micros(1_000_000)
        );
        assert_eq!(
            data_file.upper_bounds()[&4],
            Datum::timestamp_micros(5_000_000)
        );
        assert_eq!(data_file.lower_bounds()[&5], Datum::date(10));
        assert_eq!(data_file.upper_bounds()[&5], Datum::date(50));
        assert_eq!(
            data_file.lower_bounds()[&8].literal(),
            &crate::spec::PrimitiveLiteral::Int128(250)
        );
        assert_eq!(
            data_file.upper_bounds()[&8].literal(),
            &crate::spec::PrimitiveLiteral::Int128(1250)
        );
        assert!(!data_file.lower_bounds().contains_key(&6));
        assert!(!data_file.lower_bounds().contains_key(&7));
        assert!(!data_file.upper_bounds().contains_key(&7));
        assert!(!data_file.value_counts().contains_key(&7));

        let file_size = std::fs::metadata(&file_path).unwrap().len();
        assert_eq!(data_file.file_size_in_bytes(), file_size);

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        let task = test_scan_task(&file_path, test_schema(), file_size);
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result = reader.read(tasks).unwrap();
        let batches: Vec<RecordBatch> = result.stream().try_collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 5);

        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);

        let names: Vec<Option<String>> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("name")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|name| name.map(|s| s.to_string()))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(names, vec![
            Some("a".to_string()),
            Some("b".to_string()),
            None,
            Some("d".to_string()),
            Some("e".to_string())
        ]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_predicate() {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/data.vortex", tmp_dir.path().to_str().unwrap());
        let file_io = FileIO::new_with_fs();
        write_test_file(&file_path, &file_io).await;
        let file_size = std::fs::metadata(&file_path).unwrap().len();

        let schema = test_schema();
        let predicate = Reference::new("id")
            .greater_than(Datum::long(3))
            .bind(schema.clone(), true)
            .unwrap();
        let task = test_scan_task_filtered(&file_path, schema, file_size, Some(predicate), vec![]);

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![4, 5]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_evolved_schema() {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/data.vortex", tmp_dir.path().to_str().unwrap());
        let file_io = FileIO::new_with_fs();
        write_test_file(&file_path, &file_io).await;
        let file_size = std::fs::metadata(&file_path).unwrap().len();

        // The table schema gained a column after the file was written; it must
        // be filled with nulls when reading old files.
        let evolved_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(99, "extra", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        let task = test_scan_task(&file_path, evolved_schema, file_size);
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 5);
        for batch in &batches {
            assert_eq!(batch.num_columns(), 2);
            let extra = batch.column_by_name("extra").unwrap();
            assert_eq!(extra.null_count(), extra.len());
        }
    }

    const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: u64 = 2147483546;
    const FIELD_ID_POSITIONAL_DELETE_POS: u64 = 2147483545;

    fn write_positional_delete_file(path: &str, data_file_path: &str, positions: &[i64]) {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(
                std::collections::HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    FIELD_ID_POSITIONAL_DELETE_FILE_PATH.to_string(),
                )]),
            ),
            Field::new("pos", DataType::Int64, false).with_metadata(
                std::collections::HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    FIELD_ID_POSITIONAL_DELETE_POS.to_string(),
                )]),
            ),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(StringArray::from(vec![data_file_path; positions.len()])),
            Arc::new(Int64Array::from(positions.to_vec())),
        ])
        .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_equality_delete_file(path: &str, ids_to_delete: &[i64]) {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(
                std::collections::HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "1".to_string(),
                )]),
            ),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(
            ids_to_delete.to_vec(),
        ))])
        .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn delete_file_entry(path: &str, file_type: DataContentType) -> FileScanTaskDeleteFile {
        let equality_ids = (file_type == DataContentType::EqualityDeletes).then(|| vec![1]);
        FileScanTaskDeleteFile::builder()
            .with_file_path(path.to_string())
            .with_file_size_in_bytes(std::fs::metadata(path).unwrap().len())
            .with_file_type(file_type)
            .with_file_format(DataFileFormat::Parquet)
            .with_partition_spec_id(0)
            .with_equality_ids(equality_ids)
            .build()
    }

    async fn collect_ids(reader: &crate::arrow::ArrowReader, task: FileScanTask) -> Vec<i64> {
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .clone()
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[tokio::test]
    async fn test_vortex_read_with_positional_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/data.vortex", tmp_dir.path().to_str().unwrap());
        let delete_path = format!("{}/pos-del.parquet", tmp_dir.path().to_str().unwrap());
        let file_io = FileIO::new_with_fs();
        write_test_file(&file_path, &file_io).await;
        let file_size = std::fs::metadata(&file_path).unwrap().len();

        // Delete row ordinals 0 and 3 (ids 1 and 4).
        write_positional_delete_file(&delete_path, &file_path, &[0, 3]);

        let task = test_scan_task_filtered(&file_path, test_schema(), file_size, None, vec![
            delete_file_entry(&delete_path, DataContentType::PositionDeletes),
        ]);

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        assert_eq!(collect_ids(&reader, task).await, vec![2, 3, 5]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_equality_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/data.vortex", tmp_dir.path().to_str().unwrap());
        let delete_path = format!("{}/eq-del.parquet", tmp_dir.path().to_str().unwrap());
        let file_io = FileIO::new_with_fs();
        write_test_file(&file_path, &file_io).await;
        let file_size = std::fs::metadata(&file_path).unwrap().len();

        write_equality_delete_file(&delete_path, &[2, 5]);

        let task = test_scan_task_filtered(&file_path, test_schema(), file_size, None, vec![
            delete_file_entry(&delete_path, DataContentType::EqualityDeletes),
        ]);

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        assert_eq!(collect_ids(&reader, task).await, vec![1, 3, 4]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_deletes_and_predicate() {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/data.vortex", tmp_dir.path().to_str().unwrap());
        let pos_delete_path = format!("{}/pos-del.parquet", tmp_dir.path().to_str().unwrap());
        let eq_delete_path = format!("{}/eq-del.parquet", tmp_dir.path().to_str().unwrap());
        let file_io = FileIO::new_with_fs();
        write_test_file(&file_path, &file_io).await;
        let file_size = std::fs::metadata(&file_path).unwrap().len();

        // Positional delete of ordinal 4 (id 5), equality delete of id 3, and
        // a predicate id > 1: expect [2, 4].
        write_positional_delete_file(&pos_delete_path, &file_path, &[4]);
        write_equality_delete_file(&eq_delete_path, &[3]);

        let schema = test_schema();
        let task = test_scan_task_filtered(
            &file_path,
            schema.clone(),
            file_size,
            Some(
                Reference::new("id")
                    .greater_than(Datum::long(1))
                    .bind(schema.clone(), true)
                    .unwrap(),
            ),
            vec![
                delete_file_entry(&pos_delete_path, DataContentType::PositionDeletes),
                delete_file_entry(&eq_delete_path, DataContentType::EqualityDeletes),
            ],
        );

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        assert_eq!(collect_ids(&reader, task).await, vec![2, 4]);
    }

    async fn ids_matching(predicate: crate::expr::Predicate) -> Vec<i64> {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/data.vortex", tmp_dir.path().to_str().unwrap());
        let file_io = FileIO::new_with_fs();
        write_test_file(&file_path, &file_io).await;
        let file_size = std::fs::metadata(&file_path).unwrap().len();

        let schema = test_schema();
        let task = test_scan_task_filtered(
            &file_path,
            schema.clone(),
            file_size,
            Some(predicate.bind(schema.clone(), true).unwrap()),
            vec![],
        );

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        collect_ids(&reader, task).await
    }

    #[tokio::test]
    async fn test_vortex_read_with_timestamp_predicate() {
        // ts = id seconds; ts >= 3s selects ids 3, 4, 5.
        let ids = ids_matching(
            Reference::new("ts").greater_than_or_equal_to(Datum::timestamp_micros(3_000_000)),
        )
        .await;
        assert_eq!(ids, vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_date_predicate() {
        // date = id * 10 days; date > 20 days selects ids 3, 4, 5.
        let ids = ids_matching(Reference::new("date").greater_than(Datum::date(20))).await;
        assert_eq!(ids, vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_decimal_predicate() {
        // price = id * 2.50; price > 5.00 selects ids 3, 4, 5.
        let ids = ids_matching(
            Reference::new("price").greater_than(Datum::decimal_from_str("5.00").unwrap()),
        )
        .await;
        assert_eq!(ids, vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_in_predicate() {
        let ids = ids_matching(Reference::new("id").is_in([
            Datum::long(2),
            Datum::long(4),
            Datum::long(7),
        ]))
        .await;
        assert_eq!(ids, vec![2, 4]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_starts_with_predicate() {
        // names are [a, b, null, d, e].
        let ids = ids_matching(Reference::new("name").starts_with(Datum::string("d"))).await;
        assert_eq!(ids, vec![4]);

        // Rows with a null name are excluded by NOT STARTS WITH.
        let ids = ids_matching(Reference::new("name").not_starts_with(Datum::string("d"))).await;
        assert_eq!(ids, vec![1, 2, 5]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_nan_predicates() {
        // scores are [1.5, NaN, null, 6.0, NaN].
        let ids = ids_matching(Reference::new("score").is_nan()).await;
        assert_eq!(ids, vec![2, 5]);

        // Rows with a null score are excluded by NOT NAN.
        let ids = ids_matching(Reference::new("score").is_not_nan()).await;
        assert_eq!(ids, vec![1, 4]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_nested_field_predicate() {
        // info.points = id * 100; points > 300 selects ids 4, 5.
        let ids = ids_matching(Reference::new("info.points").greater_than(Datum::long(300))).await;
        assert_eq!(ids, vec![4, 5]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_predicate_on_missing_column() {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/data.vortex", tmp_dir.path().to_str().unwrap());
        let file_io = FileIO::new_with_fs();
        write_test_file(&file_path, &file_io).await;
        let file_size = std::fs::metadata(&file_path).unwrap().len();

        // "extra" was added to the table schema after the file was written.
        // Following the parquet read path, predicate leaves on columns missing
        // from the file keep all rows, so only the id predicate takes effect.
        let evolved_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(99, "extra", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let predicate = Reference::new("extra")
            .equal_to(Datum::string("x"))
            .and(Reference::new("id").greater_than(Datum::long(3)));

        let task = test_scan_task_filtered(
            &file_path,
            evolved_schema.clone(),
            file_size,
            Some(predicate.bind(evolved_schema.clone(), true).unwrap()),
            vec![],
        );

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        assert_eq!(collect_ids(&reader, task).await, vec![4, 5]);
    }
    async fn scan_batches(file_io: FileIO, task: FileScanTask) -> Vec<RecordBatch> {
        ArrowReaderBuilder::new(file_io, Runtime::current())
            .with_batch_size(2)
            .build()
            .read(Box::pin(futures::stream::iter([Ok(task)])))
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_vortex_metadata_after_filter_and_deletes() {
        use crate::metadata_columns::*;
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("data.vortex").to_str().unwrap().to_string();
        let delete_path = tmp
            .path()
            .join("delete.parquet")
            .to_str()
            .unwrap()
            .to_string();
        let io = FileIO::new_with_fs();
        let file = write_test_file(&path, &io).await;
        write_positional_delete_file(&delete_path, &path, &[3]);
        let schema = test_schema();
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(file.file_size_in_bytes())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(path.clone())
            .with_data_file_format(DataFileFormat::Vortex)
            .with_schema(schema.clone())
            .with_case_sensitive(true)
            .with_project_field_ids(vec![
                RESERVED_FIELD_ID_POS,
                RESERVED_FIELD_ID_ROW_ID,
                RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                RESERVED_FIELD_ID_FILE,
                RESERVED_FIELD_ID_SPEC_ID,
            ])
            .with_partition_spec(Some(Arc::new(
                crate::spec::PartitionSpec::unpartition_spec(),
            )))
            .with_first_row_id(Some(100))
            .with_data_sequence_number(Some(7))
            .with_predicate(Some(
                Reference::new("id")
                    .greater_than(Datum::long(1))
                    .bind(schema, true)
                    .unwrap(),
            ))
            .with_deletes(vec![delete_file_entry(
                &delete_path,
                DataContentType::PositionDeletes,
            )])
            .build()
            .unwrap();
        let batches = scan_batches(io, task).await;
        let longs = |name: &str| -> Vec<i64> {
            batches
                .iter()
                .flat_map(|b| {
                    arrow_cast::cast(b.column_by_name(name).unwrap(), &DataType::Int64)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect()
        };
        assert_eq!(longs(RESERVED_COL_NAME_POS), vec![1, 2, 4]);
        assert_eq!(longs(RESERVED_COL_NAME_ROW_ID), vec![101, 102, 104]);
        assert_eq!(longs(RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER), vec![
            7, 7, 7
        ]);
        for batch in batches {
            assert_eq!(batch.num_columns(), 5);
            let paths = arrow_cast::cast(
                batch.column_by_name(RESERVED_COL_NAME_FILE).unwrap(),
                &DataType::Utf8,
            )
            .unwrap();
            let paths = paths.as_any().downcast_ref::<StringArray>().unwrap();
            assert!(paths.iter().all(|v| v == Some(path.as_str())));
        }
    }

    #[tokio::test]
    async fn test_vortex_renamed_nested_fields_and_reused_name() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("data.vortex").to_str().unwrap().to_string();
        let io = FileIO::new_with_fs();
        let file = write_test_file(&path, &io).await;
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "renamed_id", Type::Primitive(PrimitiveType::Long))
                        .into(),
                    NestedField::optional(20, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(
                        6,
                        "renamed_info",
                        Type::Struct(crate::spec::StructType::new(vec![
                            NestedField::optional(
                                7,
                                "renamed_points",
                                Type::Primitive(PrimitiveType::Long),
                            )
                            .into(),
                            NestedField::optional(
                                21,
                                "added",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let predicate = Reference::new("renamed_info.renamed_points")
            .greater_than(Datum::long(200))
            .bind(schema.clone(), true)
            .unwrap();
        let task = test_scan_task_filtered(
            &path,
            schema,
            file.file_size_in_bytes(),
            Some(predicate),
            vec![],
        );
        let batches = scan_batches(io, task).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("renamed_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![3, 4, 5]);
        let points: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let info = b
                    .column_by_name("renamed_info")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                info.column_by_name("renamed_points")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .map(Option::unwrap)
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(points, vec![300, 400, 500]);
        for b in batches {
            assert_eq!(
                b.column_by_name("id").unwrap().logical_null_count(),
                b.num_rows()
            );
            let info = b
                .column_by_name("renamed_info")
                .unwrap()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert!(info.column_by_name("renamed_points").is_some());
            assert_eq!(
                info.column_by_name("added").unwrap().logical_null_count(),
                b.num_rows()
            );
        }
    }

    #[tokio::test]
    async fn test_vortex_empty_projection_and_null_lineage() {
        use crate::metadata_columns::*;
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("data.vortex").to_str().unwrap().to_string();
        let io = FileIO::new_with_fs();
        let file = write_test_file(&path, &io).await;
        for ids in [vec![], vec![
            RESERVED_FIELD_ID_ROW_ID,
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        ]] {
            let task = FileScanTask::builder()
                .with_file_size_in_bytes(file.file_size_in_bytes())
                .with_start(0)
                .with_length(0)
                .with_data_file_path(path.clone())
                .with_data_file_format(DataFileFormat::Vortex)
                .with_schema(test_schema())
                .with_project_field_ids(ids.clone())
                .with_case_sensitive(true)
                .build()
                .unwrap();
            let batches = scan_batches(io.clone(), task).await;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
            for batch in batches {
                assert_eq!(batch.num_columns(), ids.len());
                for col in batch.columns() {
                    assert_eq!(col.logical_null_count(), batch.num_rows());
                }
            }
        }
    }

    #[tokio::test]
    async fn test_vortex_delete_files() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("data.vortex").to_str().unwrap().to_string();
        let io = FileIO::new_with_fs();
        let file = write_test_file(&path, &io).await;
        let eq_path = tmp.path().join("eq.vortex").to_str().unwrap().to_string();
        let pos_path = tmp.path().join("pos.vortex").to_str().unwrap().to_string();
        let eq_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );
        let pos_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        2147483546,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(2147483545, "pos", Type::Primitive(PrimitiveType::Long))
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let mut deletes = vec![];
        for (path, schema, columns, content) in [
            (
                &eq_path,
                eq_schema,
                vec![Arc::new(Int64Array::from(vec![2])) as arrow_array::ArrayRef],
                DataContentType::EqualityDeletes,
            ),
            (
                &pos_path,
                pos_schema,
                vec![
                    Arc::new(StringArray::from(vec![path.as_str()])) as arrow_array::ArrayRef,
                    Arc::new(Int64Array::from(vec![3])),
                ],
                DataContentType::PositionDeletes,
            ),
        ] {
            let batch =
                RecordBatch::try_new(Arc::new(schema.as_ref().try_into().unwrap()), columns)
                    .unwrap();
            let mut writer = VortexWriterBuilder::new(schema, VortexSession::default())
                .build(io.new_output(path).unwrap())
                .await
                .unwrap();
            writer.write(&batch).await.unwrap();
            writer.close().await.unwrap();
            let mut entry = delete_file_entry(path, content);
            entry.file_format = DataFileFormat::Vortex;
            deletes.push(entry);
        }
        let task = test_scan_task_filtered(
            &path,
            test_schema(),
            file.file_size_in_bytes(),
            None,
            deletes,
        );
        let reader = ArrowReaderBuilder::new(io, Runtime::current()).build();
        assert_eq!(collect_ids(&reader, task).await, vec![1, 3, 5]);
    }
    async fn write_batch_file(
        path: &str,
        io: &FileIO,
        schema: SchemaRef,
        batch: &RecordBatch,
    ) -> crate::spec::DataFile {
        let mut writer = VortexWriterBuilder::new(schema, VortexSession::default())
            .build(io.new_output(path).unwrap())
            .await
            .unwrap();
        writer.write(batch).await.unwrap();
        writer
            .close()
            .await
            .unwrap()
            .pop()
            .unwrap()
            .partition_spec_id(0)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_vortex_list_roundtrip() {
        use arrow_array::ListArray;
        use arrow_array::types::Int64Type;
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("list.vortex").to_str().unwrap().to_string();
        let io = FileIO::new_with_fs();
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        3,
                        "items",
                        Type::List(crate::spec::ListType::new(Arc::new(NestedField::optional(
                            4,
                            "element",
                            Type::Primitive(PrimitiveType::Long),
                        )))),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let list = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(1), None, Some(3)]),
            Some(vec![]),
            None,
        ]);
        let arrow_schema: ArrowSchema = schema.as_ref().try_into().unwrap();
        let batch = RecordBatch::try_new_with_options(
            Arc::new(arrow_schema),
            vec![Arc::new(list)],
            &arrow_array::RecordBatchOptions::new().with_match_field_names(false),
        )
        .unwrap();
        let file = write_batch_file(&path, &io, schema.clone(), &batch).await;
        assert!(!file.value_counts().contains_key(&4));
        assert!(!file.null_value_counts().contains_key(&4));
        assert!(!file.lower_bounds().contains_key(&4));
        assert!(!file.upper_bounds().contains_key(&4));
        let batches =
            scan_batches(io, test_scan_task(&path, schema, file.file_size_in_bytes())).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }

    #[tokio::test]
    async fn test_vortex_map_roundtrip() {
        use arrow_array::builder::{Int64Builder, MapBuilder, StringBuilder};
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("map.vortex").to_str().unwrap().to_string();
        let io = FileIO::new_with_fs();
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        1,
                        "map",
                        Type::Map(crate::spec::MapType::new(
                            Arc::new(NestedField::required(
                                2,
                                "key",
                                Type::Primitive(PrimitiveType::String),
                            )),
                            Arc::new(NestedField::optional(
                                3,
                                "value",
                                Type::Primitive(PrimitiveType::Long),
                            )),
                        )),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        builder.keys().append_value("a");
        builder.values().append_value(42);
        builder.keys().append_value("b");
        builder.values().append_null();
        builder.append(true).unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        let values = Arc::new(builder.finish());
        let arrow_schema: ArrowSchema = schema.as_ref().try_into().unwrap();
        let batch = RecordBatch::try_new_with_options(
            Arc::new(arrow_schema),
            vec![values],
            &arrow_array::RecordBatchOptions::new().with_match_field_names(false),
        )
        .unwrap();
        let file = write_batch_file(&path, &io, schema.clone(), &batch).await;
        assert!(!file.value_counts().contains_key(&3));
        assert!(!file.null_value_counts().contains_key(&3));
        assert!(!file.lower_bounds().contains_key(&3));
        let batches =
            scan_batches(io, test_scan_task(&path, schema, file.file_size_in_bytes())).await;
        let combined =
            arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
        let expected = arrow_cast::cast(batch.column(0), combined.column(0).data_type()).unwrap();
        assert_eq!(combined.column(0).to_data(), expected.to_data());
    }

    #[tokio::test]
    async fn test_vortex_variant_roundtrip() {
        use arrow_array::BinaryArray;
        use vortex::array::VortexSessionExecute;
        use vortex::arrow::ArrowSessionExt;
        use vortex::dtype::{DType, Nullability, PType};
        let tmp = TempDir::new().unwrap();
        let io = FileIO::new_with_fs();
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "variant", Type::Variant(crate::spec::VariantType))
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let fields: arrow_schema::Fields = vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("typed_value", DataType::Int64, true),
        ]
        .into();
        let storage = Arc::new(StructArray::new(
            fields.clone(),
            vec![
                Arc::new(BinaryArray::from_vec(vec![&[1, 0, 0]; 3])),
                Arc::new(Int64Array::from(vec![Some(42), None, Some(-7)])),
            ],
            Some(arrow_buffer::NullBuffer::from(vec![true, false, true])),
        ));
        let field = Field::new("variant", DataType::Struct(fields), true)
            .with_extension_type(crate::arrow::schema::VariantExtensionType);
        let mut batch =
            RecordBatch::try_new(Arc::new(ArrowSchema::new(vec![field])), vec![storage]).unwrap();
        // First write shredded input, then rewrite the serialized output.
        for name in ["shredded", "serialized"] {
            let path = tmp
                .path()
                .join(format!("{name}.vortex"))
                .to_str()
                .unwrap()
                .to_string();
            let file = write_batch_file(&path, &io, schema.clone(), &batch).await;
            let session = VortexSession::default();
            let (opened, _) = super::open_vortex_file(
                &path,
                file.file_size_in_bytes(),
                None,
                &io,
                Arc::new(std::sync::atomic::AtomicU64::new(0)),
                &session,
            )
            .await
            .unwrap();
            let physical = session.arrow().to_arrow_schema(opened.dtype()).unwrap();
            assert_eq!(
                physical.field(0).extension_type_name(),
                Some("arrow.parquet.variant")
            );
            assert_eq!(file.value_counts().get(&1), Some(&3));
            assert!(!file.null_value_counts().contains_key(&1));
            assert!(!file.lower_bounds().contains_key(&1));
            assert!(!file.upper_bounds().contains_key(&1));
            let batches = scan_batches(
                io.clone(),
                test_scan_task(&path, schema.clone(), file.file_size_in_bytes()),
            )
            .await;
            batch = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
            assert_eq!(batch.num_rows(), 3);
            let native = session
                .arrow()
                .from_arrow_array(batch.column(0).clone(), batch.schema().field(0))
                .unwrap();
            assert!(native.dtype().is_variant());
            let mut ctx = session.create_execution_ctx();
            for (row, expected) in [Some(42_i64), None, Some(-7)].into_iter().enumerate() {
                let scalar = native.execute_scalar(row, &mut ctx).unwrap();
                let actual = scalar.as_variant().value().map(|value| {
                    value
                        .cast(&DType::Primitive(PType::I64, Nullability::NonNullable))
                        .unwrap()
                        .as_primitive()
                        .typed_value::<i64>()
                        .unwrap()
                });
                assert_eq!(actual, expected);
            }
        }
    }

    #[tokio::test]
    async fn test_vortex_native_uuid() {
        use arrow_array::FixedSizeBinaryArray;
        use vortex::arrow::ArrowSessionExt;
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("uuid.vortex").to_str().unwrap().to_string();
        let io = FileIO::new_with_fs();
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "uuid", Type::Primitive(PrimitiveType::Uuid)).into(),
                    NestedField::optional(
                        2,
                        "nested",
                        Type::Struct(crate::spec::StructType::new(vec![
                            NestedField::optional(3, "uuid", Type::Primitive(PrimitiveType::Uuid))
                                .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let uuids = [uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(u128::MAX)];
        let values = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some(uuids[0].as_bytes()), None, Some(uuids[1].as_bytes())].into_iter(),
                16,
            )
            .unwrap(),
        );
        let arrow_schema: ArrowSchema = schema.as_ref().try_into().unwrap();
        let DataType::Struct(fields) = arrow_schema.field(1).data_type() else {
            panic!()
        };
        let nested = Arc::new(StructArray::new(fields.clone(), vec![values.clone()], None));
        let batch =
            RecordBatch::try_new(Arc::new(arrow_schema), vec![values.clone(), nested]).unwrap();
        let file = write_batch_file(&path, &io, schema.clone(), &batch).await;
        assert_eq!(file.null_value_counts().get(&1), Some(&1));
        assert!(!file.null_value_counts().contains_key(&3));
        for id in [1, 3] {
            assert!(!file.lower_bounds().contains_key(&id));
            assert!(!file.upper_bounds().contains_key(&id));
        }
        let session = VortexSession::default();
        let (opened, _) = super::open_vortex_file(
            &path,
            file.file_size_in_bytes(),
            None,
            &io,
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            &session,
        )
        .await
        .unwrap();
        let physical = session.arrow().to_arrow_schema(opened.dtype()).unwrap();
        assert_eq!(physical.field(0).extension_type_name(), Some("arrow.uuid"));
        assert!(
            !physical
                .field(0)
                .metadata()
                .contains_key("ARROW:extension:metadata")
        );
        let DataType::Struct(fields) = physical.field(1).data_type() else {
            panic!()
        };
        assert_eq!(fields[0].extension_type_name(), Some("arrow.uuid"));
        let batches = scan_batches(
            io.clone(),
            test_scan_task(&path, schema.clone(), file.file_size_in_bytes()),
        )
        .await;
        let actual: Vec<_> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(|v| v.to_vec()))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            actual,
            values
                .iter()
                .map(|v| v.map(|v| v.to_vec()))
                .collect::<Vec<_>>()
        );
        for name in ["uuid", "nested.uuid"] {
            for (predicate, expected) in [
                (Reference::new(name).equal_to(Datum::uuid(uuids[1])), 1),
                (
                    Reference::new(name).is_in([Datum::uuid(uuids[0]), Datum::uuid(uuids[1])]),
                    2,
                ),
                (Reference::new(name).is_null(), 1),
            ] {
                let task = test_scan_task_filtered(
                    &path,
                    schema.clone(),
                    file.file_size_in_bytes(),
                    Some(predicate.bind(schema.clone(), true).unwrap()),
                    vec![],
                );
                let batches = scan_batches(io.clone(), task).await;
                assert_eq!(
                    batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    expected
                );
            }
        }
    }

    #[test]
    fn test_vortex_byte_range_boundaries() {
        use super::byte_range_to_rows;
        assert_eq!(byte_range_to_rows(0, 1, 100, 1), 0..0);
        assert_eq!(byte_range_to_rows(50, 50, 100, 1), 0..1);
        assert_eq!(byte_range_to_rows(100, 1, 100, 5), 5..5);
        assert_eq!(byte_range_to_rows(0, 100, 100, 0), 0..0);
        assert_eq!(byte_range_to_rows(0, 1, 0, 0), 0..0);
        assert_eq!(
            byte_range_to_rows(u64::MAX - 1, 10, u64::MAX, u64::MAX),
            (u64::MAX - 1)..u64::MAX
        );
    }

    #[tokio::test]
    async fn test_vortex_byte_range_splits() {
        use crate::metadata_columns::*;
        let tmp = TempDir::new().unwrap();
        let path = tmp
            .path()
            .join("split.vortex")
            .to_str()
            .unwrap()
            .to_string();
        let delete_path = tmp
            .path()
            .join("delete.parquet")
            .to_str()
            .unwrap()
            .to_string();
        let io = FileIO::new_with_fs();
        let file = write_test_file(&path, &io).await;
        let size = file.file_size_in_bytes();
        write_positional_delete_file(&delete_path, &path, &[3]);
        for filtered in [false, true] {
            let mut positions = Vec::new();
            let mut row_ids = Vec::new();
            // More splits than rows exercises empty ranges as well as adjacency.
            for split in 0..11 {
                let start = size * split / 11;
                let end = size * (split + 1) / 11;
                let task = FileScanTask::builder()
                    .with_file_size_in_bytes(size)
                    .with_start(start)
                    .with_length(end - start)
                    .with_data_file_path(path.clone())
                    .with_data_file_format(DataFileFormat::Vortex)
                    .with_schema(test_schema())
                    .with_case_sensitive(true)
                    .with_project_field_ids(vec![RESERVED_FIELD_ID_POS, RESERVED_FIELD_ID_ROW_ID])
                    .with_first_row_id(Some(100))
                    .with_predicate(filtered.then(|| {
                        Reference::new("id")
                            .greater_than(Datum::long(1))
                            .bind(test_schema(), true)
                            .unwrap()
                    }))
                    .with_deletes(if filtered {
                        vec![delete_file_entry(
                            &delete_path,
                            DataContentType::PositionDeletes,
                        )]
                    } else {
                        vec![]
                    })
                    .build()
                    .unwrap();
                for batch in scan_batches(io.clone(), task).await {
                    for (name, values) in [
                        (RESERVED_COL_NAME_POS, &mut positions),
                        (RESERVED_COL_NAME_ROW_ID, &mut row_ids),
                    ] {
                        let array =
                            arrow_cast::cast(batch.column_by_name(name).unwrap(), &DataType::Int64)
                                .unwrap();
                        values.extend_from_slice(
                            array
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .values(),
                        );
                    }
                }
            }
            let expected = if filtered {
                vec![1, 2, 4]
            } else {
                vec![0, 1, 2, 3, 4]
            };
            assert_eq!(positions, expected);
            assert_eq!(
                row_ids,
                expected.iter().map(|v| v + 100).collect::<Vec<_>>()
            );
        }
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(size)
            .with_start(size)
            .with_length(1)
            .with_data_file_path(path)
            .with_data_file_format(DataFileFormat::Vortex)
            .with_schema(test_schema())
            .with_case_sensitive(true)
            .with_project_field_ids(vec![1])
            .build()
            .unwrap();
        assert!(scan_batches(io, task).await.is_empty());
    }

    #[tokio::test]
    async fn test_vortex_rejects_fixed_width_writer_schemas() {
        let tmp = TempDir::new().unwrap();
        let path = tmp
            .path()
            .join("unsupported.vortex")
            .to_str()
            .unwrap()
            .to_string();
        let io = FileIO::new_with_fs();
        let primitive = PrimitiveType::Fixed(3);
        let leaf = Arc::new(NestedField::optional(
            2,
            "element",
            Type::Primitive(primitive.clone()),
        ));
        let types = [
            Type::Primitive(primitive),
            Type::Struct(crate::spec::StructType::new(vec![leaf.clone()])),
            Type::List(crate::spec::ListType::new(leaf.clone())),
            Type::Map(crate::spec::MapType::new(
                Arc::new(NestedField::required(
                    3,
                    "key",
                    Type::Primitive(PrimitiveType::String),
                )),
                leaf,
            )),
        ];
        for ty in types {
            let schema = Arc::new(
                Schema::builder()
                    .with_fields(vec![NestedField::optional(1, "value", ty).into()])
                    .build()
                    .unwrap(),
            );
            let error = VortexWriterBuilder::new(schema, VortexSession::default())
                .build(io.new_output(&path).unwrap())
                .await
                .err()
                .expect("unsupported schema must fail");
            assert_eq!(error.kind(), crate::ErrorKind::FeatureUnsupported);
            assert!(!std::path::Path::new(&path).exists());
        }
    }

    #[tokio::test]
    async fn test_vortex_rejects_binary_as_fixed_width() {
        for (primitive, datum) in [
            (PrimitiveType::Fixed(3), Datum::fixed(*b"abc")),
            (PrimitiveType::Uuid, Datum::uuid(uuid::Uuid::nil())),
        ] {
            let schema = Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "value", Type::Primitive(primitive)).into(),
                ])
                .build()
                .unwrap();
            let field = Field::new("value", DataType::Binary, true).with_metadata(
                std::collections::HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "1".to_string(),
                )]),
            );
            let batch =
                RecordBatch::try_new(Arc::new(ArrowSchema::new(vec![field])), vec![Arc::new(
                    arrow_array::BinaryArray::from(vec![b"abc".as_slice()]),
                )])
                .unwrap();
            assert_eq!(
                crate::arrow::evolve_nested_fields(batch, &schema, &VortexSession::default())
                    .unwrap_err()
                    .kind(),
                crate::ErrorKind::FeatureUnsupported
            );
            assert_eq!(
                super::datum_to_vortex_scalar(
                    &datum,
                    &vortex::dtype::DType::Binary(vortex::dtype::Nullability::Nullable)
                )
                .unwrap_err()
                .kind(),
                crate::ErrorKind::FeatureUnsupported
            );
        }
    }

    #[tokio::test]
    async fn test_vortex_physical_lineage_coalesces() {
        use crate::metadata_columns::*;
        let tmp = TempDir::new().unwrap();
        let path = tmp
            .path()
            .join("lineage.vortex")
            .to_str()
            .unwrap()
            .to_string();
        let io = FileIO::new_with_fs();
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(
                        RESERVED_FIELD_ID_ROW_ID,
                        RESERVED_COL_NAME_ROW_ID,
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                    NestedField::optional(
                        RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                        RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let batch = RecordBatch::try_new(Arc::new(schema.as_ref().try_into().unwrap()), vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![Some(50), None, Some(70)])),
            Arc::new(Int64Array::from(vec![Some(2), None, Some(3)])),
        ])
        .unwrap();
        let file = write_batch_file(&path, &io, schema.clone(), &batch).await;
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(file.file_size_in_bytes())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(path)
            .with_data_file_format(DataFileFormat::Vortex)
            .with_schema(schema)
            .with_project_field_ids(vec![
                RESERVED_FIELD_ID_ROW_ID,
                RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
            ])
            .with_first_row_id(Some(100))
            .with_data_sequence_number(Some(9))
            .with_case_sensitive(true)
            .build()
            .unwrap();
        let batches = scan_batches(io, task).await;
        let values = |name: &str| -> Vec<i64> {
            batches
                .iter()
                .flat_map(|b| {
                    arrow_cast::cast(b.column_by_name(name).unwrap(), &DataType::Int64)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect()
        };
        assert_eq!(values(RESERVED_COL_NAME_ROW_ID), vec![50, 101, 70]);
        assert_eq!(
            values(RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER),
            vec![2, 9, 3]
        );
    }

    #[tokio::test]
    async fn test_vortex_name_mapping_without_embedded_schema() {
        use vortex::array::stream::ArrayStreamAdapter;
        use vortex::arrow::ArrowSessionExt;
        use vortex::file::WriteOptionsSessionExt;
        let tmp = TempDir::new().unwrap();
        let path = tmp
            .path()
            .join("external.vortex")
            .to_str()
            .unwrap()
            .to_string();
        let io = FileIO::new_with_fs();
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "old",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let session = VortexSession::default();
        let array = session
            .arrow()
            .from_arrow_record_batch(batch.clone(), batch.schema_ref())
            .unwrap();
        let stream =
            ArrayStreamAdapter::new(array.dtype().clone(), futures::stream::iter([Ok(array)]));
        session
            .write_options()
            .write(tokio::fs::File::create(&path).await.unwrap(), stream)
            .await
            .unwrap();
        for (id, mapping) in [
            (1, None),
            (
                42,
                Some(Arc::new(crate::spec::NameMapping::new(vec![
                    crate::spec::MappedField::new(Some(42), vec!["old".to_string()], vec![]),
                ]))),
            ),
        ] {
            let schema = Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::required(id, "renamed", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    ])
                    .build()
                    .unwrap(),
            );
            let task = FileScanTask::builder()
                .with_file_size_in_bytes(std::fs::metadata(&path).unwrap().len())
                .with_start(0)
                .with_length(0)
                .with_data_file_path(path.clone())
                .with_data_file_format(DataFileFormat::Vortex)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![id])
                .with_name_mapping(mapping)
                .with_predicate(Some(
                    Reference::new("renamed")
                        .greater_than(Datum::long(1))
                        .bind(schema, true)
                        .unwrap(),
                ))
                .with_case_sensitive(true)
                .build()
                .unwrap();
            let batches = scan_batches(io.clone(), task).await;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
            assert_eq!(batches[0].schema().field(0).name(), "renamed");
        }
    }
    #[tokio::test]
    async fn test_vortex_promoted_predicate_and_partition_metadata() {
        use crate::metadata_columns::*;
        use crate::spec::{Literal, PartitionSpec, Struct, Transform};
        let tmp = TempDir::new().unwrap();
        let path = tmp
            .path()
            .join("promoted.vortex")
            .to_str()
            .unwrap()
            .to_string();
        let io = FileIO::new_with_fs();
        let old_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );
        let batch = RecordBatch::try_new(Arc::new(old_schema.as_ref().try_into().unwrap()), vec![
            Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3])),
        ])
        .unwrap();
        let file = write_batch_file(&path, &io, old_schema, &batch).await;
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(2, "region", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );
        let spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(7)
                .add_partition_field("region", "region", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );
        let unified = Arc::new(spec.partition_type(&schema).unwrap());
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(file.file_size_in_bytes())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(path)
            .with_data_file_format(DataFileFormat::Vortex)
            .with_schema(schema.clone())
            .with_project_field_ids(vec![1, 2, RESERVED_FIELD_ID_PARTITION])
            .with_partition_spec(Some(spec))
            .with_partition(Some(Struct::from_iter([Some(Literal::int(42))])))
            .with_unified_partition_type(Some(unified))
            .with_predicate(Some(
                Reference::new("id")
                    .greater_than(Datum::long(1))
                    .bind(schema, true)
                    .unwrap(),
            ))
            .with_case_sensitive(true)
            .build()
            .unwrap();
        let batches = scan_batches(io, task).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        for batch in batches {
            assert_eq!(batch.column(0).data_type(), &DataType::Int64);
            let region = arrow_cast::cast(batch.column(1), &DataType::Int32).unwrap();
            assert!(
                region
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .all(|v| *v == 42)
            );
            let partition = batch
                .column_by_name(RESERVED_COL_NAME_PARTITION)
                .unwrap()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let region = arrow_cast::cast(partition.column(0), &DataType::Int32).unwrap();
            assert!(
                region
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .all(|v| *v == 42)
            );
        }
    }

    #[tokio::test]
    async fn test_vortex_rejects_encryption() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("data.vortex").to_str().unwrap().to_string();
        let io = FileIO::new_with_fs();
        let file = write_test_file(&path, &io).await;
        let (start, length, key) = (0, 0, Some(vec![1].into_boxed_slice()));
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(file.file_size_in_bytes())
            .with_start(start)
            .with_length(length)
            .with_data_file_path(path.clone())
            .with_data_file_format(DataFileFormat::Vortex)
            .with_schema(test_schema())
            .with_project_field_ids(vec![1])
            .with_key_metadata(key)
            .with_case_sensitive(true)
            .build()
            .unwrap();
        let result = ArrowReaderBuilder::new(io.clone(), Runtime::current())
            .build()
            .read(Box::pin(futures::stream::iter([Ok(task)])))
            .unwrap()
            .stream()
            .try_collect::<Vec<_>>()
            .await;
        assert!(result.is_err());
    }
}
