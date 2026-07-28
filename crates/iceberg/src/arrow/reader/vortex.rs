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

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use vortex::VortexSessionDefault;
use vortex::array::VortexSessionExecute;
use vortex::array::arrow::ArrowSessionExt;
use vortex::array::buffer::BufferHandle;
use vortex::array::stream::ArrayStream;
use vortex::buffer::{Alignment, Buffer, ByteBuffer};
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
use crate::arrow::{convert_temporal_value, to_iceberg_error};
use crate::expr::{BoundPredicate, BoundReference, PredicateOperator};
use crate::io::{FileIO, FileRead};
use crate::metadata_columns::RESERVED_FIELD_ID_FILE;
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
    let delete_filter_rx = delete_file_loader.load_deletes(&task.deletes, Arc::clone(&task.schema));

    // The session captures the current tokio runtime handle at construction
    // time, so it is created here, inside the runtime driving the scan.
    let session = VortexSession::default();

    // Open the vortex file through iceberg's FileIO.
    let input_file = file_io.new_input(&task.data_file_path)?;
    let file_size = if task.file_size_in_bytes > 0 {
        task.file_size_in_bytes
    } else {
        input_file.metadata().await?.size
    };
    let reader = CountingFileRead::new(input_file.reader().await?, bytes_read_counter);
    let source = Arc::new(FileReadVortexSource {
        reader: Arc::new(reader),
        size: file_size,
        uri: Arc::from(task.data_file_path.as_str()),
    });

    let vortex_file = session
        .open_options()
        .with_file_size(file_size)
        .open(source)
        .await
        .map_err(to_iceberg_error)?;

    // Vortex files do not record iceberg field ids, so columns are matched by
    // name. Fields that were added to the table schema after the file was
    // written are filled in by the RecordBatchTransformer below.
    let file_field_names: Vec<&str> = vortex_file
        .dtype()
        .as_struct_fields_opt()
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Vortex data file does not contain a struct dtype",
            )
        })?
        .names()
        .iter()
        .map(|name| name.as_ref())
        .collect();
    let projected_names: Vec<String> = task
        .project_field_ids
        .iter()
        .filter_map(|id| task.schema.name_by_field_id(*id))
        .filter(|name| file_field_names.contains(name))
        .map(|name| name.to_string())
        .collect();

    let delete_filter = delete_filter_rx.await.unwrap()?;
    let delete_predicate = delete_filter.build_equality_delete_predicate(&task).await?;

    // In addition to the optional predicate supplied in the `FileScanTask`,
    // we also have an optional predicate resulting from equality delete files.
    // If both are present, we logical-AND them together to form a single filter
    // predicate that gets pushed down into the vortex scan.
    let final_predicate = match (&task.predicate, delete_predicate) {
        (None, None) => None,
        (Some(predicate), None) => Some(predicate.clone()),
        (None, Some(predicate)) => Some(predicate),
        (Some(filter_predicate), Some(delete_predicate)) => {
            Some(filter_predicate.clone().and(delete_predicate))
        }
    };
    let filter = final_predicate
        .as_ref()
        .map(|predicate| convert_predicate_to_vortex(predicate, &task.schema, vortex_file.dtype()))
        .transpose()?;

    let mut scan_builder = vortex_file
        .scan()
        .map_err(to_iceberg_error)?
        .with_projection(select(
            projected_names
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>(),
            root(),
        ))
        .with_some_filter(filter);
    if let Some(batch_size) = batch_size {
        scan_builder = scan_builder.with_split_by(SplitBy::RowCount(batch_size));
    }

    // Positional deletes are applied by excluding the deleted row ordinals
    // from the scan. Vortex selections compose with the filter expression.
    if let Some(positional_deletes) = delete_filter.get_delete_vector(&task) {
        let deleted_rows: Buffer<u64> = {
            let guard = positional_deletes.lock().unwrap();
            guard.iter().collect()
        };
        scan_builder = scan_builder.with_selection(Selection::ExcludeByIndex(deleted_rows));
    }

    let array_stream = scan_builder.into_array_stream().map_err(to_iceberg_error)?;

    // Mirror the parquet pipeline: adapt file batches to the task schema.
    let mut record_batch_transformer_builder =
        RecordBatchTransformerBuilder::new(task.schema_ref(), task.project_field_ids());
    if task.project_field_ids().contains(&RESERVED_FIELD_ID_FILE) {
        record_batch_transformer_builder = record_batch_transformer_builder.with_constant(
            RESERVED_FIELD_ID_FILE,
            Datum::string(task.data_file_path.clone()),
        );
    }
    if let (Some(partition_spec), Some(partition_data)) =
        (task.partition_spec.clone(), task.partition.clone())
    {
        record_batch_transformer_builder =
            record_batch_transformer_builder.with_partition(partition_spec, partition_data)?;
    }
    let mut record_batch_transformer = record_batch_transformer_builder.build();

    let arrow_field = session
        .arrow()
        .to_arrow_field("", array_stream.dtype())
        .map_err(to_iceberg_error)?;
    let mut execution_ctx = session.create_execution_ctx();
    let task_schema = task.schema_ref();

    let record_batch_stream = array_stream.map(move |chunk| {
        let chunk = chunk.map_err(to_iceberg_error)?;
        let arrow_array = session
            .arrow()
            .execute_arrow(chunk, Some(&arrow_field), &mut execution_ctx)
            .map_err(to_iceberg_error)?;
        let batch = RecordBatch::from(arrow_array.as_struct());
        let batch = attach_field_id_metadata(batch, &task_schema)?;
        record_batch_transformer.process_record_batch(batch)
    });

    Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
}

/// Tags every top-level column of `batch` that exists in `schema` with its
/// iceberg field id, so that the [`RecordBatchTransformer`] can match columns
/// by id like it does for parquet files.
fn attach_field_id_metadata(batch: RecordBatch, schema: &Schema) -> Result<RecordBatch> {
    let fields: Vec<ArrowField> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let field = field.as_ref().clone();
            match schema.field_id_by_name(field.name()) {
                Some(field_id) => {
                    let mut metadata = field.metadata().clone();
                    metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
                    field.with_metadata(metadata)
                }
                None => field,
            }
        })
        .collect();
    let schema = Arc::new(ArrowSchema::new(fields));
    RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(|err| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to attach field id metadata to record batch",
        )
        .with_source(err)
    })
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
    let mut accessor = Some(reference.accessor());
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

    let expr = names
        .iter()
        .fold(root(), |child, name| get_item(name.as_str(), child));
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
        FileScanTask::builder()
            .with_file_size_in_bytes(file_size)
            .with_start(0)
            .with_length(file_size)
            .with_data_file_path(file_path.to_string())
            .with_data_file_format(DataFileFormat::Vortex)
            .with_schema(schema.clone())
            .with_project_field_ids(schema.as_struct().fields().iter().map(|f| f.id).collect())
            .with_case_sensitive(false)
            .build()
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
        let mut task = test_scan_task(&file_path, schema, file_size);
        task.predicate = Some(predicate);

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

        let mut task = test_scan_task(&file_path, test_schema(), file_size);
        task.deletes = vec![delete_file_entry(
            &delete_path,
            DataContentType::PositionDeletes,
        )];

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

        let mut task = test_scan_task(&file_path, test_schema(), file_size);
        task.deletes = vec![delete_file_entry(
            &delete_path,
            DataContentType::EqualityDeletes,
        )];

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
        let mut task = test_scan_task(&file_path, schema.clone(), file_size);
        task.deletes = vec![
            delete_file_entry(&pos_delete_path, DataContentType::PositionDeletes),
            delete_file_entry(&eq_delete_path, DataContentType::EqualityDeletes),
        ];
        task.predicate = Some(
            Reference::new("id")
                .greater_than(Datum::long(1))
                .bind(schema, true)
                .unwrap(),
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
        let mut task = test_scan_task(&file_path, schema.clone(), file_size);
        task.predicate = Some(predicate.bind(schema, true).unwrap());

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

        let mut task = test_scan_task(&file_path, evolved_schema.clone(), file_size);
        task.predicate = Some(predicate.bind(evolved_schema, true).unwrap());

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        assert_eq!(collect_ids(&reader, task).await, vec![4, 5]);
    }
}
