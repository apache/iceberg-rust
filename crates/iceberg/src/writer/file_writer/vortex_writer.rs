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

//! The module contains the file writer for the vortex file format.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use bytes::Bytes;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use vortex::array::ArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::array::stats::TypedStatsSetRef;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::dtype::arrow::FromArrowType;
use vortex::dtype::extension::Matcher;
use vortex::dtype::{DType, PType};
use vortex::expr::stats::{Stat, StatsProvider, StatsProviderExt};
use vortex::extension::datetime::{AnyTemporal, TimeUnit};
use vortex::file::{WriteOptionsSessionExt, WriteStrategyBuilder, WriteSummary};
use vortex::io::{IoBuf, VortexWrite};
use vortex::layout::LayoutStrategy;
use vortex::scalar::Scalar;
use vortex::session::VortexSession;

use super::{FileWriter, FileWriterBuilder};
use crate::arrow::{convert_temporal_value, to_iceberg_error};
use crate::io::{FileWrite, OutputFile};
use crate::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, Datum, PrimitiveLiteral, PrimitiveType,
    SchemaRef, Struct, Type,
};
use crate::{Error, ErrorKind, Result};

/// VortexWriterBuilder is used to build a [`VortexWriter`].
#[derive(Clone, Debug)]
pub struct VortexWriterBuilder {
    schema: SchemaRef,
    session: VortexSession,
}

impl VortexWriterBuilder {
    /// Create a new `VortexWriterBuilder`.
    ///
    /// The session is shared by all writers the builder builds. It captures
    /// the current tokio runtime handle at construction time, so it must be
    /// created from within the runtime that will drive the writes.
    pub fn new(schema: SchemaRef, session: VortexSession) -> Self {
        Self { schema, session }
    }
}

impl FileWriterBuilder for VortexWriterBuilder {
    type R = VortexWriter;

    async fn build(&self, output_file: OutputFile) -> Result<Self::R> {
        Ok(VortexWriter {
            schema: self.schema.clone(),
            session: self.session.clone(),
            output_file,
            inner: None,
            current_row_num: 0,
        })
    }
}

/// `VortexWriter` writes arrow data into vortex files on storage.
///
/// Record batches are streamed into a vortex file writer running as a
/// background task, so data is compressed and flushed to storage as it
/// arrives instead of being buffered until close.
pub struct VortexWriter {
    schema: SchemaRef,
    session: VortexSession,
    output_file: OutputFile,
    inner: Option<StreamingWriter>,
    current_row_num: usize,
}

/// The state of an in-progress vortex file write.
///
/// Vortex's push-based writer is not `Send`, so instead of holding it in
/// place the batches are sent through a bounded channel to a spawned task
/// that owns the write end-to-end and returns the [`WriteSummary`] when
/// finished. The channel is bounded so the task applies backpressure instead
/// of buffering batches.
struct StreamingWriter {
    arrow_schema: ArrowSchemaRef,
    batches: mpsc::Sender<ArrayRef>,
    task: tokio::task::JoinHandle<Result<WriteSummary>>,
    bytes_written: Arc<AtomicU64>,
    strategy: Arc<dyn LayoutStrategy>,
}

impl StreamingWriter {
    /// Awaits the background task after a failure to surface its error.
    async fn task_error(self) -> Error {
        match self.task.await {
            Ok(Ok(_)) => Error::new(
                ErrorKind::Unexpected,
                "Vortex write task stopped accepting batches but reported success",
            ),
            Ok(Err(err)) => err,
            Err(err) => {
                Error::new(ErrorKind::Unexpected, "Vortex write task panicked").with_source(err)
            }
        }
    }
}

impl VortexWriter {
    async fn start(&self, arrow_schema: ArrowSchemaRef) -> Result<StreamingWriter> {
        let dtype = DType::from_arrow(arrow_schema.clone());
        let file = self.output_file.writer().await?;
        let bytes_written = Arc::new(AtomicU64::new(0));
        let strategy = WriteStrategyBuilder::default().build();

        let (batches, receiver) = mpsc::channel::<ArrayRef>(1);
        let stream = ArrayStreamAdapter::new(dtype, receiver.map(Ok));
        let write_options = self
            .session
            .write_options()
            .with_strategy(Arc::clone(&strategy));

        let mut sink = FileWriteSink {
            file,
            bytes_written: Arc::clone(&bytes_written),
        };
        let task = tokio::spawn(async move {
            let summary = write_options
                .write(&mut sink, stream)
                .await
                .map_err(to_iceberg_error)?;
            // The vortex writer flushes but does not close its sink; finalize
            // the iceberg output file explicitly.
            sink.file.close().await?;
            Ok(summary)
        });

        Ok(StreamingWriter {
            arrow_schema,
            batches,
            task,
            bytes_written,
            strategy,
        })
    }

    fn data_file_builder(
        &self,
        summary: &WriteSummary,
        arrow_schema: &ArrowSchemaRef,
    ) -> Result<DataFileBuilder> {
        let record_count = summary.row_count();

        // Take value/null/NaN counts and min/max bounds for top-level
        // primitive fields from the statistics collected by the vortex writer.
        // Nested fields are left unset; metrics evaluators treat missing
        // entries as "rows might match".
        let mut value_counts: HashMap<i32, u64> = HashMap::new();
        let mut null_value_counts: HashMap<i32, u64> = HashMap::new();
        let mut nan_value_counts: HashMap<i32, u64> = HashMap::new();
        let mut lower_bounds: HashMap<i32, Datum> = HashMap::new();
        let mut upper_bounds: HashMap<i32, Datum> = HashMap::new();
        let file_stats = summary.footer().statistics();
        for (index, field) in arrow_schema.fields().iter().enumerate() {
            let Some(iceberg_field) = self.schema.field_by_name(field.name()) else {
                continue;
            };
            let Type::Primitive(primitive_type) = iceberg_field.field_type.as_ref() else {
                continue;
            };
            // Top-level columns hold exactly one value per row.
            value_counts.insert(iceberg_field.id, record_count);
            let Some(stats) = file_stats.filter(|stats| index < stats.stats_sets().len()) else {
                continue;
            };
            let (stats_set, field_dtype) = stats.get(index);
            let typed_stats = stats_set.as_typed_ref(field_dtype);
            if let Some(null_count) = typed_stats.get_as::<u64>(Stat::NullCount).as_exact() {
                null_value_counts.insert(iceberg_field.id, null_count);
            }
            if matches!(primitive_type, PrimitiveType::Float | PrimitiveType::Double)
                && let Some(nan_count) = typed_stats.get_as::<u64>(Stat::NaNCount).as_exact()
            {
                nan_value_counts.insert(iceberg_field.id, nan_count);
            }
            if let Some(min) = stat_bound(&typed_stats, Stat::Min, primitive_type) {
                lower_bounds.insert(iceberg_field.id, min);
            }
            if let Some(max) = stat_bound(&typed_stats, Stat::Max, primitive_type) {
                upper_bounds.insert(iceberg_field.id, max);
            }
        }

        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::Data)
            .file_path(self.output_file.location().to_string())
            .file_format(DataFileFormat::Vortex)
            .partition(Struct::empty())
            .record_count(record_count)
            .file_size_in_bytes(summary.size())
            .value_counts(value_counts)
            .null_value_counts(null_value_counts)
            .nan_value_counts(nan_value_counts)
            .lower_bounds(lower_bounds)
            .upper_bounds(upper_bounds);
        // Vortex files can be split at arbitrary row offsets, so no physical
        // split offsets are recorded.

        Ok(builder)
    }
}

/// Converts a min/max statistic into an iceberg bound [`Datum`].
///
/// Truncated variable-length statistics remain sound bounds: the min is
/// prefix-truncated (still a lower bound) and the max is upper-adjusted or
/// absent, so both exact and inexact values are used.
fn stat_bound(
    stats: &TypedStatsSetRef,
    stat: Stat,
    primitive_type: &PrimitiveType,
) -> Option<Datum> {
    let value = stats.get(stat).into_inner()?;
    vortex_scalar_to_datum(value, primitive_type)
}

/// Converts a vortex [`Scalar`] into an iceberg [`Datum`] of the given
/// primitive type.
///
/// Returns `None` for null scalars, NaN float values (excluded from bounds by
/// the iceberg spec; NaN counts are tracked separately), and types whose
/// bounds are not computed (`Uuid`, `Fixed`).
fn vortex_scalar_to_datum(scalar: Scalar, primitive_type: &PrimitiveType) -> Option<Datum> {
    match primitive_type {
        PrimitiveType::Boolean => Some(Datum::bool(scalar.as_bool_opt()?.value()?)),
        PrimitiveType::Int => Some(Datum::int(scalar.as_primitive_opt()?.typed_value::<i32>()?)),
        PrimitiveType::Long => Some(Datum::long(
            scalar.as_primitive_opt()?.typed_value::<i64>()?,
        )),
        PrimitiveType::Float => {
            let value = scalar.as_primitive_opt()?.typed_value::<f32>()?;
            (!value.is_nan()).then(|| Datum::float(value))
        }
        PrimitiveType::Double => {
            let value = scalar.as_primitive_opt()?.typed_value::<f64>()?;
            (!value.is_nan()).then(|| Datum::double(value))
        }
        PrimitiveType::Date => {
            let days = temporal_value(scalar, TimeUnit::Days)?;
            Some(Datum::date(i32::try_from(days).ok()?))
        }
        PrimitiveType::Time => {
            Datum::time_micros(temporal_value(scalar, TimeUnit::Microseconds)?).ok()
        }
        PrimitiveType::Timestamp => Some(Datum::timestamp_micros(temporal_value(
            scalar,
            TimeUnit::Microseconds,
        )?)),
        PrimitiveType::Timestamptz => Some(Datum::timestamptz_micros(temporal_value(
            scalar,
            TimeUnit::Microseconds,
        )?)),
        PrimitiveType::TimestampNs => Some(Datum::timestamp_nanos(temporal_value(
            scalar,
            TimeUnit::Nanoseconds,
        )?)),
        PrimitiveType::TimestamptzNs => Some(Datum::timestamptz_nanos(temporal_value(
            scalar,
            TimeUnit::Nanoseconds,
        )?)),
        PrimitiveType::String => Some(Datum::string(scalar.as_utf8_opt()?.value()?.as_str())),
        PrimitiveType::Binary => Some(Datum::binary(
            scalar.as_binary_opt()?.value()?.as_slice().iter().copied(),
        )),
        PrimitiveType::Decimal { .. } => {
            let value = scalar.as_decimal_opt()?.decimal_value()?.cast::<i128>()?;
            Some(Datum::new(
                primitive_type.clone(),
                PrimitiveLiteral::Int128(value),
            ))
        }
        // Uuid and Fixed bounds are not computed.
        _ => None,
    }
}

/// Extracts a temporal scalar's storage value converted to the given unit.
///
/// Vortex stores temporal columns as extension types whose metadata carries
/// the time unit; plain integer columns are assumed to already be in the
/// requested unit.
fn temporal_value(scalar: Scalar, unit: TimeUnit) -> Option<i64> {
    match scalar.dtype() {
        DType::Extension(ext) => {
            let metadata = AnyTemporal::try_match(ext)?;
            let storage = scalar.as_extension_opt()?.to_storage_scalar();
            let storage = storage.as_primitive_opt()?;
            let value = match storage.ptype() {
                PType::I32 => i64::from(storage.typed_value::<i32>()?),
                PType::I64 => storage.typed_value::<i64>()?,
                _ => return None,
            };
            convert_temporal_value(value, metadata.time_unit(), unit).ok()
        }
        DType::Primitive(PType::I32, _) => scalar
            .as_primitive_opt()?
            .typed_value::<i32>()
            .map(i64::from),
        DType::Primitive(PType::I64, _) => scalar.as_primitive_opt()?.typed_value::<i64>(),
        _ => None,
    }
}

/// Adapts an iceberg [`FileWrite`] into a [`VortexWrite`] sink, counting the
/// bytes flushed to storage.
struct FileWriteSink {
    file: Box<dyn FileWrite>,
    bytes_written: Arc<AtomicU64>,
}

impl VortexWrite for FileWriteSink {
    async fn write_all<B: IoBuf>(&mut self, buffer: B) -> std::io::Result<B> {
        self.file
            .write(Bytes::copy_from_slice(buffer.as_slice()))
            .await
            .map_err(std::io::Error::other)?;
        self.bytes_written
            .fetch_add(buffer.as_slice().len() as u64, Ordering::Relaxed);
        Ok(buffer)
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl FileWriter for VortexWriter {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        // Skip empty batch
        if batch.num_rows() == 0 {
            return Ok(());
        }

        if self.inner.is_none() {
            self.inner = Some(self.start(batch.schema()).await?);
        }
        let inner = self.inner.as_mut().expect("writer started above");

        let array = ArrayRef::from_arrow(batch.clone(), false).map_err(to_iceberg_error)?;
        if inner.batches.send(array).await.is_err() {
            // The background task exited early; surface its error.
            let inner = self.inner.take().expect("writer started above");
            return Err(inner.task_error().await);
        }
        self.current_row_num += batch.num_rows();

        Ok(())
    }

    async fn close(mut self) -> Result<Vec<DataFileBuilder>> {
        let Some(inner) = self.inner.take() else {
            return Ok(vec![]);
        };

        let StreamingWriter {
            arrow_schema,
            batches,
            task,
            ..
        } = inner;
        // Close the input channel to signal end-of-stream to the write task.
        drop(batches);
        let summary = task.await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Vortex write task panicked").with_source(err)
        })??;

        Ok(vec![self.data_file_builder(&summary, &arrow_schema)?])
    }
}

impl super::super::CurrentFileStatus for VortexWriter {
    fn current_file_path(&self) -> String {
        self.output_file.location().to_string()
    }

    fn current_row_num(&self) -> usize {
        self.current_row_num
    }

    fn current_written_size(&self) -> usize {
        // Bytes already flushed to storage plus bytes buffered by the layout
        // strategy. This underestimates the final file size by the footer,
        // which is only serialized at close time.
        self.inner
            .as_ref()
            .map(|inner| {
                (inner.bytes_written.load(Ordering::Relaxed) + inner.strategy.buffered_bytes())
                    as usize
            })
            .unwrap_or(0)
    }
}
