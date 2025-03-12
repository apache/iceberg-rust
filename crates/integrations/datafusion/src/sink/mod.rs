use std::any::Any;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayAs;
use futures::StreamExt;
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriter;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::IcebergWriter;
use iceberg::Catalog;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct IcebergSink {
    data_file_writer: Mutex<
        DataFileWriter<ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>>,
    >,
    row_count: AtomicU64,
    table: Table,
    catalog: Arc<dyn Catalog>,
}

impl IcebergSink {
    pub(crate) fn new(
        data_file_writer: DataFileWriter<
            ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
        >,
        catalog: Arc<dyn Catalog>,
        table: Table,
    ) -> Self {
        Self {
            data_file_writer: Mutex::new(data_file_writer),
            row_count: AtomicU64::new(0),
            table,
            catalog,
        }
    }

    pub(crate) async fn commit(&self) -> Result<()> {
        let mut writer = self.data_file_writer.lock().await;
        let data_files = writer
            .close()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut append_action = Transaction::new(&self.table)
            .fast_append(None, vec![])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        append_action
            .add_data_files(data_files)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let tx = append_action
            .apply()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        tx.commit(self.catalog.as_ref())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }
}

impl DisplayAs for IcebergSink {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        f.write_str("IcebergSink")
    }
}

#[async_trait]
impl DataSink for IcebergSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        // # TODO: Implement metrics for IcebergSink
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        while let Some(data_chunk) = data.as_mut().next().await {
            if let Ok(data_chunk) = data_chunk {
                let row_num = data_chunk.num_rows();
                self.data_file_writer
                    .lock()
                    .await
                    .write(data_chunk)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                self.row_count
                    .fetch_add(row_num as u64, std::sync::atomic::Ordering::Relaxed);
            } else {
                // # TODO
                // Add warn log here
            }
        }
        self.commit().await?;
        Ok(self.row_count.load(std::sync::atomic::Ordering::Relaxed))
    }
}
