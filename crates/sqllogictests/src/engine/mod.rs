mod datafusion;

use std::sync::Arc;
pub use datafusion::*;

mod sparksql;
mod conversion;
mod output;

pub use sparksql::*;


pub enum Engine {
    DataFusion(Arc<DataFusionEngine>),
    SparkSQL(Arc<SparkSqlEngine>),
}