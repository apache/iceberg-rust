use std::path::Path;
use std::time::Duration;

use anyhow::{Context, anyhow};
use datafusion_sqllogictest::{DFColumnType, DFOutput, convert_schema_to_types, convert_batches};
use spark_connect_rs::{SparkSession, SparkSessionBuilder};
use sqllogictest::{AsyncDB, DBOutput, Record, parse_file};
use toml::Table as TomlTable;

use crate::engine::EngineRunner;
use crate::error::{Error, Result};

pub type SparkOutput = DBOutput<DFColumnType>;

pub struct SparkEngine {
    session: SparkSession,
}

#[async_trait::async_trait]
impl AsyncDB for SparkEngine {
    type Error = Error;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<DFColumnType>> {
        Self::run_query(&self.session, sql).await
    }

    async fn shutdown(&mut self) {}

    fn engine_name(&self) -> &str {
        "SparkConnect"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

#[async_trait::async_trait]
impl EngineRunner for SparkEngine {
    async fn run_slt_file(&mut self, path: &Path) -> crate::error::Result<()> {
        let path_dir = path.to_str().unwrap();
        println!("engine running slt file on path: {path_dir}");

        let session = self.session.clone();
        let runner = sqllogictest::Runner::new(move || {
            let session = session.clone();
            async move { Ok(SparkEngine { session }) }
        });

        let result: std::result::Result<(), Error> = Self::run_file_in_runner(path, runner).await;

        result
    }
}

impl SparkEngine {
    pub async fn new(configs: TomlTable) -> Result<Self> {
        let url = configs
            .get("url")
            .ok_or_else(|| anyhow!("url property doesn't exist for spark engine"))?
            .as_str()
            .ok_or_else(|| anyhow!("url property is not a string for spark engine"))?;

        let session = SparkSessionBuilder::remote(url)
            .app_name("SparkConnect")
            .build()
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(Self { session })
    }

    pub async fn run_query(session: &SparkSession, sql: impl Into<String>) -> Result<DFOutput> {
        let df = session.sql(sql.into().as_str()).await.unwrap();
        let batches = df.collect().await.unwrap();
        let schema = batches.schema();
        let types = convert_schema_to_types(schema.fields());

        // Convert batches to rows of strings
        let rows = convert_batches(vec![batches]).unwrap();

        Ok(DBOutput::Rows { types, rows })
    }

    async fn run_file_in_runner<D: AsyncDB, M>(
        path: &Path,
        mut runner: sqllogictest::Runner<D, M>,
    ) -> Result<()>
    where
        M: sqllogictest::MakeConnection<Conn = D>,
    {
        println!("run file in runner");

        let records = parse_file(&path).context("Failed to parse slt file")?;

        let mut errs = vec![];
        for record in records.into_iter() {
            if let Record::Halt { .. } = record {
                break;
            }
            if let Err(err) = runner.run_async(record).await {
                errs.push(format!("{err}"));
            }
        }

        if !errs.is_empty() {
            let mut msg = format!("{} errors in file {}\n\n", errs.len(), path.display());
            for (i, err) in errs.iter().enumerate() {
                msg.push_str(&format!("{}. {err}\n\n", i + 1));
            }
            return Err(Error(anyhow!(msg)));
        }

        Ok(())
    }
}
