use std::sync::Arc;

use async_trait::async_trait;

use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableUpdate};
use crate::spec::Schema;

/// Action to evolve a table's schema by adding a new schema version
/// and setting it as the current schema.
pub struct UpdateSchemaAction {
    schema: Option<Schema>,
}

impl UpdateSchemaAction {
    pub fn new() -> Self {
        Self { schema: None }
    }

    pub fn set_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }
}

impl Default for UpdateSchemaAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for UpdateSchemaAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        let schema = self.schema.clone().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "UpdateSchemaAction requires a schema to be set",
            )
        })?;

        Ok(ActionCommit::new(
            vec![
                TableUpdate::AddSchema { schema },
                TableUpdate::SetCurrentSchema { schema_id: -1 },
            ],
            vec![],
        ))
    }
}
