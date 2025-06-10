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

use std::any::Any;
use std::cmp::Ordering;
use std::sync::Arc;

use async_trait::async_trait;

use crate::TableUpdate::UpgradeFormatVersion;
use crate::spec::FormatVersion;
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableUpdate};

/// A transaction action to upgrade a table's format version.
///
/// This action is used within a transaction to indicate that the
/// table's format version should be upgraded to a specified version.
/// The location remains optional until explicitly set via [`set_format_version`].
pub struct UpgradeFormatVersionAction {
    format_version: Option<FormatVersion>,
}

impl UpgradeFormatVersionAction {
    /// Creates a new `UpgradeFormatVersionAction` with no version set.
    pub fn new() -> Self {
        UpgradeFormatVersionAction {
            format_version: None,
        }
    }

    /// Sets the target format version for the upgrade.
    ///
    /// # Arguments
    ///
    /// * `format_version` - The version to upgrade the table format to.
    ///
    /// # Returns
    ///
    /// Returns the updated `UpgradeFormatVersionAction` with the format version set.
    pub fn set_format_version(mut self, format_version: FormatVersion) -> Self {
        self.format_version = Some(format_version);
        self
    }
}

#[async_trait]
impl TransactionAction for UpgradeFormatVersionAction {
    fn as_any(self: Arc<Self>) -> Arc<dyn Any> {
        self
    }

    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let current_version = table.metadata().format_version();
        let updates: Vec<TableUpdate>;

        if let Some(format_version) = self.format_version {
            match current_version.cmp(&format_version) {
                Ordering::Greater => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot downgrade table version from {} to {}",
                            current_version, format_version
                        ),
                    ));
                }
                Ordering::Less => {
                    updates = vec![UpgradeFormatVersion { format_version }];
                }
                Ordering::Equal => {
                    // do nothing
                    updates = vec![];
                }
            }
        } else {
            // error
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "FormatVersion is not set for UpgradeFormatVersionAction!",
            ));
        }

        Ok(ActionCommit::new(updates, vec![]))
    }
}

mod tests {
    use crate::spec::FormatVersion;
    use crate::transaction::Transaction;
    use crate::transaction::action::ApplyTransactionAction;
    use crate::transaction::upgrade_format_version::UpgradeFormatVersionAction;

    #[test]
    fn test_upgrade_format_version() {
        let table = crate::transaction::tests::make_v1_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .upgrade_table_version()
            .set_format_version(FormatVersion::V2)
            .apply(tx)
            .unwrap();

        assert_eq!(tx.actions.len(), 1);

        let any = tx.actions[0].clone().as_any();
        let action = any.downcast_ref::<UpgradeFormatVersionAction>().unwrap();

        assert_eq!(action.format_version, Some(FormatVersion::V2));
    }
}
