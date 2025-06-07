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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::sync::Arc;

use async_trait::async_trait;

use crate::TableUpdate::UpgradeFormatVersion;
use crate::spec::FormatVersion;
use crate::table::Table;
use crate::transaction::Transaction;
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

/// TODO doc
pub type BoxedTransactionAction = Arc<dyn TransactionAction>;

/// TODO doc
#[async_trait]
pub trait TransactionAction: Sync + Send {
    /// Commit the changes and apply the changes to the transaction
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit>;
}

/// TODO doc
pub trait ApplyTransactionAction {
    /// TODO doc
    fn apply(self, tx: Transaction) -> Result<Transaction>;
}

impl<T: TransactionAction + 'static> ApplyTransactionAction for T {
    fn apply(self, mut tx: Transaction) -> Result<Transaction>
    where Self: Sized {
        tx.actions.push(Arc::new(self));
        Ok(tx)
    }
}

/// TODO doc
pub struct ActionCommit {
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
}

/// TODO doc
impl ActionCommit {
    /// TODO doc
    pub fn new(updates: Vec<TableUpdate>, requirements: Vec<TableRequirement>) -> Self {
        Self {
            updates,
            requirements,
        }
    }

    /// TODO doc
    pub fn take_updates(&mut self) -> Vec<TableUpdate> {
        take(&mut self.updates)
    }

    /// TODO doc
    pub fn take_requirements(&mut self) -> Vec<TableRequirement> {
        take(&mut self.requirements)
    }
}

/// TODO doc
pub struct UpdateLocationAction {
    location: Option<String>,
}

impl UpdateLocationAction {
    /// TODO doc
    pub fn new() -> Self {
        UpdateLocationAction { location: None }
    }

    /// TODO doc
    pub fn set_location(mut self, location: String) -> Self {
        self.location = Some(location);
        self
    }
}

#[async_trait]
impl TransactionAction for UpdateLocationAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        let updates: Vec<TableUpdate>;
        let requirements: Vec<TableRequirement>;
        if let Some(location) = self.location.clone() {
            updates = vec![TableUpdate::SetLocation { location }];
            requirements = vec![];
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Location is not set for UpdateLocationAction!",
            ));
        }

        Ok(ActionCommit::new(updates, requirements))
    }
}

/// TODO doc
pub struct UpgradeFormatVersionAction {
    format_version: Option<FormatVersion>,
}

impl UpgradeFormatVersionAction {
    /// TODO doc
    pub fn new() -> Self {
        UpgradeFormatVersionAction {
            format_version: None,
        }
    }

    /// TODO doc
    pub fn set_format_version(mut self, format_version: FormatVersion) -> Self {
        self.format_version = Some(format_version);
        self
    }
}

#[async_trait]
impl TransactionAction for UpgradeFormatVersionAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let current_version = table.metadata().format_version();
        let updates: Vec<TableUpdate>;
        let requirements: Vec<TableRequirement>;

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
                    requirements = vec![];
                }
                Ordering::Equal => {
                    // do nothing
                    updates = vec![];
                    requirements = vec![];
                }
            }
        } else {
            // error
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "FormatVersion is not set for UpgradeFormatVersionAction!",
            ));
        }

        Ok(ActionCommit::new(updates, requirements))
    }
}

/// TODO doc
pub struct UpdatePropertiesAction {
    updates: HashMap<String, String>,
    removals: HashSet<String>,
}

impl UpdatePropertiesAction {
    /// TODO doc
    pub fn new() -> Self {
        UpdatePropertiesAction {
            updates: HashMap::default(),
            removals: HashSet::default(),
        }
    }

    /// TODO doc
    pub fn set(mut self, key: String, value: String) -> Self {
        assert!(!self.removals.contains(&key));
        self.updates.insert(key, value);
        self
    }

    /// TODO doc
    pub fn remove(mut self, key: String) -> Self {
        assert!(!self.updates.contains_key(&key));
        self.removals.insert(key);
        self
    }
}

#[async_trait]
impl TransactionAction for UpdatePropertiesAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        let updates: Vec<TableUpdate> = vec![
            TableUpdate::SetProperties {
                updates: self.updates.clone(),
            },
            TableUpdate::RemoveProperties {
                removals: self.removals.clone().into_iter().collect::<Vec<String>>(),
            },
        ];
        let requirements: Vec<TableRequirement> = vec![];

        Ok(ActionCommit::new(updates, requirements))
    }
}
