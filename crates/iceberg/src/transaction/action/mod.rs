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

use std::sync::Arc;

use async_trait::async_trait;

use crate::transaction::Transaction;
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

/// TODO doc
pub type BoxedTransactionAction = Arc<dyn TransactionAction>;

/// TODO doc
#[async_trait]
pub trait TransactionAction: Sync + Send {
    /// Commit the changes and apply the changes to the transaction
    async fn commit(self: Arc<Self>, tx: &mut Transaction) -> Result<()>;
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
    async fn commit(self: Arc<Self>, tx: &mut Transaction) -> Result<()> {
        let updates: Vec<TableUpdate>;
        let requirements: Vec<TableRequirement>;
        if let Some(location) = self.location.clone() {
            updates = vec![TableUpdate::SetLocation { location }];
            requirements = vec![];
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Location is not set for SetLocation!",
            ));
        }

        tx.actions.push(self);

        tx.apply(updates, requirements)
    }
}
