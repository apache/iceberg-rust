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

use crate::transaction::Transaction;
use crate::{Result, TableUpdate};

pub type PendingAction = Box<dyn TransactionAction>;

pub(crate) trait TransactionAction: Sync {
    /// Commit the changes and apply the changes to the transaction,
    /// return the transaction with the updated current_table
    fn commit(self, tx: Transaction) -> Result<Transaction>;
}

pub struct SetLocation {
    pub location: Option<String>,
}

impl SetLocation {
    pub fn new() -> Self {
        SetLocation { location: None }
    }

    pub fn set_location(mut self, location: String) -> Self {
        self.location = Some(location);
        self
    }
}

impl TransactionAction for SetLocation {
    fn commit(self, mut tx: Transaction) -> Result<Transaction> {
        if let Some(location) = self.location.clone() {
            tx.apply(vec![TableUpdate::SetLocation { location }], vec![])?;
        }
        tx.actions.push(Box::new(self));

        Ok(tx)
    }
}
