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

use crate::spec::StatisticsFile;
use crate::transaction::Transaction;
use crate::{Error, TableUpdate};

pub struct UpdateStatistics<'a> {
    tx: Transaction<'a>,
    updates: Vec<TableUpdate>,
}

impl<'a> UpdateStatistics<'a> {
    pub fn new(tx: Transaction<'a>) -> Self {
        UpdateStatistics {
            tx,
            updates: Vec::new(),
        }
    }

    pub fn set_statistics(&mut self, statistics: StatisticsFile) -> Result<&mut Self, Error> {
        self.updates.push(TableUpdate::SetStatistics { statistics });

        Ok(self)
    }

    pub fn remove_statistics(&mut self, snapshot_id: i64) -> Result<&mut Self, Error> {
        self.updates
            .push(TableUpdate::RemoveStatistics { snapshot_id });

        Ok(self)
    }

    pub fn apply(mut self) -> Result<Transaction<'a>, Error> {
        self.tx.apply(self.updates, vec![])?;

        Ok(self.tx)
    }
}
