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

pub type ActionElement<'a> = Box<dyn TransactionAction<'a>>;

pub(crate) trait TransactionAction<'a>: Sync {
    /// Apply the pending changes and return the uncommitted changes
    /// TODO is this even needed?
    fn apply(&mut self) -> Result<Option<TableUpdate>>;
    
    /// Commit the changes and apply the changes to the associated transaction
    fn commit(self) -> Result<Transaction<'a>>;
}

pub struct SetLocation<'a> {
    pub tx: Transaction<'a>,
    location: Option<String>,
}

impl<'a> SetLocation<'a> {
    pub fn new(tx: Transaction<'a>) -> Self {
        SetLocation { 
            tx,
            location: None
        }
    }
    
    pub fn set_location(mut self, location: String) -> Self {
        self.location = Some(location);
        self
    }
}

impl<'a> TransactionAction<'a> for SetLocation<'a> {
    fn apply(&mut self) -> Result<Option<TableUpdate>> {
        if self.location.is_none() {
            return Ok(None)
        }
        Ok(Some(TableUpdate::SetLocation { location: self.location.clone().unwrap() }))
    }
    
    fn commit(mut self) -> Result<Transaction<'a>> {
        let location = &mut self.apply()?;
        if location.is_none() {
            return Ok(self.tx)    
        }
        
        self.tx.apply(vec![location.clone().unwrap()], vec![])?;
        Ok(self.tx)
        // self.tx.actions.push(Box::new(self));
    }
}
