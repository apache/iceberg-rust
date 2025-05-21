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
        Self {
            tx,
            updates: Vec::new(),
        }
    }

    pub fn set_statistics(mut self, statistics: StatisticsFile) -> Result<Self, Error> {
        self.updates.push(TableUpdate::SetStatistics { statistics });

        Ok(self)
    }

    pub fn remove_statistics(mut self, snapshot_id: i64) -> Result<Self, Error> {
        self.updates
            .push(TableUpdate::RemoveStatistics { snapshot_id });

        Ok(self)
    }

    pub fn apply(mut self) -> Result<Transaction<'a>, Error> {
        self.tx.apply(self.updates, vec![])?;

        Ok(self.tx)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::TableUpdate;
    use crate::spec::{BlobMetadata, StatisticsFile};
    use crate::transaction::Transaction;
    use crate::transaction::tests::make_v2_table;

    #[test]
    fn test_update_statistics() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let statistics_file_1 = StatisticsFile {
            snapshot_id: 3055729675574597004i64,
            statistics_path: "s3://a/b/stats.puffin".to_string(),
            file_size_in_bytes: 413,
            file_footer_size_in_bytes: 42,
            key_metadata: None,
            blob_metadata: vec![BlobMetadata {
                r#type: "ndv".to_string(),
                snapshot_id: 3055729675574597004i64,
                sequence_number: 1,
                fields: vec![1],
                properties: HashMap::new(),
            }],
        };

        let statistics_file_2 = StatisticsFile {
            snapshot_id: 3366729675595277004i64,
            statistics_path: "s3://a/b/stats.puffin".to_string(),
            file_size_in_bytes: 413,
            file_footer_size_in_bytes: 42,
            key_metadata: None,
            blob_metadata: vec![BlobMetadata {
                r#type: "ndv".to_string(),
                snapshot_id: 3366729675595277004i64,
                sequence_number: 1,
                fields: vec![1],
                properties: HashMap::new(),
            }],
        };

        // set stats1
        let tx = tx
            .update_statistics()
            .set_statistics(statistics_file_1.clone())
            .unwrap()
            .apply()
            .unwrap();

        let TableUpdate::SetStatistics { statistics } = tx.updates.get(0).unwrap().clone() else {
            panic!("The update should be a TableUpdate::SetStatistics!");
        };
        assert_eq!(statistics, statistics_file_1);

        // start a new update, remove stat1 and set stat2
        let tx = tx
            .update_statistics()
            .remove_statistics(3055729675574597004i64)
            .unwrap()
            .set_statistics(statistics_file_2.clone())
            .unwrap()
            .apply()
            .unwrap();

        assert_eq!(tx.updates.len(), 3);
        let TableUpdate::RemoveStatistics { snapshot_id } = tx.updates.get(1).unwrap().clone()
        else {
            panic!("The update should be a TableUpdate::RemoveStatistics!");
        };
        assert_eq!(snapshot_id, 3055729675574597004i64);

        let TableUpdate::SetStatistics { statistics } = tx.updates.get(2).unwrap().clone() else {
            panic!("The update should be a TableUpdate::SetStatistics!");
        };
        assert_eq!(statistics, statistics_file_2);
    }
}
