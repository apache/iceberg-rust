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

use std::collections::HashMap;

use super::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
use super::strict_metrics_evaluator::StrictMetricsEvaluator;
use crate::expr::BoundPredicate;
use crate::spec::{DataFile, PartitionSpec, Schema, Struct};
use crate::Error;

/// An evaluator that checks whether rows in a file may/must match a given expression
/// this class first partially evaluates the provided expression using the partition value
/// and then checks the remaining part of the expression using metrics evaluators.
#[allow(dead_code)]
struct PartitionAndMetricsEvaluator<'a> {
    schema: Schema,
    metrics_evaluator: HashMap<Struct, (InclusiveMetricsEvaluator<'a>, StrictMetricsEvaluator<'a>)>,
}

// TODO: Implement residual visitor
#[allow(dead_code)]
impl<'a> PartitionAndMetricsEvaluator<'a> {
    fn new(schema: Schema, _partition_spec: PartitionSpec, _predicate: BoundPredicate) -> Self {
        PartitionAndMetricsEvaluator {
            schema,
            metrics_evaluator: HashMap::new(),
        }
    }

    // Retrieve cached `InclusiveMetricsEvaluator` and `StrictMetricsEvaluator`
    // by partition.
    fn get_metrics_evaluator<'b>(
        &'b mut self,
        data_file: &'b DataFile,
    ) -> &'b mut (InclusiveMetricsEvaluator<'a>, StrictMetricsEvaluator<'a>)
    where
        'b: 'a,
    {
        let partition = data_file.partition();

        if !self.metrics_evaluator.contains_key(partition) {
            let inclusive = InclusiveMetricsEvaluator::new(data_file);
            let strict = StrictMetricsEvaluator::new(data_file);
            self.metrics_evaluator
                .insert(partition.clone(), (inclusive, strict));
        }

        self.metrics_evaluator.get_mut(partition).unwrap()
    }

    pub fn rows_might_match<'b>(
        &'b mut self,
        filter: &'b BoundPredicate,
        data_file: &'a DataFile,
    ) -> Result<bool, Error>
    where
        'b: 'a,
    {
        let (inclusive, _) = self.get_metrics_evaluator(data_file);
        inclusive.evaluate(filter, false)
    }

    pub fn rows_must_match<'b>(
        &'b mut self,
        filter: &'b BoundPredicate,
        data_file: &'a DataFile,
    ) -> Result<bool, Error>
    where
        'b: 'a,
    {
        let (_, strict) = self.get_metrics_evaluator(data_file);
        strict.evaluate(filter)
    }
}
