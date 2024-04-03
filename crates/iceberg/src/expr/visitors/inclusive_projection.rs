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

use crate::expr::{BoundPredicate, Predicate};
use crate::spec::{PartitionField, PartitionSpecRef, SchemaRef};

pub(crate) struct InclusiveProjection {
    #[allow(dead_code)]
    table_schema: SchemaRef,
    partition_spec: PartitionSpecRef,
}

impl InclusiveProjection {
    pub(crate) fn new(table_schema: SchemaRef, partition_spec: PartitionSpecRef) -> Self {
        Self {
            table_schema,
            partition_spec,
        }
    }

    pub(crate) fn project(&self, predicate: &BoundPredicate) -> crate::Result<Predicate> {
        self.visit(predicate)
    }

    fn visit(&self, bound_predicate: &BoundPredicate) -> crate::Result<Predicate> {
        Ok(match bound_predicate {
            BoundPredicate::AlwaysTrue => Predicate::AlwaysTrue,
            BoundPredicate::AlwaysFalse => Predicate::AlwaysFalse,
            BoundPredicate::And(expr) => {
                let [left_pred, right_pred] = expr.inputs();
                self.visit(left_pred)?.and(self.visit(right_pred)?)
            }
            BoundPredicate::Or(expr) => {
                let [left_pred, right_pred] = expr.inputs();
                self.visit(left_pred)?.or(self.visit(right_pred)?)
            }
            BoundPredicate::Not(_) => {
                panic!("should not get here as NOT-rewriting should have removed NOT nodes")
            }
            bp => self.visit_bound_predicate(bp)?,
        })
    }

    fn visit_bound_predicate(&self, predicate: &BoundPredicate) -> crate::Result<Predicate> {
        let field_id = match predicate {
            BoundPredicate::Unary(expr) => expr.field_id(),
            BoundPredicate::Binary(expr) => expr.field_id(),
            BoundPredicate::Set(expr) => expr.field_id(),
            _ => {
                panic!("Should not get here as these branches handled in self.visit")
            }
        };

        // TODO: cache this?
        let mut parts: Vec<&PartitionField> = vec![];
        for partition_spec_field in &self.partition_spec.fields {
            if partition_spec_field.source_id == field_id {
                parts.push(partition_spec_field)
            }
        }

        parts.iter().fold(Ok(Predicate::AlwaysTrue), |res, &part| {
            Ok(
                if let Some(pred_for_part) = part.transform.project(&part.name, predicate)? {
                    res?.and(pred_for_part)
                } else {
                    res?
                },
            )
        })
    }
}
