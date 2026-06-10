<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# map.md — crates/iceberg/src/expr/visitors/

## Purpose

Predicate visitors and evaluators over bound expressions (Java `api/.../expressions/`): the pruning
and projection machinery used by scan planning, conflict validation, and residual evaluation. All
modules are `pub(crate)`.

## Contents

| File | Java analogue | Used for |
|---|---|---|
| `bound_predicate_visitor.rs` / `predicate_visitor.rs` | `ExpressionVisitors` | visitor traits |
| `manifest_evaluator.rs` | `ManifestEvaluator` | prune whole manifests via partition-summary bounds |
| `expression_evaluator.rs` | `Evaluators` | evaluate a bound predicate against a concrete struct (e.g. a partition value) |
| `inclusive_metrics_evaluator.rs` | `InclusiveMetricsEvaluator` | "might this FILE contain matching rows?" — file pruning AND write-side conflict detection (`first_conflicting_file`) |
| `strict_metrics_evaluator.rs` | `StrictMetricsEvaluator` | "do ALL rows match?" |
| `inclusive_projection.rs` / `strict_projection.rs` | `Projections` | row filter → partition-space predicate via `Transform::project`/`strict_project` |
| `residual_evaluator.rs` | `ResidualEvaluator` | partial-evaluate a row filter against partition values → residual (strict-true ⇒ `AlwaysTrue`, inclusive-false ⇒ `AlwaysFalse`, else keep) |
| `rewrite_not.rs` | `RewriteNot` | NOT-elimination pre-pass (evaluators assume NOT-free input) |
| `page_index_evaluator.rs` / `row_group_metrics_evaluator.rs` | (parquet) | Parquet-level pruning in the arrow reader |

## I want to...

| I want to... | go to |
|---|---|
| Prune at the right granularity | manifest → `manifest_evaluator`; file → `inclusive_metrics_evaluator`; row-group/page → the two parquet evaluators |
| Add a write-side conflict check | reuse `inclusive_metrics_evaluator` via `transaction/snapshot.rs::first_conflicting_file` — do not re-implement the metrics decision |
| Understand residuals | `residual_evaluator.rs` (the Javadoc `day(ts)` 4-case example is a test) |

## Pointers

- **Up:** `../` (expr: predicate trees + binding) · **Related:**
  [../../scan/map.md](../../scan/map.md) (consumer), `../../transform/` (`project`/`strict_project`),
  [../../transaction/map.md](../../transaction/map.md) (conflict validation consumer)

## Debug

### Known failure modes

| Symptom | Likely cause |
|---|---|
| Over- or under-pruning files | Inclusive vs strict confusion — inclusive answers "might match" (prune only on definite NO), strict answers "all match"; swapping them is a known mutation target |
| Missing-metrics file behavior wrong | A file with absent counts/bounds must be treated as "might match" (cannot prune) — check the `None` arms |
| Residual keeps/drops the wrong branch | The strict-projection-true / inclusive-projection-false short-circuits are order-sensitive; the partition-value substitution must use the file's own spec |
| Evaluator panics on NOT | Run `rewrite_not` first — evaluators assume NOT-free bound predicates |

### First checks

- Write a single-file reproduction pinning the exact bound predicate + metrics map; the unit-test
  fixtures in each module show the shape.
- Cross-check the truth table against the Java class named in Contents — line-level parity is the
  contract here.

### Escalate to

- Binding/term issues → `../` (expr root). Transform projection bugs → `../../transform/`.
