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

# map.md — crates/iceberg/src/scan/

## Purpose

Table-scan planning and execution (Java `SnapshotScan` / `DataTableScan` / `BaseFileScanTask`):
snapshot → manifest list → manifest pruning → `FileScanTask` stream → Arrow record batches (the
Arrow read itself lives in `../arrow/`). Includes merge-on-read delete application inputs, residual
propagation, and opt-in scan-metrics reporting.

## Contents

| File | What it does |
|---|---|
| `mod.rs` | `TableScanBuilder` (filter / select / snapshot_id / use_ref / concurrency limits / row-group filtering / row selection / `with_metrics_reporter`) + `TableScan::plan_files` + `to_arrow` |
| `context.rs` | `PlanContext` / `ManifestFileContext` / `ManifestEntryContext`: per-manifest evaluator caches (manifest evaluator, partition filter, residual evaluator built per manifest), `into_file_scan_task` (evaluates the **partition-reduced residual** per file, Java `residuals.residualFor(file.partition())`) |
| `task.rs` | `FileScanTask` (+ delete-file attachments for merge-on-read) |
| `cache.rs` | object cache plumbing for manifest/manifest-list reads |
| `incremental.rs` | incremental append scan (parity gap: changelog/batch scans missing) |
| `metrics_collector.rs` | `ScanMetricsCollector` (Arc'd `AtomicI64`) — opt-in; report emitted ONCE on full stream consumption |

## I want to...

| I want to... | go to |
|---|---|
| Change manifest/file pruning | `context.rs` (evaluator construction + the prune point) and [../expr/visitors/map.md](../expr/visitors/map.md) for the evaluators themselves |
| Touch residual handling | `context.rs` — the residual is built per manifest from the file's spec, evaluated per partition, then **bound back to the snapshot schema** |
| Add a scan metric | `metrics_collector.rs` + the count sites in `mod.rs`/`context.rs`; populated-vs-`None` counters are documented in `../metrics/mod.rs` |
| Understand merge-on-read reads | `task.rs` delete attachments → applied in `../arrow/` (delete_file_index / delete_vector at crate root) |

## Pointers

- **Up:** [crates/iceberg/src/](..) · **Related:** [../expr/visitors/map.md](../expr/visitors/map.md)
  (evaluators), `../arrow/` (batch reading + delete application), `../metrics/` (report model),
  [../../tests/map.md](../../tests/map.md) (`interop_scan_exec.rs` — the data-level MoR interop)

## Debug

### Known failure modes

| Symptom | Likely cause |
|---|---|
| Task carries the full snapshot filter instead of a reduced one | The residual must be evaluated against the file's partition in `into_file_scan_task` — mutation-pinned; check the per-manifest evaluator wiring |
| Identity-partition constants wrong / type errors in record batches | The `PartitionUtil.constantsMap` constant-materialization path is **deliberately deferred** — `record_batch_transformer` has latent type bugs; the task's `partition_spec` stays `None` |
| Metrics report emitted on a dropped stream / per task | The report fires ONCE on full consumption (`None` from the stream); early-drop emits NOTHING (pinned by test) |
| No-reporter path changed | With no reporter there must be **no collector, no timer, no wrapper** — the plan path is byte-unchanged (structural test pins this) |

### First checks

- Reproduce with a single-manifest fixture; check which layer prunes (manifest evaluator vs
  partition filter vs metrics evaluator) before touching code.
- For wrong-row results on MoR tables, confirm whether the bug is planning (here) or delete
  application (`../arrow/`) by scanning without deletes.

### Escalate to

- Evaluator semantics → [../expr/visitors/map.md#debug](../expr/visitors/map.md#debug).
- Cross-engine result divergence → the scan-exec interop harness,
  [dev/java-interop/map.md#debug](../../../../dev/java-interop/map.md#debug).
