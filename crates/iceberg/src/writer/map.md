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

# map.md — crates/iceberg/src/writer/

## Purpose

The file-writing layer (Java `data/` writers): Arrow batches in → data / delete files out, as
`DataFile`s ready for a transaction action to commit. Layered: **partitioning** writers route rows →
**base** writers add Iceberg semantics → **file** writers do the physical format IO.

## Contents

| Path | What it does |
|---|---|
| `mod.rs` | the `IcebergWriter` / `IcebergWriterBuilder` traits + layering docs |
| `base_writer/data_file_writer.rs` | plain data files |
| `base_writer/equality_delete_writer.rs` | equality-delete files (equality ids → projected schema) |
| `base_writer/position_delete_writer.rs` | position-delete files: `file_path` (id 2147483546) + `pos` (id 2147483545), `content(PositionDeletes)`, **write-as-given** (no sorting/merging — Java-faithful) |
| `base_writer/deletion_vector_writer.rs` | deletion vectors (V3 Puffin DVs, Java `BaseDVFileWriter`): accumulate `delete(path, pos, partition)`, `close()` → ONE Puffin file, one `deletion-vector-v1` blob per referenced data file (sorted-path order), `DeleteFile` per `createDV` L145-159; serialization in `../delete_vector.rs` (`serialize_deletion_vector_v1`, byte-identical to Java incl. run containers). Previous-deletes merge + commit path deferred to D3 |
| `file_writer/parquet_writer.rs` | Parquet IO + per-column metrics collection (the bounds the evaluators later prune on) |
| `file_writer/rolling_writer.rs` | size-based file rolling |
| `file_writer/location_generator.rs` | file naming/placement |
| `partitioning/fanout_writer.rs` | concurrent multi-partition fanout |
| `partitioning/clustered_writer.rs` | sorted-input single-partition-at-a-time |
| `partitioning/unpartitioned_writer.rs` | passthrough |

Parity gaps live in the GAP_MATRIX (deletion-vector COMMIT path — the writer landed in D2, ORC/Avro
data files, sort-order-aware writing).

## I want to...

| I want to... | go to |
|---|---|
| Write data for a partitioned table | `partitioning/` (fanout for unsorted, clustered for sorted input) |
| Produce position deletes for `RowDelta` | `base_writer/position_delete_writer.rs` → commit via [../transaction/map.md](../transaction/map.md) `row_delta` |
| Touch metrics written into files | `file_writer/parquet_writer.rs` — these bounds feed the metrics evaluators; exact-byte sensitive |
| Add a new physical format | `file_writer/` behind the `FileWriter` trait |

## Pointers

- **Up:** [crates/iceberg/src/](..) · **Related:** `../spec/` (`DataFile`/manifest types),
  [../transaction/map.md](../transaction/map.md) (commits what this produces),
  `../arrow/` (schema conversion the writers rely on)

## Debug

### Known failure modes

| Symptom | Likely cause |
|---|---|
| Java can't read a Rust-written delete file | Field-id mismatch — position-delete columns carry the reserved ids (2147483546/2147483545); equality deletes must carry the equality ids of the *projected* schema |
| Wrong field ids tolerated by Rust but not Java (or vice versa) | The Rust pos-delete READER matches by column POSITION (col 0 = file_path, col 1 = pos — it never reads field ids); JAVA matches by field id. Both contracts must hold: build the schema from `delete_file_path_field()`/`delete_file_pos_field()` in canonical order |
| Pruning broken on Rust-written files | Metrics/bounds written by `parquet_writer.rs` diverge from Java `Conversions.toByteBuffer` encoding — exact-byte fixture territory |
| Rows land in the wrong partition file | Partition-value computation in the partitioning writer vs the spec's transforms — check transform application, not the writer plumbing |

### First checks

- Round-trip the file through the Rust reader first (write → scan → compare); if that passes but
  Java fails, it's an encoding/field-id parity bug → go to the oracle.

### Escalate to

- Commit-side issues → [../transaction/map.md#debug](../transaction/map.md#debug).
- Cross-engine readability → [dev/java-interop/map.md#debug](../../../../dev/java-interop/map.md#debug).
