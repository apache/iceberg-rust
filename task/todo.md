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

# Plan / Todo

The current plan for in-flight work. The operating manuals ([skills/](../skills/)) require this file
to be written **before** any non-trivial change and kept current as work proceeds.

How to use it (see the manuals' §1):

- Write a 3–7 bullet plan here before writing code.
- Flip `[ ]` → `[x]` as items complete; add a one-sentence "what changed and why" per step.
- Add indented sub-bullets when a step reveals unexpected complexity.
- Leave an `Outcome:` / `Done:` note when the work lands.

---


> **Archival log.** Last pass: 2026-06-12 (pass 4 — post-Wave-5 union, 680 lines) →
> [todo-archive/2026-06_wave5.md](todo-archive/2026-06_wave5.md) (8 spent Wave-5 increment
> sections; the ACTIVE queue refreshed in place). Prior passes: 2026-06-12 (pass 3 — 2,358
> lines → the wave3-wave4 file), 2026-06-11 (pass 2), 2026-06-09 (pass 1). Procedure:
> [skills/compaction.md](../skills/compaction.md) §Todo Archival.

## ACTIVE (2026-06-12): I1 — theta-blob interop (LANDED)

**Plan (pre-code, 3–7 bullets per manual §1):**
- [x] **Step 1 (Oracle Java):** Added `ThetaBlobOracle` static inner class to `InteropOracle.java` with `generate-interop-theta`, `verify-interop-theta`, `generate-interop-theta-java-to-rust`. Added datasketches-java/memory deps to pom.xml.
- [x] **Step 2 (Oracle dispatch):** Wired all three modes into the InteropOracle main switch via `-Dinterop.theta.dir`.
- [x] **Step 3 (Rust test):** `crates/iceberg/tests/interop_theta.rs` — `test_theta_gen` (GEN: real 2-file table, `ComputeTableStats::execute`, `rust_stats.puffin` + `rust_stats_expected.json`) + `test_theta_d2_rust_reads_java_puffin` (D2: `java_stats.puffin` via `PuffinReader`+`CompactThetaSketch::deserialize`). Fixed: `BlobMetadata` fields are private (use methods); `FileIO::from_path` doesn't exist (use `FileIO::new_with_fs()`).
- [x] **Step 4 (Run script):** `dev/java-interop/run-interop-theta.sh` — 6-step chain ×2, sabotage battery 4 closed (6a truncate puffin; 6b Puffin-footer-parsed SOURCE corrupt; 6c truncate Java puffin; 6d corrupt ndv JSON). Puffin footer structure: `[data][footer_magic(4)][footer_json(N)][payload_len(4 LE u32)][flags(4)][trailing_magic(4)]` with blob offsets absolute from file start.
- [x] **Step 5 (GAP_MATRIX update):** ComputeTableStats row updated, I1 interop noted, pipe-count audit clean (all 61 `^|` rows have 5 pipes).
- [x] **Step 6 (Gate):** typos/fmt/clippy/lib-tests/run-interop-theta.sh/taplo all PASS. 2210 lib tests pass.
- [x] **Step 7 (journal):** Lessons appended to task/lessons.md.

**Outcome (2026-06-12):** I1 theta-blob interop COMPLETE — bidirectional, chain ×2, sabotage 4 closed. `ComputeTableStats` is now fully proven through end-to-end Java/Rust interop.

## ACTIVE (2026-06-12): I2 — view metadata interop (LANDED)

**Plan (pre-code, 7 bullets per manual §1):**
- [x] **Step 1 (Java ViewOracle — D1 generate):** Add `ViewOracle` static inner class to
  `InteropOracle.java`. `generate` mode: use `InMemoryCatalog.buildView(ident)` to create a
  view with schema + 2 SQL representations (spark+trino dialects), then `replace()` with a
  DIFFERENT SQL so `reuseOrCreateNewViewVersionId` creates version 2 (distinct — not identical).
  Write `ViewMetadataParser.toJson(metadata)` to `rust_view_metadata.json`. Emit companion
  `expected.json` (view-uuid, format-version, location, schema field list, current-version-id,
  version count, version-log count, all per-version field values). Dispatch via
  `-Dinterop.view.dir` + `generate-interop-view`. Key fix: cast `View` → `BaseView` for
  `operations().current()`.
- [x] **Step 2 (Java ViewOracle — D1 verify + D2 generate):** `verify-interop-view` mode:
  reads `rust_view_metadata.json` (Rust-written) via `ViewMetadataParser.fromJson`, asserts
  field-by-field against values Rust emitted into `rust_view_expected.json`. `generate-java-to-rust`
  mode: Java builds a view metadata object via `InMemoryCatalog`, writes via
  `ViewMetadataParser.toJson` to `java_view_metadata.json`, emits `java_view_expected.json`.
  Dispatch the three modes from the main switch.
- [x] **Step 3 (Rust interop test):** `crates/iceberg/tests/interop_view.rs`. Three tests:
  `test_view_gen` (D1 GEN: Rust creates view + ReplaceViewVersionAction commit → 2 versions,
  writes `rust_view_metadata.json` via `ViewMetadata::write_to` + `rust_view_expected.json`);
  `test_view_d2_rust_reads_java` (D2: reads `java_view_metadata.json` via
  `ViewMetadata::read_from`, asserts all fields vs `java_view_expected.json`);
  `test_view_tolerance_controls` (control: permuted-field-order JSON still parses on Rust
  side; empty-properties omission tolerated both ways). Fixed clippy::never_loop in
  `first_sql_repr` helper.
- [x] **Step 4 (Chain script):** `dev/java-interop/run-interop-view.sh` — 6-step chain ×2:
  (1) reset tmp; (2) Rust GEN test writes `rust_view_metadata.json`; (3) Java verify-interop-view
  reads Rust metadata, 0 failures sentinel; (4) Java generate-java-to-rust writes
  `java_view_metadata.json`; (5) Rust D2 reads Java metadata; (6) sabotage battery 5 closed
  (6a alter SQL→Java D1 FAIL; 6b drop default-namespace→Rust parse FAIL; 6c dangling
  current-version-id=99→Rust FAIL; 6d alter SQL in java metadata→Rust assert FAIL; 6e tolerance
  control both sides PASS).
- [x] **Step 5 (Tolerance controls):** The field-order control and empty-properties control
  documented in `test_view_tolerance_controls` and confirmed in the chain script (6e). Java's
  omit-empty-properties (read by Rust) and Rust's always-emit-empty (read by Java) are pinned
  as COSMETIC ONLY — byte-level byte-order is a NEXT-WAVE item.
- [x] **Step 6 (GAP_MATRIX update):** Re-audited ViewCatalog row. Row stays 🟡 because: (a)
  MemoryCatalog `update_view` has no base-location CAS (O1 on other branch); (b) Glue/S3Tables
  views need credentialed sprint; (c) SessionCatalog/LockManager separate. Residue text updated
  precisely; I2 interop landing noted. Pipe-count audit clean (61 rows all 5 pipes).
- [x] **Step 7 (Gate + journal):** `typos` PASS, `cargo fmt --check` PASS, `cargo clippy` PASS
  (0 warnings), `cargo test -p iceberg --lib` 2210 tests, 0 failures (exact baseline). Plus
  run-interop-view.sh ×2 green, taplo check (pre-existing 4 failures, 0 new). Lessons appended.

**Outcome (2026-06-12):** I2 view metadata interop COMPLETE — bidirectional, chain ×2, sabotage
5 closed. `ViewMetadata::read_from`/`write_to` proven against Java `ViewMetadataParser` through
real catalog operations (`MemoryCatalog` + `InMemoryCatalog`). Wire-format field-order divergence
pinned as cosmetic-only; byte-exact view round-trip is next-wave.

## ACTIVE (2026-06-12): I3 — data-level WAP interop (LANDED)

**Plan (pre-code, 7 bullets per manual §1):**
- [x] **Step 1 (Java WapDataOracle — D1 generate + D2 verify):** Added `WapDataOracle` static inner
  class to `InteropOracle.java`. S-replay order: base→stage→bump so staged.parent≠head → REPLAY.
  Uses REAL parquet via `MergeAppendDataOracle.writePartitionedDataFile`. Bump row id=99 is a real
  data fast-append (not updateProperties which doesn't create a snapshot — key lesson). verifyRustTable
  uses `Files.createTempDirectory` per call to avoid v0.metadata.json collision on repeated runs.
- [x] **Step 2 (Rust interop test):** `crates/iceberg/tests/interop_wap_data.rs`. Two tests:
  `test_wap_data_gen_rust_writes_staged_table` (D1 GEN: S-replay order, asserts staged_id≠current
  after stage_only(), writes final.metadata.json + rust_staged_snapshot_id.json);
  `test_wap_data_d2_rust_reads_java_cherrypick_table` (D2: reads java_cherrypick_table, 8 rows,
  WAP semantics, partition routing a/b pinned).
- [x] **Step 3 (Chain script):** `dev/java-interop/run-interop-wap-data.sh` — 7-step chain ×2
  green. Step 5 sentinel "verify-interop-wap-data: 0 failures", Step 7 sabotage battery 4 closed.
- [x] **Step 4 (Sabotage battery):** 4 closed: 7a STRUCTURAL truncate metadata; 7b STRUCTURAL bogus
  manifest-list path; 7c SEMANTIC corrupt wap.id w1→w1-CORRUPTED (cherry-pick emits wrong
  published-wap-id → pin fires); 7d STRUCTURAL remove staged snapshot entirely.
- [x] **Step 5 (Dedup):** deferred — inflates scope; recorded as next-wave residue in GAP_MATRIX.
- [x] **Step 6 (GAP_MATRIX update):** cherry-pick/WAP row residue updated; I3 landing noted; pipe-count audit clean.
- [x] **Step 7 (Gate + journal):** typos/fmt/clippy/lib-tests (2210/0)/chain×2/taplo all PASS. Lessons appended.

**Outcome (2026-06-12):** I3 data-level WAP interop COMPLETE — bidirectional REPLAY-shape, REAL
parquet, 8-row fixture (base 4 + bump 1 + staged 3), chain ×2, sabotage 4 closed. Key lessons:
S-replay order (stage before bump); updateProperties does not create a snapshot; LocalTableOperations
v0.metadata.json collision fixed with Files.createTempDirectory.

## ACTIVE (2026-06-12): Near-full-parity direction — open queue (planning record)

Directive (user, 2026-06-11): run this fork's Roadmap to **almost the full 1:1 Java replacement**.
Waves 3–5 landed PRs #28–#41 (write-engine closeout, maintenance actions end-to-end incl.
ComputeTable/PartitionStats + the iceberg-sketches crate, the variant arc, stage_only + WAP,
views end-to-end, and TEN interop chains). Statuses live ONLY in the GAP_MATRIX.

- [ ] **Named next-wave interop items:** the theta-blob interop (Java reads our
      apache-datasketches-theta-v1 puffin blobs / ndv — the ComputeTableStats row's last residue);
      view interop (the wire field-order divergence is read-tolerant both ways — byte-order
      comparison is the open half); variant file-level I/O + interop (the parquet-crate boundary);
      data-level WAP interop (the cherrypick row's named residue).
- [ ] **Partition-stats residue:** the INCREMENTAL compute path; time/uuid/fixed/binary partition
      values in stats files (loud errors today).
- [ ] **The shared-seam concurrency-parity increment** (U1 reviewer): no location-CAS on
      MemoryCatalog update_table/update_view (stale second commit last-write-wins); the SQL
      catalog has it per-catalog — port the posture to the shared seam + MemoryCatalog.
- [ ] **Reported divergences awaiting their increments:** manifest-list carried-vs-new entry ORDER
      (cosmetic, readers reconcile); Java lowercases ALL type names on parse (Rust exact-lowercase);
      Rust sort orders unbound on metadata parse; struct map-value Avro record naming hazard.
- [ ] **Scheduled with the user:** real-catalog (Glue + S3 Tables) hardening — needs credentials
      (now incl. the Glue/S3Tables VIEW surface).
- [ ] **Opus-queue (post-handoff or parallel):** ORC/Avro breadth, SessionCatalog + LockManager,
      incremental-scan interop, scan completion (BatchScan / CDC / split planning), encryption
      (frontier-grade — the 2026-06-22 window).

## Carried-forward open items (full context in todo-archive/)

**Explicitly NOT decided:** the "platform cut line" through the GAP_MATRIX (which rows block the
user's trading platform vs continuous-parity backlog, incl. re-ordering maintenance actions ahead of
Phase-4 format exotica) was proposed but is an **open user decision — do not assume it.**
  _RESOLVED-AS-TABLED 2026-06-11: the user tabled the DataFusion/RePark direction and redirected
  the fork to near-full 1:1 Java parity — recorded in Roadmap.md (decision record item 5 + the
  re-sequenced headline areas). Originating narrative:
  [todo-archive/2026-06_ops-hardening.md](todo-archive/2026-06_ops-hardening.md)._


## Archived increment narratives

Completed-increment narratives moved verbatim out of this file (see [skills/compaction.md](../skills/compaction.md)
§Todo Archival). Not session-start reading — grep/open on demand.

- [todo-archive/phase1.md](todo-archive/phase1.md) — Phase 1 spec & metadata completeness (schema /
  partition / snapshot evolution + spec-read robustness).
- [todo-archive/phase2.md](todo-archive/phase2.md) — Phase 2 write engine (write actions + the
  concurrent-commit conflict-validation cluster, incl. the merged write-validation PR #9).
- [todo-archive/phase3.md](todo-archive/phase3.md) — Phase 3 scan parity (residual evaluation,
  inspection tables, scan-metrics emission, and inspection / scan-execution interop).
- [todo-archive/2026-06_ops-hardening.md](todo-archive/2026-06_ops-hardening.md) — the doc-infrastructure / hardening meta-sprints (not phase work).
- [todo-archive/2026-06_wave3-wave4-overnight.md](todo-archive/2026-06_wave3-wave4-overnight.md) — Waves 3–4 + the overnight session (PRs #25–#37; pass-scoped).
- [todo-archive/2026-06_wave5.md](todo-archive/2026-06_wave5.md) — Wave 5 (PRs #39–#41; pass-scoped).
- Index: [todo-archive/map.md](todo-archive/map.md).
