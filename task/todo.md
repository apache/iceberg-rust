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


> **Archival log.** Last pass: 2026-06-12 (size trigger — 2,358 lines; pass 3) →
> [todo-archive/2026-06_wave3-wave4-overnight.md](todo-archive/2026-06_wave3-wave4-overnight.md)
> (24 sections: 2 kept live, 22 archived to the one pass-scoped file incl. the superseded wave
> planning section; open items lifted into the fresh ACTIVE section). Prior passes: 2026-06-11
> (pass 2 — 1,381 lines → phase files + ops-hardening) and 2026-06-09 (pass 1 — 4,344 lines →
> phase files). Procedure: [skills/compaction.md](../skills/compaction.md) §Todo Archival.
> Archives are not read by default.

## ACTIVE (2026-06-12): Z2 — multi-spec metadata-level interop fixture (BUILDER Sonnet, wt-interop3)

**Structure choice:** SIBLING script `dev/java-interop/run-interop-multi-spec.sh` (separate from
`run-interop-write-actions.sh`) so the new multi-spec steps do not disturb the existing metadata
chains. New Rust test `crates/iceberg/tests/interop_multi_spec.rs`.

**Single multi-spec commit constructibility verdict:**
CONSTRUCTIBLE on BOTH sides. Java `newFastAppend().appendFile(f0).appendFile(f1).commit()` where
f0 carries spec_id=0 partition and f1 carries spec_id=1 partition routes to `newDataFilesBySpec`
(a `Map<Integer, DataFileSet>`) in `MergingSnapshotProducer.add()` which `newDataFilesAsManifests`
iterates to produce one manifest per spec group — bytecode-verified. Rust `fast_append().
add_data_files(vec![f0, f1])` routes through `group_files_by_spec` → one writer per spec group
→ two manifests with different `partition_spec_id` values. Both sides produce TWO manifests in the
single multi-spec commit snapshot.

**Tie-shaping proof:** F0 and F1 have IDENTICAL record count (10), so the two manifests produced
in the multi-spec commit tie on ALL 9 prior sort keys (content=data, seq=same, min_seq=same,
added_files_count=1, existing_files_count=0, deleted_files_count=0, added_rows=10, existing_rows=0,
deleted_rows=0) and differ ONLY on `partition_spec_id` (0 vs 1). The spec-id tiebreaker (position
10, W3 ruling) is the ONLY disambiguator — assert this property explicitly in the fixture.

**Plan:**

- [x] Record plan (this section).
- [x] **Step 1: Java `MultiSpecOracle` (new inner class in InteropOracle.java).** Operations:
  create table partitioned by identity(a) → append F1(spec 0) → evolve spec: add identity(b)
  (spec 1) → append F2(spec 1) → ONE multi-spec fast_append carrying both F0-spec0 and F1-spec1
  stamped files. Emit `java_meta.json` (canonical view). Add `generate-interop-multi-spec` and
  `verify-interop-multi-spec` dispatch cases to `main()`. Key fix: Java oracle builds spec 0
  directly (not from unpartitioned default) so spec_ids align with Rust (0=identity(a),
  1=identity(a)+identity(b)).
- [x] **Step 2: Rust interop test `crates/iceberg/tests/interop_multi_spec.rs`.** GEN test (performs
  the same chain via Rust production write paths), READ parity test (Rust view of Java chain ==
  java_meta.json), WRITE parity test (Rust view of Rust chain == java_meta.json). Tie-shaping
  assertion: F0.record_count == F3.record_count == 10; different partition tuple arities prove
  different spec ids (1 vs 2 fields).
- [x] **Step 3: Shell script `dev/java-interop/run-interop-multi-spec.sh`.** 5-step chain + sabotage
  battery (structural corruption + control + spec-id-swap mutation). SB2 refs-aware snapshot
  stripping (update current-snapshot-id AND refs["main"]["snapshot-id"] to ms3). SB4 Python-driven
  subprocess spec-id swap in the canonical view JSON; fixed cwd path depth (fixture_dir + 3 levels
  up to reach pom.xml, not 2).
- [x] **Step 4: map.md updates** (dev/java-interop/map.md, crates/iceberg/tests/map.md).
- [x] **Step 5: GAP_MATRIX.md update** (multi-spec interop cells — landed, scoped).
- [x] **Step 6: Run full chain ×2, paste output; run verbatim gate ×2.**
  Full chain ×2 green (5 steps + SB1/SB2/SB3/SB4 all PASS). Gate: typos clean, fmt clean,
  clippy clean (fixed doc_overindented_list_items), 2168 lib tests ×2.
- [x] **Step 7: task/lessons.md update.**

**Outcome:** Z2 multi-spec interop fixture landed 2026-06-12. Single multi-spec fast-append
commit constructible on both sides (CONFIRMED). Spec-id tiebreaker is the sole disambiguator for
tie-shaped ms4 manifests. 4-sabotage battery all closed. Verbatim gate ×2 green (2168 lib tests).

- [x] **REVIEWER (Opus 2-of-2, 2026-06-12):** Cold-start verified. Chain ×2 green incl. genuine D1
  (step 4/5: Java judges the Rust-written table via emit+diff). Tie decoded from the emitted JSON:
  the two ms4 manifests are byte-identical on all 9 prior sort keys (content/seq=3/min_seq=3/added=1/
  existing=0/deleted=0/added_rows=10/existing_rows=0/deleted_rows=0), differ ONLY on
  partition_spec_id 0 vs 1; their entries render `{1000:q}` (spec-0, 1-field) vs `{1000:r,1001:s}`
  (spec-1, 2-field) per the file's-own-spec rule (Rust + Java both render under the manifest's own
  spec). **STRENGTHENED SB4:** the builder's original SB4 only post-edited the spec_id INTEGER in the
  emitted view JSON (proved the field is in the comparison key, but NOT the per-own-spec rendering it
  claimed). Rewrote SB4 to swap the spec-0/spec-1 FIELD DEFINITIONS in the SOURCE metadata and
  RE-EMIT → the partition tuples re-render under the wrong-arity spec (`1001:s` drops) and the view
  diverges — now a true artifact-level, re-derived fail-closed proof. Updated dev map.md + GAP_MATRIX
  SB4 wording to match. Cell scoping HONEST (both ✅ cells scoped to multi-spec FAST-APPEND metadata
  interop; merge_append/deletes/replay-cherrypick interop correctly stay open in their own rows).
  Cross-chain ×1 green (write-actions, staged-wap, cherrypick — shared oracle untouched by Z2). Gate
  ×2 green (2168). Tree clean. Flagged: dead `verify-interop-multi-spec` Java dispatch (unused;
  builder-owned, left in place).

## ACTIVE (2026-06-12): Z3 — partition-stats file interop (BUILDER Sonnet, wt-interop3)

**Structure choice:** SIBLING script `dev/java-interop/run-interop-partition-stats.sh` + new Rust
test `crates/iceberg/tests/interop_partition_stats.rs`. Oracle inner class `PartitionStatsOracle`
added to `InteropOracle.java`.

**Fixture:** V2 table `identity(category)` {id long, category string, data string optional},
S1 fast-append (cat=a 3rec 300B + cat=b 2rec 200B), S2 row-delta pos-delete (cat=a 1rec 50B).

**Key discoveries:**
- `DataFiles.Builder` has NO `withContent()` — position-delete files must use
  `FileMetadata.deleteFileBuilder(spec).ofPositionDeletes()` (class is `org.apache.iceberg.FileMetadata`).
- `PartitionStatisticsFile.statisticsPath()` does NOT exist — the method is `path()` (confirmed
  via `javap` of the 1.10.0 api jar).
- `PartitionStats.partition()` returns `StructLike` (not `PartitionData`) when decoded via
  `readPartitionStatsFile` (the reader instantiates a `GenericRecord` / `StructProjection`, not a
  `PartitionData`). The oracle uses `StructLike.get(0, Object.class)` without casting.

**Plan:**

- [x] Record plan (this section).
- [x] **Step 1: Java `PartitionStatsOracle` (new inner class in InteropOracle.java).** Fixture
  build + `PartitionStatsHandler.computeAndWriteStatsFile(table)` + register +
  `readPartitionStatsFile` read-back + emit `java_stats.json` + `table/metadata/final.metadata.json`.
  `verify()` reads `rust_table/metadata/final.metadata.json`, locates the registered stats path via
  `partitionStatisticsFiles()`, decodes with `readPartitionStatsFile`, compares against `expected_stats.json`.
  Fix: `withContent(FileContent.POSITION_DELETES)` → `FileMetadata.deleteFileBuilder(spec).ofPositionDeletes()`;
  `statisticsPath()` → `path()`; `(PartitionData)` cast → `StructLike.get(0, Object)`.
- [x] **Step 2: Rust interop test `crates/iceberg/tests/interop_partition_stats.rs`.** Three tests:
  GEN (build fixture, compute+write+register, emit expected_stats.json + final.metadata.json),
  D2 (read Java stats parquet, compare against java_stats.json), cross-version (V2 file vs V3 schema
  — `project_struct_type_to_batch` null-fills dv_count to 0).
- [x] **Step 3: Shell script `dev/java-interop/run-interop-partition-stats.sh`.** 8 steps, chain ×2.
  Sabotage 4: truncate Rust parquet (7a), SOURCE byte-edit counter + re-read (7b, Z2 lesson), truncate
  Java parquet (7c), remove partition-statistics from metadata (7d).
- [x] **Step 4: map.md updates** (dev/java-interop/map.md, crates/iceberg/tests/map.md).
- [x] **Step 5: GAP_MATRIX.md update** — Z3 interop landed cell, pipe audit.
- [x] **Step 6: Run full chain ×2, paste output; run verbatim gate ×2.**
  Chain ×2 green (D1 0 failures both chains; D2 2/2 rows; cross-version dv_count=0;
  sabotage 7a/7b/7c/7d all closed). Gate: typos/fmt/clippy/lib all clean (2168 lib tests ×2).
- [x] **Step 7: task/lessons.md update.**

**Outcome:** Z3 partition-stats file interop landed 2026-06-12. Bidirectional: D1 (Rust writes,
Java's `readPartitionStatsFile` judges, 0 failures both chains) + D2 (Java writes, Rust reads,
all rows match) + cross-version V2→V3 projection (dv_count null-filled to 0). 4-sabotage battery
all closed (SOURCE byte-edit re-derive for 7b, per Z2 lesson). Verbatim gate ×2 green (2168).

- [x] **REVIEWER (Opus 2-of-2, 2026-06-12): cold-start verify + STOP-grade harness fix.** Chain ×2
  re-run green (D1/D2/cross-version PASS, sabotage 7a-7d closed). **Found + fixed a false-green in the
  sabotage battery:** the Java `verify` rebuilds a `Table` via `LocalTableOperations.commit(null,meta)`
  which writes `v0.metadata.json` with non-overwriting `create()`; the clean D1 step writes it first, so
  every later verify (7a/7b/7d) crashed on "File already exists" BEFORE reading the stats parquet — the
  sabotage check (`! grep '0 failures'`) misread that crash as a fail-closed. PROVEN: 3 clean verifies
  pre-fix collided on #2/#3 (an UNcorrupted file "passed" sabotage). FIX (scoped to
  `PartitionStatsOracle.buildTableFromMetadata`): clear leftover `vN.metadata.json` before each commit.
  Post-fix: 3 clean verifies all "0 failures"; patched chain ×2 still green. **Decode-depth independently
  proven** via parquet-aware per-field rewrites (the byte-search in 7b merely structurally corrupts):
  mutating `data_record_count`/`position_delete_record_count`/`last_updated_snapshot_id` each yields the
  exact `FAIL row … <field> expected=X actual=Y` line. Anti-circularity: D1 `expected_stats.json` is
  gated by the GEN test's hard `assert_eq!` against hand-declared constants before emission; D2
  `java_stats.json` anchored to the same constants via fixture construction (`withRecordCount(...)`).
  `last_updated_at` (wall-clock millis) intentionally uncompared (snapshot_id is its stable proxy).
  Cross-chain (multi-spec + staged-wap) re-run green — shared oracle classes untouched. Gate ×2 green
  (2168). Tree = allowed set.

## ACTIVE (2026-06-12): Z1 — staged-WAP interop fixture (BUILDER Sonnet, wt-interop3)

**Structure choice:** a SIBLING script `dev/java-interop/run-interop-staged-wap.sh` (separate from
`run-interop-cherrypick.sh`) so the new staged-state-comparison steps do not disturb the existing
3-fixture chain that is already green.

**Java-emitter enumeration verdict (PRE-DECIDED CAVEAT):** `SnapshotMetaOracle.emit()` at line
6479 iterates `metadata.snapshots()` — the FULL metadata snapshot list, NOT `currentAncestors` or
`history`. A staged ref-less snapshot IS included. Confirmed all-snapshots — no STOP needed.

**Plan:**

- [x] Record plan (this section).
- [x] **Step 1: Java `StagedWapOracle` (new inner class in InteropOracle.java).** Three fixtures
  (S-ff / S-replay / S-dedup). Java uses REAL `stageOnly()`. S-dedup redesigned to the
  `testDuplicateCherrypick` same-parent pattern (stage BOTH w3 off S0 before any cherry-pick;
  first cherry-pick FF, second cherry-pick REPLAY → validate_wap_publish → DuplicateWAPCommitException).
  Both views per fixture + `wap_dedup_expected_rejection.json`. `generate-interop-staged-wap` +
  `verify-interop-staged-wap` modes landed.
- [x] **Step 2: Rust interop test `crates/iceberg/tests/interop_staged_wap.rs`.** Both directions,
  both views. S-dedup redesigned to match Java's same-parent pattern. Direction 2 assertions updated
  (staged count=3, final count=3, no source-snapshot-id on FF current, wap.id=w3 on current).
- [x] **Step 3: Shell script `dev/java-interop/run-interop-staged-wap.sh`.** 7 steps landed.
  Sabotage 7d redesigned from "inject main ref" (ineffective — canonical view omits refs) to
  "remove staged snapshot from metadata" (canonical view loses 1 ordinal → diverges → PASS).
- [x] **Step 4: map.md updates** (dev/java-interop/map.md, crates/iceberg/tests/map.md).
- [x] **Step 5: GAP_MATRIX.md update** (cherrypick row — staged-WAP interop ✅ 2026-06-12 landed).
- [x] **Step 6: Run full chain ×2, paste output; run verbatim gate ×2.**
  Full chain ×2 green (0 failures, all 4 sabotages closed). Verbatim gate ×2: 2168 lib tests.
- [x] **Step 7: task/lessons.md update.** Three lessons added: S-dedup same-parent pattern,
  canonical view does not include refs/current-snapshot-id (sabotage redesign), `cargo fmt` order.

**Outcome:** Z1 staged-WAP interop fixture landed 2026-06-12. Three fixtures × two views × two
directions green. The S-dedup same-parent correction was the key insight; the sabotage redesign
was secondary.

**Z1 OPUS REVIEWER (2 of 2), 2026-06-12 — cold-start verification:**

- [x] Cold-start required reading (CLAUDE.md, Opus.md + Sonnet addendum, lessons, git diff full,
      the three new/changed pieces, the canonical view, cherry_pick.rs + stage_only surface).
- [x] RUN the chain ×2 + 4-sabotage battery (both green; 7a/7b/7c/7d closed). Ref-state on BOTH
      sides: D1 verify + D2 test both assert it; FOUND S-replay D2 staged current loose → tightened.
- [x] S-dedup rejection arm: verbatim "Duplicate request to cherry pick wap id..." + DataInvalid +
      non-retryable on BOTH sides; FF-first (count unchanged) → REPLAY-second fires dedup. Confirmed.
- [x] wap.id coverage: canonical view excludes it; FOUND D1 value-pin missing (corruption passed) →
      added expectedStagedWapId pin + FF current-wap pin; mutation now fails closed BOTH directions.
- [x] ROW-STATUS JUDGMENT (headline): verdict = ✅-scoping HONEST (surface-scoped, row stays 🟡,
      data-level residue named); made airtight by the D1 wap.id-value pin I added.
- [x] 5 mutations beyond the battery: wap.id-value (D1+D2), ordinal/seq perturbation, current-id move
      (ref-state), cross-fixture swap — all fail closed.
- [x] Verbatim gate ×2 (2168 lib tests ×2, typos/fmt/clippy clean). Cross-chain: cherrypick + expire
      green; write-data running.
- [x] GAP_MATRIX pipe audit clean (5 pipes/row). Tier-ledger data point recorded in final report.

## ACTIVE (2026-06-12): Near-full-parity direction — open queue (planning record)

Directive (user, 2026-06-11): run this fork's Roadmap to **almost the full 1:1 Java replacement**;
DataFusion/RePark tabled. Waves 3–4 + the overnight session landed PRs #28–#37 (multi-spec writes,
constants-map, ExpireSnapshots + interop, DeleteOrphanFiles, RewriteDataFiles,
RemoveDanglingDeleteFiles, the variant arc end-to-end, stage_only + WAP dedup, ComputePartitionStats,
data-level interop fixtures A–G, cherrypick interop). Statuses live ONLY in the GAP_MATRIX.

- [x] **THIS BRANCH (Wave 5 Group Z, SONNET-builder + OPUS-critic per the calibrated split,
      user-approved 2026-06-12): the named interop items.** Z1: staged-WAP fixture DONE (2026-06-12).
      Z2: multi-spec fixture DONE (2026-06-12). Z3: partition-stats file interop DONE (2026-06-12) —
      bidirectional D1+D2 + cross-version V2→V3 projection + 4-sabotage battery. Worktree
      `wt-interop3`, parallel to Group Y (`phase6/compute-table-stats`, Opus) and Group U
      (`phase5/view-ops`, Opus).
- [ ] **Partition-stats residue:** the INCREMENTAL compute path; time/uuid/fixed/binary partition
      values in stats files (loud errors today).
- [ ] **`ComputeTableStats` (NDV/theta sketches) — DEPENDENCY-GATED, user decision:** needs a
      DataSketches-equivalent crate; Cargo frozen.
- [ ] **Reported divergences awaiting their increments:** manifest-list carried-vs-new entry ORDER
      differs from Java (cosmetic — readers + the canonical oracle reconcile by seq; O1 reviewer);
      F1 pre-existing global flags (Java lowercases ALL type names on parse, Rust exact-lowercase;
      Rust sort orders unbound/unvalidated on metadata parse; struct map-value Avro record naming
      shares the duplicate-name hazard variant's fix closed).
- [ ] **THIS BRANCH (Wave 5 Group U, Opus actor-critic, user-approved 2026-06-12): views in
      catalogs.** U1: view-metadata + view-ops completeness vs Java 1.10.0 (`ViewMetadata`
      builder parity, `ReplaceViewVersion`/`UpdateViewProperties` ops, version-log semantics,
      the `replace` dialect rules — bytecode-pinned); U2: catalog view CRUD parity (the `Catalog`
      view surface across MemoryCatalog + the SQL catalog; REST view shapes verified against the
      existing REST machinery; Glue/S3Tables deferred to the credentialed sprint). Worktree
      `wt-views`, parallel to Group Y (`phase6/compute-table-stats`) and Group Z
      (`interop/wave5`).
### Wave-5 Group U / U1 — view metadata + view ops + in-tree catalog CRUD (BUILDER, wt-views)

**Survey verdict (read before assuming a gap):** the SPEC side is already a faithful 1:1 port of
Java's `ViewMetadata.Builder` — `spec/view_metadata.rs` + `view_version.rs` +
`view_metadata_builder.rs` (58 KB) carry version-id assignment, the identical-version REUSE
(`reuse_or_create_new_view_version_id` == Java `reuseOrCreateNewViewVersionId`/`sameViewVersion`),
schema interning by id, version-log append + expiry (`version.history.num-entries`, default 10),
dialect rules (unique-per-version, replace-must-not-drop unless `replace.drop-dialect.allowed`),
parser via serde `_serde::ViewMetadataV1`. **What is MISSING:** (a) no `View` type, (b) no
view methods on the `Catalog` trait, (c) no view CRUD in any catalog, (d) no `ViewCommit` /
`ViewRequirement` / view-ops seam, (e) no `ViewMetadata::read_from`/`write_to`.

- [x] **U1a — spec IO + wire-format pin:** added `ViewMetadata::read_from`/`write_to` (gzip-aware,
      mirrors `TableMetadata`); pinned the Java `ViewMetadataParser`/`ViewVersionParser` FIELD SET
      round-trip + read/write FileIO round-trip. (Field order DOES differ Rust↔Java — see lesson.)
- [x] **U1b — `ViewRequirement` + `ViewUpdate::apply`:** `ViewRequirement::{NotExist, UuidMatch}`
      with `check(Option<&ViewMetadata>)` (case-insensitive uuid per `AssertViewUUID`) + serde tags
      `assert-create`/`assert-view-uuid`; `ViewUpdate::apply(ViewMetadataBuilder)`. Requirement set
      = `[AssertViewUUID]` ALONE, bytecode-confirmed (`forReplaceView`: null base ⇒ AddSchema arm
      no-ops, no AddViewVersion arm).
- [x] **U1c — `View` type + `ViewCommit`:** `crates/iceberg/src/view.rs` — `View` + `ViewBuilder`,
      `ViewCommit{ident, requirements, updates}` with `apply(View)->View` (check reqs, apply updates,
      bump metadata-file version via `MetadataLocation::with_next_version`).
- [x] **U1d — view ops seam:** `ReplaceViewVersionAction` (Java `ViewVersionReplace.internalApply`
      — requires query/schema/namespace, version-id `max+1` then builder REUSE) +
      `UpdateViewPropertiesAction` (set/remove + can't-set-and-remove guard), both emit `ViewCommit`.
- [x] **U1e — `Catalog` trait view surface + MemoryCatalog:** seven default-erroring view methods
      on the trait + full MemoryCatalog impl (new `view_metadata_locations` namespace state +
      `ViewNotFound`/`ViewAlreadyExists` ErrorKinds). REST view shapes + SQL catalog views deferred
      to U2/Task-3 (NOT this increment).
- [x] **U1f — tests (18, each risk-named) + GAP_MATRIX row 107 + gate ×2 (2168→2186, both green).**

Outcome: U1 landed. Spec builder was already a 1:1 Java port (the survey's key finding); the gap was
the entire catalog-facing view surface. Pre-existing flag: SQL catalog `lib.rs` doctest fails to
compile (tokio `rt-multi-thread` feature gate) — unrelated, SQL crate untouched, its 52 unit tests
pass. Deferred per scope: SQL/REST/Glue/S3Tables view CRUD, view interop (next wave).

U1 REVIEWER (2026-06-12, adversarial vs 1.10.0 bytecode): forReplaceView=`[AssertViewUUID]` ALONE
re-derived from bytecode (null base ⇒ AddSchema no-ops, no AddViewVersion arm) — CONFIRMED, pinned at
the commit object. Replace dialect-drop guard + escape, identical-version reuse, the missing-required-
field serde pin, and Java-parser tolerance of Rust's wire format all VERIFIED (Java reads Rust's JSON
today — order-insensitive + tolerates always-present empty `properties`; cosmetic divergence only).
FIXED one real divergence: table↔view name collisions were silently allowed (Java rejects both
directions) — added symmetric cross-guards to `insert_new_table`/`insert_new_view` + 2 tests
(fail-before/pass-after). REPORTED, not fixed (pre-existing, shared with `update_table`): the in-tree
`update_view` has no base-location CAS, so a stale concurrent commit lands last-write-win (Java's
`doCommit` throws CommitFailedException) — belongs in a dedicated concurrency-parity increment.
Gate green ×2 (2186→2188 with the 2 collision tests). Verdict: APPROVE with the concurrency-CAS gap
flagged for the orchestrator.

### Wave-5 Group U / U2 — SQL catalog views + REST view shapes (BUILDER Opus, wt-views)

**JDBC storage-scheme verdict (1.10.0 bytecode, `JdbcUtil`):** views live in the SAME `iceberg_tables`
table, discriminated by the EXISTING `iceberg_type` column = `'VIEW'` (constant `VIEW_RECORD_TYPE`).
Tables use `'TABLE'` OR-NULL (V0 backcompat); views are V1-only (`JdbcViewOperations.doCommit` uses
`SchemaVersion.V1` unconditionally — exact-match `iceberg_type = 'VIEW'`, no OR-NULL). The Rust SQL
catalog already creates the V1 schema (the `iceberg_type` column exists), so it is V1-native — mirror
the posture, no schema-version flag needed. The view SQL constants (`GET_VIEW_SQL`, `LIST_VIEW_SQL`,
`RENAME_VIEW_SQL`, `DROP_VIEW_SQL`, `V1_DO_COMMIT_VIEW_SQL`, `V1_DO_COMMIT_CREATE_SQL`) are the exact
table-CRUD SQL with `AND iceberg_type = 'VIEW'`. CAS: `V1_DO_COMMIT_VIEW_SQL` = `UPDATE ... WHERE ...
AND metadata_location = ?` (0 rows = conflict, `CommitFailedException "Cannot commit %s: metadata
location %s has changed from %s"`). Collisions: create-view checks `tableExists` → `AlreadyExists
"Table with same name already exists: %s"`; create-table checks `viewExists` → `"View with same name
already exists: %s"`; rename cross-checks the other map.

**REST verdict (1.10.0 bytecode):** routes `GET/POST /v1/{prefix}/namespaces/{ns}/views`,
`GET/POST/DELETE/HEAD .../views/{view}`, `POST /v1/{prefix}/views/rename`. Wire types: CreateViewRequest
`{name, location, view-version, schema, properties}`; LoadViewResponse `{metadata-location, metadata,
config}`; the replace/commit reuses `UpdateTableRequest` shape `{identifier, requirements, updates}` but
with VIEW requirements/updates (`UpdateRequirements.forReplaceView`). `ViewUpdate`/`ViewRequirement`
already carry the correct REST serde tags. Rename reuses the `{source, destination}` shape.

- [x] **U2a — SQL catalog view CRUD (7 methods):** done. `iceberg_type = 'VIEW'` discriminator
      (`CATALOG_FIELD_VIEW_RECORD_TYPE`), view error helpers, all 7 methods, collision guards both
      directions (added the table-side guards to `create_table`/`rename_table` too), location-CAS in
      `update_view` (`AND metadata_location = ?` → 0 rows = `CatalogCommitConflicts`).
- [x] **U2b — SQL tests (10, the full U1 lifecycle e2e ported + risk-named):** create/load/list;
      duplicate-fails; full lifecycle; update_properties; identical-version reuse; load-missing;
      collision BOTH directions; rename-onto-table rejected; list_views scoping; the CONCURRENT
      location-CAS conflict (barrier-synchronized two-task race — deterministic, ran ×5).
- [x] **U2c — REST view shapes:** `CreateViewRequest`/`LoadViewResult`/`CommitViewRequest` in
      `types.rs` + 3 serde round-trip tests vs hand-pinned Java-wire JSON.
- [x] **U2d — REST view methods:** all 7 wired (endpoint builders + `build_view_from_load_result`
      helper). Full impls landed (machinery cheap) + 7 mockito route/status-mapping tests.
- [x] **U2e — gate ×2 (SQL 62×2), GAP_MATRIX row 107 (5-pipe audit clean), lessons, todo.**

Outcome: U2 landed. SQL catalog views are a faithful JDBC-scheme port (`iceberg_type='VIEW'` in the
shared `iceberg_tables`, V1-schema exact-match — the Rust catalog is already V1-native). The SQL
`update_view` IMPLEMENTS the JDBC location-CAS (Java `V1_DO_COMMIT_VIEW_SQL`), a deliberate per-catalog
divergence from MemoryCatalog's no-CAS posture. REST landed BOTH the typed shapes AND full method
impls (the machinery made full impls cheap). One flagged shared-type fix: `ViewRepresentations` had no
public constructor, so `ViewCreation` was unconstructable from out-of-crate catalogs — added
`ViewRepresentations::new` in `catalog/mod.rs` (in-scope shared-type escape). Gate: iceberg 2188, SQL
62×2, REST 51, clippy/fmt/typos clean. Deferred per scope: Glue/S3Tables views (credentialed), view
interop (next wave).

U2 REVIEWER (2026-06-12, adversarial vs 1.10.0 bytecode): JDBC verbatim-ness CONFIRMED — re-derived all
six `JdbcUtil` view constants; Rust queries are SEMANTICALLY verbatim (conjunct SET identical; only
AND-clause/SET-column order differs, commutative-safe), CAS clause present, exact-match VIEW posture vs
TABLE-or-NULL backcompat mirrored, collision messages byte-verbatim. CROSS-CATALOG error-kind
consistency HOLDS (Memory U1 + SQL U2 both: view-over-table→TableAlreadyExists,
table-over-view→ViewAlreadyExists). CAS race test NON-VACUOUS — drop `AND metadata_location = ?` ⇒ both
win ×5 (corruption); loser kind/retryable match the table-side `update_table` convention; auto-rebase
off the freshly-reloaded base verified. REST wire EXACT vs bytecode (`ResourcePaths` routes,
`CreateViewRequestParser` keys, `RESTViewOperations`→`UpdateTableRequest.create` commit body). FIXED two
real gaps in touched files: (1) REST `update_view` 5xx now maps to CommitStateUnknown (Java
`ViewCommitErrorHandler` 500/502/503/504; was folded into the generic default arm, diverging from the
table side) + test; (2) tightened the body-blind commit mockito test with a hand-pinned body matcher.
ADDED two permanent pins the builder lacked: `rename_view` rejects a TABLE source (discriminator-on-
rename, mutation-proven), and a compile-level out-of-crate accessibility probe (pins the
`ViewRepresentations::new` gap CLASS forever — removing the fix fails the SQL test build). REPORTED, not
fixed (benign/unreachable): create-view collision-check ORDER is reversed vs Java (only matters if both
a table AND view of the same name exist — guards make that unconstructible). Mutations ×5 all killed
(CAS-drop, list_views filter-drop, REST route typo, rename discriminator-drop, accessibility-fix
removal). Gate ×2 green: iceberg 2188, SQL 64, REST 52, clippy/fmt/typos clean; GAP_MATRIX 5-pipe
audit clean; tree = allowed set + the flagged mod.rs line. Verdict: APPROVE.

- [ ] **Scheduled with the user:** real-catalog (Glue + S3 Tables) hardening — needs credentials.
- [ ] **Opus-queue (post-handoff or parallel):** ORC/Avro breadth, view ops + SessionCatalog +
      LockManager (Sonnet-builder + Opus-critic per the calibrated split), incremental-scan interop,
      scan completion (BatchScan / CDC / split planning).

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
- Index: [todo-archive/map.md](todo-archive/map.md).
