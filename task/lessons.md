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

# Lessons

Accumulated DO / DO NOT lessons. The operating manuals ([skills/](../skills/)) require reading this
file **in full at the start of every session**, and appending to it after **any** correction from
the user.

How to use it (see the manuals' §2):

- After any correction, append a **date-stamped** entry immediately.
- Write each as a concrete **DO** or **DO NOT** statement with the *why* and how to apply it.
- Supersede an outdated rule with a dated note (`_superseded YYYY-MM-DD: see ..._`) rather than
  editing the original in place.

> **Compaction log.** Last pass: 2026-06-11 (size trigger — 1,369 lines / 128 KB on the settled
> post-#24 main; pass 2, agentic-pace amendment, user-approved tally 17 KEEP / 25 ARCHIVE /
> 6 promoted) → [lessons-archive/2026-06_phase2-completion.md](lessons-archive/2026-06_phase2-completion.md).
> Promoted that pass: 6 rules (2 → [docs/testing.md](../docs/testing.md), 2 →
> `dev/java-interop/map.md#debug`, 1 → `crates/iceberg/src/transaction/map.md#debug`, 1 →
> [CLAUDE.md](../CLAUDE.md)). Prior pass: 2026-06-09 (size trigger — 2,650 lines vs the ~800-line trigger;
> first pass, run under the agentic-pace recency amendment, user-approved) →
> [lessons-archive/2026-06_phase1-phase3.md](lessons-archive/2026-06_phase1-phase3.md).
> Promoted that pass: 31 entries' durable rules (12 → [docs/testing.md](../docs/testing.md)
> "Mutation-testing & review discipline" + gate-widening notes, 2 → [CLAUDE.md](../CLAUDE.md)
> [commit-hygiene chain; read order], the rest → the `## Debug` sections of
> `crates/iceberg/src/{transaction,inspect,scan,writer}/map.md`, `dev/java-interop/map.md`, and
> `crates/iceberg/tests/map.md`). Archives are not read by default — see
> [skills/compaction.md](../skills/compaction.md).

---

<!-- Newest entries at the bottom. Example shape:

### YYYY-MM-DD
- **DO** carry context on every fallible Rust call (`.with_context(...)` / `.expect("msg")`).
  *Why:* a bare `.unwrap()` panic gives the operator no cause from logs alone.
- **DO NOT** edit upstream crate files to land a fork feature when an additive module would do.
  *Why:* it makes the next upstream merge conflict-prone. Prefer additive changes.
-->


### 2026-06-08 (Phase 3 Increment 2 — residual scan-wiring, BUILDER Opus)
- **DO state + test the residual-validity invariant as the load-bearing correctness claim.** *Why:* the whole
  reduction is only result-equivalent because every row in a data file belongs to that file's SINGLE partition
  tuple, so the partition-implied leaves the residual drops are TRUE for all rows in the file. Wrote it as a
  comment in `into_file_scan_task` and proved it with read tests that assert the residual-path rows EXACTLY
  equal the full-filter row set (identity AND truncate partitions). A residual regression silently returns
  wrong scan results, so this is the one invariant the tests must pin, not just "the predicate is reduced."
- **DO resolve the `partition_spec: None` TODO via `table_metadata.partition_spec_by_id(manifest_file.
  partition_spec_id)` in `PlanContext::create_manifest_file_context` (it has `table_metadata`), thread it onto
  `ManifestFileContext` → `ManifestEntryContext` → the task.** *Why:* all files in one manifest share the
  manifest's `partition_spec_id`, so the spec (and the `ResidualEvaluator` built from spec+filter) is resolved
  ONCE per manifest file and shared across its entries — the per-spec cache pattern, but per-manifest, so no
  shared mutable cache is needed. Per-file rebuild would re-run `spec.partition_type(schema)` + a `Schema`
  build for every file.
- **FLAG, DON'T MISS: setting `task.partition_spec = Some(spec)` ACTIVATES the arrow reader's
  identity-partition constant materialization (`reader.rs:451-455` → `record_batch_transformer::constants_map`,
  Java `PartitionUtil.constantsMap`), which is dormant while the field is `None`.** *Why it bites:* threading
  the spec (a required TODO) silently changes how identity-partition columns are READ — they become run-end-
  encoded constants from the partition METADATA, not values read from the data file. This is correct Iceberg
  behavior, but it exposed a pre-existing inconsistency in `TableTestFixture` (partition `x`=100/200/300 vs the
  parquet `x` column `[1; 1024]`) and broke 9 tests asserting `x`==1 as a plain `Int64Array`. A change that
  only "sets a field on a struct" can still alter downstream read behavior — grep the field's consumers
  (`task.partition_spec`) before assuming "reader unchanged."
  **REVERTED (2026-06-08, post-push): the constants-map activation was BACKED OUT — `task.partition_spec` is
  left `None`.** *Why:* CI's INTEGRATION tests (which my `-p iceberg` + workspace-BUILD gate never RAN) exposed
  two latent bugs in `record_batch_transformer::constants_map`: it emits a `RunEndEncoded` column where the
  declared scan schema is plain `Utf8` (`test_insert_into_partitioned` — "expected Utf8 but found
  RunEndEncoded"), and it cannot widen an `Int(i32)` partition literal to an `Int64` column
  (`test_evolved_schema` — "Unsupported constant type combination: Int64 with Some(Int(19))"). The constants-map
  is a SEPARATE parity feature with real bugs; activating it was a BUNDLED extra in the residual increment, not
  the residual goal — the residual works with `partition_spec = None` (the reader reads identity-partition
  columns from the data file). Reverting kept the residual + the (still-necessary) fixture-consistency fix and
  removed the blast radius; the constants-map + its fixes are deferred to a dedicated increment gated by the
  datafusion + integration read tests. Reverting also required removing the now-dead `partition_spec` field
  threading through `ManifestFileContext`/`ManifestEntryContext` (the local spec that builds the residual
  evaluator stays) and undoing the RunEndEncoded read assertions (`x` is plain `Int64` again).
- **PROCESS (2nd gate-widening): the per-increment gate must RUN the downstream crates' TESTS, not just BUILD
  them.** *Why:* the residual scan-wiring built the workspace clean and passed `-p iceberg`, but
  `iceberg-datafusion`'s INTEGRATION tests (MemoryCatalog-backed, runnable locally — NO Docker) failed on the
  constants-map activation, and the Docker-gated `iceberg-integration-tests` failed too. A workspace BUILD
  compiles datafusion's tests but does not RUN them. Add `cargo test -p iceberg-datafusion` (lib + the
  MemoryCatalog `integration_datafusion_test`) to the gate for any change that touches the SCAN/READ path
  (first gate-widening was the non-exhaustive-match → workspace BUILD; this one is read-path → datafusion
  TESTS). The Docker-only suites (`iceberg-integration-tests` REST/MinIO) still can't run locally, so a
  read-path change that can't be proven locally is a flag to scope conservatively.
- **DO make a contrived test fixture INTERNALLY CONSISTENT rather than asserting on inconsistent data.** *Why:*
  the old fixture's partition values were arbitrary distinct constants (100/200/300) for manifest realism,
  never reconciled with the parquet data because `partition_spec` was always `None` (the constant path never
  ran). Once the spec is threaded, the partition value IS authoritative for an identity column — so the
  truthful fix is partition `x`=1 (= the data), not "assert `x`==100 (the now-materialized constant)". A
  consistent fixture is the higher-integrity resolution and keeps the read tests meaningful. The change rippled
  one `inspect/manifests.rs` expect-snapshot (partition-summary bounds `"100"`/`"300"` → `"1"`); a shared
  fixture's partition values can reach any test that snapshots partition summaries — grep for the old values.

### 2026-06-09 (RowDelta.validateNoNewDeletesForDataFiles — ORCHESTRATOR + REVIEWER Opus)
- **A VALIDATION-ONLY partial port of a mutating API must be UNMISTAKABLY documented at EVERY surface.**
  Java `RowDelta.removeRows` both records the file for validation AND removes it from the table in apply; the
  Rust `RowDeltaOperation` is add-only, so the port lands the safety VALIDATION but defers the apply-side
  removal. That's a faithful-but-minimal scope — but a public `remove_data_files` that doesn't remove is a
  footgun unless the deferral is stated in the MODULE doc, the FIELD doc, AND the METHOD doc, each pointing to
  the real removal path (`overwrite_files().delete_data_files`). Mirror an existing validation-only precedent
  (`referenced_data_files`) so the pattern is consistent.
- **When two sub-checks share ONE opt-in flag (Java faithfulness), isolate the one under test by making the
  OTHER quiet.** RowDelta's `validate_no_conflicting_delete_files()` gates BOTH the new partition-scoped
  removed-data-file check (2a) AND the pre-existing filter-based check (2b, partition-blind inclusive-metrics).
  A "should-commit" negative for 2a must give the concurrent deletes metric bounds the conflict filter
  EXCLUDES, or 2b (which is at least as strict) fires and masks 2a's logic. Document this in the test. Order
  matters for the asserted message (2a runs first → its message, not 2b's).
- **Reuse the shared helper UNCHANGED across the second caller.** Increment 2 wired Increment 1's
  `validate_no_new_deletes_for_data_files` into RowDelta with ZERO edits to the helper — the reviewer confirms
  the first caller's (OverwriteFiles') tests stay green as the behavior-preservation proof. If the second
  caller needs a tweak, that's a signal to re-scope, not to fork the logic.

### 2026-06-10 (E1 — RowDelta metadata-level interop — ORCHESTRATOR Fable + REVIEWER Opus)
- **A metadata-level interop needs a CANONICAL VIEW, not byte comparison — and the canonicalization
  choices are the increment's real judgment calls.** Snapshot ids → ordinals (by sequence number);
  COUNT summary keys only (byte sizes legitimately differ between writers); entries sorted by an
  EXPLICIT cross-language tuple (never rendered-string order — Jackson and serde_json differ);
  partitions via the spec's single-value JSON (`SingleValueParser.toJson` ↔ `Literal::try_into_json`
  — the one cross-language-canonical tuple rendering). *Limit:* ordinals assume DISTINCT sequence
  numbers — all V1 snapshots share seq 0, so the scheme is V1-unsafe without a tiebreaker
  (documented in both emitters).
- **DO default `SnapshotSummaryCollector.trust_partition_metrics` to TRUE — a derived
  `#[derive(Default)]` bool is FALSE and silently suppressed `changed-partition-count` on every
  per-file commit.** Java `SnapshotSummary.Builder` starts trusted and only distrusts when a whole
  MANIFEST (unknown per-partition breakdown) is added. The harness caught this within minutes of
  first running: Java's summaries carried `changed-partition-count` (`setIf(trustPartitionMetrics,
  …)`, written even when 0; the unpartitioned EMPTY partition counts as one), Rust's omitted it
  (only-if-positive + unpartitioned files never tracked). *Why it matters beyond parity:* a derived
  `Default` on a semantically-true-by-default bool is a silent behavior class to audit for.
- **DO reconcile test fixtures that exploit a removed guard rather than weaken the fix.** Two
  summary tests paired an EMPTY partition struct with a PARTITIONED spec — impossible for real
  files, tolerated only by the old skip-gate; tracking every file made `partition_to_path` panic on
  them. The fix is consistent fixtures (a real partition value / an unpartitioned spec), not a
  defensive branch in production that would mask genuinely inconsistent data.
- **The reviewer's distinctive catch: a parity FIX can be green everywhere and still unpinned.**
  Both of the reviewer's mutations (count back to only-if-positive; `partition-summaries-included`
  emitted unconditionally) passed the ENTIRE offline suite — the first is interop-visible only via
  the env-gated harness, the second is invisible even there (the canonical view excludes the key).
  One added unit test (the trusted-but-empty-changeset case: count=="0" present, marker absent)
  catches both. When a fix's evidence is an EXTERNAL harness, add the offline unit pin too.

### 2026-06-10 (Phase-2 completion arc Increment 4 — metadata interop extension, BUILDER Opus)
- **The three Phase-2 ports (`rewrite_manifests` cluster-by, `merge_append` one-bin merge,
  `rewrite_files` seq-preservation) were GREEN against Java 1.10.0 on the FIRST round-trip run —
  zero canonicalization fixes, zero production changes.** Extending the E2 chain (s6 cluster-by-
  partition → s7 property-set → s8 merge-append) + a sibling delete-bearing fixture B all matched
  Java byte-for-byte immediately. The unit-level builder+reviewer cycles for Increments 1-3 had
  already pinned the exact provenance/seq semantics, so the metadata interop confirmed rather than
  discovered. (When the upstream increments were rigorous, the interop increment is a confirmation
  step, not a debugging one — but it is still the ONLY 1:1 proof, so it lands regardless.)
- **A property-set commit (`updateProperties`/`update_table_properties`) produces NO snapshot — the
  snapshot-level canonical view is unaffected, and the ordinal scheme stays consistent.** s7 set
  `commit.manifest.min-count-to-merge=2` on both sides; the chain has 7 snapshots (s1-s6 + s8), seq
  numbers 1-7 with no gap (the property commit consumes no sequence number either). Confirmed both
  sides agree the merge-arming property is visible to s8's MERGING append within the same chain.
- **Empirical 1.10.0 dangling-delete probe (the optional one): a `RewriteFiles` that rewrites the
  data file a position-delete REFERENCES commits and KEEPS the now-dangling delete manifest.**
  MECHANISM (re-traced against 1.10.0 by the reviewer — the BUILDER's original attribution was
  WRONG): a `RewriteFiles` commit DOES run `deleteFilterManager.removeDanglingDeletesFor(...)`
  (`MergingSnapshotProducer` L995) — `deleteFilterManager` is a `DeleteFileFilterManager` that does
  NOT override it, so the base impl runs fine (it just records the removed data-file paths). The
  `UnsupportedOperationException` throw at L1220-1222 lives on the SIBLING `DataFileFilterManager`
  (the DATA-file side) and is NEVER reached for delete pruning. The real reason a dangling
  POSITION-DELETE PARQUET survives is that the only two delete-drop paths both miss it: (a)
  `isDanglingDV` is gated on `ContentFileUtil.isDV` == `FileFormat.PUFFIN`, so a parquet position
  delete (V2, non-DV) is structurally exempt; (b) the `minSequenceNumber` cutoff
  (`dropDeleteFilesOlderThan`) does not drop it (the carried A'@seq1 holds the min data seq at 1,
  below the delete's seq 2). NET: only dangling *DVs* are pruned on a `RewriteFiles` in 1.10.0; a
  dangling parquet position-delete is kept — which CONVERGES with the Rust action's documented
  carry-unchanged posture (PARITY, not divergence). Lesson: drive the divergence question
  EMPIRICALLY with a throwaway probe (commit it, emit the canonical view, read it) — and when you
  cite a source mechanism for WHY, trace which concrete subclass/instance is actually invoked (the
  data-vs-delete filter-manager pair is a classic misattribution trap). DELETE the probe (it must
  not enter the byte-diffed chain) and record the finding in prose + the GAP_MATRIX cell.
- **Cluster keys are GROUPING-only: any key fn producing the same partition of entries is
  equivalent across languages, because the key string never appears in metadata.** Java
  `String.valueOf(file.partition())` and Rust `format!("{:?}", data_file.partition())` render
  differently but both yield one group per distinct partition tuple ⇒ identical manifest grouping.
  Document the chosen key fns on both sides; the comparison guards the rest.

### 2026-06-10 (DV arc D1 — deletion-vector scan READ path, BUILDER + REVIEWER Fable)
- **A GAP_MATRIX ✅ inherited from upstream-sync NOTES is unverified until the OUTERMOST behavior
  is empirically exercised.** *Why:* the read row claimed "position-deletes + DVs during scan ✅"
  from the 0.9.1 sync; the `DeleteVector` type and puffin reader existing did NOT mean DVs were
  scannable — `caching_delete_file_loader` routed every position delete to the PARQUET reader and
  the DV loader was a literal TODO. A V3+DV scan failed outright. Audit rule: a sync-inherited ✅
  needs a behavior-level probe (a real scan/commit), not a type-level one. Corrected the row
  ✅→🟡 with an honesty note.
- **DV blob facts (settled empirically vs Java 1.10.0):** framing = BE u32 length prefix (magic+
  bitmap), LE magic `D1 D3 39 64`, BE CRC-32 (the zlib CRC from the existing deflate dependency,
  identical to `java.util.zip.CRC32`) over magic+bitmap; portable 64-bit roaring DECODE is byte-compatible with `roaring-rs` treemap
  containers per key — BUT Java's `RoaringPositionBitmap.serialize` writes a DENSE bitmap array
  including EMPTY gap bitmaps while roaring-rs writes sparse; the decoder tolerates both, the D2
  WRITER must emit dense + Java's `runLengthEncode()` for byte parity. Read DVs with ONE ranged
  read via `content_offset`/`content_size_in_bytes` (Java `BaseDeleteLoader.readDV` — the
  PuffinReader path costs ≥3 requests). Cache/notify key must be `{puffin_path}@{offset}` — one
  puffin file holds MANY blobs; a bare-path key marks blob 2 "already loaded" = silent
  under-delete (pinned with a two-DVs-one-file test).
- **`roaring-rs` 0.11.3 `RoaringBitmap::deserialize_from` is the validating variant and caps the
  container count (≤65536, ~256KB max pre-read allocation)** — adversarial container-count blobs
  fail fast without allocation DoS (probed + pinned). Still wrap it per-key with payload-bound
  checks and an exact-consumption check; reject keys > i32::MAX-1 and non-ascending keys (Java
  `readKey` L302-308).
- **Serde-compat defaults on scan-task delete entries: default `file_format` to Parquet.** An old
  serialization carrying a DV then fails LOUDLY in the parquet reader (pre-D1-equivalent), never
  silently wrong; rejecting absent fields would break every genuinely-old parquet-delete
  serialization. Verified no in-repo serializer exists (downstream-API surface only).
- **Pin fail-loud-on-corruption for any storage-parsed structure:** flip one byte of the
  Java-written blob → the SCAN must error (CRC named, computed-vs-stored), never silently return
  unmasked rows. The reviewer ran this against the real harness fixture — make it a standard
  probe for every future storage decoder.

### 2026-06-10 (DV arc D2 — DV serialization + DVFileWriter, BUILDER + REVIEWER Fable)
- **The orchestrator's brief cited the WRONG reserved field id (2147483545 = DELETE_FILE_POS) for
  the DV blob's `fields` list — Java writes `MetadataColumns.ROW_POSITION.fieldId()` =
  Integer.MAX_VALUE − 2 = 2147483645.** The builder caught it by reading MetadataColumns.java and
  proving against the live oracle (the Java verify asserts the constant). The recurring rule both
  directions: the brief is never the spec — and reserved-id constants are exactly the kind of
  off-by-a-digit a paraphrase corrupts.
- **roaring-rs 0.11.3 CAN emit run containers (`RoaringBitmap::optimize`) with Java-identical
  array→run/bitmap→run criteria INCLUDING exact ties** — byte parity with Java holds
  unconditionally for insert()-built vectors (proven: run/dense-gap/tie fixtures byte-identical
  69/76/46 B). ONE caveat for D3: a store that is ALREADY Run (a deserialized previous DV being
  re-serialized after merge) ties differently at `cardinality == 2·runs` (Java keeps run, Rust
  emits array — readable everywhere, byte-only divergence; documented in delete_vector.rs).
- **A dense-layout size door must count the ABSENT (gap) entries' bytes, not just present
  bitmaps** — count = max_key+1 means one position at a high key implies gigabytes of empty
  8-byte entries. Pinned: 3 GB-by-gaps rejected in ~430 µs BEFORE allocation; the
  drop-the-absent-term mutation ground a 60+ s dense loop. Java's serializedSizeInBytes iterates
  its dense array (same accounting, slower); the O(present-keys) closed form is strictly better.
- **Java `MAX_POSITION = toPosition(2^31−2, Integer.MIN_VALUE)` — the LOW WORD IS 0x8000_0000,
  not 0xFFFF_FFFF** (0x7FFFFFFE_80000000). The writer-side `set()` door rejects above it while
  the DESERIALIZER accepts up to the key ceiling — mirror the LAYERING (door on delete(), key-only
  check in serialize), not a single bound.
- **When the oracle pins a jar version with no matching local source, verify Java behavior from
  the JAR's bytecode (javap/decompile), not MAIN-source line numbers** — the D2 reviewer
  re-derived MAX_POSITION, the run criteria (RoaringBitmap 1.3.0 — the version 1.10.0 actually
  pulls), and the fields constant from bytecode. MAIN line citations are navigation hints, never
  proof, across versions.

### 2026-06-10 (DV arc D3 — DV commit path, BUILDER + REVIEWER Fable; 2 reviewer bugs fixed)
- **A "X is unrepresentable in the Rust enum" claim STALES the day the variant lands — grep for
  those claims whenever an enum grows.** `validateAddedDVs`' walk had reused the `{Overwrite,
  Delete}` op set with a comment that REPLACE was unrepresentable; `Operation::Replace` landed
  with the rewrite actions and the claim silently became a missing-conflict-window bug (1.10.0's
  `VALIDATE_ADDED_DVS_OPERATIONS` = {overwrite, delete, replace} — bytecode-verified). Fixed +
  pinned with a Replace-op commit through the production producer.
- **An applicability door must mirror the READ PATH's resolution exactly: resolve the REFERENCED
  file's LIVE manifest entry — (spec id, partition, inherited data seq) — never key on the ADDED
  file's own fields.** The fresh-DV door keyed partition matching on the DV's own (spec,
  partition): after partition evolution the spec ids never match (UNDER-fire — the DV committed
  over a still-applying legacy parquet delete = resurrection class), and it had no seq filter
  (OVER-fire — a predating legacy delete froze all DV writes to that partition). One fix: resolve
  the live entry, match path-scope OR (spec, partition) against IT, AND `delete_seq >= data_seq`.
  Both directions pinned (the docs/testing.md mutate-both-directions rule, vindicated again).
- **`BaseRowDelta.validate` (1.10.0) runs `validateNoConflictingFileAndPositionDeletes`
  UNCONDITIONALLY** (removed-data-files ∩ new-deletes' referenced files → "Cannot delete data
  files %s that are referenced by new delete files") — was missing from the Rust validate hook.
- **Java 1.10.0 summary semantics for DVs (bytecode):** a DV bumps `added-dvs` INSTEAD of
  `added-position-delete-files`, but BOTH paths bump `added-delete-files` and
  `added-position-deletes` (+= record_count); size accounting uses `contentSizeInBytes` (the
  blob), not the shared puffin file size (`ScanTaskUtil.contentSizeInBytes`).
- **The V3-requires-DV gate breaks every V3 fixture committing parquet position deletes — budget
  the migration** (56 tests here: subject-preserving fixture swaps to a V2 in-catalog template;
  the migrated suite then doubles as a 60-test regression pin on the V2 gate arm).

### 2026-06-10 (DV arc D4 — table + metadata DV interop, BUILDER + REVIEWER Fable)
- **`mvn exec:java` exit-code propagation is MACHINE-DEPENDENT — design run-script verdicts to be
  exit-code-agnostic and FAIL-CLOSED:** capture with `|| true`, then fail when the success
  sentinel is ABSENT (not only when a `^FAIL` line is present — the absence branch is what makes
  an early mvn crash/OOM fail the script). The old "-q exec:java does not propagate System.exit"
  lesson held on the CI-era machine but NOT here (probed: MVN-EXIT=1) — under `set -e` the
  un-guarded capture aborted before echoing Java's diagnostics. Verdicts from shell VARIABLES
  (command substitution), never capture files; reset all temp dirs at step 1.
- **Distinct per-fixture cardinalities (DV A card 1, DV B card 2) keep the canonical entry sort
  tie-free for free** — design fixture values so no two entries share a sort tuple (the E2
  tie lesson, applied at fixture-design time instead of comparator-extension time).
- **DV arc outcome:** all four levels proven vs Java 1.10.0 — blob bytes (D2), scan Direction-1
  (D1), table-level Direction-2 + metadata-level 3-way incl. `added-dvs` (D4) — with ZERO
  canonicalization changes and ZERO production fixes in D4 (the D1-D3 surface held). The
  DV-writer row stays 🟡 SOLELY for the previous-deletes merge + superseded-delete removal
  (BaseDVFileWriter L117-126) — the next natural increment (needs apply-side delete-file removal).

### 2026-06-10 (post-arc logic + security audit, ORCHESTRATOR Fable — two parity bugs found + fixed)
- **Java's merge `first` is the unconditional STREAM HEAD (`manifestIter.next()`,
  ManifestMergeManager L85), NOT "this commit's new manifest".** For an empty-data merging append
  (properties-only commit) the head is the first EXISTING manifest and ITS bin still gets the
  min-count protection. Gating `first` on `added_snapshot_id == this snapshot` returned None on
  that path and silently dropped the protection — Rust merged 2 manifests where Java keeps 2
  (empirically pinned: the audit test failed 1≠2 pre-fix). When porting a "the first element"
  concept, port the CODE's selection rule, not the comment's intent.
- **Java `deletedManifests` is a Set (path equality); a Vec-backed port double-counts a duplicate
  `delete_manifest` on the replaced side of `validateFilesCounts`** ("0 (new), 2 (old)" vs Java's
  1 — spurious rejection, fail-loud not corruption, but a divergence). Mirror the COLLECTION
  semantics of the Java field, not just its uses: Set-backed fields dedupe at insertion.
- **DO use saturating arithmetic on any accumulator fed by UNTRUSTED on-disk metadata**
  (`manifest_length` from a manifest list): `bin_weight + weight` and the rolling size estimate
  could panic in debug builds (overflow check) or wrap in release on a hostile value. Saturation
  makes an absurd sum "never fits"/"roll now" — strictly safe, identical to Java for every
  realistic value. Audit greps that pay off: `as u64|as u32` casts on spec-struct fields (clamp
  negatives first), `+=` on u64 accumulators, `debug_assert` guarding anything reachable in
  release, `zip_eq` on data derived from storage.

### 2026-06-10 (Arc-E Inc 1 — apply-side DELETE-FILE removal `removeDeletes` + door relaxation, BUILDER Opus)
- **`BaseRowDelta.operation()` is VERSION-SENSITIVE — the 1.10.0 JAR has a TWO-branch form with NO
  APPEND arm; MAIN's three-branch form is post-1.10.0. The 2026-06-08 lesson's "third condition"
  was answered by the bytecode, not by adding MAIN's condition.** *Why:* the 2026-06-08 lesson said
  "when removeRows/removeDeletes land, the third condition (`&& !deletesDataFiles()`) MUST be added or
  add+remove records Append wrongly." Reading `iceberg-core-1.10.0.jar` BYTECODE
  (`javap -c BaseRowDelta`) shows 1.10.0 `operation()` is `if (addsDeleteFiles() && !addsDataFiles())
  return "delete"; return "overwrite";` — NO append branch at all (the MAIN source's leading
  `addsDataFiles() && !addsDeleteFiles() && !deletesDataFiles() ⇒ APPEND` is a later addition). The
  interop oracle pins 1.10.0, so the faithful fix is to DROP to the two-branch form (which handles
  every removal case: add+remove → Overwrite; remove-only → Overwrite; add-deletes-only → Delete),
  NOT to port MAIN's third condition. This RE-CLASSIFIES add-data-only RowDelta as Overwrite (was
  Append per the pre-fix MAIN mirror) — unobservable via interop (the oracle data-appends via
  `newFastAppend`, never an add-data-only `newRowDelta`). LESSON: when a lesson says "port condition
  X from Java," re-derive X from the PINNED JAR's bytecode first — the source may have moved. Also:
  `deletesDataFiles()` = the DATA filter manager (`removeRows`), `deletesDeleteFiles()` = the DELETE
  filter manager (`removeDeletes`) — two separate methods, and 1.10.0 `operation()` consults NEITHER.
- **The DELETE-manifest filter is the SAME `process_deletes` machinery keyed off the SOURCE manifest's
  CONTENT — a removed file's path is matched across the FULL manifest list, so a DATA removal lands in
  a rewritten DATA manifest and a DELETE removal in a rewritten DELETE manifest, no second code path.**
  *Why:* Java's `DataFileFilterManager`/`DeleteFileFilterManager` are the same abstract
  `ManifestFilterManager<F>`, differing only in `newManifestWriter` (DATA vs DELETE writer) +
  `removeDanglingDeletesFor` (the DATA one throws). The Rust port mirrors this by making
  `new_filtering_manifest_writer` content-keyed off `source_manifest.content` (`build_v2/v3_deletes`
  for a DELETE source) — ONE `process_deletes`/`rewrite_manifest_with_deletes` serves both. The
  load-bearing pin: a rewritten DELETE manifest MUST stay a DELETE manifest (`_file_type` 1) or the
  manifest list misclassifies it and the read path stops applying its surviving deletes
  (resurrection). Mutation (force always-DATA writer) ⇒ exactly the 7 delete-removal tests fail; the
  data-side test stays green (proving byte-identical data behavior — it always used the DATA writer).
- **A door RELAXATION must only ADD an escape hatch, never weaken the original guard — pin BOTH the
  new positive (with-removal commits) AND keep the original negative (without-removal still rejected)
  GREEN.** *Why:* the fresh-DV door rejected a DV for a file with a live position-scoped delete. The
  relaxation: legal IFF the existing delete's path is in this commit's `remove_deletes` set (Java's
  merge-and-replace contract). Implemented as a single `continue` at the top of the door's per-entry
  loop (covers both the live-DV and legacy-parquet branches). The D3 door negatives
  (`second_dv..._rejected`, `legacy_parquet..._still_applies`) stayed green untouched — the escape
  hatch is purely additive. Mutation (disable the `continue`) ⇒ exactly the 3 removal-commit tests
  fail, the D3 negatives unaffected — confirming the relaxation is scoped to the removed-set path.
- **`removed-dvs` became reachable end-to-end via the producer's `remove_file` summary loop — the D3
  collector-level-only branch is now driven by a real commit.** D3 wired `SnapshotSummaryCollector
  .remove_file`'s DV branch but had no commit path to feed it (documented "collector-level only"). The
  producer's new `removed_delete_files` summary loop (mirroring the `removed_data_files` loop) feeds it
  — a removed DV bumps `removed-dvs`, a removed parquet pos delete `removed-position-delete-files`, eq
  `removed-equality-delete-files`. NO `snapshot_summary.rs` edit needed (the branch existed) — the
  "edit only if a counter gap shows" condition was not met. When a prior increment pre-wires a summary
  branch "for parity, unreachable," the increment that adds the driving path just connects the loop.

### 2026-06-10 (Arc-E Inc 2 — DVFileWriter previous-deletes MERGE hook + replacement interop, BUILDER Opus)
- **`ContentFileUtil.isFileScoped` is the BROADER `referencedDataFile(df) != null`, NOT `isDV`
  (1.10.0 bytecode-verified).** *Why:* the brief asked "DV OR path-scoped position delete — what
  exactly?" `javap -c ContentFileUtil` over the 1.10.0 jar shows `isFileScoped` = `referencedDataFile(df)
  != null`, and `referencedDataFile` is: EQUALITY→null; else the explicit `referencedDataFile()` field;
  else the `_file_path` (id 2147483546) lower==upper bound (a position delete pinning ONE data file). A
  DV is file-scoped because it carries `referenced_data_file`, NOT because it is a DV. So `is_file_scoped`
  must mirror that three-branch predicate (eq→false, explicit field, equal-`_file_path`-bounds fallback)
  — implemented next to the writer via the public `DataFile` accessors, NOT a fork of `is_deletion_vector`
  (which is `format==Puffin`, a strictly narrower set). A partition-scoped parquet pos delete (no
  ref field, unequal/absent path bounds) is NOT file-scoped and must NOT be rewritten (Java L121-124:
  "only DVs and file-scoped deletes can be discarded") — rewriting it drops a delete still applying to
  OTHER data files (resurrection on those).
- **The JAVA oracle's `newRowDelta` defaults `startingSnapshotId = null` = "check ALL history," so
  `validateAddedDVs` sees the PRIOR DV1 as "concurrently added" and rejects the replacement — set
  `validateFromSnapshot(currentSnapshotId)` to mirror the engine (and the Rust tx-captured start).**
  *Why:* the Rust replacement chain committed fine because `Transaction::new` captures the head AFTER
  DV1, so the Rust `validate_added_dvs` window excludes DV1. Java's default null start checks all
  versions → DV1 is in-window → "Found concurrently added DV for <path>". The engine
  (`SparkPositionDeltaWrite`) captures the operation-start snapshot and passes it to
  `validateFromSnapshot`; the Java oracle must do the same (`newRowDelta().validateFromSnapshot(headId)`)
  — it is the twin of Rust's tx-captured start, not a hack. Without it the metadata-mirror GEN crashes
  AFTER the (passing) table-level Direction-2 read, so the failure is the Java MIRROR, not the Rust write.
- **The Run-store re-serialization byte tie did NOT materialize for the {1}→{1,3} replacement — the
  merged blob is byte-identical to Java's merge (array container).** *Why:* the D2 caveat fires only when
  the deserialized previous store is ALREADY a Run container re-serialized at `cardinality == 2·runs`;
  {1} deserializes to a 1-element array, {1,3} re-serializes to a 2-element array (2 non-contiguous runs
  ⇒ array strictly smaller), so no Run store sits on the tie. Empirically pinned BOTH the positive (the
  interop byte-compare passes) and the documented universal caveat (positions identical, bytes may differ
  at the tie, Java reads our blob either way — proven by the table-level read). Do NOT contort production
  to chase the tie; the oracle proves Java reads the blob regardless of the byte divergence.
- **A public API that TAKES a type from a private module is callable but NOT constructible downstream —
  making the module `pub` is the real cost, and it cascades doc/clippy lints.** *Why:* `PreviousDeletes::
  new(positions: DeleteVector, …)` is `pub`, but `delete_vector` was `mod` (crate-private), so the
  `DeleteVector` arg could not be NAMED by a downstream caller — the hook was unusable externally. The fix
  is `pub mod delete_vector` (the on-disk DV type is genuinely public-worthy), but `#![deny(missing_docs)]`
  then demands docs on EVERY now-public item (struct + 5 methods + the iterator) AND clippy's
  `len_without_is_empty` demands a sibling `is_empty()`. Budget the doc/lint tail when promoting a module
  to satisfy a new public signature; flag the `lib.rs` edit (out of the usual file set) loudly since it is
  a public-surface change downstream pins must follow.
- **Keep `close()` returning just the DVs and ADD `close_with_result()` for the richer Java
  `DeleteWriteResult` shape — zero blast radius beats a breaking signature change.** *Why:* ~10 existing
  callers (tests + 4 interop files) call `close() -> Vec<DataFile>`. Java's `result()` returns the full
  `DeleteWriteResult`; callers wanting only the DVs call `.deleteFiles()`. Mirroring that — `close()` a
  thin wrapper over `close_with_result().delete_files` — keeps every existing caller untouched AND the
  no-previous path byte-identical (the D2/D4 pins stay green without edits), while the new replacement
  flow uses `close_with_result()`.

### 2026-06-11 (Arc E orchestrator notes — DV merge + removal complete; DV-writer row ✅)
- **`ContentFileUtil.isFileScoped` = `referencedDataFile != null`, a THREE-branch chain (1.10.0
  bytecode): equality → null; the explicit `referenced_data_file` field; ELSE the `_file_path`
  (reserved id 2147483546) lower==upper bounds fallback** — a parquet position delete pinning
  exactly one file IS file-scoped even without the explicit field. Port all three branches or the
  DV-replacement flow diverges (an explicit-only port would make the fresh-DV door reject what
  Java accepts — fail-loud, but a parity gap). The E2 builder ported all three + the pin.
- **1.10.0 `BaseRowDelta.operation()` is the TWO-branch form — NO Append arm** (add-data-only row
  deltas record OVERWRITE); MAIN's three-branch source is post-1.10.0. The 2026-06-08 lesson's
  "add the third condition when removeDeletes lands" resolved the OPPOSITE way; the E1-family
  oracle never exercised the branch (audited), which is why the old Append form looked
  interop-proven.
- **Prove an oracle knob's NECESSITY by removing it:** the Java replacement chain needs
  `.validateFromSnapshot(headId)` (the twin of Rust's tx-captured start) or `validateAddedDVs`
  walks to root and flags the SAME-CHAIN prior DV as concurrent — the E2 reviewer re-ran the
  oracle without it and got the exact rejection, turning a plausible claim into proof.
- **A `pub` hook signature drags its parameter types public — choose the minimal surface
  deliberately and write the breaking-surface callout.** `pub mod delete_vector` exposes exactly
  `DeleteVector` + its iterator (the `pub use` alternative leaks the same types via `iter()`).
  `#![deny(missing_docs)]` + clippy `len_without_is_empty` cascade onto newly-public types.
### 2026-06-11 (Arc F — `cherrypick` / WAP publish, BUILDER Opus)
- **PORT THE FAST-FORWARD PRECEDENCE FROM `apply()`, NOT the case split in `cherrypick(long)`.** Java
  `CherryPickOperation.apply()` (L193-204) checks `requireFastForward || isFastForward(base)` BEFORE the
  replay path, so an APPEND (or replace-partitions OVERWRITE) whose `parent == current head` FAST-FORWARDS
  (publishes the staged snapshot verbatim — NO new id, NO replay), even though `cherrypick(long)` routed it
  into the APPEND branch at config time. Reading only `cherrypick(long)` (which sets `requireFastForward`
  ONLY in the else-branch) would make an append-with-parent==head MINT A NEW SNAPSHOT — wrong id, duplicated
  audit lineage. The Rust dispatch must run the FF check first, unconditionally. Pinned by a snapshot-COUNT
  assertion (FF = count unchanged); the FF-broken mutation (`if false && is_fast_forward`) fails it.
- **CHERRYPICK'S CONCURRENT-WINDOW IS `picked.parentId`, NOT the tx-captured `starting_snapshot_id` — the
  no-override rule is satisfied by re-deriving the window from the PICKED snapshot, not by a captured field.**
  Java `validateReplacedPartitions` walks `SnapshotUtil.ancestorsBetween(currentSnapshot, picked.parentId)`
  (inclusive head, exclusive parent — exactly `files_after`'s shape with `starting = picked.parentId`). There
  is no `validateFromSnapshot` override and the transaction's read point is IRRELEVANT, so the
  `do_commit`-supplied `starting_snapshot_id` is intentionally UNUSED by cherrypick's validate. State this
  explicitly (the tx-captured-start no-override pin is N/A for this shape) rather than wiring an unused
  override — the walk re-derives its window every retry, which is what actually survives a re-base.
- **STAGE a snapshot for cherrypick by append-then-rollback through the catalog — graft only for the
  catalog-reload-defeating case.** A STAGED snapshot needs REAL manifests (`added_snapshot_id == staged_id`)
  so the replay can source its added/removed files; produce them by committing a `fast_append`
  (`set_snapshot_properties({"wap.id": …})` for the WAP cases) or `replace_partitions`, then
  `manage_snapshots().set_current_snapshot(parent)` to roll `main` back so the produced snapshot is left
  dangling. The non-ancestor case (head on a SIBLING line that a single-root MemoryCatalog can't commit) is
  the exception: build a grafted metadata-only fixture and call the action's `.commit(&table)` DIRECTLY
  (bypassing `Transaction::commit`'s catalog reload, which would overwrite the graft) — the manage_snapshots
  `forked_table` direct-commit pattern. A grafted V3 snapshot needs `.with_row_range(next_row_id, 0)` or
  `add_snapshot` errors "first-row-id is null".
- **SOURCE the picked snapshot's added/removed files with the SAME manifest filter the concurrent walk uses,
  scoped to one snapshot.** Java `SnapshotChanges.cacheDataFileChanges` loads the picked snapshot's DATA
  manifests where `manifest.snapshotId() == snapshot.snapshotId()`, filters `status != EXISTING`, then
  `ADDED → added` / `DELETED → removed` — identical to `files_after`'s `added_snapshot_id == snapshot_id` +
  status filter, but for a single snapshot instead of an ancestor walk. The replayed REMOVES go by-PATH
  through the producer's `resolve_delete_paths` (which IS `failMissingDeletePaths`), so a replayed delete
  whose target is no longer live errors — matching Java L117.
- **CHERRYPICK IS A STANDALONE ACTION, not a method on `ManageSnapshotsAction`.** Java exposes it via
  `ManageSnapshots.cherrypick`, but `CherryPickOperation extends MergingSnapshotProducer` — it needs the full
  snapshot producer, which the Rust ref-op `ManageSnapshotsAction` (emits only `SetSnapshotRef`/
  `RemoveSnapshotRef`) does not have. They do not compose cleanly; a delegating method would have to special-
  case the producer path. The honest shape is a separate `CherryPickAction` + a doc pointer on
  `ManageSnapshotsAction` — the same call already made for `replace_partitions`/`overwrite_files` (Java reaches
  those through producer subclasses too).

### 2026-06-11 (Arc F — `cherrypick` REVIEWER Opus)
- **A dedup/ancestry scan over "CURRENT ancestors only" needs a DANGLING negative control, or the
  all-snapshots over-broadening passes silently.** Cherry-pick's double-publish dedup
  (`lookupAncestorBySourceSnapshot`) and the already-ancestor check both walk ONLY the current ancestry chain
  (Java `currentAncestors(meta)` = `ancestorIds(currentSnapshot)`), NOT `metadata.snapshots()`. Every
  happy/dedup test had the prior publish ON `main`, so a mutation that scanned ALL snapshots (incl. dangling)
  passed all of them — the bug only bites after a ROLLBACK leaves a prior publish dangling, where an
  all-snapshots scan FALSELY blocks a legitimate re-publish. The catching test rolls `main` back past a first
  publish (leaving it dangling) and re-cherry-picks: ancestry-only succeeds, all-snapshots wrongly rejects.
  General rule: any "walk current ancestors" port wants a dangling-snapshot negative control, since the common
  fixtures keep everything live.
- **The cherrypick precedence matrix is provably equivalent to Java's two-phase form — verify the FF predicate
  AND the non-eligible-op FF cell.** Java splits the decision across `cherrypick(long)` (config-time) and
  `apply` (`requireFastForward || isFastForward(base)`); the Rust single-resolution `plan()` runs the FF check
  FIRST, unconditionally, against the refreshed base. The two land in the same cell for EVERY combination
  because (a) the FF predicate is identical (`parent==head`, or both null) and (b) Java's else-branch accepts
  ANY operation if FF-able — so a DELETE with parent==head FAST-FORWARDS in both (publishes verbatim, no
  replay), and the same delete with parent!=head fails the not-eligible check in both. The concurrent-head-moved
  cells fall out for free: the action re-plans against the refreshed base, so an append whose parent stopped
  being head REPLAYS and a delete whose parent stopped being head FAILS — already covered by the replay
  fixtures (which advance `main` past the staged parent before picking). Pin the delete-with-parent==head FF
  cell explicitly; the append-FF and not-eligible cells were already pinned.
- **Cherrypick replaying an OLD-spec snapshot is a fail-loud divergence (the Rust producer is default-spec
  only).** Java's `add(DataFile)` keeps each file's own `specId()` and writes per-spec manifests, so picking a
  snapshot whose files predate a spec evolution SUCCEEDS. Rust reuses `validate_added_data_files`
  (default-spec-only, shared with fast append) + `write_added_manifest` (default spec), so the same replay
  FAILS-LOUD non-retryably. Fail-loud is the conservative-safe direction (accepting an old-spec file would
  misplace its partition under the wrong spec); document it in the module header and pin it so a future change
  cannot silently turn it into corruption. The replay's removed-side `copyWithoutStats` (Java) is immaterial in
  Rust — the `Deleted` tombstone is rewritten from the LIVE source manifest entry (`rewrite_manifest_with_deletes`),
  not from the picked snapshot's copy, so the removed file keeps its true spec/stats regardless.

### 2026-06-11 (Arc F — cherrypick, BUILDER + REVIEWER Opus)
- **Java's cherrypick is FF-FIRST in apply (`requireFastForward || isFastForward(base)`): ANY
  operation — append, eligible overwrite, even delete — fast-forwards (publishes verbatim, no new
  snapshot) when the picked snapshot's parent is the current head.** The case split in
  `cherrypick(long)` is config-time; apply re-checks against the refreshed base (append whose head
  moved → REPLAY; non-eligible op whose head moved → reject). A Rust single-resolution `plan()`
  with the FF check first is provably equivalent and retry-safe.
- **Dedup/ancestor lookups must walk CURRENT ANCESTORS, never all snapshots — and that scope needs
  its own pin.** The all-snapshots mutation passed the entire original suite; only a
  rollback-leaves-dangling-publish fixture distinguishes them (a dangling prior publish must NOT
  block a legitimate re-publish). Same family as the no-override tx-captured pin: the SOURCE of a
  window/scope is a distinct mutation axis.
- **Cherrypick's concurrent-window start is the PICKED snapshot's parent (re-derived each retry),
  not the tx-captured start — the docs/testing.md no-override rule is N/A there and saying so
  explicitly (with why) is the compliant form.**
- **Multi-spec replay diverges fail-loud:** the Rust producer is default-spec-only, so
  cherry-picking an old-spec staged snapshot after partition evolution rejects where Java succeeds
  (per-spec manifests). Documented + pinned; acceptable until the producer gains multi-spec
  writes.
### 2026-06-11 (Arc G — carried-forward Phase-1 closeout, BUILDER Opus)
- **DO NOT apply a carried item's PROPOSED fix without re-deriving it from Java 1.10.0 BYTECODE — a
  spec-robustness reading can be the OPPOSITE of Java's actual behavior.** *Why:* the carried item
  said "fix shape: `#[serde(default)]` on `last_sequence_number`" (citing spec line 1978 "default to
  0"). Bytecode of `TableMetadataParser.fromJson` shows `if (formatVersion <= 1) → 0` else
  `JsonUtil.getLong("last-sequence-number")`, and `JsonUtil.getLong` does
  `checkArgument(node.has(field))` ⇒ Java THROWS on an absent V2+ field. Spec line 179 (read it, not
  just line 1978) explicitly permits this: "a v2 table that is missing `last-sequence-number` can
  throw an exception" — the "default to 0" rule is for reading a **V1** document as V2. Rust's
  required-on-V2/V3 serde ALREADY matched Java; adding `#[serde(default)]` would have made Rust ACCEPT
  malformed V2 metadata Java REJECTS — a divergence, not a fix. The deliverable was verify-then-pin
  (3 tests; the `#[serde(default)]` mutation fails the V2-strict test), zero production change. When
  the spec gives readers latitude ("may be more strict… can throw"), Java's bytecode is the authority
  for which side of the latitude to land on.
- **DO trust BYTECODE over a "grep of the .java found no checkArgument" claim — the carried item's
  source grep was simply wrong.** *Why:* the carried retention item said a grep of `SnapshotRef.java`
  found no positivity `checkArgument`. Bytecode of `SnapshotRef$Builder` (api-1.10.0) shows all three:
  `minSnapshotsToKeep`/`maxSnapshotAgeMs`/`maxRefAgeMs` each `checkArgument(value == null || value > 0,
  "<verbatim message>")`. Rust's `manage_snapshots::validate_retention_positive` already reproduced the
  three messages verbatim. The repo lesson "bytecode outranks MAIN source for version-sensitive claims"
  extends to "outranks a prior agent's source-grep claim" too.
- **DO check the PARSE path separately from the write path when settling "does Java enforce X" — a
  guard on the builder fires on READ if the parser routes through that builder.** *Why:* the retention
  positivity guards live on `SnapshotRef.Builder`, and `SnapshotRefParser.fromJson` (bytecode:
  `builderFor → minSnapshotsToKeep → maxSnapshotAgeMs → maxRefAgeMs → build`) routes through it — so
  Java rejects a stored zero/negative retention ON READ, not just on the write API. Rust had the guard
  only on the write path (`manage_snapshots`); the serde derive accepted any value (empirical probe:
  `{"type":"branch","min-snapshots-to-keep":0,...}` parsed Ok). Ported the missing guard into
  `spec/snapshot.rs` (`SnapshotRetention::validate_positive` run via `SnapshotReference`'s `try_from`
  deserialize). A `#[serde(try_from = "Raw")]` on a field-flattened struct works, but NOT on the
  flattened enum itself — `try_from` is incompatible with the inner `#[serde(flatten)]`, so validate at
  the OUTER struct (`SnapshotReference`) that owns the flatten, the sole production deserialization site.
- **A guard ported to the PARSE path cannot reject what Java accepts — verify the asymmetry direction
  before worrying about over-strictness.** *Why:* the risk flagged in the brief was "Rust stricter than
  Java on read could fail to load a Java table." Here the opposite held: Java's parser ALSO rejects
  zero/negative retention (it throws), so no Java-written table carries one — porting the guard converges
  with Java, it cannot diverge. When Rust adds a parse-time guard, the safe-by-construction check is
  "does Java's parser reject the same input?" — if yes (bytecode), the guard is parity, full stop.

### 2026-06-11 (Arc G — carried-forward closeout, BUILDER + REVIEWER Opus; both items REVERSED)
- **A carried-forward item's recorded fix is a HYPOTHESIS, not a spec** — both Phase-1 items
  reversed on evidence: (1) the proposed `#[serde(default)]` for V2 `last-sequence-number` was
  ANTI-parity (spec L179 permits the strict read; 1.10.0's `JsonUtil.getLong` THROWS on absent;
  the lenient default is V1-as-V2 only, L1976-1978) — closed with pins, zero production change;
  (2) "Java may not enforce retention positivity" was wrong — `SnapshotRef$Builder` enforces all
  three (the bytecode reject-on-non-positive branch) AND `SnapshotRefParser.fromJson` routes through the validating
  builder, so Java rejects ON READ — the Rust PARSE path was the real gap, now ported
  (`try_from`-based deserialize; fires through `#[serde(flatten)]` in `SetSnapshotRef` too,
  probe-verified).
- **Escalate evidence to a LIVE ORACLE when the jars are present:** the reviewer ran
  `SnapshotRefParser`/`TableMetadataParser` directly on the disputed documents — reject/accept and
  messages matched Rust exactly. Bytecode reasoning is good; an executed oracle is proof.

### 2026-06-11 (Multi-spec writes — producer per-spec grouping, BUILDER Opus, Group A)
- **DO group added DATA *and* DELETE files by their OWN `partition_spec_id` and write ONE manifest
  per (content × spec) — the producer was DEFAULT-SPEC-ONLY (validated `spec == default`, wrote the
  added manifest under `default_partition_spec()`).** *Why:* Java's `MergingSnapshotProducer.add` /
  `FastAppend.appendFile` (1.10.0 source AND `FastAppend.java` in the jar — VERIFIED, FastAppend does
  support multi-spec adds) keep each file's own `specId()` in `newDataFilesBySpec` /
  `newDeleteFilesBySpec` (a `HashMap`) and `forEach`-write `writeDataManifests(files, spec(specId))`
  per group. Writing a spec-0 file's 1-field partition tuple into a default-spec-1 manifest (2-field
  type) is partition-tuple CORRUPTION — the mutation made the writer `zip_eq`-panic on the arity
  mismatch (the exact corruption the grouping prevents). Chose spec-id DESCENDING order (the
  established Rust convention — `merge_append`/`rewrite_manifests` reverse-sort; Java `groupBySpec` is
  a `TreeMap(reverseOrder())` for merging; HashMap `forEach` order is non-deterministic and the group
  order never appears in the spec-canonical metadata view — a manifest list is compared as a SET, the
  cluster-key GROUPING-only lesson applied to manifest ordering).
- **DO lift the added-file validation from "spec == default" to "spec EXISTS in `partition_spec_by_id`"
  with Java's EXACT two messages** ("Cannot find partition spec %s for data file: %s" /
  "...delete file: %s", spec id + `file.location()`), and check partition-value compatibility against
  the FILE's OWN spec's partition type, not the default's. *Why:* Java's `Preconditions.checkArgument(
  spec != null, ...)` accepts any known spec and rejects only an UNKNOWN id. The validation-revert
  mutation (fall back to the default spec instead of erroring) made the door-level message vanish —
  the unknown-spec commit still ultimately failed DEEPER (the cluster writer errors "unknown partition
  spec id", or the partition-value check fails on the default's arity), proving the door is
  defense-in-depth and the tests must pin the DOOR's exact message, not just "a commit fails".
- **THE RIPPLE THE BRIEF FLAGGED: `SnapshotSummaryCollector.add_file`/`remove_file` must take the
  file's OWN spec, not the table default** — Java `SnapshotSummary.Builder.addedFile(spec(file.specId()),
  file)` → `updatePartitions(spec, file)` computes `spec.partitionToPath(file.partition())` with the
  file's spec. `summary()` passed `default_partition_spec()` for every file; on a multi-spec commit a
  spec-0 file's path would be rendered under the wrong spec ⇒ wrong `changed-partition-N=` summary keys
  (the changed-partition-summaries family). Fixed via a `file_partition_spec(file)` helper (falls back
  to default only for the unreachable "spec vanished" case, since validation runs first and the summary
  path is infallible).
- **VERIFIED-UNAFFECTED ripples (cited, not changed):** the fresh-DV door
  (`row_delta::validate_fresh_dvs_only`) resolves the REFERENCED file's LIVE manifest entry and keys
  on ITS `(spec_id, partition)` — never the added file's own fields — so it is independent of the
  producer's added-file path (the cross-spec DV test stayed green). `rewrite_manifests` /
  `merge_append` cluster writers are already `(key, spec_id)`-keyed; `replace_partitions` resolves
  `(spec_id, partition)` tuples (per-spec already). The only producer-side write/validation surface
  was the default-spec assumption — everything downstream already threaded spec ids.
- **FLAG (deferred, NOT fixed): `OverwriteFiles::validate_added_files` (the opt-in
  `validateAddedFilesMatchOverwriteFilter`) builds its partition evaluators from the DEFAULT spec.**
  Java uses `dataSpec()`, which `checkState(specIds.size() == 1, ...)` — Java itself REJECTS a
  multi-spec overwrite under that opt-in check. So the Rust path diverges only for a
  multi-spec-overwrite-with-row-filter-validation combination Java forbids anyway; out of the
  producer's core scope. The WRITER-LAYER spec threading (`DataFileWriter`/`DeletionVectorWriter` stamp
  the table default) also stays deferred — a single writer instance produces single-spec files; a
  multi-spec commit is assembled by the CALLER passing already-spec-stamped files (which is what the
  producer now groups correctly). Multi-spec Java↔Rust interop is the other deferral.

### 2026-06-11 (Multi-spec writes — REVIEWER pass, Group A, wt-closeout)
- **A per-file-spec fix needs a SAME-ARITY pin, or the panic is "luck" not coverage.** The summary-collector
  fix (render each file's `partitions.{path}` under ITS own spec) had no dedicated test. Reverting it only
  failed the multi-spec MANIFEST tests — and only because those fixtures use DIFFERENT-ARITY specs
  (`identity(x)` 1-field vs `identity(x)+identity(y)` 2-field), so `partition_to_path` index-out-of-bounds
  PANICS and aborts the commit before the manifest asserts. A SAME-ARITY different-NAME multi-spec commit
  (`identity(x)` vs `identity(y)`, both 1-field) renders the WRONG field name with NO panic — silent
  corruption of `partitions.{path}` keys and `changed-partition-count`. The pin MUST use the same-arity
  shape (V2 omits a removed-only base field, so `remove(x)+add(y)` gives a clean 1-field `identity(y)`) and
  pick the SAME partition value on both files so the default-spec bug COLLAPSES two distinct tuples onto one
  path ⇒ a wrong `changed-partition-count`. General rule: when a fix routes a per-element spec/type, pin it
  with elements that differ ONLY in the routed attribute, never in arity (arity divergence masks the bug
  behind an unrelated panic).
- **`added-/total-data-files` summary counts do NOT guard against manifest-grouping FILE LOSS.** They are
  computed from `added_data_files` BEFORE `group_files_by_spec`, so a grouping bug that drops a spec group
  leaves the totals intact while the file vanishes from every manifest. File-loss is caught only by tests
  that assert manifest COUNT + per-file PRESENCE in a loaded manifest (the two-spec manifest tests do; a
  cumulative-totals test does not). Pin write-path file-loss at the manifest layer, never via the summary.
- **The canonical interop view (`snapshot_meta_view.rs`) does NOT encode manifest `partition_spec_id`** (not
  in the sort tuple, not in the emitted JSON). Same-content/same-seq/same-counts manifests of DIFFERENT
  specs tie on the whole tuple ⇒ array order is manifest-list position (Rust spec-descending vs Java HashMap
  `forEach`). Today no interop fixture is multi-spec single-commit, so the view is fine; the FIRST multi-spec
  interop fixture must add spec id to the comparator tuple (or compare the manifest SET order-insensitively).

### 2026-06-11 (Phase 3 Increment 2 RE-DONE — identity-partition constants-map ACTIVATION, BUILDER Group A)
- **DO materialize identity-partition constants as PLAIN arrays of the DECLARED scan-schema type, not
  Run-End-Encoded.** *Why:* the 2026-06-08 revert's first bug ("expected Utf8 but found RunEndEncoded",
  `test_insert_into_partitioned`) was that `record_batch_transformer` built the target schema field for a
  constant identity-partition column via `datum_to_arrow_type_with_ree` (a Rust-only storage optimization),
  so the OUTPUT batch schema declared REE where the projected scan schema (and DataFusion / `RecordBatch::
  try_new`) require a plain `Utf8`/`Int64`. The fix: for a constant field that EXISTS in the table schema
  (identity-partition), use the field's declared plain Arrow type + a plain repeated array; REE stays ONLY
  for metadata-virtual fields (`_file`) that have no schema entry. Java `PartitionUtil.constantsMap` is
  encoding-agnostic — the column is handed back in its declared physical type. Pin BOTH the field's declared
  `data_type()` AND the concrete array downcast (`StringArray`/`Int64Array`), because a value-only helper
  (`get_string_value`) passes under REE too.
- **DO coerce a partition constant to the COLUMN'S Iceberg type via `Datum::to(&field.field_type)`, not the
  literal's stored variant.** *Why:* the revert's second bug ("Unsupported constant type combination: Int64
  with Some(Int(19))", `test_evolved_schema`) was a partition tuple carrying a NARROW `Int(i32)` literal for
  a column promoted to `Long`; `Datum::new(Long, Int(19))` kept the narrow literal and the array builder's
  `(Int64, Int(19))` arm did not exist. `Datum::to` is the canonical Iceberg coercion table (`Int->Long`,
  `Int->Date`, `Long->Timestamp/Timestamptz`, `Int128->Long`, equal-type passthrough) — it mirrors Java
  `IdentityPartitionConverters.convertConstant(partitionType.field(pos).type(), value)`, where the TYPE comes
  from the schema-derived partition type (verified against 1.10.0 bytecode: the per-field loop reads
  `partitionType.fields().get(pos).type()`, and the converter's `default` case is a passthrough — the
  widening is implicit in Java's boxing/coercion).
- **DO force the constant to OVERRIDE a file-present column — the `PassThrough`/`ModifySchema` fast paths
  silently return the FILE value.** *Why:* the revert's two known bugs were the loud failures, but a SILENT
  third one lurked: `compare_schemas` is constant-unaware, so when an identity-partition column is ALSO
  physically in the data file with a matching type (the test fixtures — the partition column is in the
  parquet), the transformer took `PassThrough` and returned the FILE value, never applying the constant. The
  existing scan read tests NEVER caught this because the fixture made the file value == the partition value
  (both 1) — indistinguishable. The metadata-vs-file test (partition `x==999`, file `x==[1;1024]`) is the
  one that exposed it; the fix forces the column-rebuilding `Modify` path whenever a constant field id is
  also in the source file (Java: partition metadata wins over file data). LESSON: a "constant overrides
  file" feature is structurally untestable on a fixture where the two sources AGREE — make them DIFFER.
- **DO thread the spec the same once-per-manifest way the residual already does.** `create_manifest_file_
  context` already resolves `partition_spec_by_id(manifest.partition_spec_id)` (an `Arc`) to build the
  residual evaluator; thread THAT same Arc onto `ManifestFileContext` -> `ManifestEntryContext` -> `FileScan
  Task.partition_spec` — no new resolution, no per-file rebuild, and multi-spec falls out for free (each
  manifest's files get their own spec). Multi-spec test: two identity specs over different columns, one file
  each in its own manifest, partition values differing from the data — each file's constant materializes
  under ITS spec (mutation: disable activation ⇒ both the metadata-vs-file AND multi-spec tests fail).
- **PROCESS WIN: the gate that killed the first attempt now PASSES BY CONSTRUCTION.** Ran `cargo test -p
  iceberg-datafusion` (lib 80 + MemoryCatalog integration 9 incl. `test_insert_into_partitioned`) EARLY and
  after every transformer edit, not just at the end. The REE-mutation reproduced the ORIGINAL revert error
  byte-for-byte ("expected Utf8 but found RunEndEncoded at column index 1") in the datafusion integration
  test — proof the offline pin and the gate catch the same regression. (The pre-existing
  `table_provider_factory.rs` DOCTEST failure — `#[tokio::main]` without `rt-multi-thread`, present in HEAD,
  Cargo edits prohibited — is the only datafusion red and is unrelated.)
- **MUTATION-MECHANICS near-miss (re-learned): restore from the POST-EDIT snapshot, never a pre-edit one.**
  I mutated the file in place, then restored from a backup taken BEFORE my edits — wiping my own work. Recovered
  only because the mutation's `.mut` artifact held my edits-plus-mutation and I could reverse the mutation
  textually. RULE: snapshot the file AFTER your edits and immediately BEFORE each mutation; that is the only
  correct restore source. (Promoted-rule restatement, but it bit again — keep the post-edit `.bak` namespaced.)

### 2026-06-11 (Multi-spec closeout 3 — `removeRows` apply-side + dv_seq validation, BUILDER Group A)
- **The scheduled flip arrived: the 2026-06-09 "validation-only must be documented at EVERY surface" lesson's
  twin is "when the apply side lands, flip EVERY one of those surfaces — they all lied symmetrically."** *Why:*
  the deferral was deliberately stamped on the module doc, the field doc, BOTH method docs, AND the
  sibling-field contrast (`removed_delete_files` said "unlike removed_data_files which is validation-only").
  Landing the apply side means each of those must now say the OPPOSITE, and the symmetric ones (the two
  "unlike X (validation-only)" contrasts) flip to "like X" — miss one and the docs self-contradict. Grep the
  EXACT deferral phrase ("validation-only", "deferred", "OverwriteFiles' job") across the file, not just the
  method you changed; two test-comment instances also carried it.
- **The cheapest apply-side port is the one where the producer machinery already routes through the seam — you
  add nothing to snapshot.rs.** *Why:* `SnapshotProducer::commit` already does
  `self.removed_data_files = operation.delete_files(&self)` and feeds that into `process_deletes` (rewrite) +
  `summary()` (`remove_file`). `RowDeltaOperation::delete_files` returned `[]` (the validation-only seam). The
  ENTIRE apply-side port was: resolve the removed paths via the SAME shared `resolve_delete_paths`
  `OverwriteFiles` uses. Read overwrite_files.rs's `delete_files` FIRST and mirror it — don't invent a parallel
  removal path.
- **`operation()` confirmation is a real deliverable, not a formality — and `do_commit` order makes the
  ordering pin free.** *Why:* the brief asked to CONFIRM the 1.10.0 two-branch `operation()` is unaffected by
  removals. It is: it consults `addsDeleteFiles`/`addsDataFiles` only, never `deletesDataFiles()` — so
  remove-only and remove+add-delete are both Overwrite. And because `do_commit` runs `validate()` for ALL
  actions BEFORE any `commit()`, the removed∩referenced rejection (`validateNoConflictingFileAndPositionDeletes`,
  in validate) fires strictly before the apply-side removal (in commit) — the ordering test pins this by
  asserting the table is UNTOUCHED after the rejection, and it PASSES under the routing-sever mutation (proving
  it's a validate-time gate, independent of the apply path).
- **Place a fallibility-introducing check where the inputs already live; measure the ripple before rejecting
  "make it fallible".** *Why:* the dv_seq check needs BOTH the DV's seq and the data file's seq. The caching
  loader (the duplicate-DV check's home — the tempting "consistent placement") has NEITHER:
  `FileScanTaskDeleteFile`'s `From<&DeleteFileContext>` DROPS the manifest entry's sequence number, so placing
  it there means threading two new seqs through a public serialized struct. The index has both in hand
  (`seq_num` param + the DV's manifest entry) and ONE production caller (`scan/context.rs`, already
  `Result`-returning). The "infallible signature" that justified D1's deferral was a one-`?` ripple, not a
  scan-wide fallibility cascade — assess, don't assume. Two sibling checks need NOT share a home if their
  inputs differ.
- **A deferred-residue test often INVERTS when the residue lands — split it, don't just `.unwrap()` it.** *Why:*
  `test_dv_is_not_sequence_filtered` asserted the DV is returned at BOTH `dv_seq==data_seq` AND
  `dv_seq<data_seq` (the latter being exactly the invalid state the deferral let through). Landing the check
  makes the second assertion the ERROR case. Split: the valid-boundary half (==, >) keeps "DV applies, not
  seq-filtered"; the invalid half (<) becomes the new fail-loud test with the bytecode-exact message. Mirror
  the EXACT Java message from the jar (`javap -c` the `ldc` String constant), not the MAIN source — they can
  differ; here they matched.
### 2026-06-11 (ExpireSnapshots B1 — metadata retention semantics, BUILDER Fable, wt-expire)
- **Java 1.10.0 `RemoveSnapshots` retention facts (all bytecode-verified; MAIN source matches for
  every ported method, EXCEPT `cleanupLevel`, which is post-1.10.0):** retain a branch ancestor
  while `kept < minToKeep || ts >= cutoff` with an EARLY RETURN at the first failing ancestor (the
  retained set is a contiguous chain prefix — an out-of-order recent ancestor BEHIND the stop point
  is NOT kept); ref expiry retains on `now − ts <= maxRefAgeMs` (`main` exempt via a string compare
  BEFORE the snapshot lookup, so even an invalid main ref survives); unreferenced snapshots retain
  on `ts >= defaultExpireOlderThan` — the DEFAULT cutoff, never a per-ref one; `expireSnapshotId`
  ids form a SEED SET applied unconditionally after the only guard (retained-ref HEADS) — an
  explicit id that ancestry/age retention would keep is still removed; a pure
  `expireSnapshotId()` call STILL runs age-based expiry with the 5-day default (defaults always
  apply). Defaults: 432000000 / 1 / `Long.MAX_VALUE`; `gc.enabled` gate in the CONSTRUCTOR (fires
  even for metadata-only expiry).
- **The Rust `TableMetadataBuilder::remove_snapshots` was INCOMPLETE vs Java 1.10.0
  `rewriteSnapshotsInternal` in two ways, one of them a hard apply-path bug:** (1) no
  statistics/partition-statistics pruning for removed ids (Java records `RemoveStatistics` +
  `RemovePartitionStatistics` changes per id); (2) dangling refs swept with a SILENT `refs.retain`
  instead of Java's `removeRef` — no `RemoveSnapshotRef` change, and `current_snapshot_id` left
  pointing at the removed snapshot, so `build()` ERRORED ("Cannot set invalid snapshot log") on
  exactly the input Java handles (removing main's snapshot via the raw update). The snapshot-log
  pruning itself was already a faithful port. Lesson: when an action EMITS an update, audit the
  full Java apply-side sibling (`rewriteSnapshotsInternal`, not just `removeSnapshots`'s
  signature) — the emit half can be perfect while the apply half corrupts/errs on every catalog
  path.
- **`tracing` is NOT a dependency of the `iceberg` crate** (workspace-root only; the metrics
  module's `LoggingMetricsReporter` deferral note says a logging-facade dep needs approval). A
  Java `LOG.warn` port therefore becomes a documented silent branch — do not add the dep without
  approval, and grep the CRATE manifest (not the workspace root) before writing `tracing::`.
- **Requirements posture for snapshot removal:** Java derives NO `UpdateRequirement` for
  `RemoveSnapshots`/`RemoveSnapshotRef` (REST `UpdateRequirements` handles only `SetSnapshotRef`);
  its real safety is the `ops.commit(base, updated)` full-metadata CAS. The Rust action emits
  `RefSnapshotIdMatch` for EVERY ref consulted — between the two Java postures, and the only one
  that rejects the concurrent-rollback window (stale expiry destroying the new head) in a
  requirements-based catalog protocol. Pinned with a stale-guard-vs-rolled-back-base test.
- **Fixture trick for snapshot-log tests: build chains ONE `TableMetadataBuilder` pass per
  snapshot.** A single-pass `add_snapshot`+`set_ref` chain gets collapsed by the
  intermediate-snapshot suppression in `build()` (only the final head is logged); per-snapshot
  passes give the multi-entry log the prune tests need.
- **(Reviewer) A faithful port's COUNTERINTUITIVE branches are the unpinned ones — pin the
  early stop, not just the comparison.** The branch-walk early stop (contiguous-prefix retention)
  was perfectly ported but removable with the whole suite green: every fixture used monotonic
  timestamps, so the stop point never had a recent ancestor BEHIND it. Out-of-order timestamps are
  legal (`add_snapshot` tolerates 60s of clock skew), and Java EXPIRES a newer-than-cutoff
  ancestor behind the stop. Same pattern for the ref-age `<=` boundary and the refs-first update
  emission order (apply-side sweep self-heals a reorder, so only a shape assertion pins Java's
  REST change order). Mutation-test the SURVIVORS, not just the planned mutations.

### 2026-06-11 (ExpireSnapshots B2 — ReachableFileCleanup file cleanup, BUILDER Fable, wt-expire)
- **1.10.0's cleanup walks DELETE manifests through the DATA-manifest reader because the
  PROJECTION masks the content field — a MAIN-source reading concludes the OPPOSITE.**
  `ManifestFiles.readPaths` → `read(...)` carries `checkArgument(content == DATA, "Cannot read a
  delete manifest...")`, which reads as "delete manifests crash/leak in cleanup." Bytecode chain:
  `FileCleanupStrategy.MANIFEST_PROJECTION` selects only {manifest_path, manifest_length,
  partition_spec_id, added_snapshot_id, deleted_data_files_count} — `content` is NOT projected —
  and `GenericManifestFile`'s avro-read constructor DEFAULTS `content = ManifestContent.DATA`.
  Every cleanup-read manifest therefore claims DATA, the precondition never fires, and the
  data-reader reads delete manifests fine (the entry schemas share `file_path`), so position
  deletes / eq deletes / DV puffins ARE enumerated and cleaned. Lesson: a guard's reachability
  depends on how its INPUT was materialized — trace the projection/constructor defaults of the
  value feeding the guard, not just the guard.
- **Delete-file removal is BY PATH in Java exactly like the Rust port — so "two DVs in one
  puffin, remove one" removes BOTH, and the shared-puffin-survives cleanup pin must use the
  CROSS-MANIFEST share shape.** 1.10.0 `ManifestFilterManager.delete(F)` adds `file.location()`
  to the `deletePaths` CharSequenceSet (bytecode); the filter drops every same-path entry. The
  fixture that works: DM1 = {DV-A@P, DV-C@P3}; replacing DV-C rewrites DM1 → DMr carrying DV-A
  EXISTING (still @P); expiring DM1's snapshots then kills DM1 while P stays live in DMr — P
  survives, P3 dies, both directions in one fixture. The first fixture draft (sibling DVs, remove
  one) "failed" the survive pin — correctly: the puffin genuinely became unreachable.
- **ReachableFileCleanup's three failure tiers have OPPOSITE safety directions — port each tier's
  direction, not one blanket policy:** manifest-LIST reads `throwFailureWhenFinished` and run
  BEFORE any deletion (abort-clean ⇒ hard Err); candidate-manifest reads are SUPPRESSED (skip its
  files = under-delete; the manifest itself still dies — it is retained-by-nobody); a
  retained-manifest enumeration failure makes `findFilesToDelete` return the EMPTY set
  (catch-Throwable ⇒ when liveness cannot be proven, NO content file may die) while manifests +
  lists still get deleted (their subtraction needed no manifest reads). Rust shape:
  Err / collect-and-skip / collect-and-clear respectively, all surfaced in `CleanupReport`
  (collect-and-return replaces Java's log-and-continue under the no-logging-dep constraint).
- **Java deletes expired manifest-LIST locations UNCONDITIONALLY (no retained-shared check,
  bytecode 1.10.0 `cleanFiles` L80-103) — the Rust retained-shared guard is a deliberate
  under-deletion divergence and needs its own pin.** Safe in Java only because every Java-written
  snapshot owns a unique list file; a grafted/cloned-metadata shape breaks that. Pinned with a
  grafted snapshot whose `manifest_list` aliases a retained snapshot's list (a
  documented-but-unpinned divergence is indistinguishable from an accidental one).
- **The post-commit seam: capture `before` from the tx, `commit(catalog).await?`, clean
  `(before, committed.metadata())` — every staleness direction of that pair is safe, but NOT
  because a staler `before` "only shrinks" the expired set (REVIEWER correction): a concurrent
  EXPIRE between tx creation and commit GROWS `before − after` with ITS expirees.** Those are
  absent from the committed `after` too, so the sweep still touches only files unreachable
  from the current metadata — Java's `cleanFiles(base, current)` construction-time `base` has
  the identical property, and the growth is the benign double-delete race (the second sweep
  sees idempotent `Ok`s / collected failures per file — backend-dependent — or aborts at
  planning; pinned by the re-run test).
  Concurrent ADDS do shrink the set; using the commit's returned table instead of Java's
  `ops.refresh()` only misses later commits whose files are either already protected by the
  retained-side subtraction or newly added (never candidates). The M3 mutation
  (cleanup-not-gated-on-commit-success) needs the failing-catalog fixture to be built on a REAL
  table chain, or the pin passes vacuously — cleanup on a manifest-less fixture errors before
  the recorder is reached.

### 2026-06-11 (Variant arc B1 — variant binary format READ side, BUILDER Fable, wt-variant)
- **Java string APIs NEVER error on bad UTF-8 — `new String(bytes, UTF_8)` and `Charset.decode`
  silently substitute U+FFFD.** Any Rust port of a Java byte-parser must CHOOSE error-vs-replace
  deliberately and document it: the variant reader fails loud (DataInvalid) where Java would
  silently mangle a dictionary name; safe because Java's writer only emits valid UTF-8. This is a
  divergence CLASS for every future format port that decodes strings, not a one-off.
- **When Java binary-searches data that may not honor its sort invariant (untrusted bytes!), port
  the EXACT probe sequence — `VariantUtil.find` is inclusive-high `mid=(low+high)>>>1` — and the
  EXACT comparator — `String.compareTo` is UTF-16 code-unit order, NOT byte order.** A "merely
  equivalent" half-open binary search probes different indexes on unsorted data (hit where Java
  misses); byte-order comparison flips supplementary-vs-BMP orderings (U+10000 sorts BELOW U+FFFF
  in UTF-16, above in UTF-8). Both pinned with lying-sorted-dictionary / unsorted-object-fields /
  supplementary-char tests. A linear-scan "fix" of the miss would be a parity bug.
- **`VariantUtil.readLittleEndianUnsigned(size=4)` returns a SIGNED Java int — counts/offsets/
  lengths ≥ 2^31 are unrepresentable (negative → downstream throw).** Mirror the domain in Rust
  (reject > i32::MAX) or a ≥2GB buffer on 64-bit Rust would accept what Java cannot. Same family:
  binary/string lengths are signed i32 reads — reject negatives by name.
- **`SerializedObject.initOffsetsAndLengths` computes field lengths from SORTED-DISTINCT offsets
  (field order is by NAME, data order is free) and implicitly REJECTS duplicate offsets** (its
  `sortedOffsets.get(index+1)` throws when distinct-count < n+1); the LAST sorted offset keeps
  length 0 → empty-slice parse error on access. Arrays use CONSECUTIVE offsets instead. Two
  different length schemes in one format — porting one for both would corrupt object reads.
- **B2 note: `Variants.of(BigDecimal)` picks the SMALLEST decimal physical type by precision
  (≤9 digits → decimal4, ≤18 → decimal8, else decimal16)** — discovered when the intended
  19-digit "decimal8" fixture came back with a decimal16 header. Fixture-gen for the write side
  must control precision, not just scale. Also: 1.10.0 write-side classes live in iceberg-CORE
  (`Variants`, `PrimitiveWrapper`, `ValueArray`, `ShreddedObject`), not api; `ShreddedObject
  .writeTo` needs slf4j-api on the classpath (CloseableGroup static init).
- **Eager parsing of a lazily-specified format needs an EXPLICIT recursion budget:** Java's lazy
  reader never recurses deeper than the caller walks, so it has no depth limit; an eager Rust
  parse recurses to the data's full depth — a ~4-bytes-per-level array bomb would overflow the
  stack. `MAX_NESTING_DEPTH = 128` (serde_json's default), boundary-pinned both sides (128 Ok,
  129 Err, no overflow).

### 2026-06-11 (Variant arc B1 — REVIEWER Fable, wt-variant)
- **DO probe lazy-vs-eager accepted-set equivalence at the ZERO-COUNT boundaries — a lazy Java
  reader skips even MANDATORY trailing fields when the element count is 0.** The builder's
  "eager parse = same accepted set as a full Java traversal" claim was refuted by three
  degenerate shapes a live 1.10.0 probe ACCEPTED under full traversal: truncated empty object
  `[02 00]`, truncated empty array `[03 00]` (the constructor skips `initOffsetsAndLengths`
  when `numElements == 0`), and empty-dict metadata declaring data past the buffer end
  (`[01 00 05]` — Java keeps the buffer un-truncated when `endOffset >= limit`). Rust keeps the
  stricter spec-faithful rejection, now as a DOCUMENTED divergence. *Why it matters:* "errors
  on access" reasoning silently assumes there is something to access.
- **DO demand a POSITIVE decode test for every header-flag branch, not just hostile-input
  rejections — both is-large bit transpositions (object 0b1000000 ↔ array 0b10000) SURVIVED the
  original 57-test suite.** The reject-side tests kept passing because a misread count still
  failed the bound check; only a well-formed large array (4-byte count) and a well-formed
  NON-large object with 2-byte field ids (bit 4 set for the size field, not is-large)
  distinguish the bits. Generated those bytes WITH Java (probes p13/p14) so the pins are
  oracle-backed.
- **DO generate comparator-order fixtures WITH Java so the sort order is Java's own:** 1.10.0's
  `Variants.object` writes fields in `String.compareTo` (UTF-16) order — for names
  {U+FFFF, U+1F600} that is [😀, ￿], the INVERSE of byte order — and `Variants.metadata`
  preserves insertion order, setting the sorted flag only when the input already IS
  compareTo-sorted. A hand-built "sorted" fixture written in Rust byte order would pin the
  WRONG order and the comparator bug would pass.

### 2026-06-11 (Variant arc B2 — variant binary format WRITE side, BUILDER Fable, wt-variant)
- **Java 1.10.0 write-side width rules (bytecode-pinned, all matched MAIN):** every width is
  `VariantUtil.sizeOf(maxValue)` with UNSIGNED thresholds 0xFF/0xFFFF/0xFFFFFF — but the INPUT
  differs per site: array/object offsets use `sizeOf(dataSize)`, metadata offsets use
  `sizeOf(dataSize)` (and the dictionary COUNT is written at that same width), and object field
  ids use `sizeOf(metadata.dictionarySize())` — the dictionary SIZE, not the largest id used, so
  a 256-name dictionary writes 2-byte ids even though max id 255 fits one byte (fixture-pinned
  at both 255 and 256). A "minimal width" optimization would be byte-divergent.
- **`Variants.metadata` writes the dictionary in INSERTION order, never dedups, and sets the
  sorted flag only for STRICTLY `compareTo`-ascending input** (`last.compareTo(name) >= 0`
  clears it ⇒ duplicates clear it; single-name input IS sorted; the EMPTY metadata is NOT —
  Java returns the `01 00 00` EMPTY_V1 buffer whose flag bit is unset, a special case the
  windows()-based recompute must mirror). compareTo = UTF-16 code units: [U+10000, U+FFFF] is
  ASCENDING in Java order, DESCENDING in byte order — flag fixture-pinned.
- **`VariantUtil.writeLittleEndianUnsigned` MASKS oversized values — Java 1.10.0 silently emits
  CORRUPT metadata for >255 names whose data size still picks a 1-byte width (only reachable
  via empty names): probe-verified, 256 empty names serialize to `01 00 00`, losing every
  name.** The Rust door errors by name instead (divergence documented + pinned). When porting a
  writer, grep for the masking writes — each is a potential silent-corruption door to convert.
- **`ShreddedObject.writeTo` re-resolves field ids BY NAME at write time (`metadata.id(field)` +
  `checkState(id >= 0, "Invalid metadata, missing: %s")`) and emits fields via `SortedMerge` of
  `stream().sorted()` = String natural order = UTF-16.** The Rust split: the BUILDER sorts at
  `build()` (so serialization emits stored order), and the writer re-resolves ids against the
  passed metadata — wrong-metadata writes fail with Java's message instead of emitting dangling
  ids. Re-serializing a PARSED value canonicalizes (Java's SerializedValue copies its buffer
  verbatim); for everything Java's writer produced the bytes are identical (pinned per fixture).
- **The short-string spill decision is writeTo-time, by UTF-8 BYTE length ≤ 63
  (`PrimitiveWrapper.MAX_SHORT_STRING_LENGTH`)** — `Variants.of(String)` always constructs
  PhysicalType.STRING; the encoding form is not a constructor property. Decimal16 writing
  (reverse `BigInteger.toByteArray()` + sign-pad to 16) is exactly `i128::to_le_bytes` —
  pinned incl. i128::MIN (constructible only via the width-explicit factory; the precision
  factory rejects 39 digits with "Unsupported decimal precision: %s", boundaries 9/10 and
  18/19 fixture-pinned).
- **Big write fixtures don't need full hex pins: pin (java.util.zip.CRC32, length, first-64-bytes
  hex) — the existing deflate dependency's `Crc` computes the identical CRC-32** (the DV-arc
  discovery reused for byte evidence). Used for the 7 fixtures ≥ 513 bytes incl. the 65535/65536
  offset-width boundary arrays; everything ≤ ~530 bytes stays full-hex.
- **B1's classpath lesson extended: `ShreddedObject.writeTo` ALSO needs avro AND caffeine jars**
  (SortedMerge's `MergeIterator` init touches CloseableGroup/util statics) — slf4j alone gets
  you primitives/arrays/metadata but the FIRST object write throws NoClassDefFoundError.

### 2026-06-11 (Variant arc B2 — REVIEWER Fable, wt-variant)
- **An `is_err()` assertion does not pin a fail-fast guard.** Removing the write side's
  up-front whole-span door (`door_value_span`) survived every undersized-buffer test: the
  per-write bounds checks still returned Err, but only AFTER partially mutating the caller's
  buffer. When a guard exists for ATOMICITY (no side effects before the error), the pin must
  assert the no-side-effect property itself (buffer still all-zero on failure), not just that
  an error came back. DO byte-compare the untouched buffer in door tests; DO NOT trust
  mutation-killed status of neighboring `is_err` pins.
- **CRC+length+prefix pins are acceptable ONLY after a one-time out-of-band full-byte diff.**
  Review closed B2's CRC gap by having Java dump the COMPLETE bytes of every CRC-pinned
  fixture to /tmp and end-to-end byte-diffing Rust's output (first-divergence index reported);
  all 7 matched, so the compact in-repo pins stand. A reviewer of a new CRC-pinned fixture
  set should repeat that probe rather than trust CRC32 alone (CRC collisions are trivial to
  miss adversarially).
### 2026-06-11 (DeleteOrphanFiles A1 — `Storage::list` prefix primitive, BUILDER Opus, wt-orphan)
- **DO mirror EACH storage backend's existing `delete_prefix` prefix semantics in `list`, not a
  single global rule.** *Why:* Java's `SupportsPrefixOperations` interface doc explicitly says
  hierarchical filesystems may require the prefix to NAME A DIRECTORY while key/value object stores
  allow ARBITRARY STRING PREFIXES. The fork's `delete_prefix` already split exactly this way —
  `local_fs` uses directory semantics (`path.is_dir()` → `remove_dir_all`), `memory` uses
  string-prefix semantics (append `/`, `starts_with`). `list` and `delete_prefix` MUST agree per
  backend on "under the prefix": a disagreement means A2 (the orphan-file GC) lists with one rule
  and deletes with another, which is a data-loss class. local_fs `list` walks the dir tree
  (sibling `ab2/` never matches prefix `ab` because they are distinct directory boundaries);
  memory `list` enforces the trailing-`/` boundary so prefix `dir` excludes sibling key `dir2/...`.
  Pinned both backends' sibling-boundary case (the over-listing = over-deletion risk).
- **DO make the trait DEFAULT body for an optional capability ERROR LOUDLY (`FeatureUnsupported`),
  never return an empty `Vec`.** *Why:* an empty listing reads downstream (A2) as "no orphans /
  everything orphan" — a silent empty answer from a backend that simply cannot enumerate would
  corrupt the orphan decision. The defaulted body on the `#[typetag::serde]` `Storage` trait also
  keeps external implementors compiling (the reason it is defaulted rather than required). Pinned
  with a stub `Storage` that overrides every method EXCEPT `list` and asserts the error kind +
  message-names-the-op.
- **opendal 0.55 `Metadata::last_modified()` returns `opendal::raw::Timestamp` (a newtype over
  `jiff::Timestamp`), NOT `chrono::DateTime` and NOT a directly-millisecond-able type.** *Why it
  bites:* the obvious `.as_millisecond()` (a `jiff::Timestamp` inherent method) does NOT exist on
  the opendal newtype, and naming `jiff::` in a `use` would need `jiff` as a DIRECT dep (Cargo edit
  — forbidden). The clean escape: opendal provides an infallible `impl From<raw::Timestamp> for
  std::time::SystemTime`, so convert through `SystemTime` and `duration_since(UNIX_EPOCH)` — pure
  `std`, zero new deps. (The 0.55 upgrade note saying "metadata APIs now use `jiff::Timestamp`" is
  about the INNER type; the surfaced public type is `opendal::raw::Timestamp`.)
- **`Duration::as_millis()` is `u128`; guard the `i64` conversion with `i64::try_from(...).unwrap_or(
  i64::MAX)`, not `as i64`.** *Why:* a far-future mtime would wrap silently under `as`; `try_from`
  saturates. Same guard used in all three `*_to_millis` helpers (local_fs mtime, memory write-time,
  opendal last-modified), and pre-epoch clamps to `0` so `created_at_millis` stays non-negative
  (the test asserts `>0 && <= now`).
- **The OpenDAL stretch needed a per-file `stat`, not the lister's inline metadata.** *Why:* some
  opendal backends do not populate `content_length`/`last_modified` on a `list` entry; `stat`-ing
  each FILE entry (skipping dir markers via `entry.metadata().is_file()`) guarantees authoritative
  size + mtime across backends. Cost is one extra request per file — acceptable for a
  correctness-first GC-input primitive; documented in the method doc. Location reconstruction:
  entries' `.path()` are operator-relative, so re-prefix with `base = &path[..path.len() -
  relative_path.len()]` (the scheme-qualified portion `create_operator` stripped).

### 2026-06-11 (DeleteOrphanFiles A1 — REVIEWER Opus, wt-orphan)
- **DON'T add an `is_empty()` shortcut to a `list` prefix-construction that the matching
  `delete_prefix` lacks — it silently breaks the list/delete_prefix agreement at the empty/root
  prefix.** *Bug found & fixed:* `MemoryStorage::list` used `if normalized.is_empty() ||
  normalized.ends_with('/')` while `delete_prefix` used only `if normalized.ends_with('/')`. For an
  empty normalized prefix (`memory://`), `list` matched on `""` (every key) but `delete_prefix` built
  `"/"` and matched nothing (memory keys are stored leading-slash-stripped). So `list` reported all
  keys while `delete_prefix` removed none — the over-listing direction, the exact data-loss class A2
  guards against. Fix: make `list`'s prefix construction byte-identical to `delete_prefix`'s. When two
  methods must "agree," write the boundary logic ONCE and copy it verbatim; any asymmetry is a latent
  bug. (Note: opendal had the same `is_empty()` branch but is NOT a bug there — opendal-memory treats
  `list_with("")` and `list_with("/")` identically and `remove_all("/")` removes everything, so they
  agree regardless; verified by probe before leaving it.)
- **`std::fs::DirEntry::metadata()` does NOT follow symlinks (it is `lstat`-based) — this is the
  load-bearing safety property for a filesystem prefix-walk feeding a GC.** *Why it matters:* an
  explicit-stack dir walk that classifies entries with `DirEntry::metadata()` automatically (a) does
  not loop on a symlinked-directory cycle (`a/loop -> a`), because the symlink is neither `is_dir()`
  nor `is_file()` so it's skipped; and (b) cannot pull files from OUTSIDE the prefix into the listing
  via an escaping symlink (`table/out -> /elsewhere`) — which would otherwise become A2 deleting live
  data outside the table root. It also agrees with `remove_dir_all` (removes a dir-symlink itself,
  never follows it). Verified with cycle + escape probes; pinned with permanent tests; documented the
  "skip symlinks, do not follow" contract in the method doc so nobody "fixes" it to follow links.
  (If you ever switch to `walkdir` or `fs::metadata`/`stat`, you re-open both holes.)
- **A mutation that an offline backend can't exercise is a real coverage gap to NAME, not a test to
  fake.** The opendal `entry.metadata().is_file()` filter (skip directory markers) is correct but
  un-pinnable on opendal-memory (flat store emits no dir markers); dropping it changed nothing in the
  smoke test. Don't contort a memory test to "catch" it — flag it as needing a live S3/HDFS fixture
  (the same class as the builder's `file_io_s3_test`-needs-MinIO flag) and move on.

### 2026-06-11 (DeleteOrphanFiles A2 — the `DeleteOrphanFiles` action, BUILDER Opus, wt-orphan)
- **DeleteOrphanFiles' valid-file universe is the OPPOSITE liveness rule from `expire_cleanup`'s, so
  do NOT extract a "reachability helper" — re-derive it locally.** *Why:* `expire_cleanup`
  (`ReachableFileCleanup`) computes a `before − after` DELTA and subtracts on `is_alive()` (status !=
  DELETED). DeleteOrphanFiles' universe is the FULL reachable set across ALL snapshots with NO
  liveness filter — Java `BaseSparkAction.contentFileDS` flat-maps each manifest through
  `ManifestFiles.read`/`readDeleteManifest` (the iterators that yield EVERY entry, incl. DELETED
  tombstones), NOT `liveEntries()`. The two are structurally different set-algebras over the same
  primitives (`load_manifest_list` → `load_manifest` → entries); sharing code would mean a flag that
  toggles liveness — the wrong-abstraction trap. Re-deriving cost ~40 lines and touched ZERO
  expire_cleanup lines (its 17 tests stayed green by CONSTRUCTION, not by re-running an extraction).
  When two callers want the same DATA primitive but OPPOSITE filtering, that's not a shared helper.
- **The DELETED-tombstone reachability clause is unconstructible-distinct from a live-only walk on a
  multi-snapshot table — pin the OBSERVABLE risk (history survival) and NAME the coverage limit.**
  *Why:* mutation M1 (add `if entry.is_alive()` to the universe collection — the `expire_cleanup`
  behavior) SURVIVED the whole suite. Reason: a file added by ANY snapshot is ALIVE in that snapshot's
  manifest, and the universe spans all snapshots, so a file referenced ONLY by a DELETED tombstone
  (with no live entry anywhere) cannot be produced by real commits. So the no-liveness-filter is
  Java-faithful but behaviorally identical to live-only for normally-written tables. Kept the
  Java-faithful no-filter; renamed the test to what it ACTUALLY pins (a copy-on-write-deleted file
  survives because the PRIOR snapshot references it) and documented the limit in the test + module
  doc. Same class as A1's opendal-`is_file`-untestable-on-memory gap: name it, don't fake it.
- **URI normalization's load-bearing asymmetry: a scheme-less VALID path matches an actual of ANY
  scheme, but a concrete VALID scheme does NOT match an absent actual scheme.** *Why:* Java
  `FileURI.uriComponentMatch(valid, actual)` = `Strings.isNullOrEmpty(valid) ||
  valid.equalsIgnoreCase(actual)` (core 1.10.0 bytecode-verified) — the NULL/EMPTY test is on the
  VALID (metadata) side only. So metadata storing bare `/tmp/.../a.parquet` (scheme None) matches a
  listing returning `file:///tmp/.../a.parquet` (scheme `file`) — null valid matches any actual; but
  metadata storing `file://...` vs a bare-path listing is a REAL conflict (`file` != None). Getting
  the direction backwards makes every local-fs table either report all files as scheme-conflicts
  (ERROR-fail) or conflate distinct schemes (delete live data). Pinned both directions at the unit
  level. The local `split_uri` mirrors Hadoop `new Path(s).toUri()` for the writer-produced URI
  shapes (bare path / `scheme:/p` / `scheme://auth/p` / `scheme:///p`); Windows drive letters are
  flagged as out-of-parity-scope.
- **The DeleteOrphanFiles ACTION is MAIN-source (no 1.10.0 Spark bytecode locally), but every
  load-bearing PRIMITIVE it delegates to IS in core/api 1.10.0 — javap them and flag the residue.**
  *Why:* `DeleteOrphanFilesSparkAction` lives in `iceberg-spark` (no 1.10.0 spark jar here), so the
  defaults (3-day `olderThan`, `EQUAL_SCHEMES_DEFAULT`, merge order), the messages, and the
  universe composition are MAIN-only. But `FileURI`, `DeleteOrphanFiles$PrefixMismatchMode`,
  `HiddenPathFilter`, and `FileSystemWalker$PartitionAwareHiddenPathFilter` ARE in
  iceberg-core/iceberg-api 1.10.0 — javap-verified the match rule, the enum, the `_`/`.` hidden
  rule, and the `_<field>=` partition exception. State the MAIN/bytecode split explicitly in the
  module doc so the reviewer/next-agent knows which facts are 1.10.0-pinned and which ride MAIN.

### 2026-06-11 (DeleteOrphanFiles A2 — REVIEWER Opus, wt-orphan)
- **"Unconstructible with real commits" is almost never true — reach for a SECOND maintenance action
  to construct the missing precondition.** *Why:* the A2 builder declared M1 (an `is_alive()` filter
  on the all-snapshots universe) unconstructible because a file added by any snapshot is alive in that
  snapshot's manifest. That's true UNTIL you EXPIRE the adding snapshot. The fork's own ExpireSnapshots
  is metadata-only (it deletes no files — cleanup is the opt-in B2 sibling), so expiring S1 after a
  copy-on-write delete leaves the data file on disk referenced ONLY by S2's DELETED tombstone — exactly
  the tombstone-only state. The orphan sweep must spare it (the no-liveness universe does; an `is_alive`
  filter deletes it → history corruption). Lesson: when a mutation "can't be triggered," compose two
  actions (write + expire, append + rewrite, etc.) before believing it. The kill test cost ~70 lines.
- **For a deleter, prove a URI-normalization divergence is SAFE by checking BOTH representation sides
  come from the same producer — don't just diff against the Java oracle.** *Why:* Rust `split_uri` does
  NOT collapse `//` or decode `%xx` the way Hadoop `new Path(s).toUri()` does (ground-truth via
  `java -cp hadoop-client-api PathProbe`). That LOOKS like a split-the-live-file bug, but it is SAFE
  because the metadata-writer and the lister both carry the IDENTICAL raw string for a Rust-native
  table, so they still join on the same key. The split only bites cross-engine (Java-normalized metadata
  vs Rust-listed paths) — i.e. interop, which is deferred. Verify the no-corruption claim with an e2e
  trailing-slash-warehouse probe, not just a unit diff.
- **A scheme-qualified table `location` can silently NO-OP the orphan sweep on a scheme-stripping
  backend — under-deletion, safe, but a real masking limitation.** *Why:* the hidden-path filter walks
  `relative_under(base = table.location, listed)`; when `base` is `file://…` but the local-fs lister
  returns BARE paths (it strips `file://`), `strip_prefix` fails → every file is treated as hidden →
  zero candidates → the sweep deletes nothing (orphans included). This NEVER deletes live data (the bias
  is correct), but it means a `file://`-located table is never cleaned. OpenDAL (S3/Glue) re-prefixes
  listed entries WITH the scheme, so base and listing agree there. If this needs fixing later, normalize
  `self.location` to the listing's representation BEFORE the hidden filter — but that touches a deleter's
  match surface, so pin it heavily first. Flagged, not fixed (architectural, out of reviewer scope).

### 2026-06-11 (A3 — ExpireSnapshots Java interop, BUILDER Opus, wt-orphan)
- **DO force Java down `ReachableFileCleanup` (the only ported strategy) with a SURVIVING TAG, and
  cite the selection condition from bytecode.** *Why:* Java `RemoveSnapshots.cleanExpiredSnapshots`
  (1.10.0 bytecode) picks INCREMENTAL when `incrementalCleanup==null && !specifiedSnapshotId &&
  !hasRemovedNonMainAncestors(base,current) && !hasNonMainSnapshots(current)`, else REACHABLE. Rust
  ports ONLY `ReachableFileCleanup`. A surviving non-main ref (a tag/branch) makes `hasNonMainSnapshots`
  true ⇒ Java picks Reachable — so both engines run the SAME algorithm. The tag also doubles as the
  ref-protection judgment-surface element (a tag pinning an otherwise-expirable mid-chain snapshot). The
  abstract `ReachableFileCleanup.cleanFiles` is 2-arg `(base, current)` = the Rust 2-state
  `clean_expired_files`; the 3-arg `cleanFiles(base, current, cleanupLevel)` in `cleanExpiredSnapshots`
  is a newer overload not in 1.10.0's abstract method.
- **DO compare cross-language DELETED-FILE sets via a path-INDEPENDENT `<funnel>@ord<N>` descriptor
  multiset, NEVER raw paths.** *Why:* Java and Rust write their tables at DIFFERENT absolute paths
  (random manifest/list UUIDs, different temp roots), so a `deleteWith`/`CleanupReport` path set is
  uncomparable. Normalize each deleted file to `<funnel>@ord<N>` (funnel = content/manifest/manifest_list/
  statistics; ordinal = the owning snapshot's position in the PRE-EXPIRE metadata's sequence-number
  order): a manifest list → its owning snapshot; a manifest → its `added_snapshot_id`; a content file →
  its manifest's `added_snapshot_id`; a stats file → its `snapshot_id`. The multiset is identical on
  both sides because the snapshot GRAPH is logically identical. (Same family as the E1 snapshot-id →
  ordinal canonicalization, extended to the deleted-file set.) An UNCLASSIFIED token makes a
  misattribution fail the comparison loudly rather than drop out.
- **The shared `SnapshotMetaOracle.emit` / `snapshot_meta_view` PANIC on an EXPIRED table — a surviving
  snapshot's parent was removed, so `ordinals.get(parentId)` is null (Java NPE) / `ordinals[&parent_id]`
  panics (Rust).** *Why it bit:* the expire fixtures are the FIRST interop fixtures where a retained
  snapshot's parent is expired out of the table; every prior fixture (write-actions/rowdelta/dv) only
   APPENDS, so the parent is always present and the branch was dormant. Fix: emit `null` `parent_ordinal`
  when the parent has no in-table ordinal. The Java emitter (in scope — `InteropOracle.java`) got the
  fix directly; the Rust shared `common/snapshot_meta_view.rs` is OUT of A3 scope, so `interop_expire.rs`
  carries a LOCAL `expire_meta_view` copy with the fix (and inlines `SUMMARY_COUNT_KEYS` to avoid pulling
  the unused shared view builder in as dead code). When an interop increment is the first to feed a
  shared materializer a new metadata SHAPE, audit the materializer's `null`/absent-key handling for that
  shape before assuming reuse.
- **DETERMINISTIC-by-OUTCOME beats re-stamping when the comparison is timestamp-agnostic.** *Why:* the
  Java oracle re-stamps snapshots to fixed timestamps (`T0 + 1000*ordinal`) so its cut is wall-clock-
  independent — but re-stamping through the Rust PUBLIC catalog path is impossible (`do_commit` reloads
  from the catalog, losing an in-memory re-stamp; the action's `commit()` + `Table::with_metadata` are
  private). It is also UNNECESSARY: the canonical view + the deleted descriptor both key on ORDINALS, never
  raw timestamps, so the comparison is timestamp-agnostic and only the expiry OUTCOME must match. The Rust
  side commits real appends, reads the ACTUAL head timestamp, and uses `expire_older_than(head_ts) +
  retain_last(1)` (the head is the unique newest ⇒ everything strictly older expires) — the same outcome
  as Java's fixed cut, through the production `ExpireSnapshotsAction` + `ExpireSnapshotsCleanup::
  commit_and_clean` with a collect-only deleter (so Java can still read the table). Re-stamping is only
  needed when the comparison itself reads timestamps.
- **STOP-FINDING (real Rust divergence, separate increment): `FastAppend::existing_manifest`
  (append.rs:148) DROPS all-tombstone manifests; Java `FastAppend.apply` carries ALL prior manifests
  forward.** *Why:* Java's `apply` does `manifests.addAll(snapshot.allManifests(io))` with NO filter
  (1.10.0 MAIN, verified) — every prior manifest, including a manifest left ALL-DELETED by a copy-on-write
  delete that emptied it. Rust filters to `has_added_files() || has_existing_files()`, so a fast_append
  AFTER an emptying delete drops the all-tombstone manifest from the manifest LIST. Surfaced by a `rewrite`
  fixture (append d1 → delete d1 → append): Rust's deleted descriptor carried an extra `manifest@ord2`
  (the dropped all-tombstone manifest, now unreferenced and GC'd) that Java keeps referenced. Net effect
  is benign (the dropped manifest holds only a tombstone — no data loss) but it is a real manifest-LIST
  structure divergence that would also bite general append-after-delete interop. FIXTURED AWAY for A3: the
  `rewrite` delete now leaves a sibling file EXISTING (1-existing + 1-deleted manifest, carried by both
  engines), so A3 pins ONLY the expire+cleanup surface. Reported on the GAP_MATRIX row for its own
  increment. *Rule:* when an interop fixture diverges, trace WHICH production path differs (here:
  FastAppend, not the expire modules under test) before attributing it to the increment's subject — and if
  it is outside the increment's scope, fixture it away + report rather than expanding scope.

### 2026-06-11 (A3 — ExpireSnapshots Java interop, REVIEWER Opus, wt-orphan)
- **A SURVIVING TAG ALONE DOES NOT FORCE `ReachableFileCleanup` — `hasNonMainSnapshots(current)`
  needs a survivor OFF the POST-EXPIRY main ancestry, and a head-tag leaves every survivor ON it.**
  *The central A3 claim was cross-strategy, not 1:1.* A reflection probe on Java's private
  `incrementalCleanup` proved 4/5 fixtures (linear/stats/deletes/rewrite) ran Java's
  **IncrementalFileCleanup**, not the Reachable strategy the Rust side ports. Bytecode
  (`RemoveSnapshots.cleanExpiredSnapshots`, 1.10.0): incremental iff `!specifiedSnapshotId &&
  !hasRemovedNonMainAncestors(base,current) && !hasNonMainSnapshots(current)`. `hasNonMainSnapshots`
  is true only when a SURVIVING snapshot is not in `mainAncestors(current)` = the head's parent-walk
  (which STOPS at the first expired parent). A tag on the HEAD (every survivor on main) ⇒ all three
  false ⇒ Java auto-picks INCREMENTAL; only a tag on a MID-CHAIN survivor (tag_protected) auto-picks
  Reachable. The sets coincided for these shapes (force-probed both ways), so the comparison passed
  even cross-strategy — a coincidence of shape, NOT proof of the Reachable port. FIX: force Java's
  Reachable via the REAL engine selector `((RemoveSnapshots) api).withIncrementalCleanup(false)`
  (package-private, same package; offset-verified — pre-sets the field so `cleanExpiredSnapshots`
  skips auto-derivation), against the identical fixture sets. RULE: when an interop harness pins a
  SPECIFIC algorithm the other side selects among several, ASSERT the selection (probe the strategy
  field / log), never assume a fixture knob forces it — and prefer a real public/engine selector over
  reshaping fixtures so coverage is unchanged.
- **A PATH-INDEPENDENT deleted-file descriptor keyed ONLY on the owning-snapshot ordinal is
  NON-INJECTIVE over sibling files of one commit — a swap passes set-equality vacuously.** Two
  DIFFERENT content files in a 2-file append share `added_snapshot_id` ⇒ both `content@ord<N>`, so a
  Rust-deletes-X / Java-deletes-Y swap is invisible to the multiset compare. FIX: add a
  cross-language-STABLE per-file discriminator (`#rc<record_count>` for content, `#a<>e<>d<>` file
  counts for manifests, `#sz<file_size>` for stats; a manifest LIST needs none — one per snapshot).
  Pin it OFFLINE (fail-before/pass-after on the bare-ordinal scheme), since the env-gated chain
  passing is not evidence of injectivity. The ordinal itself (sequence-number position) is fine —
  semantically meaningful + V2-stable; the hole was granularity, not order. GENERAL: a structural
  cross-language descriptor must be INJECTIVE over the set it compares, or it manufactures false
  set-equality — discriminate by a content fingerprint, not just the owning container.
- **A delete/DV-bearing interop fixture does NOT exercise delete-manifest CONTENT cleanup unless a
  manifest actually DIES — a carried-forward delete manifest is never read by the cleanup walk.**
  `deletes` (row-delta carried into the head) leaves `manifestsToDelete` EMPTY, so the content-file
  subtraction block (the only place `load_manifest`/`ManifestFiles.read` runs on a delete manifest) is
  gated off on BOTH sides; the fixture pins metadata + manifest-LIST GC on a delete-bearing table, not
  delete-manifest content cleanup (that is the B2 unit tests' job). Corrected the oracle's "the delete
  manifest is read but spared" overclaim. RULE: for a fixture asserting "feature X traverses path P,"
  confirm P actually EXECUTES for that input (here: a gate `if !manifests_to_delete.is_empty()` made it
  inert) — an inert feature is false coverage even when the comparison is green.
- **DON'T `git checkout --` an UNCOMMITTED file to revert a probe edit — it reverts to HEAD and WIPES
  the (uncommitted) builder work underneath.** I reverted a Java probe with `git checkout -- InteropOracle.java`
  and lost the builder's entire uncommitted `ExpireOracle` (HEAD predates it); recovered only because the
  full diff had been captured to a tool-results file and re-applied with `git apply`. RULE for reverting
  probes on uncommitted files: snapshot the file to `/tmp` BEFORE the probe edit and restore from THAT
  (`cp`), or undo the exact textual edit — never `git checkout`/`git restore` a path with uncommitted work.

### 2026-06-11 (Wave-4 Group O / O1 — FastAppend all-tombstone-manifest carry, BUILDER Opus, wt-rewrite)
- **`FastAppend` (`newFastAppend`) and `MergeAppend` (`newAppend`) carry prior manifests by DIFFERENT
  rules — the "same-class sweep" answer is NO-FIX for merge_append, settled from bytecode.** *Why:*
  `FastAppend.apply` (1.10.0 bytecode offsets 89-94 + `core/FastAppend.java`) does
  `manifests.addAll(snapshot.allManifests(io))` with NO predicate (`BaseSnapshot.allManifests` returns
  the manifest list verbatim) ⇒ ALL prior manifests carry forward, including a manifest left
  ALL-DELETED by a copy-on-write delete. But `MergingSnapshotProducer.apply` (the producer `MergeAppend`
  extends, L1007-1011) filters its carried set through `shouldKeep = hasAddedFiles() OR hasExistingFiles()
  OR snapshotId() == snapshotId()` — which DROPS all-tombstone prior manifests (the third clause is
  unreachable for a pure append: no carried manifest was written by the not-yet-committed snapshot). So
  the Rust `has_added/existing` filter is a DIVERGENCE in fast_append (fix = carry unfiltered) but exact
  PARITY in merge_append (keep the filter). Lesson: when sweeping a sibling for "the same bug," read the
  sibling's OWN Java carry path — `newFastAppend` and `newAppend` route through different `apply`s, and
  the merging one legitimately filters what the non-merging one keeps.
- **There are THREE places this filter pattern appears in the producer family — only ONE was the bug.**
  `append.rs` `existing_manifest` (the bug, now `entries().to_vec()`), `merge_append.rs`
  `existing_manifest` (intentional `shouldKeep` parity — KEPT), and `snapshot.rs` `process_deletes`
  L1100 (carry-if-no-matching-delete, ALSO correct: a delete-bearing op's `shouldKeep` analogue; the
  rewritten all-tombstone manifest is pushed unconditionally elsewhere in the loop). For fast_append
  `process_deletes` returns early (`delete_files` empty), so append.rs:148 was the SOLE drop point.
  Grep `has_added_files() || has_existing_files()` before assuming one site.
- **The on-disk reproduction must RE-PARSE the manifest-list FILE, and the producer of the
  all-tombstone state is an EMPTYING copy-on-write delete (append d1 → delete d1) — not a hand-built
  fixture.** *Why:* `delete_files(["d1"])` rewrites d1's manifest to {d1: Deleted}, 0 live, which
  `process_deletes` pushes into the manifest list (added_snapshot_id = delete snapshot,
  `has_deleted_files` only). The next commit's `existing_manifest` reads that real on-disk list. Fail
  before / pass after: with the filter the new snapshot's list omits the tombstone manifest path; without
  it, it is present. Two co-pins keep the fix honest WITHOUT relying on the env-gated interop chain: a
  scan-unchanged pin (the carry does NOT resurrect the deleted row — the worst regression class) and a
  merge_append-still-drops pin (the deliberate asymmetry). Both PASS under the OLD filter too, so only
  the fail-before reproduction proves the fix — the other two guard against over-correcting.
- **Carrying an extra all-tombstone manifest forward changes NO snapshot summary counter** — Rust does
  not emit `manifests-created/-kept/-replaced` (only `RewriteManifests` does), and `total-*`/`added-*`
  counters come from FILE accounting (`added_data_files`/`removed_data_files` seeded from the prior
  summary), never from counting manifest-list entries. The full lib suite (2000→2003, only the 3 new
  tests added) passed unchanged — corroborating zero knock-on. Orphan/expire universes read ALL
  manifests and are strictly SAFER (one fewer dropped-then-relisted manifest).

### 2026-06-11 (Wave-4 Group O / O1 — REVIEWER Opus, wt-rewrite)
- **The merge_append NO-FIX verdict is CONFIRMED from 1.10.0 bytecode, both sub-questions.** `javap -c
  MergingSnapshotProducer` → `lambda$apply$16` (the `shouldKeep` Predicate) is exactly `hasAddedFiles()
  (offset 1, ifne 35) || hasExistingFiles() (offset 10, ifne 35) || manifest.snapshotId().longValue()
  == this.snapshotId() (offsets 18-32)`. (a) The third clause is UNREACHABLE for a carried manifest in a
  pure merge_append: `filterManifest` returns the manifest VERBATIM (old snapshot id) when there are no
  matching deletes (`!canContainDeletedFiles` → `return manifest`, offsets 23-45) — only a REWRITTEN
  manifest gets the new snapshot id, and merge_append rewrites nothing. So an all-tombstone carried
  manifest fails all three clauses ⇒ dropped, matching Rust's `has_added || has_existing`. (b) `shouldKeep`
  (apply local 8) is applied to BOTH the data-manifest iterable (apply offset 175) AND the delete-manifest
  iterable (offset 191) — so Java drops all-tombstone DELETE manifests in the merge path identically;
  Rust's content-agnostic filter matches. MAIN `MergingSnapshotProducer.java` L1003-1011 confirms the
  bytecode (the builder's "L1007-1011" citation is accurate; the third clause is L1007).
- **Entry-copy fidelity had a TEST GAP — closed by the reviewer.** `to_vec()` clones `ManifestFile`
  verbatim (Java `allManifests` carries the cached objects with no `copyOf`/`withSnapshotId` — that
  restamp is only `lambda$apply$1`, the unported `appendManifest()` path). But the original reproduction
  test only checked the carried entry's PATH + tombstone counts, NOT `added_snapshot_id`. Mutation —
  restamping `added_snapshot_id` to the new snapshot id — left the O1 tests GREEN and only tripped
  cross-cutting concurrent-conflict tests in overwrite/row_delta/replace_partitions/cherry_pick
  (`added_*_files_after` reads `added_snapshot_id`). Reviewer strengthened the reproduction pin to a full
  `ManifestFile` `==` against the pre-carry entry (covers added_snapshot_id, both seq numbers, all counts,
  partition summaries); mutation-verified it now fails directly on the `added_snapshot_id` diff. Lesson:
  a "carried verbatim" pin must field-compare against the SOURCE entry, not just re-assert the entry's own
  derived predicates — re-inheritance corruption hides in fields the predicate doesn't read.
- **Manifest-LIST entry ORDER diverges from Java but is NOT a spec/interop contract (REPORT, not fix).**
  Java `FastAppend.apply` writes NEW manifests FIRST then carried `allManifests` LAST (bytecode offsets
  4-23 then 74-99; MAIN `FastAppend.java` L153 then L166). Rust `SnapshotProducer::manifest_file`
  (snapshot.rs:1030-1039) writes CARRIED first (`process_deletes`) then NEW (`write_added_manifests`
  extend) — the OPPOSITE order. This is PRE-EXISTING (O1 only changed which carried entries survive, not
  the carried-vs-new order) and the canonical interop oracle (`snapshot_meta_view.rs:107-127`) SORTS
  manifests by a deterministic tuple precisely because manifest-list file order is "writer-dependent, not
  a spec contract." Both readers reconcile by sequence number, order-agnostic. So the three interop chains
  pass and the divergence is cosmetic. Fixing it would touch the shared `manifest_file` path (blast radius:
  every action) — out of O1 scope.
- **Comment drift: merge_append.rs:282 now LIES after O1.** It says its `existing_manifest` "Mirrors
  `FastAppendOperation::existing_manifest` exactly so the carried set is byte-identical" — but O1 made
  fast_append carry UNFILTERED while merge_append still filters. The two are now DELIBERATELY different.
  Reported to the orchestrator (merge_append.rs production is outside the reviewer's allowed file set; a
  one-line comment correction is the only follow-up).

### 2026-06-11 (Wave-4 Group O / O2 — `RewriteDataFiles` bin-pack compaction, BUILDER Opus)
- **BIN-PACK COMPACTION READS DELETES-APPLIED, which makes it SAFER than plain `RewriteFiles` for an
  EXISTING delete — and the "resurrection-without-seq" test premise is WRONG.** *Why:* the Spark
  runner (`SparkBinPackFileRewriteRunner.doRewrite`, MAIN) reads each group via the normal Iceberg
  scan, so merge-on-read deletes are APPLIED and the rewritten file contains only LIVE rows. So an
  EXISTING equality-deleted row is physically gone from the output REGARDLESS of the stamped seq —
  it cannot resurrect (that is the whole point of `DELETE_FILE_THRESHOLD`: rewrite a delete-laden file
  to physically drop its deletes). The naive "compact without `use_starting_sequence_number` ⇒ y=20
  resurrects" test FAILED because y=20 was never written to the new file. The seq flag's REAL
  load-bearing role is keeping a CONCURRENT equality delete (one added after planning, at a higher
  seq) still applying to the rewritten data — that is what `RewriteDataFilesCommitManager`'s
  `dataSequenceNumber(startingSnapshot.sequenceNumber())` preserves. Re-cast the broken test to an
  ON-DISK seq mechanism pin (raw avro, pre-inheritance, both directions: with-flag ⇒ explicit
  starting seq; without ⇒ `None`/re-inherit-fresh) — that is mutation-sensitive (the seq-drop mutation
  fails it) and CORRECT, where the scan-resurrection claim was a category error. Contrast with plain
  `RewriteFiles` (rewrite_files.rs crown jewel), which rewrites the file's RAW bytes (delete still
  present), so THAT path genuinely resurrects without seq preservation — the two actions have
  different read semantics and therefore different resurrection physics.
- **THE STAMPED SEQ IS THE STARTING SNAPSHOT'S, not the file's own (1.10.0 bytecode-pinned).**
  `RewriteDataFilesCommitManager.commitFileGroups` (core jar, offsets 81-145): `table.newRewrite()
  .validateFromSnapshot(startingSnapshotId)`; IF `useStartingSequenceNumber` (default TRUE per api
  bytecode `USE_STARTING_SEQUENCE_NUMBER_DEFAULT`): `.dataSequenceNumber(table.snapshot(
  startingSnapshotId).sequenceNumber())`. Maps cleanly onto the fork's
  `RewriteFilesAction.data_sequence_number(seq)` + `.validate_from_snapshot(id)` — the O2 action
  threads `starting_snapshot.sequence_number()`, not the input files' seqs.
- **The 1.10.0 candidate predicate + group filter (MAIN, literal constants bytecode-confirmed):**
  `filterFiles` = `outsideDesiredFileSizeRange(length<minFileSize || length>maxFileSize) || tooManyDeletes
  (deletes.size()>=deleteFileThreshold) || tooHighDeleteRatio`; `filterFileGroups` = `enoughInputFiles
  (size>1 && size>=minInputFiles) || enoughContent(size>1 && inputSize>target) || tooMuchContent(inputSize>
  maxFileSize) || anyMatch(tooManyDeletes) || anyMatch(tooHighDeleteRatio)`. Defaults: min=0.75·target,
  max=1.8·target, minInputFiles=5, maxGroupSize=100GiB, deleteFileThreshold=Integer.MAX_VALUE (disabled),
  target=`write.target-file-size-bytes` (512MiB). The `size>1` guard in `enoughInputFiles`/`enoughContent`
  is what leaves a LONE undersized file alone — a single-file group only qualifies via `tooMuchContent`
  (oversized) or the delete clause, never by being merely small (pin the boundary: 2 files < min=3 ⇒ no-op,
  3 == min ⇒ rewritten).
- **PER-PARTITION grouping (`groupByPartition`): key by `task.file().partition()` ONLY when the file's spec
  id == the table's CURRENT default spec id, else the EMPTY struct** ("an incompatible-spec file could span
  multiple current partitions, so group it un-partitioned"). The pure-fn pin builds an old-spec (id 1) task
  + a current-spec (id 0) task and asserts they bucket SEPARATELY (never merged into one qualifying group) —
  the mutation that always-empties the key fails both the unit pin and the e2e partition-isolation pin.
- **Java's PLANNER does NOT split an oversized INPUT file — it bin-packs whole `FileScanTask`s and lets the
  WRITE-time rolling writer (`inputSplitSize`/`writeMaxFileSize`) control OUTPUT rolling.** An input file >
  max is SELECTED (oversized candidate, `tooMuchContent`) and rewritten, but never split before reading.
  Ported faithfully: bin-pack whole tasks, roll output at the target via `RollingFileWriter`. (The brief
  flagged oversized-split as a possible deferral — the answer is "Java doesn't do input-split," not "deferred.")
- **Bin packing = FORWARD `pack` (lookback-1), NOT `packEnd`.** `SizeBasedFileRewritePlanner.planFileGroups`
  uses `new BinPacking.ListPacker(maxGroupSize, 1, false, maxGroupCount).pack(...)` — the forward greedy
  first-fit, whereas the fork's MERGE-APPEND manifest packer uses `packEnd` (reversed, so the underfilled bin
  is first). DIFFERENT entry point, same `PackingIterator` core. The fork's `bin_packing` module is
  `pub(crate)`-PRIVATE to `transaction/merge_append.rs`; reusing it would require making it visible = a
  `transaction/` change (out of a maintenance action's scope), so the lookback-1 forward case is reimplemented
  locally (~15 lines) + algorithm-verified against Java + the fork's `pack`. When a needed helper is private
  to a read-only module, reimplement-locally beats opening the module if the algorithm is small and pinnable.
- **A maintenance action that reads + writes + commits is cleanly composable from the existing surface with
  ZERO transaction/scan/writer edits.** O2 used `table.scan().filter().plan_files()` (tasks carry deletes),
  `ArrowReaderBuilder::read` (deletes applied), `DataFileWriter`/`RollingFileWriter` (roll at target), and
  `Transaction::rewrite_files(...).data_sequence_number().validate_from_snapshot()` — every seam already
  public. The brief's "STOP and report if you need a visibility change in transaction/scan/writer" never
  fired. The A2 (`DeleteOrphanFiles`) builder idiom (`new(table)` → builder methods → `execute(catalog)`)
  ported directly; the rewrite_files.rs test fixtures (real parquet write + scan + on-disk-seq raw-avro read)
  ported directly into the maintenance test module (the transaction `tests` module is private, so the helpers
  were re-authored, not imported).

### 2026-06-11 (Wave-4 Group O / O2 — `RewriteDataFiles`, REVIEWER Opus)
- **A COMPACTION/REWRITE READ MUST STRIP THE SCAN RESIDUAL (`FileScanTask.predicate`) BEFORE READING, or
  `.filter(Predicate)` SILENTLY DROPS LIVE ROWS.** *Found + fixed in O2.* `scan().with_filter(p)` computes a
  per-file partition-reduced RESIDUAL and stores it on each `FileScanTask.predicate`; `arrow/reader.rs` turns
  that residual into a row-level `RowFilter` (`final_predicate` → `with_row_filter`, reader.rs ~471/521). A
  maintenance action that re-feeds the planned tasks into `ArrowReaderBuilder::read` (to read the group's live
  rows) therefore ALSO applies the filter PER ROW — so a file whose rows only PARTIALLY match the filter has its
  non-matching LIVE rows DISCARDED from the rewritten output ⇒ permanent data loss (row count fell 10→5 in the
  repro). Java does NOT do this: `BinPackRewriteFilePlanner.planFileGroups` builds the plan scan with
  **`.ignoreResiduals()`** (core MAIN ~L291) so tasks carry NO residual, and `SparkBinPackFileRewriteRunner.doRewrite`
  reads the group by SCAN_TASK_SET_ID with NO row filter (reads ALL rows). FIX (the Rust analogue of
  `ignoreResiduals`): set `task.predicate = None` on the group tasks before `read`; keep the delete files (deletes
  still apply) and keep `.with_filter` on the PLANNING scan (file-selection only). RULE: when an action reads a
  pre-planned `FileScanTask` for a purpose OTHER than answering the filter (rewrite/compact/copy), STRIP
  `task.predicate` — the residual belongs to query reads, not data-movement reads. Pin: an e2e with rows
  straddling the filter boundary, asserting the post-rewrite scan still has BOTH sides.
- **The on-disk-seq mechanism pin is NECESSARY but not SUFFICIENT — add the behavioral concurrent-eq-delete e2e.**
  The builder pinned `use_starting_sequence_number` only via raw-avro on-disk seq. The REAL behavior is
  constructible by driving the action's internals: capture starting snapshot S; `write_compacted_files`; commit a
  CONCURRENT equality delete at seq S+1 (deleting a row in the to-be-rewritten files); then commit the rewrite
  stamping S — `ignore_equality_deletes = data_sequence_number.is_some()` makes the commit SUCCEED, and the
  post-rewrite scan shows the row GONE (data at seq S < delete seq S+1). Dropping the seq stamp either resurrects
  the row OR (more often) flips `ignore_equality_deletes` off ⇒ the delete now CONFLICTS ⇒ commit FAILS — either
  way the mutation is caught.
- **`validate_from_snapshot(start)` is REDUNDANT for a single-group first commit but LOAD-BEARING across groups.**
  `RewriteFilesAction.validate` uses `effective_start = validate_from_snapshot.or(starting_snapshot_id)` where the
  fallback is the TRANSACTION-captured base. In `rewrite_group`, `Transaction::new(table)` for the FIRST group has
  base == starting snapshot, so dropping `validate_from_snapshot` is behaviorally identical there (a single-group
  concurrent-delete e2e through `.execute()` does NOT catch the drop). It diverges only on the 2nd+ group (base has
  advanced past the original start). Keep the call (Java-faithful + multi-group-correct) but know a one-group e2e
  can't pin it; the staged conflict tests + the `.execute()` conflict pin cover the single-group path, and the
  bytecode + the `.or()` trace cover the rest.

### 2026-06-11 (Wave-4 Group O / O3 — `RemoveDanglingDeleteFiles` + RewriteFiles delete-removal surface, BUILDER Opus)
- **The dangling predicate's OFF-BY-ONE (pos `<` min vs eq `<=` min) is the EXACT COMPLEMENT of the read-path
  applicability rule — derive it from BOTH sides and pin the boundary with a pure-fn test, not an e2e.** Java's
  `RemoveDanglingDeletesSparkAction.findDanglingDeletes` SQL (lines 152-165): a POSITION delete dangles when `seq <
  min_data_sequence_number` (STRICT), an EQUALITY delete when `seq <= min` (NON-strict). Why the difference: position
  deletes APPLY to same-seq data (`delete_file_index.rs` L289 `delete_seq >= data_seq`), equality deletes do NOT
  (L238 `delete_seq > data_seq` STRICTLY). So a delete applies to the partition's MIN-seq data file iff `delete_seq
  >= min` (pos) / `delete_seq > min` (eq) ⇒ dangles iff `< min` (pos) / `<= min` (eq). The e2e fixtures (rewrite data
  to a FRESH higher seq) put the delete strictly below min, so `<` and `<=` BOTH fire there — only a PURE-FN test at
  the exact boundary (min=5; pos@5 kept, pos@4 removed, eq@5 removed, eq@6 kept) distinguishes the two and catches
  EITHER flip. The off-by-one IS the resurrection corruption edge; isolate it boundary-exactly.
- **The vehicle was ALREADY half-built: `RowDelta.remove_deletes` (Arc E) drives the exact delete-filter-manager
  removal Java's `RewriteFiles.deleteFile(DeleteFile)` does — they differ ONLY in the recorded operation
  (Overwrite vs Replace) + the 3 RewriteFiles preconditions.** `MergingSnapshotProducer.delete(DeleteFile)`
  (1.10.0 bytecode: `deleteFile(DeleteFile)` → `invokevirtual delete`) is the same machinery the producer's
  `with_removed_delete_files` → `resolve_delete_file_paths` → `process_deletes` + summary `remove_file` already
  drives. Extending `RewriteFiles` with the removal surface was therefore ADDITIVE — a field + 2 builders + the
  producer routing call + the precondition arithmetic — with ZERO snapshot.rs change (the routing exists). When a
  new commit-vehicle's machinery overlaps an existing one, the delta is usually just the operation classification +
  the action-specific preconditions; reuse the producer plumbing, don't fork it.
- **All THREE `BaseRewriteFiles.validateReplacedAndAddedFiles()` preconditions disassembled (1.10.0 bytecode) —
  the matrix's "third precondition" is `deletesDeleteFiles() || !addsDeleteFiles()` ("Delete files to add must be
  empty because there's no delete file to be rewritten").** Preconditions: (1) `deletesDataFiles() ||
  deletesDeleteFiles()` "Files to delete cannot be empty"; (2) `deletesDataFiles() || !addsDataFiles()` "Data files
  to add must be empty…"; (3) "Delete files to add must be empty…". With the REMOVAL-only surface (no add-delete),
  `addsDeleteFiles()` is always false ⇒ (3) always passes (presently unreachable). Kept the check + Java-exact
  message anyway so the third precondition fires the moment the add-delete builder lands — an unreachable-but-correct
  guard is cheaper than a missing one, but DOCUMENT the unreachability (clippy would otherwise be right to flag the
  dead `false`). `operation()` = "replace" UNCONDITIONALLY (bytecode `ldc "replace"; areturn`), even for a
  delete-file-only rewrite — so a `RemoveDanglingDeleteFiles` commit records Replace, NOT the RowDelta Overwrite.
- **Severing the producer routing (`with_removed_delete_files` gated on `false`) doesn't silently misroute — it
  HARD-FAILS the empty-commit guard.** A delete-file-ONLY rewrite has nothing for the producer to do once the
  removal set is severed (no added data, no removed data, no removed deletes) ⇒ the producer rejects with
  `PreconditionFailed "No added data files, added delete files, deleted data files, or added snapshot properties
  found when write a manifest file"`. The routing mutation test catches this as a loud panic, which is even
  stronger than a silent-stays-live divergence. Good defense-in-depth: the empty-commit guard backstops a severed
  routing.
- **MAIN-only impl flag: `RemoveDanglingDeletesSparkAction` is SPARK MAIN-source (no 1.10.0 spark bytecode in
  ~/.m2 — only the api interface + Result shape are in `iceberg-api-1.10.0.jar`).** The api `RemoveDanglingDeleteFiles`
  interface (`Result.removedDeleteFiles()`) and the RewriteFiles vehicle (`BaseRewriteFiles`, `deleteFile(DeleteFile)`,
  the 3 preconditions, `operation()`) ARE 1.10.0-bytecode-verified; the predicate SQL + the per-partition+spec
  grouping + the unpartitioned-single-spec early return are derived from the Spark v3.5/v4.0/v4.1 MAIN source
  (identical across the three). Flag the impl-source asymmetry on the GAP_MATRIX cell.

### 2026-06-11 (O3 REVIEWER Opus — `RemoveDanglingDeleteFiles` adversarial review; verdict PASS w/ added pins+docs)
- **A delete-only `RewriteFiles` has a KNOWN Java-faithful RESURRECTION RACE — confirm it with a staged probe, then
  DOCUMENT it (don't guard it).** `BaseRewriteFiles.validate` runs `validateNoNewDeletesForDataFiles` only
  `if (!replacedDataFiles.isEmpty())` (1.10.0 bytecode — disassembled the gated call), and the dangling-removal has
  an EMPTY replaced-DATA set, so NO concurrent-conflict check runs; Java's `RemoveDanglingDeletesSparkAction` adds no
  `validateFromSnapshot` either. A concurrent SEQ-PRESERVING compaction (O2's `use_starting_sequence_number`) that
  lands a data file at a LOWER seq in the dangling delete's partition between plan and commit makes the "dangling"
  delete APPLICABLE again — and its removal RESURRECTS rows. Reviewer probe staged exactly this (eq@2 dangles vs
  min=3 → land Z@seq1 carrying the masked row → remove eq@2 → y=20 resurrected, scan went {10,30}→{10,20,30}).
  IDENTICAL in Java, so pin to Java: documented on the module doc + matrix cell, NOT guarded (a guard would diverge).
- **The DV simplification (DV → ref-gone check only, never the per-partition min-seq comparison) is BEHAVIORALLY
  EQUIVALENT to Java's union of `findDanglingDeletes` ∪ `findDanglingDvs` for VALID tables — prove it, don't assume.**
  Java flows DVs through BOTH (the `findDanglingDeletes` filter is `content != 0`, which includes content==1 PUFFIN
  DVs) and unions via `DeleteFileSet`. But a DV whose referenced file is LIVE can never additionally trip the seq
  branch: `dv_seq >= referenced_data_seq` (read-path invariant, `delete_file_index` L263) and `referenced_data_seq >=
  partition_min` (the ref file is in the min group), so `dv_seq >= partition_min` always ⇒ `dv_seq < min` impossible.
  So Java's union reduces to just the ref-gone check for live-ref DVs; the Rust simplification matches. Built the
  missing DV e2e (real Puffin DV → rewrite referenced data away → action removes it, `removed-dvs:1`, scan unchanged).
- **The global/unpartitioned EQUALITY delete is keyed by `(its-own-spec_id, empty-partition)`, NOT "matches all
  specs" — so under a multi-spec table it can be flagged dangling while the READER still applies it table-wide. This
  action↔reader inconsistency is inherited 1:1 from Java.** The reader (`global_equality_deletes` / Java
  `findGlobalDeletes`) applies a global eq delete to data under ANY spec; the action (and Java's SQL join on `spec_id
  AND partition`) groups it only against same-unpartitioned-spec data. Reachable after unpartitioned→partitioned spec
  evolution. Probe confirmed the action flags it dangling; documented as Java-faithful, not a Rust bug.
- **`RewriteFiles` SETS `failMissingDeletePaths()` in its CONSTRUCTOR (bytecode offset 18-21) — so the action's
  fail-loud on a missing delete-removal path is Java-faithful for THIS caller** (unlike `RowDelta`, which only sets
  it conditionally). The missing-path test is correct and matches Java.
- **Mutation sweep (8 run, all caught, all reverted):** drop spec_id from the group key → cross-spec test; min-IS-NULL
  as keep → no-live-data test; counter cross-wire pos→eq → dangling-pos test; content-guard removal → data-content
  rejection test; operation()=Overwrite → 8 tests incl. delete-only-Replace + on-disk changelog; pos `<`→`<=` and eq
  `<=`→`<` → the off-by-one pure-fn boundary pin; producer-routing sever → both tombstone tests (loud via the
  empty-commit guard). The builder's pins held under every mutation.
- **Strengthen an incompatible-spec partition pin by giving the off-spec file the SAME partition struct as a
  current-spec file.** The original pin used DIFFERENT partition values ([7] vs [0]), so the "always key by
  partition" mutation still bucketed them apart (different structs) and the pin passed under mutation. Making both
  structs `[0]` (with `min_input_files=2` so a co-grouped pair WOULD qualify) makes the mutation observable: correct
  code buckets the off-spec file under the EMPTY struct (separate), the mutation co-groups them into one qualifying
  group. A mutation-insensitive guard test is worse than none — pick fixture values that make the guard's job
  actually necessary.
