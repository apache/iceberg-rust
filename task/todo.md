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


> **Archival log.** Last todo-archival pass: 2026-06-09 (size trigger — 4,344 lines) → [todo-archive/](todo-archive/) (phase1/phase2/phase3). Completed-increment narratives moved verbatim; this file keeps the active sprint + open items + archive pointers. Procedure: [skills/compaction.md](../skills/compaction.md) §Todo Archival. Archives are not read by default.

## ACTIVE (2026-06-10/11 overnight): autonomous 8-hour plan (user asleep, auto mode)

User instruction: complete the DV sequence, then plan + execute ~8 hours autonomously. **Tier
decision (documented):** the Fable-subagent authorization was scoped to the DV sequence (now
complete); overnight arcs revert to the STANDING default — Opus builder → Opus reviewer (the
parity-orchestration feedback memory + CLAUDE.md subagent policy; budget-prudent while the user
cannot approve frontier spend). Orchestrator (Fable) preps, briefs, gates, commits, pushes;
merges NOTHING; compaction passes NOT run (interactive-only — triggers have fired, re-flagged
for morning).

- [ ] **Arc E — DV previous-deletes merge + superseded-delete removal** (branch `phase2/dv-merge`
      STACKED on `phase2/dv-writer`; if the user squash-merges dv-writer in the morning, rebase
      `--onto origin/main` per the standing instruction). The deferred half of Java's DV surface:
      the DELETE-manifest filter machinery (the `deleteFilterManager` sibling of `process_deletes`
      — rewrite DELETE manifests tombstoning superseded delete files), RowDelta `removeDeletes`
      apply-side, `DVFileWriter` `loadPreviousDeletes` merge hook, then LIFT the fresh-DV door.
      Completes the DV story; `removed-dvs` becomes reachable end-to-end. 1-2 increments.
  - [ ] **Arc-E Increment 1 — apply-side DELETE-FILE removal (BUILDER Opus, 2026-06-10).** The
        `deleteFilterManager` sibling of the data-manifest filtering + `RowDelta::remove_deletes`
        + the fresh-DV door relaxation. (The DVFileWriter merge hook + interop = NEXT increment.)
        Plan:
        1. **Producer (`snapshot.rs`):** add `removed_delete_files: Vec<DataFile>` + builder setter
           `with_removed_delete_files` (mirroring `with_added_delete_files`). In `commit()`, resolve
           them against the CURRENT snapshot's DELETE manifests BY PATH (new
           `resolve_delete_file_paths`, missing path → "Missing required files to delete: %s"). Feed
           them to the SAME `process_deletes` rewrite path (matched by path) AND to the summary's
           `remove_file` (DV → removed-dvs, parquet pos → removed-pos-delete-files, eq → removed-eq).
           Extend `new_filtering_manifest_writer` to build a DELETE-content writer when the SOURCE
           manifest is a DELETE manifest (`build_v2_deletes`/`build_v3_deletes`) — keyed off
           `source_manifest.content`. The DATA-side behavior must be byte-identical (its tests prove it).
        2. **RowDelta surface (`row_delta.rs`):** `remove_deletes(DataFile)` + `remove_deletes_many`,
           rejecting DATA-content files (Java `delete(DeleteFile)` is delete-content-only). Wire to
           the producer via `with_removed_delete_files` + `RowDeltaOperation::delete_files` returning
           removed delete files (so they flow through `process_deletes`).
        3. **Door relaxation (`validate_fresh_dvs_only`):** a DV add for a referenced file with an
           EXISTING live DV (or shadowed legacy-parquet delete) is legal IFF that existing delete's
           path is in this commit's removed set. The D3 door tests stay green (add-DV-without-removal
           still rejected); the removed-set escape hatch is purely additive.
        4. **OPERATION CLASSIFICATION FIX (the 2026-06-08 lesson's third condition — TODAY).**
           BYTECODE FINDING (1.10.0 jar): `BaseRowDelta.operation()` is TWO-branch — `addsDeleteFiles
           && !addsDataFiles → DELETE; else → OVERWRITE`. NO APPEND branch (MAIN's 3-branch
           `addsDataFiles && !addsDeleteFiles && !deletesDataFiles → APPEND` is POST-1.10.0). The
           interop oracle pins 1.10.0, so the faithful classification is the 1.10.0 two-branch form.
           This SUPERSEDES the current Rust 3-branch form + flips `test_row_delta_add_data_only_
           records_append` (add-data-only RowDelta → Overwrite per 1.10.0, not Append). `removeRows`
           feeds `deletesDataFiles`, `removeDeletes` feeds `deletesDeleteFiles` — NEITHER is used by
           1.10.0 `operation()`, so the two-branch form handles all removal cases correctly.
        5. **CROWN JEWEL:** V3 → parquet → DV1 {1} committed → scan → DV2 {1,3} (hand-merged) →
           `row_delta().add_deletes(dv2).remove_deletes(dv1)` → scan = survivors of {1,3}; old DV
           tombstoned (raw-avro provenance: tombstone carries new snapshot id, survivors keep
           original); summary `removed-dvs: 1` + `added-dvs: 1`; manifest list holds exactly ONE live
           DV. Mutations: (a) skip removal + door off → scan rejects at load door (two DVs); (b)
           survivors re-stamped (`add_existing_entry`→`add_entry`) → provenance pin fails.
        6. **Other tests:** remove parquet pos delete (V2) e2e; remove eq delete; missing-removal-path
           error; remove-only commit (operation classification per 1.10.0); door-relaxation pair;
           provenance pin on rewritten delete manifest; cumulative totals append→row_delta(DV)→
           row_delta(replace DV). NO new concurrent-window validation added (removal reuses the
           existing `resolve` + `process_deletes` path; the tx-captured-start pin is N/A — say so).
        7. **Docs:** GAP_MATRIX (DV-writer + RowDelta rows), transaction/map.md, this outcome.
        BUILDER OUTCOME (2026-06-10, Arc-E Inc 1 — Opus; awaiting reviewer): **CROWN JEWEL GREEN ON
        FIRST RUN — all-Rust DV-replaces-DV closed the merge-and-replace loop.** Producer gained
        `removed_delete_files` + `with_removed_delete_files` + `resolve_delete_file_paths` (the
        by-path DELETE-manifest sibling of `resolve_delete_paths`, missing-path → Java
        `failMissingDeletePaths` shape); removed delete files flow through the SAME `process_deletes`
        rewrite (matched by path across the full manifest list) + the summary's `remove_file` (DV →
        `removed-dvs`, parquet pos → `removed-position-delete-files`, eq → `removed-equality-delete-
        files`). `new_filtering_manifest_writer` now CONTENT-KEYED off the source manifest
        (`build_v2/v3_deletes` for a DELETE source) — the LOUD change; data-side stays byte-identical
        (`build_v2/v3_data`, its existing rewrite tests + a new explicit pin prove it). RowDelta:
        `remove_deletes(DeleteFile)` + `remove_deletes_many` (reject Data content), wired via
        `with_removed_delete_files`; the fresh-DV door gained the `remove_deletes` escape hatch (a DV
        may shadow a live delete IFF it is removed in the same commit — Java's merge-and-replace).
        **OPERATION CLASSIFICATION FIX (the 2026-06-08 lesson's third condition, TODAY):** read the
        1.10.0 JAR BYTECODE of `BaseRowDelta.operation()` — it is the TWO-branch `addsDeleteFiles &&
        !addsDataFiles ⇒ DELETE; else ⇒ OVERWRITE` with NO APPEND branch (MAIN's append arm + its
        `!deletesDataFiles()` guard are POST-1.10.0). The interop oracle pins 1.10.0, so the faithful
        fix DROPS to the two-branch form (not "add MAIN's third condition") — re-classifying
        add-data-only RowDelta as Overwrite (was Append); flipped `test_row_delta_add_data_only_
        records_append` → `..._records_overwrite_per_1_10_0`. 11 new tests (lib 1756→1767): crown
        jewel (DV-replaces-DV: one live DV, old tombstoned, removed-dvs:1+added-dvs:1, raw-avro
        provenance) + door pair (with-removal commits / without-removal still rejected) + remove
        parquet pos delete e2e + remove eq delete + missing-removal-path message + remove-only op
        classification + remove-Data-content rejection + survivor-provenance pin + cumulative totals +
        data-side-unchanged regression guard. 5 mutations (snapshot to /tmp, restored byte-clean, full
        suite re-run): (1) escape-hatch disabled ⇒ exactly the 3 removal-commit tests; (2) survivor
        `add_existing_entry`→`add_entry` ⇒ my provenance pin + 6 data-side provenance tests (shared
        helper covered from EVERY consumer); (3) content-keyed writer→always-data ⇒ exactly the 7
        delete-removal tests (data-side test stays green ⇒ byte-identical); (4) re-add APPEND branch ⇒
        exactly the operation pin; (5) missing-path validation off ⇒ exactly the missing-removal test.
        NO new concurrent-window validation added (removal reuses resolve + process_deletes); the
        tx-captured-start pin is N/A — stated. `snapshot_summary.rs` NOT touched (D3 already wired the
        `remove_file` DV branch — no counter gap). Gate: typos/fmt/clippy(workspace excl. sqllogictest)
        clean; lib 1767 ×2; datafusion 80+9 (the documented pre-existing rt-multi-thread doctest
        artifact — unrelated, fails on clean tree); `run-interop-dv.sh` GREEN end-to-end (D1-D4 surface
        intact). Files: only transaction/{snapshot,row_delta}.rs + transaction/map.md + GAP_MATRIX +
        todo. DEFERRED LOUDLY (next increment): the WRITER-side `loadPreviousDeletes` auto-merge (the
        test HAND-merges the super-set DV); apply-side `removeRows` data removal still validation-only;
        interop for the removal path.
  - [x] **Arc-E Increment 2 — DVFileWriter previous-deletes MERGE hook + DV-replacement interop
        (BUILDER Opus, 2026-06-10).** DONE — see BUILDER OUTCOME below (DV-writer row flipped ✅).
        Completes Java's DV write surface — the WRITER-side
        `BaseDVFileWriter.loadPreviousDeletes` half E1 deferred. After this the GAP_MATRIX DV-writer
        row is judged for ✅. Plan:
        1. **The merge hook (`deletion_vector_writer.rs`):** `DVFileWriter::with_previous_deletes(...)`
           — a Rust-pragmatic mirror of Java's ctor `loadPreviousDeletes: Function<String,
           PositionDeleteIndex>`. Per referenced data-file path it carries the previous positions
           (`DeleteVector`) + the SOURCE delete `DataFile`(s) they came from (Java
           `PositionDeleteIndex.deleteFiles()`). On `close()`: union previous positions into the new
           DV for that path (record_count/cardinality = MERGED set); previous source files that are
           FILE-SCOPED (`is_file_scoped` — Java `ContentFileUtil.isFileScoped` = `referencedDataFile
           != null`, BYTECODE-verified: DV OR path-scoped position delete, NOT equality, NOT
           partition-scoped) are returned as `rewritten_delete_files` (a `DeleteWriteResult`-shaped
           return — `DVWriteResult { delete_files, rewritten_delete_files }`, mirroring Java's
           `DeleteWriteResult(dvs, referencedDataFiles, rewrittenDeleteFiles)`). Files NOT file-scoped
           are NOT rewritten (Java L121-124). No-previous case BYTE-IDENTICAL to today (D2/D4 pins are
           the floor). `is_file_scoped`: REUSE `is_deletion_vector` (it lives in `delete_file_index.rs`
           — NOT in scope; implement a small `is_file_scoped` predicate IN `deletion_vector_writer.rs`
           via the public `referenced_data_file()` accessor, NOT forking `is_deletion_vector`).
        2. **Merge support (`delete_vector.rs` if needed):** a positions-out accessor / union helper.
           `BitOrAssign` already exists; a `clone`/`from_iter` from positions may be needed. The
           serializer itself does NOT change.
        3. **Crown jewel (`row_delta.rs` tests):** V3 → data file → DV1 {1} committed → load DV1's
           positions back via the PRODUCTION read path (`CachingDeleteFileLoader`/decoder, NOT a
           hand-built vector) → feed as previous-deletes to a new DVFileWriter writing position {3} →
           writer outputs merged DV {1,3} + rewritten=[dv1] → `row_delta().add_deletes(dv2)
           .remove_deletes(rewritten...)` commits (E1's escape hatch unlocks) → scan = survivors of
           {1,3} = {10,30,50}. Mirrors the REAL engine flow (Spark `SparkPositionDeltaWrite`
           L251+L255-256: `addDeletes(dv)` + `for rewritten: removeDeletes(file)`).
        4. **The Run-store re-serialization question (the D2 caveat, now LIVE):** the previous DV
           deserializes into Run containers; after merge we re-serialize. Determine EMPIRICALLY whether
           the merged blob byte-matches Java's merged blob (extend Direction-2 byte-compare — the
           oracle does the SAME merge in Java via `BaseDVFileWriter` + a `loadPreviousDeletes` fn). If
           the tie diverges, document precisely + scope the byte claim (positions identical, bytes may
           differ at the documented tie); Java reads our blob either way (the oracle proves it). Do NOT
           contort production code to chase the tie.
        5. **Interop (`run-interop-dv.sh` + oracle):** (a) table-level Dir-2: the Rust REPLACEMENT
           chain's final table read by Java's production scan (rows reflect merged DV; old DV absent
           from manifests — Java manifest cross-check); (b) metadata-level: both sides run {append,
           row_delta(DV1), row_delta(add DV2 + remove DV1)} → canonical views 3-way (`removed-dvs`
           allowlist key, first LIVE comparison); fail-closed sentinels per the D4 rule. Mutations: (i)
           skip the remove in the Rust chain → metadata diff fails (extra live DV + missing
           removed-dvs); (ii) skip the merge (DV2={3} only) → Java table-read shows position-1 rows
           RESURRECTED. Restore + green.
        6. **GAP_MATRIX reckoning:** with merge + removal + replacement interop both directions, judge
           the DV-writer row against DoD (API matches Java + unit tests + interop both directions). If
           ✅, flip with dated evidence chain + name residue moving elsewhere (read row keeps its own
           residue). RowDelta + read rows: terse note updates. transaction/ should need NOTHING — STOP
           and report if otherwise. Pipe audit.
        7. **Docs:** writer/map.md, this outcome, lessons entry if a correction lands.
        BUILDER OUTCOME (2026-06-10, Arc-E Inc 2 — Opus; awaiting reviewer): **DV-WRITER ROW FLIPPED
        🟡→✅ — the writer surface is now complete vs Java's BaseDVFileWriter.** Hook:
        `DVFileWriter::with_previous_deletes(HashMap<path, PreviousDeletes>)` (mirrors Java's
        `loadPreviousDeletes` ctor arg) + `close_with_result() -> DVWriteResult { delete_files,
        rewritten_delete_files }` (mirrors Java `DeleteWriteResult`; `close()` kept returning just the
        DVs — ZERO blast radius for ~10 existing callers). Merge unions previous positions
        (`DeleteVector::merge`) into the new DV; file-scoped source files returned for removal.
        **isFileScoped finding (1.10.0 BYTECODE-verified):** `ContentFileUtil.isFileScoped(df) ==
        (referencedDataFile(df) != null)` — NOT just `isDV`; it is eq-delete→false, then non-null
        `referenced_data_file`, then the `_file_path`-bounds-equal fallback (DV OR path-scoped pos
        delete). `is_file_scoped` lives in the writer module (NOT a fork of `is_deletion_vector`, which
        is `format==Puffin`). Engine-caller contract mirrored: Spark `SparkPositionDeltaWrite`
        L251+L255-256 `addDeletes(dv)` + `for rewritten: removeDeletes(file)` → Rust
        `add_deletes(result.delete_files).remove_deletes_many(result.rewritten_delete_files)`.
        **Run-store byte question: ANSWERED.** The merged blob (prev {1} ∪ new {3} = {1,3}, array
        container) is BYTE-IDENTICAL to Java's same merge (interop `test_dv_replace_merged_blob_bytes`,
        44 B). The documented universal caveat stands (a previous DV whose store is ALREADY a Run
        container ties at `card == 2·runs` to array on roaring-rs vs run on Java — positions identical,
        bytes may differ; Java reads our blob either way — not contorted around). **Tests:** 7 unit
        (merge cardinality, file-scoped selectivity, eq-delete exclusion, unwritten-path ignore,
        byte-identical floor, `is_file_scoped` predicate, the crown jewel: DV1{1} → loaded back via the
        production DECODER → writer merges {1,3} → add+remove → scan {10,30,50}) + 3 interop (table
        Dir-2 read incl. DV1-absent manifest check, metadata 3-way incl. first LIVE `removed-dvs`, the
        merged-blob byte-compare). Mutations: merge→no-op (crown jewel + unit fail), `is_file_scoped`→
        always-true (3 scope tests fail); interop (i) skip remove → Rust commit REJECTED at the fresh-DV
        door (stronger than the briefed metadata-diff — fail-loud at commit); (ii) skip merge → Rust
        scan sanity shows id 20 RESURRECTED (stronger than the briefed Java-read — caught at GEN). Both
        restored byte-clean. **lib 1775 ×2; datafusion 80 lib + 9 integration (the documented
        rt-multi-thread doctest artifact still fails on the clean tree — unrelated); full
        `run-interop-dv.sh` GREEN end-to-end (16 steps, both mutations shown failing then restored).**
        **OUT-OF-SCOPE EDIT FLAGGED:** `lib.rs` `mod delete_vector` → `pub mod delete_vector` (the
        public `PreviousDeletes::new(DeleteVector, …)` API requires `DeleteVector` to be NAMEABLE
        downstream — it was a private-module pub type, callable but not constructible externally). Added
        7 doc comments + `is_empty()` to satisfy `#![deny(missing_docs)]` + clippy on the now-public
        type. transaction/ took ZERO production change (only the crown-jewel TEST, as expected). Files:
        ONLY the allowed set + the flagged `lib.rs`.
- [ ] **Arc F — `cherrypick`** (branch `phase2/cherrypick` off MAIN — no DV dependency): Java
      `SnapshotManager.cherrypick` / cherry-pick operation (WAP semantics: `wap.id`,
      `published-wap-id`, `source-snapshot-id`; fast-forward when the source is directly ahead;
      conflict validation between source base and current head). The last Phase-2-gated
      ManageSnapshots item, now unblocked by the write machinery. 1-2 increments.
- [ ] **Arc G (as time remains) — carried-forward small items off MAIN:** (1) table-metadata
      `last-sequence-number` lenient read (`#[serde(default)]`, the Phase-1 carried item) with
      spec citation + tests; (2) the retention-positivity question — settle WHERE (if anywhere)
      Java enforces non-positive rejection from 1.10.0 bytecode, then port or close the item with
      evidence. Then, if hours remain: data-level write-actions interop starter.
- [ ] **Morning report** in this file + the final session message: per-arc outcomes, branches +
      compare URLs, anything skipped, the compaction flags.

## DONE (2026-06-10): Deletion-vector arc (branch `phase2/dv-writer`, 4 commits 88f852b4→67aa056f, pushed — ONE PR ready for morning review)

Actor-critic per increment with **FABLE builder + FABLE reviewer** (user-authorized for this
sequence, naming the tier explicitly — supersedes the Opus default for this arc only).
Orchestrator re-runs the gate + commits; one commit per increment, pushed; Cargo FROZEN —
**exception pre-authorized: NONE; if a dep is truly needed, STOP.**

**Scope correction found during orchestrator prep:** the GAP_MATRIX read row claims
"position-deletes + DVs during scan ✅" but `caching_delete_file_loader.rs` routes ALL
`PositionDeletes` content to the PARQUET reader (`parquet_to_batch_stream`); the Puffin DV loader
is a literal TODO (L52). The ✅ came from the 0.7→0.9.1 sync notes and was never scan-verified —
the scan-exec interop cross-product covered parquet position/equality deletes only, never DVs.
The read path is therefore Increment D1, before any writer work.

- [x] **D1 — DV scan READ path** (DONE 2026-06-10 — Fable builder + Fable reviewer APPROVED +
      orchestrator gate/commit): dispatch Puffin-format position deletes in
      `CachingDeleteFileLoader` to a DV loader (direct ranged read → `deletion-vector-v1` blob →
      magic/length/CRC framing → `RoaringTreemap` → `DeleteVector` keyed by
      `referenced_data_file`); DV-vs-position-delete precedence per Java `DeleteFileIndex`;
      corrected the over-claiming GAP_MATRIX read row. Crown jewel GREEN: Rust scans a JAVA
      1.10.0-written V3+DV table (incl. >2^32 positions + run containers). Reviewer added 3 pins
      (hostile-container-count DoS, max-valid-key boundary, two-DVs-in-one-puffin) + proved
      fail-loud-on-corruption against the real fixture + settled the serde-default blast radius
      (old serializations fail loudly in the parquet reader — correct posture). Gate: lib 1722 ×2,
      datafusion 80+9 (known doctest artifact), interop script green ×2 runs.
      BUILDER PLAN (2026-06-10, D1 builder — FABLE):
      - [x] `delete_vector.rs`: `DeleteVector::deserialize_deletion_vector_v1(&[u8])` — framing
            per puffin-spec.md L146-164 + Java `BitmapPositionDeleteIndex.deserialize` (BE u32
            length prefix over magic+bitmap; LE magic 0x6439D3D1 = bytes D1 D3 39 64; BE CRC-32
            of magic+bitmap via the existing CRC-32 dependency, checked BEFORE bitmap parse); portable 64-bit
            roaring decoded MANUALLY mirroring Java `RoaringPositionBitmap.deserialize` L260-307
            (u64 LE count ≤ i32::MAX + payload bound; u32 LE keys ≥0, ≤ i32::MAX-1, strictly
            ascending; checked `RoaringBitmap::deserialize_from` per key; exact consumption — no
            trailing bytes); positions appended as `(key << 32) | low`. Unit tests: round-trip
            (incl. >2^32), empty, dense run-container, EVERY malformed class (truncations at each
            boundary, wrong magic, CRC mismatch, length-prefix mismatch both ways, bitmap
            garbage, count overflow, non-ascending keys, trailing bytes) — all clean `Err`, no
            panics; env-gated Java-blob byte test (`ICEBERG_INTEROP_DV_DIR`).
      - [x] **SCOPE ADDITION (flagged):** `scan/task.rs` — extend `FileScanTaskDeleteFile` with
            `file_format`, `referenced_data_file`, `content_offset`, `content_size_in_bytes`,
            `record_count` (serde-defaulted) + fill in the `From<&DeleteFileContext>` impl. The
            DV discriminator Java uses (`ContentFileUtil.isDV` = format == PUFFIN, L142-144) and
            the direct-ranged-read inputs (`BaseDeleteLoader.readDV` L171-183) can only travel on
            the task's delete entry; no in-scope alternative exists. Test literals in
            `arrow/reader.rs` (3 sites, test-only) gain the new fields.
      - [x] `delete_file_index.rs`: `dv_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>>`
            keyed by `referenced_data_file` (Java `DeleteFileIndex.build` L505-506, `add` L528-535);
            `get_deletes_for_data_file` mirrors `forDataFile` L151-168: a data file with a DV gets
            {global eq, partition eq, DV} and NO parquet position deletes. DEFERRED (documented):
            the two ValidationException paths (duplicate DV → moved to the loader's door;
            `dv.dataSequenceNumber() >= seq` L209-213 → infallible signature, residue).
      - [x] `arrow/caching_delete_file_loader.rs` + `arrow/delete_file_loader.rs`: dispatch
            PositionDeletes+Puffin to a DV load (direct ranged read via new
            `BasicDeleteFileLoader::read_bytes_range`, per Java `BaseDeleteLoader.readDV` L171-183
            — deliberately NOT PuffinReader, Java cites ≥3 requests, L144-147); validations per
            `validateDV` L266-283 (offset/length present, ≤2GB, referenced path present) +
            cardinality == record_count (Java `deserializeBitmap` L203-209); duplicate-DV reject
            before load; cache key `{path}@{offset}` (one puffin file holds MANY blobs — the bare
            path key would mark blob 2 "already loaded"); decoded vector upserted under
            `referenced_data_file`. Loader tests incl. the sibling-file negative control + an
            ArrowReader-level scan test (Rust-synthesized DV; deleted positions absent, sibling
            file intact).
      - [x] `dev/java-interop/`: `DvScanOracle` (generate-interop-dv) — Java 1.10.0 writes a V3
            table (2 real parquet data files + a real DV via `BaseDVFileWriter` deleting positions
            {1,3} of file 1) + `java_dv_scan_rows.json`; ALSO emits a synthetic
            high-bits/run-container DV blob (`dv_blob.bin` + expected positions JSON) for the
            byte-level decode pin. `run-interop-dv.sh` drives generate → both env-gated Rust tests.
            New `crates/iceberg/tests/interop_dv_scan.rs` (env-gated, empty-string-safe, offline
            no-op).
      - [x] Docs: GAP_MATRIX merge-on-read read row corrected (pre-change reality: DVs were NOT
            loadable — routed to the parquet reader; D1 adds the DV scan path + Direction-1
            interop); todo outcome; scan/map.md only if stale.
      - [x] Mutations (with /tmp backups + full-suite re-run after restore): (a) skip CRC check;
            (b) key vector by the DV file's own path; (c) drop the `<<32` high-bits shift.
      - [x] Gate: typos; fmt; clippy workspace (excl. sqllogictest); `cargo test -p iceberg --lib`
            ×2; `cargo test -p iceberg-datafusion` (read-path rule); offline no-op of the new
            interop test; the REAL `run-interop-dv.sh` end-to-end.
      BUILDER OUTCOME (2026-06-10, D1 builder — FABLE; awaiting reviewer): **CROWN JEWEL GREEN ON
      THE FIRST RUN** — Rust scanned the Java-1.10.0-written V3 table with a real `BaseDVFileWriter`
      DV to exactly Java's own read ({10,30,50,60,70,80}; 20/40 deleted, sibling file intact), AND
      the raw Java-serialized blob (5005 positions incl. 2^32+7 / 2^33+1 + a 5000-position run →
      RUN containers) decoded to the exact position set — `roaring-rs`'s portable treemap layout is
      EMPIRICALLY byte-compatible with Java `RoaringPositionBitmap.serialize` (count is non-padded
      vs Java's dense-with-empty-gap-bitmaps array, but the decoder accepts both; pinned by the
      empty-gap-bitmap unit test). Implementation: manual outer decode (Java `readBitmapCount`/
      `readKey` validations + exact-consumption check) + checked `RoaringBitmap::deserialize_from`
      per key; CRC via the crate's existing gzip dependency (no Cargo change); loader does ONE ranged read per
      Java `BaseDeleteLoader.readDV` (not PuffinReader), cache-keyed `{path}@{offset}` (one Puffin
      file holds many blobs — the bare-path key would mark blob 2 already-loaded = silent
      under-delete); index mirrors `forDataFile` precedence (DV supersedes parquet pos-deletes for
      its file; eq deletes still apply). 25 new lib tests *(reviewer-corrected count: 14
      delete_vector incl. 8 malformed-input tests + 4 index + 6 loader incl. ArrowReader-level
      scan + 1 serde-default; one of the 14 env-gated)* + 1 env-gated integration test
      (`interop_dv_scan.rs`). Mutations all caught + restored byte-clean + full suite re-run: (a) CRC skip
      → exactly the CRC test; (b) key-by-DV-path → path-keying + cache + scan tests AND the crown
      jewel (ids 20/40 resurrect); (c) `<<32` drop → exactly the 3 high-bits tests. Gate: typos/
      fmt/clippy(workspace excl. sqllogictest) clean; lib 1719 ×2; datafusion 80+9 (doctest
      failure = the documented pre-existing rt-multi-thread artifact); all interop tests no-op
      offline; `run-interop-dv.sh` end-to-end green twice. Cargo/pom 0-diff. SCOPE ADDITIONS
      (flagged): `scan/task.rs` (5 serde-defaulted fields on `FileScanTaskDeleteFile` + the From
      impl — the only carrier for Java's isDV discriminator + readDV inputs) and `arrow/reader.rs`
      (3 test literals gained the new fields, test-only). DEFERRED (documented in code + matrix):
      Java's two index-level ValidationExceptions — duplicate-DV moved to the load door
      (fail-loud `Err`), `dv_seq >= data_seq` deferred (infallible index signature; invalid-table
      state only); DV caching beyond one loader instance (Java doesn't cache DVs either);
      Direction-2 DV interop (needs the D2 writer).
      REVIEWER OUTCOME (2026-06-10, D1 reviewer — FABLE): APPROVED with 3 added pins (lib 1722 ×2).
      All 8 review points verified; no correctness bug found. Pins added (each mutation-verified):
      `test_two_dvs_in_one_puffin_file_both_load_under_own_data_file` (the exact case the
      `{path}@{offset}` cache key exists for — fails under a bare-path key),
      `test_dv_blob_hostile_inner_container_count_rejects_fast` (DoS-by-allocation via the INNER
      roaring container count; roaring 0.11.3 caps containers at 65536 pre-allocation — verified in
      its source), `test_dv_blob_max_valid_key_boundary_accepted` (the ACCEPT side of Java
      `readKey`'s `i32::MAX-1` bound — builder pinned only the reject side). Adversarial probes all
      clean (no panic): hostile inner cookie/count, `read_bytes_range` overflow/past-EOF, declared
      length 4/8, old-serialized DV task (defaults to Parquet → LOUD "Corrupt footer" error, the
      pre-D1-equivalent; default confirmed right — no in-repo serializer of `FileScanTask` exists).
      Fail-loud on REAL corruption proven against the Java fixture: CRC byte-flip → loud
      `Invalid deletion vector CRC`, truncation → loud error; crown jewel re-verified to catch
      mutation (b) (ids 20/40 resurrect). Builder mutations (a)/(c) + reviewer mutations (pos
      deletes alongside DV → supersede pin fails; duplicate-DV door disabled → duplicate test
      fails) all caught. Known pre-existing (NOT D1): a delete-file load error leaves a stale
      `Loading` notify entry in the shared `DeleteFilter` (same class as the parquet pos-del path);
      scan still errors loudly. Test-count breakdown in the builder block corrected above.
- [x] **D2 — DV serialization + `DVFileWriter`** (DONE 2026-06-10 — Fable builder + Fable
      reviewer APPROVED + orchestrator gate/commit. Byte parity with Java proven UNCONDITIONALLY
      incl. run containers — roaring-rs `optimize()` matches Java's criteria incl. ties; 3 interop
      fixtures byte-identical 69/76/46 B; builder corrected the BRIEF's wrong reserved id
      (ROW_POSITION = 2147483645, not 2147483545); reviewer confirmed the dense-gap size door
      (25 GB-by-gaps rejected in 303 µs, gap-term mutation caught), MAX_POSITION bit-exact
      (0x7FFFFFFE_80000000), added the tie pin + duplicate-position pin, softened the Run-store
      re-serialization caveat for D3. Gate: lib 1737 ×2, Direction-2 oracle green ×2.):
      bitmap serialization byte-exact vs Java
      `BitmapPositionDeleteIndex.serialize` (portable 64-bit roaring + index framing), puffin blob
      w/ `referenced-data-file`+`cardinality` properties (BaseDVFileWriter.java L52-53, L173-186),
      DeleteFile metadata (content_offset/content_size_in_bytes/referenced_data_file/record_count
      L145-159); exact-byte fixtures + round-trip through D1's reader.
      BUILDER PLAN (2026-06-10, D2 builder — FABLE):
      - [x] `delete_vector.rs`: production `DeleteVector::serialize_deletion_vector_v1(&self) ->
            Result<Vec<u8>>` — Java-faithful DENSE layout (`RoaringPositionBitmap.serialize`
            L245-252 writes `bitmaps.length` = max key + 1 entries INCLUDING empty gap bitmaps;
            `roaring-rs` treemap serialize is SPARSE so the outer layout is hand-rolled per
            sub-bitmap), per-sub-bitmap run-length encode via `RoaringBitmap::optimize()` on a
            clone (Java `runLengthEncode` L176-182 → `runOptimize()`; roaring 0.11.3 HAS
            `optimize()` with the identical run-iff-strictly-smaller criterion — verified in
            registry source `bitmap/container.rs:243`), framing per
            `BitmapPositionDeleteIndex.serialize` L124-137 (BE u32 length of magic+bitmap, LE
            magic D1 D3 39 64, BE zlib CRC-32 of magic+bitmap). Errors: empty vector (never
            serialized per BaseDVFileWriter flow), key > i32::MAX-1 (unrepresentable in Java's
            dense array — `validatePosition`/`MAX_POSITION` L342-348), total > 2GB
            (`computeBitmapDataLength` L158-163). Test encoder `encode_deletion_vector_v1`
            delegates to the production fn; empty-decode test switches to a raw count=0 frame.
      - [x] `delete_vector.rs` tests: HAND-COMPUTED golden bytes for {0,5,2^32+1} (66 bytes incl.
            CRC 0x9ACC8CA4, derived via python struct+zlib independent of the production code) and
            the DENSE-GAP pin {0,2^33} → count 3 with a literal empty key-1 entry (76 bytes, CRC
            0xBC98851A); round-trip through D1's decoder (gaps, 0, >2^32, run shape); empty/key-
            bound/2GB-guard error tests.
      - [x] `puffin/writer.rs` (minimal write-side extension, FLAGGED): `add()` returns
            `Result<BlobMetadata>` (Java `PuffinWriter.write(Blob)` returns BlobMetadata —
            BaseDVFileWriter L164 consumes it for content_offset/content_size); `close()` returns
            `Result<u64>` file size (Java `fileSize()`, consumed at L134). All existing callers
            are tests; call sites compile unchanged (`?;` discards the value).
      - [x] NEW `writer/base_writer/deletion_vector_writer.rs` + `mod.rs` wiring: `DVFileWriter`
            mirroring `BaseDVFileWriter` — `new(OutputFile)`, `delete(path, pos,
            Option<&PartitionKey>)` accumulating per path (partition captured at FIRST delete per
            path, Java `computeIfAbsent` L74-79; pos validated vs MAX_POSITION), async
            `close() -> Result<Vec<DataFile>>`: no deletes ⇒ NO file (L106-109); else ONE puffin,
            one uncompressed `deletion-vector-v1` blob per path in SORTED path order (determinism
            is OUR contract; Java iterates a HashMap — order is not Java's contract), blob fields
            = [ROW_POSITION id 2147483645 = i32::MAX-2] (BRIEF CORRECTION: the brief said
            2147483545 which is DELETE_FILE_POS; MetadataColumns.java L39-44 says MAX-2),
            snapshot_id/sequence_number −1 (L177-178), properties referenced-data-file +
            cardinality (L181-185); per-path DeleteFile per createDV L145-159. DEFERRED LOUDLY to
            D3: `loadPreviousDeletes` merge + `rewrittenDeleteFiles` (L117-126, the commit-path
            concern) — this writer takes only fresh positions.
      - [x] Writer tests: multi-file blob offsets distinct + full DeleteFile metadata; determinism
            across two runs (byte-identical puffin); no-deletes ⇒ no file; file_size ==
            on-disk size; round-trip write → D1 `CachingDeleteFileLoader` → positions match
            (crate-internal unit test).
      - [x] Direction-2 oracle: new `crates/iceberg/tests/interop_dv_write.rs` (env
            `ICEBERG_INTEROP_DV_WRITE_DIR`, offline no-op): GEN writes a real puffin via
            DVFileWriter (2 referenced files; positions incl. 0, a 5000-run, >2^32, a dense-gap
            key) + expected JSON {path → positions + blob offset/size}. InteropOracle new mode
            `verify-interop-dv-write`: Java reads the RUST puffin via Puffin.read footer + ranged
            blob read + `PositionDeleteIndex.deserialize` (the production scan path,
            BaseDeleteLoader.readDV L171-183), asserts positions; ALSO emits Java's own
            serialization of the SAME position sets via BaseDVFileWriter → `java_dv_blob_*.bin`.
            Rust byte-parity test asserts rust-puffin blob bytes == java blob bytes (incl. the
            run-shaped set — roaring-rs CAN emit runs, so byte-exactness is pinned for runs too).
            `run-interop-dv.sh` extended to drive D1 AND D2 phases with the output-sentinel grep.
      - [x] Mutations (snapshot to /tmp, restore, full-suite re-run): (a) sparse-not-dense ⇒
            dense-gap golden fails; (b) CRC over bitmap only ⇒ golden + round-trip fail; (c)
            count = max_key ⇒ golden + decoder fail; (d) blob offset off by footer magic ⇒ writer
            round-trip fails.
      - [x] Docs: GAP_MATRIX DV-writer row ❌→🟡; `writer/map.md` row; `dev/java-interop/map.md`
            run-interop-dv.sh row; this todo outcome.
      BUILDER OUTCOME (2026-06-10, D2 builder — FABLE; awaiting reviewer): **CROWN JEWEL GREEN ON
      THE FIRST RUN, BYTE-EXACT INCLUDING RUN CONTAINERS** — Java's production reader
      (`Puffin.read` footer + the `readDV`-style ranged read + `PositionDeleteIndex.deserialize`)
      decoded the Rust-written Puffin DVs exactly (5003 positions incl. the 5000-run + 2^32+7;
      dense-gap set), AND every Rust blob is BYTE-IDENTICAL to Java's own `BaseDVFileWriter`
      serialization of the same positions — the run-container question is SETTLED: roaring 0.11.3
      `optimize()` (verified in registry source) makes Java-identical run-iff-strictly-smaller
      container choices, so byte parity holds for run-shaped inputs too (69-byte run blob
      matched). BRIEF CORRECTION: blob `fields` = [2147483645] (ROW_POSITION = i32::MAX−2,
      MetadataColumns.java L39-44), NOT the brief's 2147483545 (that is DELETE_FILE_POS).
      Implementation: production `serialize_deletion_vector_v1` (dense layout hand-rolled —
      roaring's treemap serialize is sparse; per-sub-bitmap optimize on clones; errors: empty /
      key>i32::MAX−1 / >2GB pre-alloc) absorbing the D1 test encoder; `PuffinWriter.add` →
      `Result<BlobMetadata>` + `close` → `Result<u64>` (file size) mirroring Java's returns (all
      callers were tests, call sites unchanged); new `DVFileWriter` (sorted-path blob order = our
      determinism contract, partition captured at first delete per path, MAX_POSITION door incl.
      the Java low-word quirk 0x80000000). 13 new lib tests (1735 ×2): hand-computed exact-byte
      goldens ({0,5,2^32+1} 66B CRC 0x9ACC8CA4; dense-gap {0,2^33} 76B with the literal empty
      key-1 entry), round-trips, run-container cookie pin, 3 error doors, 6 writer tests incl.
      the D1-loader round-trip. Mutations all caught + restored byte-clean (cmp): (a) sparse ⇒
      dense-gap golden fails; (b) CRC-sans-magic ⇒ goldens + all round-trips; (c) count=max_key ⇒
      12 tests incl. decoder trailing-bytes; (d) offset-before-header-magic ⇒ loader round-trip +
      coordinates + puffin Java-bit-identical tests. KNOWN RESIDUE (flagged): the Puffin FOOTER
      JSON is not byte-deterministic (HashMap property order, pre-existing `puffin/metadata.rs`,
      outside the file set) — blob region + structural footer pinned instead; Java reads footers
      as JSON so interop is unaffected. DEFERRED LOUDLY: previous-deletes merge +
      `rewrittenDeleteFiles` (BaseDVFileWriter L117-126) → D3 with the commit path.
      REVIEWER OUTCOME (2026-06-10, D2 reviewer — FABLE): **APPROVED, 2 pins added, 1 doc
      correction, no production-code bugs.** All seven attack points held: (1) the dense-gap size
      door INCLUDES gap bytes (probed: one position at key 10_000 ⇒ exactly 120_042-byte blob =
      12 B/gap entry; key 250M ⇒ 3.0 GB-by-gaps REJECTED in 433 µs; key i32::MAX−2 ⇒ 25.77 GB
      rejected in 303 µs — O(present keys), pre-allocation; matches Java `serializedSizeInBytes`
      over the dense array + `computeBitmapDataLength` ≤ Integer.MAX_VALUE, re-derived from
      1.10.0 BYTECODE); reviewer mutation (drop the absent×empty term) caught by 4 tests incl.
      the 2GB door test. Write loop for legal gappy blobs is O(dense) like Java's (~418 ns/gap
      entry debug) — flagged, not fixed. (2) MAX_POSITION re-derived from bytecode:
      `toPosition(2147483646, Integer.MIN_VALUE)` = 0x7FFFFFFE_80000000 = 9223372030412324864;
      Rust constant + boundary tests sit exactly on it; positions in (MAX, key-ceiling] rejected
      by the delete door like Java's `validatePosition` (Java's DESERIALIZER would accept them —
      the serializer layer matches Java's serialize, the delete door matches set(); layering
      identical). (3) run-criterion parity verified at SOURCE level both sides (roaring-rs
      0.11.3 container.rs vs RoaringBitmap 1.3.0 bytecode): Array/Bitmap branches IDENTICAL
      incl. ties; CAVEAT found+documented — for an already-RUN store (deserialized DVs, D3
      merge) roaring-rs omits Java's 2-byte array overhead, so at cardinality == 2·runs Java
      keeps run / Rust would emit array (readable, byte-parity-only; doc softened). PIN ADDED:
      the exact array/run size tie {0,1,2} (6 == 6 bytes) as lib test + THIRD interop fixture
      file — Java byte-compare settled it (46-byte blob byte-identical). (4) puffin diff is
      signature+return only; reviewer mutation (offset captured AFTER blob bytes) caught by 7
      tests incl. the pre-existing Java-bit-identical pins; all non-DV callers are tests. (5)
      createDV metadata verified against bytecode (toBlob fields=[2147483645=ROW_POSITION,
      bytecode-confirmed], −1/−1, two properties; shared fileSize after close like Java's
      Optional). PIN ADDED: duplicate-position ⇒ record_count 1. NOTE (D3): `delete(path, pos,
      None)` loses the spec id (DataFileBuilder default) — Java always receives the spec;
      revisit when the commit path wires real specs. (6)(7) no-deletes ⇒ no file (filesystem
      probed); interop re-run END-TO-END ×2 (incl. extended fixture) green; oracle
      CAN-fail proven (tampered expected JSON ⇒ FAIL line ⇒ script grep trips; NOTE the verify
      step is not re-runnable in place — emit table collides — harmless, script resets dirs).
      Suite 1737 ×2 (1735 + 2 reviewer pins); gate green; Cargo/pom 0-diff.
- [x] **D3 — commit path** (DONE 2026-06-10 — Fable builder + Fable reviewer + orchestrator
      gate/commit. Builder: gate + fresh-DV door + validateAddedDVs op-set fix [stale "REPLACE
      unrepresentable" claim — 1.10.0 set is {overwrite, delete, replace}] + the missing
      validateNoConflictingFileAndPositionDeletes + summary DV counters + 56-test V2-fixture
      migration. REVIEWER FOUND + FIXED 2 DOOR BUGS with fail-before proof: under-fire (door keyed
      on the DV's own spec/partition — cross-spec legacy delete shadowed = resurrection class) and
      over-fire (no seq filter — predating legacy delete froze DV writes); fix resolves the
      referenced file's LIVE entry and mirrors read-path applicability incl. delete_seq >=
      data_seq; +5 reviewer pins incl. the concurrent-format-upgrade refreshed-base race. Gate:
      lib 1756 ×3, datafusion 80+9, interop script green both directions, Cargo/pom frozen.):
      RowDelta DV adds; V2-forbids/V3-requires gating
      (`validateDeleteFileForVersion`, MergingSnapshotProducer L295-316); `validateAddedDVs`
      (L824-870, "Found concurrently added DV for %s: %s") + the no-override tx-captured-start
      pin; write→scan crown jewel on V3.
      BUILDER PLAN (2026-06-10, D3 builder — FABLE). Pre-flight findings: `validateAddedDVs` ALREADY
      landed pre-D1 (commit c1c58f7b) incl. the tx-captured pin + disjoint negative + self-skip +
      malformed-DV tests — D3 task 3 is verify/fix, not build. Found one REAL bug in it: its walk
      reuses `added_delete_files_after` (`{Overwrite, Delete}`) but Java 1.10.0
      `VALIDATE_ADDED_DVS_OPERATIONS` = `{overwrite, delete, replace}` (bytecode-verified) and
      `Operation::Replace` IS representable in Rust since the rewrite actions landed — the stale
      "REPLACE unrepresentable" doc claim hid a missed concurrent-REPLACE-adds-DV window. 1.10.0
      bytecode also shows: NO apply-time re-validation of buffered deletes (that is MAIN-only);
      the gate fires in `add(DeleteFile)` → `validateNewDeleteFile`; `BaseRowDelta.validate` ALSO
      calls `validateNoConflictingFileAndPositionDeletes()` (present in 1.10.0 bytecode, missing in
      Rust). Summary bytecode: a DV increments `added-dvs` INSTEAD of `added-position-delete-files`,
      but STILL increments `added-delete-files` + `added-position-deletes`; sizes use
      `ScanTaskUtil.contentSizeInBytes` (DV ⇒ `content_size_in_bytes`, not file size).
      - [x] `snapshot.rs`: per-file format-version gate in `validate_added_delete_files`
            (V1 throws / V2 rejects DVs / V3 requires DVs for position deletes, eq exempt; exact
            Java messages incl. `dvDesc`); `dv_desc` helper; `operation_adds_dvs` op filter
            (`{Overwrite, Delete, Replace}`) + `added_dv_candidate_delete_files_after` wrapper.
      - [x] `row_delta.rs`: switch `validate_added_dvs` to the DV op-set walk + Java-exact `dv_desc`
            message; add always-on `validateNoConflictingFileAndPositionDeletes`; add the
            fresh-DV-only door (Rust-conservative: reject a DV add when the CURRENT snapshot already
            has a live DV — or a shadowed parquet position delete — for the referenced file; Java
            instead merges previous deletes, BaseDVFileWriter L117-126 — deferred).
      - [x] `spec/snapshot_summary.rs`: `added_dvs`/`removed_dvs` counters + DV content-size
            accounting, offline unit pins on exact keys.
      - [x] Tests: gating × versions/content; door 3-way (+ the V2→V3-upgrade parquet-shadow pin);
            DV-op-set walk (hand-built REPLACE snapshot); manifest round-trip pin (committed DV's
            referenced_data_file/content_offset/content_size_in_bytes survive Rust manifest
            write→read — CLEAN, no spec/manifest fix needed); crown jewel (DVFileWriter →
            row_delta → scan survivors) + resurrection mutation.
      - [x] TRAP-1 migrations: V3 fixtures that row_delta PARQUET position deletes move to a new
            `make_v2_minimal_table_in_catalog` (same schema as V3 minimal — verified identical);
            DV/equality tests stay V3. Affected files (test-only, flagged): row_delta.rs,
            delete_files.rs, overwrite_files.rs, replace_partitions.rs, rewrite_files.rs,
            merge_append.rs, rewrite_manifests.rs, scan/incremental.rs (synthetic delete became
            DV-shaped, stays V3), transaction/mod.rs (the new fixture).
      - [x] Docs: GAP_MATRIX (DV writer/RowDelta rows), transaction/map.md, this outcome.
      BUILDER OUTCOME (2026-06-10, D3 builder — FABLE; awaiting reviewer): **CROWN JEWEL GREEN —
      the all-Rust chain closed** (real parquet → D2 `DVFileWriter` DV {1,3} → D3
      `row_delta().add_deletes` commit → D1 scan returns exactly {10,30,50}; stripping the delete
      manifest from the commit resurrects {10,20,30,40,50} — mutation-verified). Format gate
      Java-byte-exact at all three versions (1.10.0-bytecode-verified; 1.10.0 has NO apply-time
      re-validation — that is MAIN-only; Rust's commit-time placement vs the refreshed base
      subsumes both). validateAddedDVs was pre-existing (c1c58f7b) but its walk MISSED Java's
      REPLACE op (`VALIDATE_ADDED_DVS_OPERATIONS` has 3 members; `Operation::Replace` became
      representable with the rewrite actions) — fixed + pinned with a hand-built REPLACE-op DV
      commit whose message assert isolates the walk from the door. Missing 1.10.0 check
      `validateNoConflictingFileAndPositionDeletes` added (exact message). Fresh-DV-only door
      (documented Rust-conservative divergence): rejects a DV add when the file already has a live
      DV (two-DVs = fail-late scan rejection) OR a shadowed legacy parquet position delete
      (V2→V3-upgrade fixture; DV-supersedes precedence would silently resurrect) — 3-way + upgrade
      tests, both mutation directions caught. Summary: added-dvs/removed-dvs landed exactly per
      bytecode (DV counts INSTEAD of added-position-delete-files, still in added-delete-files +
      added-position-deletes; size = blob content_size_in_bytes per ScanTaskUtil) — collector pins
      + the commit-level pin inside the crown jewel; removed-dvs reachable only collector-level
      (no delete-file removal path yet, documented). Manifest round-trip FINDING: CLEAN — the Rust
      V3 manifest writer already carries fields 143/144/145; raw `Manifest::try_from_avro_bytes`
      pin added, no spec/manifest change. TRAP-1: 56 tests broke under the gate; 53 migrated to a
      new V2 in-catalog fixture (subject = parquet position deletes, now the V2-only reality),
      1 stayed V3 with an equality delete (the DV-check self-skip pin), 1 became a DV-shaped
      fixture (incremental changelog), + the new fixture itself. 14 new tests (lib 1737 → 1751).
      Mutations (8, all caught, restored byte-clean, full suite re-run): delete-manifest strip ⇒
      resurrection; gate off ⇒ exactly the 3 gate pins; V3-eq-exemption drop ⇒ eq tests; V2 arm
      invert ⇒ 33 (the migrated suite IS the regression pin); door off ⇒ exactly the 2 door pins;
      door key-blind ⇒ exactly the 2 negative controls; walk op-set revert ⇒ exactly the REPLACE
      pin; summary DV-branch kill / blob-size kill ⇒ the summary pins + crown jewel.
      REVIEWER OUTCOME (2026-06-10, D3 reviewer — FABLE): all bytecode claims RE-VERIFIED against
      the 1.10.0 jars (`VALIDATE_ADDED_DVS_OPERATIONS` = {overwrite, delete, replace};
      `validateNewDeleteFile` switch incl. exact messages + V4 arm; `validateAddedDVs` walk +
      message; `validateNoConflictingFileAndPositionDeletes` semantics — intersects
      `removedDataFiles` locations with the `validateDataFilesExist`-fed `referencedDataFiles`,
      Java `List` rendering matched; `ContentFileUtil.dvDesc`/`isDV`; `SnapshotSummary
      .UpdateMetrics.addedFile/removedFile` branch ordering + `ScanTaskUtil.contentSizeInBytes`;
      `ADDED_DVS_PROP`/`REMOVED_DVS_PROP` keys). Re-ran mutations: walk op-set revert ⇒ exactly
      the REPLACE pin; shadow-arm disable ⇒ exactly the upgrade-fixture pin; V2-arm invert ⇒ 60
      fail (the migrated suite is the regression pin); rewrite_files seq-strip ⇒ exactly the eq
      crown jewel (migration did not weaken it). TWO DOOR BUGS FOUND + FIXED (fail-before/
      pass-after probes kept as pins, mutation-verified): (1) UNDER-fire — the parquet-shadow arm
      keyed applicability on the added DV's own (spec id, partition); after a partition evolution
      the legacy spec-0 delete never matched the spec-1 DV, so the DV COMMITTED and silently
      superseded a still-applying delete (resurrection class); (2) OVER-fire — no sequence test,
      so a partition-matched legacy delete PREDATING the referenced data file (delete_seq <
      data_seq, applies to nothing) froze all DV writes into that partition. Fix: the door now
      resolves each referenced file's LIVE data-manifest entry and mirrors the read-path test —
      path/(spec id, partition) scope vs THAT entry + delete_seq >= data_seq; referenced file
      with no live entry (same-commit add) ⇒ nothing applies. +5 pins (eq-delete door control,
      path-scoped-other-file door control, cross-spec under-fire, seq over-fire, and the
      refreshed-base gate race — a parquet delete built on V2 is rejected after a CONCURRENT
      V2→V3 upgrade, probing the do_commit re-base claim empirically); lib 1751 → 1756 ×2
      deterministic; fmt/typos/clippy clean; run-interop-dv.sh green both directions; Cargo/pom
      0-diff. Noted (not fixed): Java skips ALL of `BaseRowDelta.validate` when parent == null —
      Rust runs the removed∩referenced check on an empty table too (conservative-only divergence);
      Rust's summary `content_size_in_bytes` keys on PositionDeletes+Puffin where Java keys on
      non-DATA+Puffin (differs only for a pathological Puffin EQUALITY delete, on which Java NPEs
      for a null size — unreachable from real writers); V2/V3 fixture schemas differ by V3 x's
      initial/write-defaults (doc claim softened in mod.rs).
- [x] **D4 — interop:** bidirectional DV round-trips (Java writes V3+DV → Rust scans; Rust writes
      → Java reads) on the scan-exec harness; metadata-level chain notes; GAP_MATRIX flips with
      evidence.
      BUILDER PLAN (2026-06-10, D4 builder — FABLE):
      - [x] NEW `crates/iceberg/tests/interop_dv_table.rs` (one env var
            `ICEBERG_INTEROP_DV_TABLE_DIR`, empty-string-safe, offline no-op; phases invoked by
            test name from the script, like `interop_dv_write.rs`):
            (1) `test_dv_table_gen_rust_writes_java_readable_v3_dv_table` — the HEADLINE
            Direction-2 table: V3 identity(category) table at `<dir>/rust_table` on a real-FS
            MemoryCatalog; TWO real parquet data files (cat=a: (10,a,x)(20,a,y)(30,a,z); cat=b:
            (40,b,p)(50,b,q)(60,b,r)) `fast_append`ed at seq 1; D2's `DVFileWriter` writes ONE
            puffin holding TWO DVs (A: pos {1} = id 20; B: pos {0,2} = ids 40/60 — distinct
            record counts so the canonical entry sort never ties); D3's `row_delta` commits both
            at seq 2; Rust scan sanity = {10,30,50}; `final.metadata.json` via
            `TableMetadata::write_to`; emit `expected_rows.json`.
            (2) `test_dv_meta_views_match_java` — Rust's `snapshot_meta_view` of the JAVA mirror
            table AND of the Rust table both == `java_meta.json` (the E1 3-way, directions 1+2;
            direction 3 = the script's byte-diff).
      - [x] `InteropOracle.java`: new `DvTableOracle` — mode `verify-interop-dv-table` (load the
            RUST V3 metadata → `IcebergGenerics` PRODUCTION read → rows == expected_rows.json;
            PLUS the manifest-API cross-check: every delete entry content==POSITION_DELETES,
            format==PUFFIN, referencedDataFile/contentOffset/contentSizeInBytes set, recordCount
            == cardinality; sentinel "verify-interop-dv-table: N failures") and mode
            `generate-interop-dv-table` (the JAVA mirror chain for the metadata fixture:
            same schema/spec/V3, real parquet via the PartScanExec machinery, `newFastAppend`,
            `BaseDVFileWriter` two DVs in one puffin, `newRowDelta().addDeletes`,
            final.metadata.json under `<dir>/table`).
      - [x] `run-interop-dv.sh`: TMP3 reset in step 1 (idempotency — the D2 reviewer's collision
            note); new steps: Rust GEN → Java verify (sentinel grep) → Java mirror-table GEN →
            `emit-snapshot-meta` ×2 + `diff -u` (Java judging Rust's metadata byte-for-byte) →
            Rust meta test. D1/D2 steps preserved.
      - [x] Mutations: (a) GEN drops B's DV from the commit → Java table-read step FAILS with
            resurrected ids 40/60; (b) GEN skips the row_delta → the metadata byte-diff FAILS
            (missing snapshot). Restore byte-clean + full script green.
      - [x] Docs: GAP_MATRIX (read row: DV both directions data-level proven; DV-writer row STAYS
            🟡 — previous-deletes merge + old-delete removal deferred, BaseDVFileWriter L117-126;
            RowDelta row DV note) + pipe-count audit; tests/map.md + dev/java-interop/map.md rows;
            this todo outcome.
      - [x] Gate: typos; fmt; clippy (workspace excl. sqllogictest); `cargo test -p iceberg
            --lib` ×2; offline no-ops of ALL interop tests (env unset); `run-interop-dv.sh`
            end-to-end green.
      BUILDER OUTCOME (2026-06-10, D4 builder — FABLE; awaiting reviewer): **EVERYTHING GREEN ON
      THE FIRST RUN — both new proofs, zero canonicalization surprises, zero production changes.**
      (1) TABLE-level Direction-2 (the headline): Java's PRODUCTION scan (`IcebergGenerics` →
      `BaseDeleteLoader.readDV`) read the Rust-COMMITTED V3 table — 2 identity(category)
      partitions of real parquet `fast_append`ed at seq 1, ONE puffin holding TWO DVs (cat=a pos
      {1}, cat=b pos {0,2} — distinct cardinalities 1/2) `row_delta`'d at seq 2 — to exactly
      {(10,x),(30,z),(50,q)}, AND the manifest-API cross-check matched every committed DeleteFile
      field (content/format/referencedDataFile/contentOffset/contentSizeInBytes/recordCount +
      both DVs sharing ONE puffin location). Java 1.10.0 parsed the Rust V3 metadata
      (`next-row-id`), manifest list, and V3 delete manifests with no issue. (2) METADATA-level
      (E1-family): Java's canonical snapshot-meta view of the Rust DV chain byte-diffed IDENTICAL
      to Java's view of its own mirror chain (`newFastAppend` + `BaseDVFileWriter` +
      `newRowDelta`) on the FIRST diff — `added-dvs: 2` (with NO `added-position-delete-files`,
      the instead-of branch D3 wired), operation `delete`, `changed-partition-count: 2`, delete
      manifest split + post-inheritance seq 2 — and Rust's views of BOTH tables equal it
      (3-way complete). No `snapshot_meta_view.rs` change needed (the E2 lesson's
      order-insensitive fallback never even fired). SCOPE NOTE stated in the test module: the
      canonical entry tuple omits referenced_data_file/content_offset — covered instead by the
      table-level manifest cross-check. New: `interop_dv_table.rs` (2 env-gated tests,
      `ICEBERG_INTEROP_DV_TABLE_DIR`, offline + empty-string no-op), `DvTableOracle`
      (verify-interop-dv-table + generate-interop-dv-table), `run-interop-dv.sh` steps 7-11
      (D1/D2 steps preserved; TMP3 reset in step 1). HARNESS FIX found while mutation-testing:
      on this machine `mvn exec:java` DOES surface the oracle's `System.exit(1)` (contra the
      D2-era note), so under `set -e` the `VERIFY_OUT="$(...)"` captures aborted BEFORE echoing
      Java's diagnostics — both sentinel captures (steps 5 + 8) now `|| true` with the verdict
      taken ONLY from the output sentinel (success line present, no `^FAIL`), which is robust to
      either mvn behavior. Mutations (test-only, /tmp backup, restored byte-clean via cmp):
      (a) drop cat=b's DV from the commit ⇒ step 8 FAILS loudly — Java reads RESURRECTED ids
      40/60 ({10,30,40,50,60}) + manifest count 1≠2 ⇒ script exit 1; (b) skip the row_delta ⇒
      the metadata byte-diff FAILS showing the entire missing DV snapshot (added-dvs block) ⇒
      diff exit 1. Full script re-run END-TO-END GREEN after restore. GAP_MATRIX: DV-writer row
      gains the D4 table+metadata proofs but STAYS 🟡 (previous-deletes merge + superseded-delete
      removal, `BaseDVFileWriter` L117-126 — deferred; `removed-dvs` collector-level only);
      merge-on-read READ row now claims DVs data-level interop ✅ BOTH directions (stays 🟡 for
      its own residue); RowDelta row gains the D4 DV note; pipe-count audit clean. Gate:
      typos/fmt/clippy(workspace excl. sqllogictest) clean; lib 1756 ×2 (no lib change in D4);
      ALL 11 interop test binaries no-op offline; script green ×2 (initial + post-restore).
      Cargo/pom 0-diff. NO production parity bug found — the D1-D3 surface held under both new
      proofs.
      REVIEWER OUTCOME (2026-06-10, D4 reviewer — FABLE): **APPROVED, zero fixes needed — the
      harness is non-vacuous and fails closed.** Adversarial probes (all different from the
      builder's two mutations, each restored byte-clean via cmp): (a) EMPTY-OUTPUT probe — step
      8's capture pointed at /bin/true (mvn dying early with NO output) ⇒ script exit 1 via the
      success-sentinel-ABSENT branch (the verdict is not merely ^FAIL-present); (b) POISONED
      GROUND TRUTH — expected_rows.json edited between steps 7 and 8 to claim DV-deleted id 20
      survives ⇒ step 8 FAILS loudly (java-read {10,30,50} ≠ expected {10,20,30,50}, "1
      failures", exit 1) WITH Java's diagnostics echoed before the verdict — proving the `||
      true` change does what it claims; (c) DIRECT mvn exit-code probe — `verify-interop-dv-table`
      against an empty dir returns MVN-EXIT=1, empirically confirming the builder's claim that
      `mvn exec:java` DOES surface `System.exit(1)` here (so without `|| true`, `set -e` would
      abort the capture assignment pre-echo). No capture FILES exist (verdicts from shell vars;
      TMP/TMP2/TMP3 rm-rf'd in step 1) ⇒ no stale-sentinel leak possible. Mirror-chain
      equivalence read side-by-side: same schema/spec/V3, same rows, same DV positions/
      cardinalities (1 vs 2), same commit shape (append seq 1, row_delta seq 2); step 10 emits
      the two DIFFERENT tables (`table/` vs `rust_table/`) — diff non-vacuous (java_meta.json
      inspected: operation `delete`, `added-dvs: 2`, no `added-position-delete-files`, delete
      manifest entries at seq 2). Manifest cross-check is VALUE-level (offsets 4/46, sizes
      42/44, cardinalities 1/2 compared against expected_dvs.json), not presence-only. Note
      (accepted, E1-convention precedent): "byte-identical 3 ways" is strictly byte-level only
      for the script's diff direction; the two Rust-side directions are serde_json::Value
      structural equality. Independent gate re-run: typos/fmt/clippy clean; lib 1756 ×2; all 11
      interop binaries no-op with env unset AND the new one with empty-string env; zero src/
      diff + snapshot_meta_view.rs untouched confirmed; full script green (baseline + final
      post-probe runs, exit 0).

## DONE (2026-06-10 overnight): Phase-2 write-engine completion arc (branch `phase2/write-engine-completion`, squash-merged as PR #20)
## ACTIVE (2026-06-10): Arc F — `cherrypick` (branch `phase2/cherrypick`, BUILDER Opus)

The last Phase-2-gated `ManageSnapshots` item: Java `CherryPickOperation` (288 lines) — write-audit-publish
(WAP) semantics. Correctness-critical: a wrong replay or a missed dedup publishes staged data twice.

Plan (recorded before writing code):
- [x] Read all required Rust (`manage_snapshots`/`snapshot`/`append`/`replace_partitions`/`mod`/`spec/snapshot`)
      + Java `CherryPickOperation`/`WapUtil`/`SnapshotSummary`/`SnapshotChanges` + the two exceptions FULLY.
      Bytecode-verified all version-sensitive strings (4 summary keys + 2 exception formats + 6 cherrypick
      messages) against the 1.10.0 jars in `~/.m2` — all match the `/tmp/iceberg-java-ref` source.
- [x] **API + surface decision:** standalone `CherryPickAction` in new `transaction/cherry_pick.rs` +
      `Transaction::cherry_pick(snapshot_id)` ctor; a doc pointer on `ManageSnapshotsAction` (NOT a delegating
      method — the ref-op action only EMITS ref updates and has no snapshot-producing path; cherrypick needs
      the full `SnapshotProducer`. They do not compose cleanly; standalone is the honest shape, like
      `replace_partitions`/`overwrite_files`).
- [x] `cherry_pick.rs`: store `snapshot_id`; resolve everything at commit/validate against the REFRESHED table.
      Three cases mirroring `cherrypick(long)` L69-141 with the FF-precedence of `apply()` L193-204:
      APPEND replay / OVERWRITE+replace-partitions replay / else FF-required — but FF (parent==head) takes
      precedence over replay for BOTH append and overwrite (`requireFastForward || isFastForward(base)`).
- [x] The `validate` hook (non-FF only, L161-171): `validateNonAncestor` (both variants),
      `validateReplacedPartitions` (ancestors-between walk since `picked.parent`), WAP re-check.
- [x] Tests (MemoryCatalog, grafted STAGED snapshots) + mutations. Docs: GAP_MATRIX cell, map.md row,
      this todo, lessons.
- Window pin: `validateReplacedPartitions` walks `ancestorsBetween(currentSnapshot, picked.parentId)` —
      starting id = `picked.parentId`, NOT the tx-captured start. The tx-captured `starting_snapshot_id` is
      **N/A for cherrypick's shape** (its concurrent window is defined by the picked snapshot's parent, not the
      transaction's read point) — stated explicitly in the doc comment + report.
- **Outcome (2026-06-11):** DONE. `transaction/cherry_pick.rs` (new) + mod.rs ctor + manage_snapshots doc
      pointer. 13 MemoryCatalog tests (builder), all green; 5 mutations run, every one caught (validateNonAncestor
      disable → ancestor+dedup fail; source-snapshot-id drop → dedup+happy fail; published-wap-id→wap.id swap →
      WAP-prop test fails; FF-precedence break → both FF snapshot-count pins fail; changed-partition
      over-broaden → negative control + happy replace fail). All version-sensitive strings bytecode-verified
      against 1.10.0 jars. Deferred: Java↔Rust interop (🟡), the stage-only WAP write path.
- **Reviewer (2026-06-11):** VERIFIED + 3 pins added → 16 tests. Precedence matrix confirmed cell-by-cell
      against Java `cherrypick(long)` L69-141 + `apply` L193-204 + `isFastForward` L173-182 (FF predicate
      provably equivalent; concurrent-head-moved cells already covered by the replay fixtures — append replays,
      delete fails, exactly as Java). Found + fixed THREE coverage gaps (each fail-before/pass-after):
      (1) the double-publish dedup SCOPE was unpinned — a mutation scanning ALL snapshots instead of the
      current ancestry passed every existing test (a dangling prior-publish would FALSELY block a re-publish);
      added `test_cherrypick_dangling_prior_publish_does_not_block_republish`. (2) the non-eligible-op
      fast-forward cell (delete with parent==head FF's verbatim, Java's else-branch accepts any FF-able op) was
      untested; added `test_cherrypick_delete_with_parent_equal_head_fast_forwards`. (3) the MULTI-SPEC replay
      divergence (Java preserves per-file specId + per-spec manifests and SUCCEEDS; Rust's
      `validate_added_data_files` requires the default spec ⇒ FAILS-LOUD non-retryably) was undocumented +
      untested; documented in the module header + added `test_cherrypick_multispec_replay_fails_loud`. Replay
      removed-side `copyWithoutStats` divergence is immaterial (the tombstone is rewritten from the live source
      manifest entry, not the picked copy). Gate clean: typos, fmt, clippy (workspace excl. sqllogictest,
      -D warnings), lib 1710 ×2 (baseline 1694 + 16). Tree restored, no commit.

## ACTIVE (2026-06-10 overnight): Phase-2 write-engine completion arc (branch `phase2/write-engine-completion`)

Session brief: `../FABLE_SESSION_BRIEF_2026-06-10_phase2-completion.md`. Actor-critic per increment
(Opus builder → Opus reviewer, orchestrator re-runs the gate + commits). One commit per increment,
pushed; merge nothing. Gate chained in ONE `&&` chain; Cargo files FROZEN.

**POST-ARC AUDIT (2026-06-10, orchestrator Fable — user-requested logic + security audit of
everything built):** full manual read of every production region the arc touched. TWO parity bugs
found, empirically pinned (fail-before), fixed: (1) merge_append's `first` was gated on
`added_snapshot_id == this snapshot` — Java's `first` is the unconditional stream HEAD
(ManifestMergeManager L85), so a properties-only merging append dropped the min-count protection
and over-merged; (2) duplicate `delete_manifest` args double-counted the replaced side of
`validateFilesCounts` (Java's field is a path-equality Set — now deduped at insertion). THREE
saturating-arithmetic hardenings on accumulators fed by untrusted `manifest_length` (bin weights
×2, rolling estimate ×1 — debug-build panic / release wrap on hostile values, now saturate).
Verified clean: u64 count accumulation (no overflow, stronger than Java's int), division-by-zero
guards, negative-length clamps, add_manifest None-count rejection == Java's null semantics,
kept-manifest integrity, no unsafe/no logging surface, eager-vs-commit error placement, retry
statelessness, release-mode debug_assert acceptable (unreachable via the only constructor).
Interop round-trip re-run GREEN post-fix. Lib 1694 ×2.

**ARC OUTCOME (2026-06-10, all six increments DONE — 6 commits on
`phase2/write-engine-completion`, each pushed, nothing merged):** RewriteManifests (8f2fc3a3) →
RewriteFiles seq-preservation + guard lift + validateNoNewDeletes (e96719e3) → the sibling
delete-manifest-carry corruption fix (fcf8da9d) → MergeAppend + bin-packing port (601eef30) → the
8-step Java-judged interop extension + delete-bearing rewrite fixture, ALL SIX comparisons green
with ZERO production changes (b140319a) → the stale-deferral correction + matrix cell-split repair
(2173feb3) + this Roadmap refresh. Lib suite 1643 → 1692 (+49); every increment
builder→reviewer→independent-gate; 20+ mutations run, every one caught (after two test fixes the
reviewers forced). Headline save: the arc surfaced and fixed a FOUR-action silent-corruption class
(delete manifests dropped from every delete-bearing commit on MoR tables — masked in rewrite_files
by the old guard, UNGUARDED in the three siblings). **Compaction triggers FIRED, not run (per the
brief — interactive-approval-only):** lessons.md at 985 lines / 93 KB (trigger ~800 / 50 KB),
todo.md at ~580 lines (guideline < ~500). Both need a compaction/archival pass next interactive
session.

- [x] **Increment 1 — `RewriteManifests`** (DONE 2026-06-10 — builder + reviewer + gate): new
      `transaction/rewrite_manifests.rs`, cluster/keep partition of current manifests, provenance-
      preserving re-group via the existing-entry writer path, `validateDeletedManifests` +
      `validateFilesCounts`, `Operation::Replace`, live set unchanged. Provenance re-stamp mutation
      pin mandatory. Done-bar 🟡 (interop in Increment 4).
      Outcome: 17 MemoryCatalog tests (14 builder + 3 reviewer pins: on-disk explicit seqs via raw
      avro, multi-spec cluster keying, user-set vs computed-count precedence); REVIEWER verdict NO
      BUG, zero production changes — the seq-strip RESURRECTION mutation fails the MoR scan test
      (the builder's weaker re-stamp mutation had only failed the metadata pin), and the builder's
      changed-partition-count divergence claim was CORRECTED (Java emits =0 too — parity; interop
      s6 must expect it both sides). Gate: lib 1660 ×2, clippy (workspace, excl. sqllogictest)
      clean, fmt+typos clean, Cargo FROZEN.
      BUILDER PLAN (2026-06-10):
      - [x] Read all required sources + Java `BaseRewriteManifests` (386 lines) fully.
      - [x] `table_properties.rs`: add 3 consts (target-size 8388608, min-count-to-merge 100,
            merge-enabled true). Only target-size consumed now.
      - [x] `snapshot.rs` (additive only): widen `new_filtering_manifest_writer` to `pub(crate)`,
            add `pub(crate) extend_snapshot_properties`, expose `snapshot_id()`.
      - [x] `rewrite_manifests.rs`: `RewriteManifestsAction` (cluster_by/rewrite_if/add_manifest/
            delete_manifest/set/set_commit_uuid/set_key_metadata). Commit: no-current-snapshot →
            DataInvalid; build producer; load ALL manifests (data+deletes); validateDeletedManifests;
            performRewrite (cluster) OR keepActiveManifests; validateFilesCounts; stamp added
            snapshot ids; compose new-first list; feed through SnapshotProduceOperation
            (Operation::Replace) + DefaultManifestProcess. Estimated-length size rolling.
      - [x] 13 tests + provenance re-stamp mutation pin (add_entry vs add_existing_entry).
      - [x] mod.rs wiring + map.md row + GAP_MATRIX cell flip.
      - Orchestrator design decisions (pre-briefed): the action pre-computes the full new manifest
        list and feeds it through `SnapshotProduceOperation::existing_manifest` with
        `DefaultManifestProcess` (no producer-trait change); Java's `writer.length()` size-rolling
        becomes a documented estimated-length proxy (Rust's `ManifestWriter` buffers entries — no
        incremental length); `add_manifest` is V2+ only (the V1 `copyManifest` legacy path is
        deferred, rejected `FeatureUnsupported`); new `TableProperties` consts for
        `commit.manifest.target-size-bytes` (+ merge siblings for Increment 3).
- [x] **Increment 2 — `RewriteFiles` dataSequenceNumber preservation + guard lift +
      `validateNoNewDeletes`** (DONE 2026-06-10 — builder + reviewer + gate): crown-jewel
      resurrection test (MoR table, EQUALITY-delete on X at seq 2, rewrite [X]→[X'] preserving
      seq 1 ⇒ scan still drops rows; mutation strips preservation ⇒ fails). Plus the
      tx-captured-start pin for the new validation.
      Outcome: REVIEWER verdict SHIP IT, zero production fixes needed beyond the builder's; +1
      reviewer structural pin (delete-manifest count survives the rewrite — fails under the
      carry-revert mutation, insensitive to seq-strip; disambiguates the two fixes). 6 mutations
      run, all caught incl. both ignore_equality_deletes directions + the shared-helper
      cross-consumer mutation (fails rewrite_files+overwrite_files+row_delta together). All-DELETED
      delete-manifest edge: Rust drop == Java drop (shouldKeep rule) — verified. Gate: lib 1670 ×2,
      21 rewrite_files tests, clippy/fmt/typos clean, Cargo FROZEN.
- [x] **Increment 2b — fix the SAME delete-manifest-dropping bug in the three sibling actions**
      (DONE 2026-06-10 — builder + reviewer + gate. Outcome: shared
      `SnapshotProducer::current_manifests()` helper, 4 consumers switched, orphaned
      `current_data_manifests` removed; 3 crown jewels + 3 structural pins; per-action
      carry-revert mutations prove isolation (each fails ONLY its own action's tests); REVIEWER
      ACCEPT — add-only overwrite path proven behavior-identical, dangling-delete retention
      documented conservative-safe. Gate: lib 1673 ×2, clippy/fmt/typos clean, Cargo FROZEN.
      Reviewer-flagged pre-existing: the OverwriteFiles GAP_MATRIX cell has an old mid-cell `||`
      breaking the table row — future docs pass.)
      (`delete_files.rs` L262 / `overwrite_files.rs` L701 / `replace_partitions.rs` L457 all
      return `current_data_manifests()` only — UNGUARDED silent delete loss on any MoR table;
      discovered while reviewing Increment 2; see lessons 2026-06-10). Carry ALL current manifests
      (the Increment-2 fix shape); per-action crown jewel (delete still applies post-commit) + the
      structural delete-manifest-count pin; carry-revert mutation per action.
      BUILDER PLAN (2026-06-10, Increment-2b builder):
      - [x] `snapshot.rs`: added `pub(crate) async fn current_manifests(&self) -> Result<Vec<ManifestFile>>`
            — loads the current snapshot's manifest list, returns ALL entries (data + deletes), empty when
            no current snapshot. Doc cites `MergingSnapshotProducer.apply` L973-1011 (composes BOTH
            `filterManager.filterManifests(dataManifests)` AND
            `deleteFilterManager.filterManifests(deleteManifests)`) + the resurrection corruption it
            prevents + the conservative dangling-delete posture (Java L982-993 `dropDeleteFilesOlderThan`
            / `removeDanglingDeletesFor` NOT ported — keeping a stale delete is harmless, dropping a live
            one resurrects rows).
      - [x] Switched `rewrite_files.rs`'s inline `existing_manifest` to the helper (behavior-preserving —
            its structural pin + crown jewel stayed green; merged its inline doc content into the helper;
            updated the structural-pin test's mutation note since `current_data_manifests` is gone).
      - [x] Switched `delete_files.rs` / `overwrite_files.rs` / `replace_partitions.rs` `existing_manifest`
            from `current_data_manifests()` → `current_manifests()` (each keeps a short action-specific
            comment: delete manifests carry unchanged + conservative dangling-delete posture).
      - [x] `current_data_manifests` was ORPHANED by the switch (its ONLY three code callers were the
            three broken actions) → REMOVED it (renamed-by-removal into `current_manifests`; dead-code
            rule). The two remaining textual refs were doc/comment prose, updated. NOTE: `rewrite_manifests
            .rs` (out of scope) has its own inline copy of the same "load full manifest list" logic with a
            LOCAL var named `current_manifests` — a 5th candidate consumer; FLAGGED, not touched.
      - [x] Tests per action (row_delta crown-jewel fixture: real parquet data + a REAL position-delete
            via the production writer + a production scan): X (partition a, position-delete masking y=20) +
            Y (partition b); the action; scan shows X's masked y=20 STILL ABSENT + the action's effect;
            structural pin (delete-manifest count == 1). All three named
            `test_*_preserves_outstanding_delete_manifests_no_resurrection`.
      - [x] Mutations: filtered each action's `existing_manifest` to DATA-only (the old data-only
            `current_data_manifests` behavior) — three separate, surgical (one block each, restored
            in-place) ⇒ THAT action's crown jewel fails (y=20 resurrected) + others stay green. Verified
            each: delete_files {10,20} vs {10}; overwrite_files {10,20,80} vs {10,80}; replace_partitions
            {10,20,80} vs {10,80}.
      - [x] Docs: GAP_MATRIX three action cells + the Phase-2 narrative line + map.md `snapshot.rs` row.
      Outcome: shared `current_manifests` helper carries DATA + DELETE forward; all four delete-bearing
      actions (rewrite_files + the three fixed) use it; `current_data_manifests` removed (orphaned). 3 new
      crown-jewel tests (1 per action) + structural pins, all green; three per-action mutations confirm
      per-action isolation (no accidental coupling). LESSON LEARNED: back up files AFTER tests land, then
      mutate the ONE production line surgically — restoring a whole-file pre-fix backup wiped the new test
      (recovered + re-applied). Done-bar 🟡 (unit-proven; interop with a delete-bearing fixture deferred).
      BUILDER PLAN (2026-06-10):
      - [x] `snapshot.rs` (producer, additive only): add field
            `new_data_files_data_sequence_number: Option<i64>` + builder setter
            `with_new_data_files_data_sequence_number(seq)` (mirror `with_added_delete_files`);
            consume in `write_added_manifest`: when `Some(seq)` and V2/V3, build each added entry with
            `.sequence_number(seq)` (writer keeps explicit data seq; file seq still inherits). V1
            ignored (no seqs). None default ⇒ every existing caller unaffected. NOT the shared helper.
      - [x] `rewrite_files.rs`: `data_sequence_number(seq: i64)` builder + thread to producer; REJECT
            `seq < 0` (`DataInvalid`) at commit (Rust-only fail-loud — writer silently strips negatives
            into re-inheritance). `validate_from_snapshot(snapshot_id)`. Implement `TransactionAction::
            validate`: when `deleted_data_files` non-empty, call shared
            `validate_no_new_deletes_for_data_files(current, effective_start, None,
            &self.deleted_data_files, self.data_sequence_number.is_some())`, `effective_start =
            validate_from_snapshot.or(tx_captured)`, UNCONDITIONAL. REMOVED the SAFETY GUARD
            (`has_outstanding_delete_files` + its commit rejection + the guard test). Rewrote the
            three doc sites (module doc, action-struct doc, mod.rs ctor doc) to the new contract.
            Ctor stays as-is (no 3-arg overload — builder suffices).
      - [x] **BUG FOUND + FIXED (latent, exposed by the guard lift):** `RewriteFilesOperation::
            existing_manifest` returned only DATA manifests, so a rewrite DROPPED every DELETE manifest
            and lost all outstanding deletes → resurrection regardless of seq. Fixed to carry ALL current
            manifests forward (data + deletes); `process_deletes` leaves delete manifests untouched
            (their entries are delete-file paths, never in the data `delete_paths`). The old guard had
            hidden this — no rewrite ever ran on a delete-bearing table. The crown jewel only goes green
            with this fix.
      - [x] Tests (10): crown-jewel eq-delete resurrection (real parquet + eq-delete writer + scan +
            raw-avro on-disk seq=1 pin + seq-strip mutation); no-preservation Java-faithful hazard;
            conflict-pair (eq-delete ignored WITH seq / rejected WITHOUT — exact msg + !retryable());
            new position delete always fatal; no-override tx-captured-start (+ refreshed-head mutation);
            disjoint negative control; pre-existing deletes not conflicts; no-concurrent-commit clean walk;
            negative-seq rejected. 20 rewrite_files tests total.
      - [x] Docs: map.md rewrite_files row, GAP_MATRIX RewriteFiles cell, this bullet outcome.
      Outcome: 20 rewrite_files lib tests green (8 pre-existing + 12 new/revised; the removed guard test
      replaced by the crown jewel + hazard pins). BOTH mandatory mutations run + restored: (1) seq-strip
      in `write_added_manifest` ⇒ crown jewel fails with y=20 resurrected; (2) refreshed-head
      `effective_start` ⇒ no-override test fails (commit wrongly succeeds). Done-bar 🟡 (unit-proven;
      interop in Increment 4 with a delete-bearing rewrite fixture).
- [x] **Increment 3 — merge append** (`MergeAppend` / `ManifestMergeManager` merge machinery) — DONE
      2026-06-10 (builder + reviewer ACCEPT + gate). REVIEWER: bin-packing port hand-traced against
      Java on 3 adversarial cases (packEnd double-reversal, `<=` weight boundary, lookback-1
      no-lookahead) — all match; read-back seq chain independently verified; 3 mutations re-run all
      caught; 1.10.0/manifests-* question RESOLVED for Increment 4 (the canonical view's
      SUMMARY_COUNT_KEYS allowlist excludes manifests-created/-kept/-replaced ⇒ s7 insensitive,
      no production/allowlist change needed; the /tmp Java ref is a tagless shallow clone — version-
      ancestry answers from it are artifacts). Gate: lib 1692 ×2. `merge_append()` action honoring
      `commit.manifest-merge.enabled` / `commit.manifest.min-count-to-merge` /
      `commit.manifest.target-size-bytes`; provenance preserved in merged manifests; passthrough
      below threshold / property-disabled. Done-bar 🟡 (interop in Increment 4).
      BUILDER PLAN (2026-06-10, Increment-3 builder):
      - [x] `snapshot.rs` seam: changed `ManifestProcess::process_manifests` → async + `Result` +
            `&mut SnapshotProducer` (same `impl Future + Send` style as `SnapshotProduceOperation`).
            `DefaultManifestProcess` stays a passthrough (fast_append byte-identical); single
            `manifest_file()` call site now `.await?`. NOTHING else changed in snapshot.rs.
      - [x] New `transaction/merge_append.rs`: `MergeAppendAction` mirroring `FastAppendAction`
            surface (add_data_files / set_commit_uuid / set_key_metadata / set_snapshot_properties /
            with_check_duplicate + validate_added_data_files + validate_duplicate_files). Op =
            `Operation::Append`; `existing_manifest` mirrors append.rs. `MergeManifestProcess`:
            split DATA vs DELETE; DELETES carried UNCHANGED (deferred); reorder DATA [new added FIRST
            via added_snapshot_id == producer.snapshot_id, then existing]; group by spec id
            REVERSE-sorted; packEnd by manifest_length; three bin rules via pure `bin_disposition`;
            three-way routing (this-snapshot DELETED → add_delete_entry [unreachable]; this-snapshot
            ADDED → add_entry; else → add_existing_entry); output merged-data then delete manifests.
      - [x] private `bin_packing` module (ported BinPacking.PackingIterator + ListPacker.packEnd,
            general lookback/largest-bin-first/max-items) + 7 unit tests.
      - [x] `mod.rs`: `merge_append()` ctor + docs (3 properties + fast_append/newAppend contrast).
      - [x] Tests (19, MemoryCatalog, mirror rewrite_manifests fixtures): below-min-count passthrough,
            at-threshold merge w/ provenance (raw-avro on-disk seqs + summary-shape), property-disabled
            passthrough, old-tombstone suppression (live+tombstone in one manifest so it reaches the
            merge), multi-spec separation (higher-spec-first), tiny-target size-1-keep, MoR
            delete-carry crown jewel, cumulative totals, empty-reject, 3 `bin_disposition` units, 7
            `pack_end`/`pack` units. Mutations run+restored: provenance re-stamp (→ provenance test
            fails), tombstone-suppression broaden (→ suppression test fails), bin-gate broaden (→
            bin_disposition unit fails).
      - [x] Docs: map.md merge_append row + snapshot.rs ManifestProcess seam note; GAP_MATRIX ❌→🟡.
      Outcome: 19 merge_append tests green (5x stable — replaced a FLAKY length-arithmetic bin test
      with a deterministic 1-byte-target size-1 pin + pure `bin_disposition` unit tests; manifest avro
      length varies a few bytes/commit so `target = 2*one_len` was non-deterministic). Seam change kept
      fast_append byte-identical (337 transaction tests green). PHYSICS verified: new added manifest
      reads back with seq=-1, Added entries inherit Some(-1), add_entry strips to None ⇒ re-inherits.
      SUMMARY-SHAPE finding: Java's MergingSnapshotProducer adds manifests-created/-kept/-replaced;
      Rust's SnapshotProducer.summary + MergeManifestProcess do NOT ⇒ merge_append == fast_append shape.
      PHYSICS VERIFIED: the new added manifest read back has ManifestFile.sequence_number == -1;
      its Added entries inherit seq=Some(-1) via `inherit_data` (status Added branch). `add_entry`
      then STRIPS Some(-1) (since -1 >= 0 is false) → writes seq=None on disk ⇒ re-inherits the new
      snapshot's real seq at commit. Carried (committed) entries have real Some(seq) ⇒
      `add_existing_entry` preserves them explicitly on disk. Merged manifest has
      added_snapshot_id == new_snapshot_id ⇒ `assign_sequence_numbers` stamps the real new seq.
      SUMMARY SHAPE: Java's MergingSnapshotProducer.apply adds manifests-created/-kept/-replaced via
      buildManifestCountSummary; the Rust SnapshotProducer.summary does NOT, and MergeManifestProcess
      will NOT inject them either ⇒ merge_append summary == fast_append summary shape (documented
      divergence from Java).
- [x] **Increment 4 — interop extension** (DONE 2026-06-10 — builder + reviewer VERIFIED + gate.
      REVIEWER: independent script re-run green; 2 NEW sensitivity mutations both caught (constant
      cluster key → per-partition vs single-manifest diff; dropped row_delta delete → the view
      visibly loses the delete manifest — the survives-claim is load-bearing); merging-producer +
      property-arming verified on both sides (post-s8 view = ONE merged manifest); A' seq==1 pin
      confirmed in the per-entry view field; the dangling-delete probe wording CORRECTED — the
      empirical KEEP on 1.10.0 is real but the mechanism is that 1.10.0 prunes only dangling DVs
      (PUFFIN-gated isDanglingDV) — parquet position-deletes are structurally exempt; BaseRewriteFiles
      overrides nothing dangling-related. Gate: lib 1692 ×2, all offline interop tests no-op green.):
      extended the
      E2 chain (`WriteActionsOracle` + `interop_write_actions_meta.rs`) with s6 rewrite_manifests
      (cluster by partition), s7 property-set (min-count-to-merge=2, NO snapshot) + s8 merge-append
      (Java `newAppend`), and a delete-bearing seq-preserving rewrite fixture B. ROUND-TRIP GREEN on
      the FIRST run (3 directions × 2 fixtures, ZERO production/canonicalization changes); 2 mutations
      verified the harness non-vacuous. GAP_MATRIX notes scoped, rows stay 🟡. Dangling-delete probe:
      1.10.0 keeps the dangling delete on a RewriteFiles = PARITY with Rust (documented).
      BUILDER PLAN (2026-06-10, Increment-4 builder):
      - [x] **A. Extend the E2 write-actions chain.** Java `WriteActionsOracle.generate` += s6
            `rewriteManifests().clusterBy(f -> String.valueOf(f.partition()))`, s7
            `updateProperties().set("commit.manifest.min-count-to-merge","2").commit()`, s8
            `newAppend().appendFile(G cat=a,60).commit()` (the MERGING producer). Rust GEN test mirrors:
            `rewrite_manifests().cluster_by(|f| format!("{:?}", f.partition()))`,
            `update_table_properties().set(min-count-to-merge=2)`, `merge_append().add_data_files([G])`.
            Document the chosen cluster-key fns on both sides (key string never appears in metadata —
            only the GROUPING must match). s7 produces NO snapshot — confirm the view is unaffected.
      - [x] **B. New delete-bearing rewrite fixture (fixture B, E1-family, metadata-only).** Java
            `RewriteSeqOracle`: fast-append A(a,10)+B(b,20) seq1 → row-delta adding a metadata-only
            POSITION-delete referencing B (seq2) → `newRewrite().validateFromSnapshot(rowDeltaSnap)
            .rewriteFiles(Set.of(A), Set.of(A'), 1L)` (dataSequenceNumber=1). Rust mirror: build the
            rewrite tx AFTER the row-delta commit (tx-captured start ⇒ empty concurrent window =
            semantic twin of Java's explicit validateFromSnapshot — DOCUMENT in both) with
            `.data_sequence_number(1)`. Two load-bearing assertions: A' carries data_seq 1 (not the
            rewrite snap's seq) post-inheritance; the delete manifest survives the rewrite intact.
            Delete references B (SURVIVOR) ⇒ Java dangling-delete machinery dormant both sides.
      - [x] **OPTIONAL probe:** a 2nd step rewriting B too (now-dangling delete) — EMPIRICALLY discover
            1.10.0 behavior vs Rust carry-unchanged. If divergent: do NOT force green; document
            (GAP_MATRIX + fixture comment) + leave it OUT of the byte-diffed chain. Report either way.
      - [x] **C. Wire-up:** extend `run-interop-write-actions.sh` to cover BOTH the extended chain AND
            fixture B in one run; extend the Rust env-gated tests (offline no-op early-return when the
            env var is unset). New Rust test goes in `interop_write_actions_meta.rs` (shares the view
            helper) gated on a fixture-B env var.
      - [x] Offline gate (typos/fmt/clippy/lib ×2/both interop binaries no-op). Round-trip green.
            Mandatory mutation (poison one Rust GEN value ⇒ comparison fails ⇒ restore ⇒ green).
            GAP_MATRIX three cells gain scoped "metadata-level interop ✅ 2026-06-10 (chain paths)"
            notes; rows STAY 🟡. map.md row updates (tests/ + java-interop/).
- [x] **Increment 5 (stretch) — RESCOPED: the deferral was STALE; matrix reconciliation instead**
      (DONE 2026-06-10, orchestrator, docs-only). The premise (`OverwriteFiles.validateDataFilesExist`
      wiring) does not exist in Java: `BaseOverwriteFiles.validate` (L135-175) has exactly three
      blocks, all already ported; `validateDataFilesExist` is RowDelta-only (single caller in core/,
      already landed 2026-06-09); concurrent-removal protection is `failMissingDeletePaths` ≡
      `resolve_delete_paths`. Building it would have been anti-parity — decision per the brief's
      "decide, document, move on".
      Outcome: (1) the stale deferral corrected in the OverwriteFiles GAP_MATRIX cell; (2) the
      2b-reviewer-flagged broken cell ROOT-CAUSED and repaired — the de-triplication mover had split
      the OverwriteFiles narrative MID-EXPRESSION on the `||` inside
      `(strict.eval(part) || metrics.eval(file))`, stranding 2.7 KB of narrative in the matrix as a
      phantom column while the archive section ended mid-sentence; strand rejoined VERBATIM in
      `archive/2026-06_matrix-cell-narratives.md` (conservation preserved), cell now terse + closed;
      (3) every matrix row pipe-count-audited (all exactly 5 `|`); (4) the three "five-commit chain"
      citations updated for the 8-step extension; (5) two lessons appended.

## DONE (2026-06-10): Sprint increment E2 — rewrite-family METADATA-level interop (branch `interop/write-actions-meta`)

All four rewrite-family actions proven in ONE five-commit chain (fast-append → DeleteFiles →
OverwriteFiles → ReplacePartitions → RewriteFiles) on a partitioned V2 table, through the E1
canonical-view oracle (no parquet — pure manifest metadata). **GREEN with ZERO Rust production
changes** — the Phase-2 ports already emit Java-identical metadata semantics.

- [x] Shared-module refactor: the E1 view builder → `tests/common/snapshot_meta_view.rs` (E1
      round-trip re-run green); allowlist += `replace-partitions` (both sides).
- [x] Manifest comparator extended with the count fields on BOTH sides — the first run surfaced a
      TIE: within one commit a rewritten (tombstone) manifest and an added manifest share
      (content, seq, min_seq) and the tie fell back to writer-dependent manifest-LIST order
      (order-insensitive re-comparison proved every hunk a pure swap — canonicalization, not
      semantics).
- [x] Java `WriteActionsOracle` (newFastAppend — NOT the merging newAppend — newDelete,
      newOverwrite, newReplacePartitions, newRewrite) + Rust GEN chain via the production actions
      + `run-interop-write-actions.sh` (Java byte-diff judge).
- [x] REVIEWER (Opus): APPROVE — chain faithfulness line-cited (DataFileSet path-equality =
      Rust's by-path resolution; FastAppend mirror correct); tie-extension exercised AND
      load-bearing in-fixture (non-total in general — flagged for future fanout fixtures);
      corrected an over-claim (Java does NOT enforce rewrite record-count conservation —
      `validateReplacedAndAddedFiles` checks non-emptiness only); 2 reviewer mutations caught
      (one-sided allowlist removal → both tests fail; delete-C-instead-of-B → legible cascade).

**Outcome:** 3 comparison directions green; s2 provenance (A tombstoned seq 1, B/C Existing
seq 1), s4 `replace-partitions=true` + C tombstoned, s5 `replace` with E tombstoned at seq 4 all
Java-identical. Poison mutation fails exactly the 2 comparison tests. Gate: lib 1643,
clippy/fmt/typos clean, both interop binaries no-op offline; Cargo FROZEN. GAP_MATRIX: the four
cells gain a SCOPED "metadata-level interop ✅ (explicit-API paths)" note — rows stay 🟡
(row-filter/conflict/multi-spec paths uncovered by the chain).

## DONE (2026-06-10): Sprint increment E1 — RowDelta METADATA-level interop (branch `interop/rowdelta-metadata`)

The snapshot/manifest SEMANTICS proof on top of the data-level scan-exec interop. Both sides emit a
CANONICAL "snapshot metadata view" (ordinal snapshots, COUNT-only summaries, manifest-list → entry
structure with POST-INHERITANCE sequence numbers, single-value-JSON partitions) over the three
EXISTING scan-exec fixtures; compared 3 ways per fixture.

- [x] Java `SnapshotMetaOracle` + `emit-snapshot-meta` mode (InteropOracle.java); explicit
      cross-language entry sort tuple.
- [x] Rust mirror `crates/iceberg/tests/interop_rowdelta_meta.rs` (env-gated, offline no-op).
- [x] `run-interop-rowdelta-meta.sh`: Java writes 3 tables + emits views; Rust writes 3 equivalents
      (existing GEN paths reused); Java emits + byte-DIFFS its view of each Rust table vs its own
      (Java judging Rust); Rust asserts its views of BOTH tables equal Java's.
- [x] **REAL parity bug found + fixed** (`spec/snapshot_summary.rs`): Rust omitted
      `changed-partition-count` from every summary (unpartitioned files never tracked; count
      emitted only-if-positive; `trust_partition_metrics` defaulted FALSE vs Java's trusted
      default). Fixed Java-faithfully (trust-gated count incl. 0, empty-partition tracked,
      `partition-summaries-included`, empty-key skip); 2 inconsistent test fixtures reconciled.
- [x] REVIEWER (Opus, actor-critic): canonicalization verified sound (null-vs-empty equality_ids
      symmetric; sort-tuple ties render identical; *-files-size exclusion correct); production fix
      line-cited exact vs Java `SnapshotSummary.java`; found 2 UNPINNED offline mutation axes
      (count only-if-positive; marker unconditional) and added the closing test; flagged the
      V1 sequence-number-tie ordinal limit (documented in both emitters).

**Outcome:** round-trip GREEN — 3 fixtures × {Java-reads-Rust byte-diff, Rust-reads-Java,
Rust-self} (9 comparisons). Mutations: pre-fix run failure = the disable-mutation; poisoned count
fails both Rust tests; reviewer's 2 mutations caught by the added test. Offline gate: lib **1643**,
datafusion 80+9 (doctest = documented pre-existing artifact), clippy/fmt/typos clean; Cargo FROZEN.
Offline write-path pin added (`fast_append` summary carries `changed-partition-count`).
DEFERRED: `file_sequence_number` in the view (no public accessor); V1-table ordinal tiebreaker.

## DONE (2026-06-10): Sprint increment D — status de-triplication (branch `docs/de-triplication`)

One home per fact. Trigger: the sprint plan (A ✅ B ✅ C ✅ E3 ✅ → D), freshly motivated by the
overnight stale-narrative incident (Roadmap said `validateAddedFilesMatchOverwriteFilter` was
deferred; it had landed in PR #9).

- [x] **GAP_MATRIX cells go terse.** The 19 cells over 400 chars (worst: 36 KB) move VERBATIM to
      `docs/parity/archive/2026-06_matrix-cell-narratives.md` (keyed by row Area); each cell is
      rewritten as: Rust location · 1–2 sentence capability summary · flip dates · links (interop
      test, archive anchor). Status icons unchanged — this pass moves prose, never status.
- [x] **Roadmap shrinks to plan + pointers.** The per-increment narrative blobs ("For a new
      session" item 2, "Current state", per-phase Progress sections) move VERBATIM to
      `task/todo-archive/roadmap-narratives-2026-06.md`; "Current state" is rewritten ≤ ~30 lines
      pointing at the matrix; phase sections keep Goal/Gates/Deliverables/Exit + one-line status.
- [x] **CLAUDE.md gains the one-home-per-fact rule** (status lives in the matrix; narrative lives
      in archives/git; never write the same status twice — link instead).
- [x] **Gates:** conservation (every moved block verbatim, anchors resolve), all links resolve,
      `typos` clean, session-start read order (CLAUDE.md+Roadmap+GAP_MATRIX+lessons+todo) measured
      and recorded — target ≤ ~40k tokens.
      **Outcome:** GAP_MATRIX 136 KB → 27 KB (19 cells → archive verbatim, conservation 19/19);
      Roadmap 823 → 298 lines (whole pre-rewrite file archived verbatim, byte-identical diff);
      read order measured **~41.6k tokens** (was ~210k pre-sprint; lessons.md is the remaining
      ~19k and shrinks at the next compaction pass); links resolve; typos clean.
- [x] Update `task/todo-archive/map.md` for the new archive file; flip the sprint-D checkbox.

## DONE (2026-06-10 overnight): OverwriteFiles validateNewDeletes branch A — Increment 2 of OVERNIGHT_BRIEF

Added the MISSING row-filter sub-branch of Java `BaseOverwriteFiles.validate`'s `validateNewDeletes`
(L168-172) — Rust previously had only branch B (`!deletedDataFiles.isEmpty()`). Builder→reviewer
actor-critic; orchestrator independently re-ran the gate + committed.

- [x] `snapshot.rs` — new `validate_deleted_data_files` (filter-based port of Java
      `MergingSnapshotProducer.validateDeletedDataFiles` L636-654; reuses `deleted_data_files_after(.., false)`
      + private `first_conflicting_file`; exact Java "Found conflicting deleted files that can contain records
      matching {filter}: {path}").
- [x] `overwrite_files.rs` — restructured the `validate_no_conflicting_deletes` block: branch A
      (`row_filter != AlwaysFalse` ⇒ `filter = conflict_detection_filter ?? row_filter` ⇒
      `validate_no_conflicting_added_delete_files` + `validate_deleted_data_files`) + branch B unchanged.
- [x] 7 `MemoryCatalog` tests (2 positives, the no-override tx-captured-start pin, flag-off control, 2
      row-filter-gate cases incl. the reviewer-added conflict-filter-only gap). Reviewer mutation-pinned each.

**Outcome:** Cargo FROZEN (0 dep changes). Independent gate green — typos/fmt/workspace-clippy clean,
`cargo test -p iceberg --lib` **1642 passed**, transaction:: 300 ×2. GAP_MATRIX `OverwriteFiles` row note
updated (stays 🟡 — data-level interop deferred). **2a (`validateAddedFilesMatchOverwriteFilter`) was already
done in PR #9; 2c (`RewriteFiles.validateNoNewDeletes`) is shadowed by the coarse `has_outstanding_delete_files`
guard (both `validate` + `commit` run on the refreshed base) so it cannot fire/be tested while that guard
stands — SKIPPED, see morning report.**

## DONE (2026-06-10 overnight): readable_metrics inspection interop — Increment 3 of OVERNIGHT_BRIEF

The LAST inspection-table surface without a Java interop round-trip. Extended the existing manifest-reading
harness (A1/A2 pattern, run.sh-driven, Direction-1, env-gated) with a dedicated `table_rm`. Builder→reviewer
actor-critic; orchestrator independently re-ran BOTH the offline gate AND the round-trip + committed.

- [x] `dev/java-interop/.../InteropOracle.java` — new `InspectionReadableMetricsRmOracle`: unpartitioned V2
      `{id long, name string, score double}` table at `<dir>/table_rm` with rich per-column metrics (distinct
      column_sizes; null counts non-zero on name; nan counts non-zero only on the double; distinct typed
      lower/upper bounds via `Conversions.toByteBuffer(<column type>, value)`); materializes Java's REAL
      FilesTable readable_metrics struct → `java_rm_files.json` keyed by leaf-column NAME → 6 metric names.
      A1/A2/A4 emitters untouched (byte-identical).
- [x] `crates/iceberg/tests/interop_inspection_manifests.rs` — new env-gated test comparing Rust
      `inspect().files()` readable_metrics to Java BY NAME (order-independent; counts distinguish absent/None
      from 0; long+string+double typed bounds, double via `f64::to_bits`; leaf-column SET pinned).
- [x] `run-inspection-manifests.sh` — header note; same single round-trip (table_rm generated alongside).

**Outcome:** Cargo/pom FROZEN (no new deps). Independent gate green — offline interop suite **11 passed**
(new test no-ops when `ICEBERG_INTEROP_MANIFEST_DIR` unset), `cargo test -p iceberg --lib` 1642 passed,
clippy/fmt clean. **Round-trip re-run by orchestrator: 11 passed — readable_metrics matched Java field-for-field
(3 leaf columns, counts + typed bounds).** Reviewer poison-fixture-pinned every axis (bound/count/drop-column/
absent↔zero/string-bound/double-bits → matching assertion fails); no production bug. By-NAME comparison
side-steps the documented interior field-id JVM-HashMap-order divergence. GAP_MATRIX inspection row updated;
inspection interop now COMPLETE (set + columns + scan A4/A5). DEFERRED: promoted-type bound (needs schema
evolution); byte-level interior field-id parity (the HashMap-order residual). Row stays 🟡.

## NEXT (plan sketch, NOT built): `RewriteManifests` (Phase 2 write engine — the next Roadmap increment)

Increment 4 of OVERNIGHT_BRIEF was STRETCH; SKETCHED not built — it is new machinery (correctness-critical
manifest re-cluster with per-entry provenance preservation) that warrants its own focused builder→reviewer
cycle, not a rushed end-of-session pass. Java `BaseRewriteManifests` (`/tmp/iceberg-java-ref/core/.../BaseRewriteManifests.java`,
386 lines) extends `SnapshotProducer<RewriteManifests>` (NOT `MergingSnapshotProducer`) and produces an
`Operation::Replace` snapshot whose LIVE FILE SET IS UNCHANGED — only manifest grouping changes.

**Scope (one increment):**
- New action `transaction/rewrite_manifests.rs` (mirror `sort_order.rs` action shape + wire `mod.rs` + a `pub fn`
  ctor). Builder surface: `cluster_by(fn: DataFile -> key)` (Java `clusterBy`), `rewrite_if(pred: ManifestFile -> bool)`
  (Java `rewriteIf`, default all), `add_manifest`/`delete_manifest` (Java `addManifest`/`deleteManifest` — the
  explicit-replacement mode), `set(property,value)`.
- `apply` (Java L170-195): partition the current snapshot's data manifests into KEPT (predicate false, or no
  cluster fn) vs REWRITTEN (predicate true); `performRewrite` (Java L239-276) reads each rewritten manifest's
  ENTRIES and re-groups them by `cluster_by_func(entry.file())` into new manifests sized to
  `manifest.target-size-bytes`. **THE LOAD-BEARING INVARIANT: each re-written entry MUST keep its ORIGINAL
  `snapshot_id` + `data_sequence_number` + `file_sequence_number` + status** (Java copies the entry verbatim via
  the manifest writer's existing-entry path) — re-stamping is the silent-corruption class (resurrects/loses rows
  on the next merge-on-read scan). The live set (paths) is identical before/after.
- Validations: `validateDeletedManifests` (Java L284-302 — every `delete_manifest` must be a current manifest,
  not concurrently gone) + `validateFilesCounts` (Java L304-322 — total entry count across new manifests ==
  count across replaced manifests; the conservation guard).
- Producer support: the existing `snapshot.rs` `SnapshotProducer` writes manifests from `added_data_files`
  (fresh `Added` entries) — it does NOT currently re-emit EXISTING entries with preserved provenance for a
  cluster. Likely needs a new producer path "write these pre-built `ManifestEntry`s verbatim into N new
  manifests" (analogous to `rewrite_manifest_with_deletes` but clustering, not filtering). Scope this carefully;
  it may be the bulk of the increment.

**Tests (MemoryCatalog, no interop): the provenance pin is MANDATORY** (docs/testing.md write-action pin #2):
after a rewrite, assert each entry's `snapshot_id`/`data_sequence_number`/`file_sequence_number` == its
pre-rewrite value (mutation: re-stamp with the new snapshot id → the pin fails); the post-rewrite SCAN live set
== pre-rewrite (paths unchanged); `validateFilesCounts` fires on a count mismatch; `clusterBy` actually groups
(N input manifests → M output by key); `rewriteIf` keeps the predicate-false manifests untouched (byte-identical
ManifestFile). Add the cumulative-totals + provenance mutation pins. Done-bar 🟡 (unit; data-level interop later).

**Why deferred, not attempted:** the Rust `SnapshotProducer` is shaped around add/delete-file PATHS, not
entry-level manifest re-clustering with preserved provenance; getting that producer path right is the increment's
real risk and deserves a full actor-critic cycle rather than a tail-of-night rush. Pick this up first next session.

## Active: Operational hardening & Opus handoff — the meta-sprint (2026-06-09)

**Decided 2026-06-09 (user-approved).** Context: frontier-tier (Fable) sessions are available only
until **2026-06-22**; after that **Opus is the default maintainer tier**. The planning files have
outgrown the session-start read contract (todo.md 380 KB + lessons.md 256 KB + Roadmap.md 72 KB +
GAP_MATRIX.md 132 KB ≈ 840 KB of mandated reading — several context windows), so this sprint hardens
the documentation infrastructure FIRST, then spends the remaining frontier budget on the
highest-judgment interop debt. Decision record + rationale: Roadmap.md §"Operational hardening
sprint (2026-06-09)".

**Ordering constraints (load-bearing):**
- [skills/compaction.md](../skills/compaction.md) routes promoted lessons into directory
  `map.md#debug` sections → **A (maps) must land before B (lessons compaction)**.
- The terse GAP_MATRIX cells produced by D link to archived increment narratives → **C (todo
  archival) must land before D (de-triplication)**.
- B and C follow compaction.md discipline: **own PR, nothing else in the diff, conservation check,
  interactive approval of the verdict tally before commit.**

- [x] **Step 0 — stabilize the base.** phase3-scan-exec-interop landed on main as PR #11 (tree
      verified identical; local main fast-forwarded to 7e56fbf7; 1636 lib tests green on this tree).
- [x] **A — `map.md` scaffolding.** Created map + `## Debug` for the seven hot directories:
      `crates/iceberg/src/{transaction,inspect,scan,expr/visitors,writer}/`, `dev/java-interop/`,
      `crates/iceberg/tests/`. Seeded from code (module lists verified against `mod.rs`); Debug
      sections seeded with already-recorded failure modes and left thin for B's promotions.
      Relative links verified resolving; `typos` clean; no code touched. Rides in the same PR as
      the sprint-plan record (separate commits) since `gh` is unavailable locally.
- [x] **B — lessons.md compaction pass (own PR, interactive).** DONE 2026-06-09 — tally + promotion
      diffs + the agentic-pace recency deviation user-approved; committed on
      `docs/lessons-compaction-pass-1` (skills bundle as its own prior commit on the branch).
      - Trigger: size (2,650 lines / 256 KB vs the ~800-line / 50 KB trigger).
      - **Tally: 74 entries → 31 PROMOTE, 22 KEEP, 21 ARCHIVE.** Conservation check reconciles
        (74 = 22 active + 52 archived; heading diff empty). Active file now 797 lines.
      - Archive: `task/lessons-archive/2026-06_phase1-phase3.md` (+ archive map.md).
      - Promotion targets: docs/testing.md (new "Mutation-testing & review discipline" section +
        gate-widening rules), CLAUDE.md (gate-chained-commit convention), and the Debug sections of
        transaction/inspect/scan/writer + dev/java-interop + tests map.md files.
      - **Recency-rule deviation (needs the user's sign-off):** ALL 74 entries are within the
        7-day window (the project is 3 days old) — a strict reading makes every pass a no-op.
        Applied the intent (protect in-flight context): all 22 same-day (2026-06-09) entries +
        older entries feeding open work KEPT; landed-and-merged increment narratives archived.
        Codified as the "agentic-pace amendment" in skills/compaction.md.
      - **Prerequisite discovered:** `skills/Fable.md`, `skills/compaction.md`, and the updated
        `skills/map.md` from the prior Fable session were never committed — added verbatim from
        the pasted bundle (separate commit on the same branch so the compaction diff stays pure);
        CLAUDE.md read order now names Fable.md.
- [ ] **C — todo.md archival (own PR).** First WRITE the procedure (a todo-archival section in
      skills/compaction.md or a sibling doc: completed increments archive by phase into
      `task/todo-archive/` with its own map.md), then execute: completed-increment narratives move
      verbatim; live todo keeps open items + current context, < ~500 lines; same conservation
      discipline.
- [x] **D — de-triplicate status (own PR, review carefully).** DONE 2026-06-10, see below. One home per fact:
      GAP_MATRIX becomes the ONLY status record with terse cells (icon, date, one sentence, links to
      the interop test + archived narrative); Roadmap "current state" shrinks to ≤ ~30 lines pointing
      at the matrix; per-increment narrative paragraphs move to the todo-archive; CLAUDE.md gains the
      one-home-per-fact rule. Gate: every link resolves; session-start read order measured ≤ ~40k
      tokens (record the number).
- [ ] **E — interop debt paydown (rest of the frontier budget; one PR per increment).** Risk order:
      - [x] **E1 — `RowDelta` metadata-level interop:** DONE 2026-06-10 (see the E1 section
            above) — canonical-view equality across 3 fixtures × 3 directions; surfaced + fixed
            the `changed-partition-count` summary parity bug.
      - [x] **E2 — the rewrite-family four:** DONE 2026-06-10 (see the E2 section above) — one
            five-commit chain through the E1 oracle; zero production changes needed.
      - [ ] **E3 — inspection-table interop:** mechanical, well-templated — the explicit leave-to-
            Opus candidate if the budget runs out.

**Explicitly NOT decided:** the "platform cut line" through the GAP_MATRIX (which rows block the
user's trading platform vs continuous-parity backlog, incl. re-ordering maintenance actions ahead of
Phase-4 format exotica) was proposed but is an **open user decision — do not assume it.**


## Arc G (2026-06-11): close the two carried-forward Phase-1 items (branch `phase1/carried-forward-closeout`)

Delegated builder (1 of 2; Opus reviewer verifies after). Settle both carried items with Java 1.10.0
BYTECODE evidence (javap from ~/.m2 jars — bytecode outranks MAIN source for version-sensitive claims).
Modify ONLY `spec/table_metadata.rs`, `spec/snapshot.rs`, `docs/parity/GAP_MATRIX.md`, `task/todo.md`,
`task/lessons.md`. No Cargo edits. No commit.

PLAN (2026-06-11, builder):
- [x] **Item 1 — `last-sequence-number` lenient read: SETTLE, do NOT auto-apply the proposed fix.**
      Bytecode of `TableMetadataParser.fromJson` (core-1.10.0): `if (formatVersion <= 1) → 0` else
      `JsonUtil.getLong("last-sequence-number")`; `JsonUtil.getLong` does
      `checkArgument(node.has(field), "Cannot parse missing long: %s")` ⇒ **THROWS** on an absent V2+
      `last-sequence-number`. Spec line 179 confirms ("a v2 table that is missing `last-sequence-number`
      can throw an exception"); spec line 1978's "default to 0" applies to reading a **V1 document** for
      V2, which Java handles by the `formatVersion <= 1` branch (never reads the field). Rust ALREADY
      matches: `TableMetadataV2V3Shared.last_sequence_number` is required (V2/V3 absent ⇒ parse fails);
      `TableMetadataV1` has no such field and conversion sets 0. **Conclusion: NO `#[serde(default)]` —
      adding it would make Rust accept malformed V2 metadata Java REJECTS (a divergence, not parity).**
      Verify-then-pin (like Increment 11): add tests pinning the strict V2 read + the V1-defaults-to-0
      read + the write-side-always-emits round-trip; cite the spec line + bytecode in a doc comment.
- [x] **Item 2 — retention positivity: Rust write path MATCHES Java; the PARSE path is the real gap.**
      Bytecode of `SnapshotRef$Builder` (api-1.10.0) PROVES the three `checkArgument(value==null ||
      value>0)` guards exist with messages "Min snapshots to keep must be greater than 0" / "Max
      snapshot age must be greater than 0 ms" / "Max reference age must be greater than 0" — Rust's
      `manage_snapshots::validate_retention_positive` reproduces all three verbatim (the carried item's
      `SnapshotRef.java` source grep missed them; bytecode is authority). BUT `SnapshotRefParser.fromJson`
      (core-1.10.0) routes through the SAME validating builder (`builderFor → minSnapshotsToKeep →
      maxSnapshotAgeMs → maxRefAgeMs → build`), so Java REJECTS zero/negative retention ON PARSE. Rust's
      `SnapshotRetention` serde does NOT validate — empirical probe: `{"type":"branch",
      "min-snapshots-to-keep":0,"max-snapshot-age-ms":-5,"max-ref-age-ms":0}` parses Ok in Rust, throws
      in Java. PORT the missing parse-path check into `spec/snapshot.rs` (custom Deserialize for
      `SnapshotRetention`) with the EXACT Java messages; a real Java table never carries zero retention
      (Java's own parser would have thrown), so this cannot reject metadata Java accepts. Tests: each
      field rejected on parse with exact message; null/positive accepted; round-trip preserved.
- [x] Docs: GAP_MATRIX ManageSnapshots + manifest read/write residual notes; both carried bullets →
      DONE with evidence; lessons appended; verify gate (typos/fmt/clippy/lib ×2).

OUTCOME (2026-06-11, builder): both carried items SETTLED with Java 1.10.0 bytecode evidence.
**Item 1 = verify-then-pin, NO production change** — the carried `#[serde(default)]` "fix" would have
been ANTI-parity (Java THROWS on absent V2+ `last-sequence-number`; Rust already matches). 3 tests in
`spec/table_metadata.rs`; the `#[serde(default)]` mutation fails the V2-strict test. **Item 2 = a REAL
gap ported** — Java validates retention positivity on the PARSE path (`SnapshotRefParser`→`SnapshotRef.
Builder`), Rust's serde did not; added `SnapshotRetention::validate_positive` run via `SnapshotReference`'s
`try_from` deserialize, exact Java messages; 4 tests, both mutation directions caught. The Rust WRITE path
(`manage_snapshots::validate_retention_positive`) already matched (untouched). Gate: typos/fmt/clippy clean,
lib **1701 ×2** (was 1694; +7). Files: `spec/table_metadata.rs`, `spec/snapshot.rs`, `docs/parity/GAP_MATRIX.md`,
`task/todo.md`, `task/lessons.md` — exactly the allowed set. No Cargo edits, no commit.

## Carried-forward open items (full context in `todo-archive/`)

Genuinely-open follow-ups lifted out of otherwise-shipped phase narratives so they stay visible in
the live plan. Full originating context is archived verbatim (pointers below). The active sprint's
own open items (C/D/E) live in the hardening section above, not here.

- [x] **Retention positivity validation — DONE (Arc G, 2026-06-11, `phase1/carried-forward-closeout`).**
      EVIDENCE (bytecode > MAIN source — the carried `SnapshotRef.java` grep missed them): Java 1.10.0
      `SnapshotRef$Builder` DOES `checkArgument(value == null || value > 0)` in each of `minSnapshotsToKeep`
      / `maxSnapshotAgeMs` / `maxRefAgeMs`, messages "Min snapshots to keep must be greater than 0" / "Max
      snapshot age must be greater than 0 ms" / "Max reference age must be greater than 0". Rust's
      `manage_snapshots::validate_retention_positive` already reproduced all three VERBATIM on the WRITE
      path. NEW finding: `SnapshotRefParser.fromJson` routes through that validating builder, so Java rejects
      zero/negative retention ON PARSE too — Rust's serde did NOT (probe: zero/neg branch+tag retention parsed
      Ok). PORTED the parse-path guard into `spec/snapshot.rs` (`SnapshotRetention::validate_positive`, run via
      `SnapshotReference`'s `try_from` deserialize) with the exact Java messages; cannot reject metadata Java
      accepts (Java's own parser would have thrown). 4 tests + 2-direction mutation. _Originating: [todo-archive/phase1.md](todo-archive/phase1.md) §"Review remediation"._
- [x] **Table-metadata `last-sequence-number` lenient read — DONE/verify-then-pin (Arc G, 2026-06-11,
      `phase1/carried-forward-closeout`).** The proposed `#[serde(default)]` fix was WRONG. EVIDENCE
      (bytecode): Java 1.10.0 `TableMetadataParser.fromJson` reads `last-sequence-number` via `JsonUtil.getLong`
      (which `checkArgument(node.has(field))` → THROWS on absent) when `formatVersion > 1`, and hard-defaults
      to 0 when `formatVersion <= 1`. Spec line 179 explicitly permits the V2 throw; the "default to 0" at
      spec line 1978 is for reading a V1 doc as V2. Rust ALREADY matches: `last_sequence_number` required on
      V2/V3 (absent ⇒ parse fails); `TableMetadataV1` has no such field, conversion seeds 0. Adding
      `#[serde(default)]` would accept malformed V2 metadata Java rejects (a divergence). NO production change;
      3 tests pin the V2-strict read + V1-defaults-to-0 + write-always-emits, with the `#[serde(default)]`
      mutation caught. _Originating: [todo-archive/phase1.md](todo-archive/phase1.md) §"Increment 10 — tracked follow-up" + §"Increment 11"._


## Archived increment narratives

Completed-increment narratives moved verbatim out of this file (see [skills/compaction.md](../skills/compaction.md)
§Todo Archival). Not session-start reading — grep/open on demand.

- [todo-archive/phase1.md](todo-archive/phase1.md) — Phase 1 spec & metadata completeness (schema /
  partition / snapshot evolution + spec-read robustness).
- [todo-archive/phase2.md](todo-archive/phase2.md) — Phase 2 write engine (write actions + the
  concurrent-commit conflict-validation cluster, incl. the merged write-validation PR #9).
- [todo-archive/phase3.md](todo-archive/phase3.md) — Phase 3 scan parity (residual evaluation,
  inspection tables, scan-metrics emission, and inspection / scan-execution interop).
- Index: [todo-archive/map.md](todo-archive/map.md).
