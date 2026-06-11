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


> **Archival log.** Last pass: 2026-06-11 (size trigger — 1,381 lines; pass 2) → [todo-archive/](todo-archive/) (phase1/phase2/phase3 + 2026-06_ops-hardening). 16 `##` sections: 2 kept live, 14 archived (9 → phase2, 2 → phase1, 1 → phase3, 2 → ops-hardening); the platform-cut-line paragraph lifted into Carried-forward. Prior pass: 2026-06-09 (size trigger — 4,344 lines) → [todo-archive/](todo-archive/) (phase1/phase2/phase3). Completed-increment narratives moved verbatim; this file keeps the active sprint + open items + archive pointers. Procedure: [skills/compaction.md](../skills/compaction.md) §Todo Archival. Archives are not read by default.

## DONE (2026-06-11): Lessons compaction pass 2 (branch `docs/lessons-compaction-pass-2`, user-approved)

- Trigger: SIZE — 1,369 lines / 128 KB on the settled post-#24 main (trigger ~800 / 50 KB).
- Tally: **42 entries → 17 KEEP / 25 ARCHIVE / 6 rules PROMOTED** (the 25-entry archive set and
  all six promotion diffs are byte-identical to the version presented for approval pre-merges;
  the KEEP set absorbed every 2026-06-10/11 arc entry — current work feeding the platform plan).
- Archive: `task/lessons-archive/2026-06_phase2-completion.md` (+ archive map row). Promotions:
  2 → docs/testing.md, 2 → dev/java-interop/map.md#debug, 1 → transaction/map.md#debug,
  1 → CLAUDE.md. Conservation: 42 == 17 + 25, no duplicates. Active file 583 lines.

## DONE (2026-06-11): Todo archival pass 2 (branch `docs/todo-archival-pass-2`, stacked on the lessons pass)

- Trigger: SIZE — 1,381 lines on the settled post-#24 main (target < ~500).
- 16 `##` sections: 2 KEEP live (the lessons-pass record + the pointer section), 14 ARCHIVE
  verbatim — 9 → phase2.md (the write-engine arc ×2 incl. the union-merge bare-header artifact,
  the DV arc, the overnight plan + morning report, Arc F, E1, E2, the OverwriteFiles branch-A
  increment, the superseded RewriteManifests sketch), 2 → phase1.md (Arc G + the closed
  carried-forward items), 1 → phase3.md (readable_metrics interop), 2 → the NEW
  `2026-06_ops-hardening.md` (increment D + the hardening meta-sprint — meta work, deliberately
  not phase-filed; deviation documented in that file's header).
- LIFTED (the sanctioned carve-out): the platform-cut-line open decision → Carried-forward below.
- Conservation: every pre-pass `##` heading in exactly one place; no checkbox flipped; no
  paraphrase. typos clean.
- Stale-box audit (the rule's verify-before-deciding step): 8 unticked `[ ]` boxes ride the
  archived sections, ALL stale-done, verified — Arc E (merged PR #22), Arc-E Inc 1 (#22), Arc F
  (#23), Arc G (#24), the morning report (delivered via the wrapper-root file), increment C
  (archival pass 1, 2026-06-09, recorded in the archival log), E / E3 (inspection interop
  COMPLETE per the GAP_MATRIX row). None surfaced as live work; preserved unflipped in context.

## ACTIVE (2026-06-11): Near-full-parity direction — next arcs (planning record)

Directive (user, 2026-06-11): table DataFusion/RePark; run this fork's Roadmap to **almost the
full 1:1 Java replacement**. Sequencing in Roadmap.md "Headline gap AREAS" (handoff-aware:
judgment-heavy → frontier window before 2026-06-22; templated breadth → Opus).

- [x] **Phase-2/3 closeout** — LANDED (PR #28, 2026-06-11): multi-spec writes, constants-map,
      `removeRows` apply-side, `dv_seq >= data_seq` index validation.
- [x] **Maintenance actions: `ExpireSnapshots`** — LANDED (PR #29, 2026-06-11): B1 retention +
      B2 `ReachableFileCleanup`. Deferred residue on the GAP_MATRIX row.
- [ ] **THIS BRANCH (Group A, Opus actor-critic, user-approved 2026-06-11):
      `DeleteOrphanFiles`** — A1: `Storage::list` primitive (trait default + local_fs/memory
      impls + `FileIO::list`); A2: the action itself (reachable-set vs listed-set, URI
      normalization + `PrefixMismatchMode`, `olderThan` grace, hidden-path filtering). Runs in
      worktree `wt-orphan` parallel to Group B (`phase4/variant-groundwork`, Fable).
  - [x] **A1 BUILDER (2026-06-11, wt-orphan, Opus):** `Storage::list` / `FileIO::list` prefix-listing
        primitive. `FileInfo { location, size, created_at_millis }` (mirrors Java `FileInfo(String,
        long, long)`; `created_at_millis` ← last-modified, per Java object-store impls). Trait default
        body errors `FeatureUnsupported` (naming the op) so external `#[typetag::serde]` implementors
        keep compiling. RECURSIVE semantics (Java `HadoopFileIO.listPrefix` → `listFiles(prefix,
        recursive=true)`, files-only). **Prefix-semantics decision: mirror each backend's existing
        `delete_prefix` so `list`/`delete_prefix` agree on "under the prefix"** — local_fs = DIRECTORY
        semantics (walk the dir tree; sibling `ab2/` never matches prefix `ab`), memory = STRING-PREFIX
        semantics (append trailing `/`, then `starts_with`). Documented divergence: returns
        `Vec<FileInfo>` (eager), not Java's lazy `Iterable`. OpenDAL stretch: IMPLEMENTED via
        `op.list_with(prefix).recursive(true)` + per-file `stat` for authoritative size/last-modified
        (timestamp via `From<raw::Timestamp> for SystemTime` — no jiff Cargo dep); 2 memory-service
        smoke tests (recursive+prefix-bounded, empty-prefix).
        Outcome: gate CLEAN from wt-orphan root — typos clean, fmt clean, clippy `-D warnings` clean
        (workspace ex-sqllogictest), `cargo test -p iceberg --lib` **1882 passed ×2** (baseline 1870
        +12: 1 default-method + 5 local_fs + 4 memory + 2 file_io), `cargo test -p
        iceberg-storage-opendal --lib` 3 passed (incl. 2 new). Files: io/{mod,file_io}.rs,
        io/storage/{mod,local_fs,memory}.rs, storage/opendal/src/lib.rs. FLAG: opendal
        `file_io_s3_test` (4 tests) fail — pre-existing, need a live MinIO at localhost:9000 (not the
        offline gate; untouched `exists`/`input`/`output` paths). A2 NOT started.
  - [x] **A1 REVIEWER (2026-06-11, wt-orphan, Opus, adversarial):** Verified all 9 points 3 ways
        (read+cite / independent probes / mutation-test). 1 BUG FOUND + fixed: memory `list` had an
        `is_empty()` shortcut absent from `delete_prefix`, so `list("memory://")` reported ALL keys
        while `delete_prefix("memory://")` removed NONE — broke the builder's stated list/delete_prefix
        agreement invariant at the empty/root prefix (over-listing = over-delete direction). Fixed to
        match `delete_prefix` exactly (drop `is_empty()`); fail-before/pass-after test
        `test_list_set_equals_delete_prefix_set_including_empty_prefix`. CONFIRMED-SAFE: symlinks
        (cycle terminates, prefix-escape can't leak outside files — `DirEntry::metadata` is lstat-based
        and agrees with `remove_dir_all`; now documented + 2 tests); walk errors propagate loud
        (unreadable-subdir → `Unexpected`, matches Java RemoteIterator; +1 test); opendal timestamp math
        exact ms (epoch→0, pre-epoch clamps 0, round-trips; +1 test); all int casts saturate not wrap;
        default loud-error pins ErrorKind+msg; `MemoryEntry` pub(crate) no public-API/serde change.
        DOCUMENTED divergence: file-as-prefix returns empty vs Hadoop `listFiles(file)` returning the
        file (conservative, can only under-delete; noted in `FileIO::list` doc). Mutations run: memory
        boundary-guard (caught), local_fs no-descend (caught), default→`Ok(vec![])` (caught), opendal
        boundary-guard (caught), opendal base-reconstruction (caught), opendal `is_file()` filter (NOT
        caught — directory markers only appear on object-store/HDFS backends, untestable on
        opendal-memory; left as a noted offline-coverage gap). Gate CLEAN: typos/fmt/clippy `-D warnings`
        clean; `cargo test -p iceberg --lib` **1887 passed ×2** (1882 +5 reviewer tests); opendal --lib
        4 passed. Same file set + task/. No commit.
  - [x] **A2 BUILDER (2026-06-11, wt-orphan, Opus): the `DeleteOrphanFiles` ACTION.** New module
        `crates/iceberg/src/maintenance/{mod.rs,delete_orphan_files.rs,tests.rs}`, wired
        `pub mod maintenance` in lib.rs (1 doc'd line). **This DELETES files.**
    - [x] **API:** `DeleteOrphanFiles::new(table)` + `.location`, `.older_than` (default now−3d),
          `.delete_with`, `.prefix_mismatch_mode` (default Error), `.equal_schemes` (defaults
          `{s3n,s3a→s3}` MERGED with user, comma-flatten), `.equal_authorities` (user-only, flatten),
          `.execute() -> Result<DeleteOrphanFilesResult{orphan_file_locations, delete_failures}>`.
          `PrefixMismatchMode {Error,Ignore,Delete}` + `from_string`. `executeDeleteWith` DEFERRED.
    - [x] **Valid-files universe — RE-DERIVED locally (NOT extracted from expire_cleanup).** The
          orphan universe spans ALL snapshots WITHOUT the `is_alive()` filter expire_cleanup uses
          (Java `ManifestFiles.read`); expire_cleanup computes a `before−after` delta — structurally
          different, no shared helper worth the rule-of-three. NET: ZERO expire_cleanup lines touched,
          map.md not stale. Universe = all content files (every entry) + all manifests + all manifest
          lists + current/previous metadata.json (non-recursive) + version-hint + stats/partition-stats.
    - [x] **Listing + hidden-path filter:** `FileIO::list` + `created_at_millis < older_than` +
          `PartitionAwareHiddenPathFilter` (segment-wise, named-partition `_<field>=` exception).
    - [x] **Orphan join + URI normalization:** local Hadoop-`Path`-equivalent `split_uri` →
          (scheme/authority/path); join on PATH; `uriComponentMatch` (null/empty valid matches any
          actual ⇒ scheme-less local path matches `file://` actual); PrefixMismatchMode ×3 with Java's
          verbatim ERROR message.
    - [x] **Deletion:** sequential via delete_with; per-file failures collected (not abort); full
          orphan list returned regardless of delete success.
    - [x] **Tests:** 7 unit (split/normalization/equal-schemes/PrefixMismatchMode×3/any-compatible/
          hidden-path/version-hint) + 9 e2e (crown-jewel by-category, olderThan grace, hidden-path
          named-partition, location override, delete_with-exact-set, delete-failure-collected,
          GC gate, copy-on-write history survival, ERROR-mode-clean). 7 mutations run, all caught
          except the unconstructible is_alive-on-all-snapshots one (documented coverage limit).
    - [x] **Docs:** GAP_MATRIX ❌→🟡 (5-pipe audit clean), this file, lessons.

      **Outcome (2026-06-11):** A2 landed. Files: `maintenance/{mod,delete_orphan_files,tests}.rs`
      (new), `lib.rs` (+1 `pub mod maintenance`), GAP_MATRIX (1 row), todo, lessons. ZERO
      expire_cleanup.rs / map.md edits (re-derived the universe — no extraction). Gate CLEAN from
      wt-orphan root: typos clean, fmt clean, clippy `-D warnings` clean (workspace ex-sqllogictest),
      `cargo test -p iceberg --lib` **1903 passed ×2** (baseline 1887 + 16). expire_cleanup's 17 tests
      green + untouched. 7 builder mutations: M2 partition-exception-drop, M3 olderThan-flip,
      M4 full-list→deleted-only, M5 manifest-list-drop, M6 manifest-drop, M7 GC-gate-skip ALL caught;
      M1 (is_alive filter on the all-snapshots universe) SURVIVED — documented as unconstructible with
      real commits (the difference is invisible for normally-written tables). No commit. Deferred
      loudly: executor parallelism, bulk deletes, `compareToFileList`/streaming, Java interop (row 🟡).
  - [x] **A2 REVIEWER (2026-06-11, wt-orphan, Opus): adversarial review — VERDICT: PASS with one
        builder claim refuted + two SAFE divergences documented.** Read the module in full, the Java
        MAIN action + core/api 1.10.0 helpers (FileURI/HiddenPathFilter/FileSystemWalker), and ran a
        Hadoop-`Path` ground-truth probe (`java -cp hadoop-client-api`). **M1 IS CONSTRUCTIBLE and was
        KILLED** — the builder's "unconstructible" claim is wrong: commit data_a → copy-on-write-delete
        it (tombstone) → EXPIRE the adding snapshot via the fork's own ExpireSnapshots (metadata-only,
        deletes no files) → data_a is now referenced ONLY by a Deleted tombstone, on disk. The
        no-liveness-filter universe spares it; the `is_alive()` mutation deletes it (history
        corruption). Added `test_tombstone_only_referenced_file_is_not_orphan_after_expire` (kills M1)
        and updated the stale "unconstructible" NOTE. Also found the olderThan `<`→`<=` mutation
        SURVIVED (no boundary test) → added `test_older_than_cut_is_strict_less_than_at_the_exact_boundary`
        (kills it) + `test_default_older_than_makes_fresh_table_sweep_a_no_op`; partition-stats inclusion
        was coded-not-tested → added `test_statistics_and_partition_statistics_files_are_not_orphan`
        (kills the category-drop); list-error propagation, hidden-parent-dir, and the inverse-`file://`
        / trailing-slash compositions now pinned (8 reviewer tests total). Re-ran the builder's 5
        in-scope mutations (scheme-detect, ERROR-as-orphan, abort-on-first-delete-fail, M-stats,
        list-`?`-drop) — ALL killed by the suite. **Two SAFE divergences documented (under-deletion,
        never corruption):** (a) `split_uri` does NOT collapse `//` nor decode `%xx` like Hadoop —
        harmless because both metadata + listing sides carry the identical raw string for a Rust-native
        table; (b) a scheme-qualified table `location` (`file://…`) on the local-fs backend makes the
        hidden filter's `relative_under` fail to strip the base → every file masked-as-hidden → the
        sweep is a silent no-op (S3/Glue via OpenDAL re-prefix listed entries WITH the scheme, so they
        agree and sweep normally). MINOR: the ERROR-message text says `'IGNORE'` where Java MAIN says
        `'NONE'` (a deliberate correction — `NONE` is not a valid mode — but NOT "verbatim" as the
        module doc claims; no test pins the text). Gate CLEAN from wt-orphan root: typos clean, fmt
        clean, clippy `-D warnings` clean (workspace ex-sqllogictest), `cargo test -p iceberg --lib`
        **1911 passed ×2** (1903 baseline + 8 reviewer tests). expire_cleanup.rs byte-untouched
        (`git diff --stat` empty); production `delete_orphan_files.rs` matches its pre-review backup
        byte-for-byte (every mutation reverted). No commit.
- [ ] **Scheduled with the user:** real-catalog (Glue + S3 Tables) hardening — needs credentials.
- [ ] **Opus-queue (post-handoff or parallel):** data-level write-action interop paydown,
      cherrypick interop + `stageOnly`, ORC/Avro breadth, view ops, incremental-scan interop.
- [ ] **THIS BRANCH (Group B, Fable actor-critic, user-approved 2026-06-11): variant-type
      groundwork** — B1: the variant binary format read side (`org.apache.iceberg.variants`
      Serialized* + VariantUtil parity: metadata dictionary, value headers, all primitive
      physical types, objects, arrays — bounds-checked, no panics on malformed bytes); B2: the
      write side (`Variants` factory, `PrimitiveWrapper`, `ValueArray`, `ShreddedObject`
      serialization — byte-exact vs Java). Runs in worktree `wt-variant` parallel to Group A
      (`phase6/delete-orphan-files`, Opus).
  - [x] **B1 contract verified vs the 1.10.0 jar (javap):** `BasicType` {PRIMITIVE=0,
        SHORT_STRING=1, OBJECT=2, ARRAY=3}; `PhysicalType` = the 23-constant set with type-info
        values 0..=20 (`Primitives` constants + the `from(int)` tableswitch) — MAIN source and
        1.10.0 bytecode IDENTICAL for the whole read surface (SerializedMetadata /
        Primitive / ShortString / Object / Array ctor math, `VariantUtil.basicType`,
        `Variant.from`); no divergence found.
  - [x] **B1 module:** `crates/iceberg/src/variant/` (mod.rs + types.rs + util.rs + metadata.rs
        + value.rs + tests.rs + map.md), `pub mod variant` in lib.rs. EAGER parse (documented
        divergence: Java parses lazily; eager = same accepted set as a full Java traversal,
        errors surface at parse time), recursion depth explicitly guarded (limit 128).
        Invalid UTF-8 → Err (documented divergence: Java silently replaces with U+FFFD).
        Binary-search lookups use Java's exact `VariantUtil.find` probe sequence + UTF-16
        `String.compareTo` order, not byte order (lying-sorted/unsorted-field misses pinned).
  - [x] **B1 tests (57):** per-primitive vectors (all 21 type ids, boundary values incl.
        i64::MIN / i128::MIN / negative + max-scale-255 decimals, exact float bits),
        short-string 0/1/63 + multibyte UTF-8, metadata offset sizes 1-4 + sorted/unsorted/
        lying-sorted/empty dict + UTF-16-order lookup, objects (empty/single/2-byte-ids-and-
        offsets-large/data-order≠field-order/nested obj→arr→obj, get hit+miss, Java
        binary-search miss parity), arrays (empty/mixed/out-of-range get), MALFORMED suite
        (every truncation point, offsets past end + descending, duplicate object offsets,
        dict id past size, field id past dict, bad UTF-8 short+long+dict, hostile/negative
        lengths, absurd counts fail fast pre-allocation, 129-deep nesting bomb → Err not
        overflow — all Err, never panic), Java-1.10.0-pinned fixtures (26 hex constants from
        /tmp/variant-fixture-gen running `Variants.*` on iceberg-core-1.10.0; provenance +
        exact generator command in tests.rs module doc; Java round-trip-asserted at gen time).
  - [x] **B1 docs:** GAP_MATRIX variant row ❌→🟡 (read-side decode only; deferrals named:
        B2 write side, shredding, schema-type entry, interop) + pipe-count audit CLEAN;
        variant/map.md (new dir, convention present in siblings); this file; lessons entry
        (UTF-8 replace-vs-error class, find probe-sequence parity, signed-int domain,
        object-vs-array length schemes, B2 decimal-precision + slf4j notes, depth budget).
  - [x] **B1 gate:** typos clean, fmt clean, clippy `-D warnings` (workspace ex-sqllogictest)
        clean, `cargo test -p iceberg --lib` **1927 passed ×2** (baseline 1870 + 57 new) + a
        third green run after mutation restores. Mutation probes (post-edit snapshots in
        /tmp/wtB1_*_postedit.rs.bak, restored surgically): (A) find() probe-sequence swap ⇒
        the lying-sorted + unsorted-object parity tests fail (8 failures); (B) decimal16
        byte-order flip ⇒ exactly the i128::MIN boundary pin + the Java decimal16 fixture
        fail. GAP_MATRIX pipe audit clean. No commit (per brief — changes left in tree).
  - [x] **B1 REVIEW (Fable reviewer, 2026-06-11):** bytecode re-derived (VariantUtil / all
        Serialized* ctors / PhysicalType+Primitives / Variant.from) — masks, is-large bits
        (object 64, array 16), find probe sequence, unsigned scale byte all CONFIRMED; 16 live
        Java 1.10.0 probes (/tmp/variant-probe/VariantProbe.java) incl. a Java-written
        UTF-16-order object (supplementary-vs-BMP names). Fixture honesty: generator re-run,
        all 26 hex pins byte-identical. AMENDED the builder's "same accepted set as a full
        Java traversal" claim: Java lazily ACCEPTS truncated zero-count containers
        (`02 00`/`03 00`) and an empty-dict metadata with an over-declared data end
        (`01 00 05`) — Rust deliberately rejects; divergence documented in mod.rs/map.md and
        test-pinned. 10-mutation sweep: 2 SURVIVORS found (object/array is-large bit
        transpositions) → killed by new Java-probe tests; 8 tests added, suite 57→65, lib
        **1935 passed ×2**.
## ACTIVE (2026-06-11): Variant arc B2 — variant binary format WRITE side (worktree wt-variant, BUILDER Fable, Group B)

Port the Java 1.10.0 write surface (`Variants` factory, `PrimitiveWrapper`, `ValueArray`,
`ShreddedObject`-as-plain-object-writer, `Variants.metadata`) onto the B1 read module. Byte-exact
vs Java-generated fixtures is the bar. All write logic in NEW `variant/write.rs`; minimal additive
constructors in value.rs (private fields); mod.rs mapping table + re-exports; B1 read surface
otherwise untouched.

- [x] **Bytecode pass (1.10.0 vs MAIN):** javap'd `VariantUtil.{sizeOf,metadataHeader,primitiveHeader,
      objectHeader,arrayHeader,shortStringHeader,writeLittleEndianUnsigned}`, `Variants.metadata
      (Collection)` + `of(BigDecimal)` + `of(boolean)`, `PrimitiveWrapper.{ctor,sizeInBytes,writeTo}`
      (full switch), `ValueArray$SerializationState`, `ShreddedObject$SerializationState` + `put`,
      `SerializedValue/SerializedMetadata.writeTo` (verbatim buffer copy), `SortedMerge.of`
      (naturalOrder). NO MAIN-vs-1.10.0 divergence found on the write surface. Pinned rules:
      metadata dictionary is INSERTION-order, no dedup, sorted flag = strictly-ascending
      `compareTo` (UTF-16); offset/width selection = `sizeOf(dataSize)` thresholds 0xFF/0xFFFF/
      0xFFFFFF; short-string spill at UTF-8 len > 63 (`MAX_SHORT_STRING_LENGTH`, writeTo-time);
      object `fieldIdSize = sizeOf(dictionarySize)` (the SIZE, not max id); is-large = count >
      0xFF (object bit 6 = 0x40, array bit 4 = 0x10); object fields written name-sorted
      (UTF-16) with ids re-resolved via `metadata.id(name)` at write time; decimal width by
      PRECISION ≤9/≤18/≤38 else throw.
- [x] **write.rs:** width/header helpers (checked; Java's silent `writeLittleEndianUnsigned`
      mask becomes a named Err door; offset doors prevent hostile-offset wrap);
      `VariantMetadata::from_field_names` + `to_bytes`; `VariantValue::{size_in_bytes,write_to,
      to_bytes}` (metadata-threaded — object fieldIdSize needs `dictionarySize` — and
      depth-guarded with MAX_NESTING_DEPTH on BOTH size and write recursion);
      `Variants.of`-factory constructors incl. `of_decimal(unscaled i128, scale u8)` precision
      rule; `VariantObjectBuilder` (ShreddedObject minus the shred overlay — flagged in the
      module doc); `VariantArray::new/push` + `VariantObject::from_fields`/`from_parts` seams in
      value.rs/metadata.rs; `PhysicalType::to_type_info` inverse in types.rs; `Variant::to_bytes`.
- [x] **Fixture generator:** /tmp/variant-fixture-gen/VariantWriteFixtureGen.java against the
      pinned 1.10.0 jars (needed avro + caffeine jars beyond B1's classpath — ShreddedObject
      writeTo → SortedMerge statics) — 36 fixtures: every primitive id at boundary values,
      short-string 0/1/63/64-spill, binary/UUID, arrays (empty/mixed/offset-width 255-vs-256/
      is-large 256/CRC-pinned 65535-vs-65536), objects (empty/unsorted-puts/UTF-16-order/2-byte
      ids via 255-vs-256 dict/large-256-fields/nested), metadata (empty/sorted/unsorted/dup/
      UTF-16-sorted/offset escalation 255-vs-256) + the Java 256-empty-names truncation evidence.
      Round-trip asserted by Java at generation time; provenance quoted per constant.
- [x] **Rust tests (+21 → lib 1956):** byte-exact pins for every fixture (shared
      `assert_write_fixture` also asserts size_in_bytes, B1 decode equality, AND canonical
      re-serialization); CRC+length+prefix pins with parse round-trips for the 7 large fixtures;
      round-trip sweep (constructed primitives + nested containers); NaN bit-exact; error paths
      (unknown put name, missing-name write "Invalid metadata, missing", precision 39, depth
      128/129 boundary, buffer-too-small/hostile-offset, the 256-empty-names divergence door,
      non-canonical-metadata canonicalization). 5 mutations all killed by their designated pins
      (object-is-large bit transposed → large-object CRC; spill 63/64 off-by-one → short-string-63
      pin; sorted-flag-tolerates-duplicates → duplicate-name metadata pin; byte-order field sort →
      UTF-16 object pin; fieldIdSize-from-max-id → dict256 pin); restored from
      /tmp/wtB2_write_postedit.rs.bak, suite green after.
- [x] **Docs:** mod.rs mapping table + write-side divergence pointer (full list in write.rs's
      module doc); map.md (contents, intents, 3 new debug rows); GAP_MATRIX variant row (stays
      🟡, both sides + B2 date; deferrals = shredding, schema-type entry + format-version gate,
      file-level interop) + pipe audit CLEAN; lessons (durable only).
- [x] **Gate:** typos clean, fmt clean, clippy `-D warnings` (workspace ex-sqllogictest) clean,
      `cargo test -p iceberg --lib` **1956 passed ×2** (baseline 1935 + 21 new). No commit.
- [x] **B2 REVIEW (Fable, 2026-06-11):** APPROVED with 4 test additions (lib 1956 → 1959). CRC gap
      CLOSED out-of-band: all 7 large fixtures FULL-byte-diffed vs Java 1.10.0's complete on-disk
      bytes (`/tmp/variant-fixture-gen/ReviewerProbe.java` → `/tmp/variant-review/*.bin`) —
      byte-identical end-to-end, plus 8 unpinned straddles (object dataSize 65535/65536,
      array/object/metadata 0xFFFFFF vs 0x1000000 → the 4-byte-offset emission) also identical.
      Bytecode re-derivation: `dataSize` = sum of element/field `sizeInBytes()` ONLY (no
      header/offsets) in both `SerializationState`s — Rust matches. Java probes: decimal16
      sign-padding (−1/−10^20/+1), put-last-wins bytes, case-only names "a"/"A",
      `metadata.id` on duplicate names = FIRST match (linear scan) — all match Rust;
      parsed-object-referencing-second-duplicate-id canonicalizes (documented divergence class,
      now pinned). 10 NEW mutations: 9 killed; 1 SURVIVOR (door_value_span removal — `is_err`
      pins don't pin fail-fast) → added buffer-untouched-on-failure pin, now killed. 3 random
      expected-value corruptions all detected (first-run-green is honest). Added tests: object
      65535/65536 CRC pins, array 16777215/16777216 CRC pins (the only width-4 fixtures),
      duplicate-name id-resolution pin, M9 pin. House clean (pipes, Cargo, lib.rs, no bare
      unwrap). Lib **1959 ×2**.

## DONE (2026-06-11): Multi-spec writes — producer per-spec grouping (BUILDER, Group A, wt-closeout)

Goal: lift the Rust `SnapshotProducer` from DEFAULT-SPEC-ONLY to Java-parity PER-SPEC manifest groups.

- [x] **Producer grouping (snapshot.rs):** `write_added_manifests`/`write_added_delete_manifests` group
  `added_data_files` / `added_delete_files` by `partition_spec_id` (helper `group_files_by_spec`, spec-id
  DESCENDING) and write one manifest per (content × spec) via `new_cluster_manifest_writer(spec, content)`
  (generalized to take content; the two existing callers pass `Data`). The explicit-data-seq (RewriteFiles)
  + V1 snapshot-id stamping paths preserved. Removed the now-dead default-spec `new_manifest_writer` (which
  also carried a bare `.unwrap()`).
- [x] **Validation lift (snapshot.rs):** `validate_added_data_files`/`validate_added_delete_files` now check
  spec EXISTENCE via `partition_type_for_added_file` with Java's exact "Cannot find partition spec %s for
  {data,delete} file: %s"; partition-value compat against the FILE's own spec.
- [x] **Summary ripple:** `summary()` passes each file's own spec via `file_partition_spec(file)` (Java
  `addedFile(spec(file.specId()), file)`), not the default — the changed-partition-summaries fix.
- [x] **Cherrypick conversion:** `test_cherrypick_multispec_replay_fails_loud` →
  `test_cherrypick_multispec_replay_produces_per_spec_manifest` (replay SUCCEEDS, manifest stamped spec 0,
  scan correct); module-doc note rewritten to the per-spec parity contract.
- [x] **Tests:** 6 producer tests in `snapshot::multispec_tests` (two-spec data + delete manifests,
  unknown-spec data + delete rejection, wrong-spec-type, cumulative totals) + the cherrypick conversion.
  Renamed `row_delta::test_row_delta_rejects_partition_spec_mismatch` →
  `test_row_delta_rejects_unknown_partition_spec` (stale default-spec assertion fixed).
- [x] **Mutations:** grouping-revert (default-spec-only) ⇒ all 4 grouping tests fail (`zip_eq` tuple-arity
  panic = partition corruption); validation-revert (default fallback) ⇒ all 3 unknown-spec tests fail
  (door message gone). Both restored from /tmp/wtA_snapshot_pre_mutation.rs.
- [x] **Docs:** GAP_MATRIX (multi-op row + cherrypick cell), transaction/map.md, lessons.

**Outcome:** Producer is Java-parity per-spec. Verification: typos clean, fmt clean, clippy `-D warnings`
clean (workspace ex-sqllogictest), `cargo test -p iceberg --lib` 1804 passed ×2 (was 1798 baseline +6 new
−1 renamed... net 1798→1804 = +6 producer +6 unchanged... 1798+6=1804), `iceberg-datafusion` lib+integration
9/9 + write-path insert tests green. PRE-EXISTING unrelated failure flagged: an `iceberg-datafusion` DOCTEST
(`table_provider_factory.rs:41`) fails to compile (`#[tokio::main]` multi_thread w/o `rt-multi-thread`) — not
touched by this increment. Deferred (flagged): WRITER-LAYER spec threading; `OverwriteFiles::validate_added_files`
default-spec (Java's `dataSpec()` rejects multi-spec there anyway); multi-spec Java↔Rust interop. No commit.

**REVIEWER PASS (Group A, 2026-06-11, wt-closeout).** Verdict: APPROVE with two added pins.
- **THE MISSING SUMMARY PIN (point 1) — confirmed gap, fixed.** The builder shipped NO test that fails
  CLEANLY under a summary-collector revert. The summary-revert mutation only crashed the 3 arity-differing
  manifest tests via a `partition_to_path` index-out-of-bounds PANIC (the "lucky" version) — a same-arity
  different-NAME multi-spec commit would silently render the WRONG `partitions.{path}` key with NO panic.
  Added `test_fast_append_multispec_partition_summary_keys_use_file_spec` (spec0=`identity(x)`, spec1=
  `identity(y)` via a same-arity rename; both files partition value 5): asserts `partitions.x=5` present
  (NOT `partitions.y=5`-only) AND `changed-partition-count=2` (the default-spec bug collapses both onto
  `y=5` ⇒ 1). Fails CLEANLY under the summary-revert (asserted, not panic); passes on fixed. Verified Java
  `SnapshotSummary.Builder.addedFile(spec(file.specId()), file)` → `updatePartitions` → `partitionToPath`
  uses the FILE's spec (1.10.0 bytecode).
- **V1 multi-spec (point 4) — probed, WORKS.** Added `test_v1_fast_append_two_specs_produces_per_spec_data_manifests`:
  a V1 two-spec DATA append produces one V1 manifest per spec (not fail-loud) — Java parity.
- **Mutations re-run (point 6):** grouping-revert ⇒ 3 manifest tests fail (zip_eq); validation-revert ⇒ 3
  unknown-spec tests fail (door message gone, deeper failure confirms defense-in-depth); cherrypick
  default-spec-stamp ⇒ conversion test fails (zip_eq) — pins per-spec, not just success. NEW reviewer
  mutation: `group_files_by_spec` file-LOSS (truncate to 1 group) ⇒ caught by the two-spec manifest tests
  (count + per-file presence). NOTE: `test_fast_append_multispec_cumulative_totals` does NOT catch file-loss
  — `added-data-files`/`total-data-files` come from `added_data_files` BEFORE grouping, so its docstring
  ("a dropped spec group would under-count") slightly overclaims; the manifest tests are the real loss guard.
- **Ordering (point 2) — FLAG for future interop (view NOT changed).** `snapshot_meta_view.rs` manifest sort
  tuple (L113) is `(content_rank, seq, min_seq, 6×counts)` and does NOT include `partition_spec_id`; the
  emitted manifest JSON also omits it. Two same-content/same-seq/same-counts manifests of DIFFERENT specs
  TIE on the whole tuple ⇒ array order falls back to manifest-LIST position (Rust spec-descending vs Java
  HashMap order). NO current interop fixture is multi-spec single-commit, so nothing is broken today; a
  FUTURE multi-spec interop fixture must either add spec id to the comparator tuple or assert the manifest
  SET (order-insensitively).
- **Pre-existing, untouched:** `validate_partition_value` has two near-duplicate messages (L843 "...not
  compatible WITH partition type" arity branch vs L859 "...not compatible partition type" per-field branch);
  both present in HEAD, the increment's test asserts the variant it triggers. Cosmetic; out of scope.
- Stale cherry_pick.rs banner comment (L1442 "fail-loud divergence") corrected to the converted contract.
  GAP_MATRIX/todo test count 6→8. Gate clean: typos, fmt, clippy -D warnings (workspace ex-sqllogictest),
  `cargo test -p iceberg --lib` 1806 ×2, `iceberg-datafusion` lib 80 + integration 9. Tree clean, no commit.

## IN PROGRESS (2026-06-11): Identity-partition constants-map ACTIVATION (BUILDER, Group A, increment 2, wt-closeout)

Goal: re-thread `task.partition_spec` and ACTIVATE the arrow reader's identity-partition constant
materialization (Java `PartitionUtil.constantsMap`), fixing the two transformer bugs that caused the
2026-06-08 revert. The decisive gate is `cargo test -p iceberg-datafusion` (lib + integration).

- [x] **Bug (a) — REE leak.** Constant identity-partition columns were materialized as `RunEndEncoded`
  (via `datum_to_arrow_type_with_ree`), so the output batch schema declared REE where the projected scan
  schema says plain `Utf8`/`Int64` ("expected Utf8 but found RunEndEncoded", `test_insert_into_partitioned`).
  FIX: materialize identity-partition constants as PLAIN arrays whose Arrow type equals the field's declared
  scan-schema type — the output batch schema now equals the declared scan schema EXACTLY. (Java's
  `constantsMap` is type-agnostic about Arrow encoding; REE was a Rust-only storage optimization that broke
  the schema contract. _file metadata + initial_default still use the existing REE path — unchanged.)
- [x] **Bug (b) — int->long widening.** A partition literal stored as `Int(i32)` could not materialize into
  an `Int64`/`Long` column ("Unsupported constant type combination: Int64 with Some(Int(19))",
  `test_evolved_schema`). FIX: derive the constant's value from the FIELD's iceberg type via
  `Datum::to(&field.field_type)` — the canonical Iceberg coercion (mirrors Java
  `IdentityPartitionConverters.convertConstant(partitionType.field(pos).type(), value)`): it widens
  `Int->Long`, `Int->Date`, `Long->Timestamp/Timestamptz`, passes through equal types, audited matrix.
- [x] **Threading.** `create_manifest_file_context` already resolves the manifest's spec (Arc) for the
  residual; thread that Arc onto `ManifestFileContext` -> `ManifestEntryContext` -> `FileScanTask.partition_spec`
  (once per manifest). Reader activation site (reader.rs:451) already consumes it — was dormant only because
  the field was `None`.
- [x] **Multi-spec interaction (sits on increment 1).** Each task's spec comes from ITS manifest's
  `partition_spec_id`; a multi-spec scan materializes each file's constants under its OWN spec. Tested.
- [x] **Tests:** transformer unit pins for both bug classes (REE-leak schema-equality + int->long); a
  metadata-vs-file-value scan test (file value DIFFERS from partition value -> scan returns PARTITION value);
  multi-spec scan test; bucket/truncate negative control (NOT materialized); null-partition-value case.
- [x] **Mutations:** disable activation -> metadata-vs-file-value test fails (reads file value); break the
  widening coercion -> int->long pin fails.
- [x] **Gate:** `cargo test -p iceberg-datafusion` (lib + integration incl. `test_insert_into_partitioned`,
  `test_evolved_schema`) run EARLY and often.
- [x] **Docs:** GAP_MATRIX residual/constants-map row, scan/map.md, lessons, this file.

## DONE (2026-06-11): Multi-spec closeout 3 — `removeRows` apply-side + dv_seq validation (BUILDER, Group A, increment 3, wt-closeout)

Goal: land the two residue items left by the merge-on-read arc — `RowDelta::removeRows` apply-side data
removal (was validation-only) and the `dv_seq >= data_seq` validation (was deferred for the infallible
index signature).

- [x] **Item 1 — `removeRows` apply-side (row_delta.rs):** `RowDeltaOperation` gained
  `removed_data_file_paths`; `delete_files()` resolves them via the shared `SnapshotProducer::resolve_delete_paths`
  EXACTLY as `OverwriteFilesOperation::delete_files` does, so the producer's existing `commit()` routing
  (`removed_data_files` → `process_deletes` rewrite + summary `remove_file`) drops the file from the scan in
  the SAME row-delta snapshot. NO snapshot.rs change needed — the producer machinery already routes
  `delete_files()` through the rewrite + summary; only the operation's seam was empty. `operation()`
  CONFIRMED unaffected: the 1.10.0 two-branch `addsDeleteFiles && !addsDataFiles ⇒ Delete; else Overwrite`
  consults neither `deletesDataFiles()` nor the removal set — a remove+add-delete and a remove-only row
  delta are both Overwrite. The removed∩referenced rejection fires FIRST (in `validate()`, which `do_commit`
  runs for ALL actions before any `commit()`).
- [x] **Item 1 docs flipped:** EVERY "validation-only / deferred" surface in row_delta.rs (module doc,
  "Out of scope", the `removed_data_files` field doc, the `remove_data_files`/`remove_rows` method docs, the
  `removed_delete_files` contrast, the `remove_deletes` contrast, 2 test comments). NO rename — Java's
  `removeRows` is already mirrored by `remove_rows`; `remove_data_files` is the bulk primitive (kept).
- [x] **Item 1 tests (5):** drops-from-scan e2e (remove A + add delete for B ⇒ scan {B}, A tombstoned,
  DELETE manifest present); remove-only ⇒ Overwrite + drops A; missing-path fail-loud + no partial add;
  ordering pin (removed∩referenced rejects before apply-side removal, table untouched); summary counters
  (deleted-data-files/deleted-records appear, cumulative total-data-files/total-records pin). MUTATION:
  sever `delete_files` → `Ok(vec![])` ⇒ 4 tests fail (scan shows A, remove-only empty-commit, missing-path
  silent), the ordering test correctly STILL passes (validate-time rejection independent of routing).
- [x] **Item 2 — dv_seq validation (delete_file_index.rs):** made the index FALLIBLE (`get_deletes_for_data_file`
  → `Result<Vec<…>>`). PLACEMENT JUSTIFIED: the index is the ONLY place both sequence numbers are in hand
  (`seq_num` = the data file's, the DV's via its manifest entry); the caching-loader door (the duplicate-DV
  door's home) receives NEITHER — `FileScanTaskDeleteFile` drops the sequence number in its
  `From<&DeleteFileContext>` conversion, so candidate (a) would need to thread two new seqs through a public
  serialized struct + the loader. Ripple of (b) was SMALL: one production caller (`scan/context.rs:144`,
  already `Result`-returning, just added `?`). The check fires `dv_seq < data_seq` ⇒ the EXACT 1.10.0 message
  (bytecode-verified against `iceberg-core-1.10.0.jar`): "DV data sequence number (%s) must be greater than
  or equal to data file sequence number (%s)".
- [x] **Item 2 tests:** invalid-table (hand-built DV at seq 5 vs data file seq 9 ⇒ loud DataInvalid naming
  both seqs); the prior `test_dv_is_not_sequence_filtered` SPLIT — the valid boundary half kept
  (`dv_seq==data_seq` / `dv_seq>data_seq` apply the DV) + the invalid half is the new test. MUTATION: disable
  the check (`&& false`) ⇒ the invalid-table test sees silent `Ok(vec![dv])` instead of the error.
- [x] **Docs:** GAP_MATRIX (RowDelta row residue flip + Read row dv_seq residue flip), transaction/map.md,
  this file, lessons.

**Outcome:** both residue items landed. Verification: typos clean (reworded a parenthesized prefix to dodge a
false positive), fmt clean, clippy `-D warnings` clean (workspace ex-sqllogictest), `cargo test -p iceberg --lib`
**1818 passed ×2** (baseline 1812 + 5 row_delta + 1 net delete_file_index split), `iceberg-datafusion` lib
80 + 9 green. PRE-EXISTING unrelated failure flagged: the `iceberg-datafusion` DOCTEST
(`table_provider_factory.rs:41`, `#[tokio::main]` multi_thread w/o `rt-multi-thread`) — not touched (no
datafusion files changed). Files changed: row_delta.rs, delete_file_index.rs, scan/context.rs (the `?` —
flagged as the item-2 placement consequence), transaction/map.md, GAP_MATRIX.md, todo.md, lessons.md.
Deferred (flagged): multi-spec delete commits + full conflict-validation interop (RowDelta stays 🟡); the
manifest-comparator multi-spec tie (from increment 1). No commit.

**REVIEWER PASS (Group A, increment 3, 2026-06-11, wt-closeout). Verdict: APPROVE with one added doc + one added pin.**
- **Point 1 (fallibility ripple) — VERIFIED.** Grepped every caller: the async `DeleteFileIndex::get_deletes_for_data_file`
  threads the inner `Result` at BOTH populated call sites (L115/L125); the sole production caller `scan/context.rs:146`
  adds `?`. Traced the error END-TO-END: `into_file_scan_task`(`?`) → `process_data_manifest_entry`(`?`, mod.rs:706)
  → `try_for_each_concurrent` short-circuits → `Err` sent into `file_scan_task_rx` (mod.rs:610) ⇒ the scan stream
  yields the `DataInvalid` as a LOUD item, NOT swallowed into an empty delete set nor a dropped task. Message + arg
  order BYTECODE-verified (`javap -c DeleteFileIndex.findDV`, 1.10.0): slot-0 `%s` = `dv.dataSequenceNumber()` (DV
  FIRST), slot-1 = `seq` (data file); comparison `lcmp; iflt` ⇒ check is `dv_seq >= seq` (boundary `==` VALID) —
  Rust's `dv_seq < data_seq ⇒ Err` is the exact complement.
- **Point 2 (ordering) — VERIFIED, wording accurate.** Read `transaction/mod.rs::do_commit`: structure is
  ALL-validates-then-ALL-commits (loop 1 L374-378 runs every action's `validate`; loop 2 L380-389 runs every
  `commit`). The doc/test/lessons wording ("`do_commit` runs `validate()` for ALL actions before any `commit()`")
  matches this exactly — no overstatement. The ordering test correctly STILL PASSES under mutation A (validate-time
  gate, apply-path-independent). Re-ran: green.
- **Point 3a (failMissingDeletePaths posture) — REAL DOC GAP, FIXED.** Bytecode (`javap -c BaseRowDelta`):
  `removeRows` = `removedDataFiles.add(file)` + `delete(file)` (no LIVE check beyond delete, matches builder); the
  ctor does NOT set `failMissingDeletePaths`, and the only `failMissingDeletePaths()` call sits in `validate()` behind
  `if (validateDeletes)` (gates the UNRELATED `validateDataFilesExist` walk). `StreamingDelete`(1)/`BaseOverwriteFiles`(2)
  DO call it. ⇒ Rust's `resolve_delete_paths` unconditional fail-loud is Java-faithful for DeleteFiles/OverwriteFiles
  but STRICTER than Java's `RowDelta` default for the NEW `removeRows` caller — and the docs cited `failMissingDeletePaths`
  as if it were parity. ADDED the divergence note (module-doc apply-side block in row_delta.rs + the shared
  `resolve_delete_paths` doc in snapshot.rs, mirroring the Arc-E `removeDeletes` posture note on `resolve_delete_file_paths`).
- **Point 3b (replace-in-place) — PROBED, sensible, PINNED.** Added `test_row_delta_remove_and_add_same_path_replaces_in_place`:
  remove X + add a fresh file at the SAME path X ⇒ old entry tombstoned (Deleted), new Added, X stays live, summary
  counts both (deleted=1, added=1, cumulative total=1). Matches Java's `removeRows(X)`-tombstones + `addRows(X)`-adds.
  No silent weirdness. Kept as a permanent pin (+1 test; matrix count 5→6).
- **Point 4 (mutations) — all 3 confirmed.** (A) sever `RowDeltaOperation::delete_files`→`Ok(vec![])` ⇒ exactly 4
  removeRows tests fail (drops-from-scan, remove-only, missing-path, summary), ordering test correctly STILL passes.
  (B) disable dv_seq check (`&& false`) ⇒ `test_dv_lower_seq_than_data_file_is_invalid_table` fails (silent Ok),
  valid-boundary still passes. (C, mine) change `>=`→`>` (`<`→`<=`) ⇒ `test_dv_is_not_sequence_filtered_at_valid_boundary`
  fails at `dv_seq==data_seq` (boundary pinned from BOTH sides), invalid test still passes. All restored from /tmp/wtA3_rev_*.bak.
- **Gate:** typos clean, fmt clean, clippy `-D warnings` clean (workspace ex-sqllogictest), `cargo test -p iceberg --lib`
  **1819 ×2** (1818 + my replace-in-place pin), `iceberg-datafusion` lib 80 + integration 9 green; the
  `table_provider_factory.rs:41` DOCTEST failure CONFIRMED pre-existing + unrelated (no datafusion files changed).
  Pipe audit CLEAN. Files added to the changed set by the reviewer: `transaction/snapshot.rs` (the `resolve_delete_paths`
  posture note). Tree clean, no commit.
## ACTIVE (2026-06-11): ExpireSnapshots Increment B1 — METADATA retention semantics (worktree wt-expire, BUILDER Fable, Group B)

Java `RemoveSnapshots` retention computation, metadata-only (`cleanExpiredFiles(false)` semantics).
**B1 is METADATA-ONLY: no file deletion of any kind — file cleanup is Increment B2, not this one.**

- [x] `transaction/expire_snapshots.rs` (new): `ExpireSnapshotsAction` — `expire_older_than(ts)`,
      `retain_last(n)` (deferred-to-commit validation, Java's exact message), `expire_snapshot_id(id)`
      repeatable; commit-time retention computation against the refreshed table (1.10.0
      `internalApply`, bytecode-verified): retained refs (main never ref-expired; `now − ts <=
      maxRefAgeMs` retains; missing-snapshot refs dropped) → explicit-id-vs-retained-ref precondition
      ("Cannot expire %s. Still referenced by refs: %s") → per-branch contiguous-prefix retention
      (`kept < minToKeep || ts >= cutoff`, EARLY STOP) → unreferenced retention (`ts >=
      defaultExpireOlderThan`) → emit `RemoveSnapshotRef` (expired refs, sorted) +
      `RemoveSnapshots` (sorted ids) + `RefSnapshotIdMatch` guards for every ref consulted.
      GC gate (`gc.enabled`, Java ctor message verbatim) at commit. No-op emits nothing.
- [x] `spec/table_properties.rs`: `history.expire.max-snapshot-age-ms` (default 432000000),
      `history.expire.min-snapshots-to-keep` (default 1), `history.expire.max-ref-age-ms`
      (default i64::MAX) — all three bytecode-verified vs 1.10.0; plus `gc.enabled` (default true,
      needed by the GC gate — flagged as the 4th const).
- [x] `spec/table_metadata_builder.rs` — complete `remove_snapshots` vs Java 1.10.0
      `rewriteSnapshotsInternal`: prune statistics + partition statistics per removed id (with
      changes), and remove dangling refs via `remove_ref` semantics (pushes `RemoveSnapshotRef`,
      resets `current_snapshot_id` for main) instead of the silent `refs.retain`. (catalog/mod.rs
      apply routing already correct — no edit needed there.)
- [x] Wire `Transaction::expire_snapshots()` in `transaction/mod.rs` + `transaction/map.md` row.
- [x] Tests both directions of every boundary (age boundary ON the cutoff, retain_last n/1/n>len,
      per-ref overrides both ways, tag expiry removed/kept, main never expired, explicit-id error +
      seed-set semantics, ancestors survive, unreferenced boundary, no-op, idempotent re-run,
      snapshot-log prune, stats prune, dangling-main, GC gate, ref guards + stale-guard conflict,
      end-to-end memory-catalog commit). Then the 4 mutations (age-boundary flip, min-floor drop,
      ancestor-retention drop, tag-expiry invert) + restore + full suite ×2.
- [x] Docs: GAP_MATRIX `ExpireSnapshots` ❌→🟡 (metadata semantics landed; file cleanup = B2;
      interop deferred), map.md row, lessons.

Deferred loudly: B2 file cleanup (`cleanExpiredFiles`, delete callbacks, incremental-cleanup
strategy selection), `cleanExpiredMetadata` (spec/schema GC — needs manifest IO), interop.

Outcome (2026-06-11): landed in one increment. 32 action tests + 2 builder tests (1832 lib total,
×2 green). 5 builder mutations all caught (branch-walk `>=`→`>`; unreferenced `>=`→`>`; min-floor
drop; ancestor-retention drop; ref-age-expiry invert + the M5 builder revert proving the apply-side
tests fail pre-fix). `catalog/mod.rs` needed NO edit (apply routing was already correct — the
completeness gap was in the builder). One extra const beyond the planned three: `gc.enabled`
(needed by the GC gate; same file, flagged). `tracing` is NOT an iceberg-crate dep, so the
invalid-ref drop is silent with a comment (Java WARN-logs) — no Cargo edit made.

Review (2026-06-11, REVIEWER Fable): re-derived `internalApply` / `computeRetainedRefs` /
`computeBranchSnapshotsToRetain` / `unreferencedSnapshotsToRetain` / `rewriteSnapshotsInternal` /
`removeRef` / `removeStatistics` from 1.10.0 bytecode — semantics, messages, defaults, and change
recording all confirmed; M5 revert re-run (exactly the 2 builder tests catch it);
requirements-drop mutation caught by the stale-guard + requirements-shape tests. THREE survivor
mutations found and pinned (+5 tests, 1 extension): the branch-walk EARLY STOP under clock-skewed
timestamps (a newer-than-cutoff ancestor behind the stop point IS expired — previously removable
with the whole suite green), the ref-age `<=` boundary (age == maxRefAgeMs retained), and the
refs-first update emission order (Java's change order; the apply-side dangling sweep self-heals,
so only a shape pin catches a reorder). Also pinned: explicit id == main's head errors with
`[main]`; a mid-ancestry explicit-id hole clears the snapshot log at the hole on apply; a branch's
own `max_snapshot_age_ms` beats even the EXPLICIT `expire_older_than` (Java: `expireOlderThan`
only overwrites the default). Known acceptable gap (Java's REST posture shares it): a ref CREATED
concurrently at a to-be-expired snapshot is not guardable via `RefSnapshotIdMatch`; the apply-side
sweep then drops it — full-CAS catalogs (Java's primary path) reject it; revisit at B2.

## ACTIVE (2026-06-11): ExpireSnapshots Increment B2 — FILE CLEANUP (worktree wt-expire, BUILDER Fable, Group B)

Port Java 1.10.0 `ReachableFileCleanup` (the general-correct strategy) as a post-commit cleanup
seam. **THE most dangerous increment: it deletes files. Every choice biases under-deletion; every
test pins the deletion set BOTH directions.**

- [x] `transaction/expire_cleanup.rs` (new): `ExpireSnapshotsCleanup` (FileIO + injectable
      delete fn) with `clean_expired_files(before, after) -> CleanupReport` (the two-state core,
      1.10.0 `ReachableFileCleanup.cleanFiles` bytecode-rederived) and
      `commit_and_clean(tx, catalog)` (the commit-THEN-clean wrapper; deletion structurally
      unreachable on a failed commit — Java `RemoveSnapshots.commit()` ordering). GC gate
      re-honored at the cleanup door (Java's is in the ctor, which also covers cleanup).
- [x] Set algebra (bytecode-cited): expired = before.snapshots − after.snapshots (by id);
      manifest-lists of expired snapshots (RUST SAFETY DIVERGENCE: minus retained lists — Java
      deletes unconditionally, unreachable case for Java-written tables); candidate manifests =
      ∪ expired lists' entries; retained = ∪ after-snapshots' lists' entries (path equality —
      `GenericManifestFile.equals` is manifestPath-only); manifests-to-delete = candidates −
      retained; content files = ∪ LIVE entries (status != DELETED — `isLiveEntry`) of
      manifests-to-delete, minus ∪ LIVE entries of retained manifests (BOTH data + delete
      manifests — the 1.10.0 cleanup projection omits `content` and the avro ctor defaults DATA,
      so Java walks both identically; DV puffin path dedup via path-set semantics); stats =
      before-locations − after-locations.
- [x] Failure posture (divergence from Java's log-and-continue, no-logging-dep constraint):
      manifest-LIST read errors → hard Err BEFORE any deletion (Java throwFailureWhenFinished);
      candidate-manifest read error → collect + skip its files; retained-manifest read error →
      collect + CLEAR the whole content-file set (Java catch-Throwable→empty, fail-safe);
      per-file delete errors → collect + continue. `CleanupReport {deleted_* per funnel,
      failures: Vec<CleanupFailure {path, kind, error}>}`.
- [x] Tests (15, each class both directions): grafted shared manifest-list survives (the pinned
      Rust divergence); carried-forward shared manifest SURVIVES + expired list dies (the #1
      pin); rewritten-but-live data file survives (rewrite_manifests); expired-only data file
      dies + retained tombstone does NOT protect (delete_files chain); shared puffin survives
      via cross-manifest carried-EXISTING DV + replaced puffin dies (NOTE: the planned
      two-DVs-one-puffin-remove-one shape is unbuildable — delete-file removal is BY PATH in
      Java too, 1.10.0 `ManifestFilterManager.delete` adds `file.location()` to `deletePaths`,
      so removing one DV tombstones every same-path entry; fixture reshaped, finding recorded
      in lessons); expired-only DV puffin dies; stats file dies / retained stats survives;
      failed-commit ⇒ zero deletes (MockCatalog + recorder); injected failing delete → failure
      listed, sweep continues; dry-run by injection (storage untouched); unreadable retained
      manifest → ALL content files spared; unreadable candidate manifest → its files skipped,
      manifest still dies; unreadable manifest list → Err before any deletion; empty-expiry
      no-op; GC gate refused with Java's message.
- [x] Mutations (`wtB2_*`): M1 drop the `!` in the manifest subtraction → 9 tests fail,
      headlined by the carried-forward pin ("the SHARED manifest must survive: [...m0.avro]" —
      the data-loss class); M2 `if false` the retained-files subtraction in (c) → 3 fail
      (rewritten-but-live "the still-live data file must NOT die", shared-puffin,
      unreadable-retained); M3 cleanup-not-gated-on-commit-success (fabricated post-state on
      Err) → failed-commit pin fails ("the failed commit must propagate" + recorder
      non-empty). Snapshot-copied before each, restored surgically, full suite green after.
- [x] Docs: GAP_MATRIX ExpireSnapshots row (B2 landed; Incremental deferred with the
      optimization-with-stricter-eligibility rationale; interop deferred), transaction/map.md
      `expire_cleanup.rs` row, expire_snapshots.rs + mod.rs module-doc pointers, lessons.
- [x] Gate: typos clean; fmt clean; clippy workspace -D warnings (excl. sqllogictest) clean;
      `cargo test -p iceberg --lib` ×2 — 1847 passed (baseline 1832 + 15 new).

Outcome (2026-06-11): B2 landed in one increment — `ReachableFileCleanup` semantics ported as
the explicit post-commit `ExpireSnapshotsCleanup` seam (Java's `cleanExpiredFiles(true)` default
deliberately NOT mirrored: deletion is opt-in via `commit_and_clean`/`clean_expired_files`,
documented in-module + GAP_MATRIX). Java-side findings that reshaped the port, all
bytecode-derived: (1) 1.10.0's cleanup walks DELETE manifests through the DATA reader because
`MANIFEST_PROJECTION` omits `content` and the avro ctor defaults it to DATA — so delete files /
DV puffins ARE cleaned, despite `readPaths`' delete-manifest precondition reading as if they
could not be; (2) `GenericManifestFile` equality is path-only; (3) `findFilesToDelete` returns
the EMPTY set on any retained-side enumeration failure (catch-Throwable) — ported as
clear-and-report; (4) delete-file removal is by-path (shared-puffin fixture reshaped). The B1
concurrently-created-ref gap is unchanged by B2: cleanup computes reachability from the
COMMITTED post-state (Java refreshes; we use the returned table — equivalence argued in-module),
so the window is inherited from the metadata commit, not widened. Files touched:
`transaction/expire_cleanup.rs` (new), `transaction/expire_snapshots.rs` (doc pointer +
`parse_property` → `pub(super)`), `transaction/mod.rs` (mod + re-export + doc), map.md,
GAP_MATRIX, todo, lessons.

Review (2026-06-11, REVIEWER Fable): re-derived `cleanFiles` / `readManifests` /
`pruneReferencedManifests` / `findFilesToDelete` (incl. the lambda exception tables) /
`FileCleanupStrategy.{MANIFEST_PROJECTION,deleteFiles,statsFileLocations}` /
`RemoveSnapshots.{commit,cleanExpiredSnapshots}` / `GenericManifestFile.{equals,avro-ctor}` /
`ManifestReader.{liveEntries,isLiveEntry}` / `ManifestFiles.readPaths` from the 1.10.0
bytecode — set algebra, three failure tiers (Err / skip / clear-all scopes), sweep order, gate,
and the MANIFEST_PROJECTION finding all confirmed exactly. Timing verdict: no new concurrency
window beyond B1's recorded ref gap (post-commit append files can never be candidates;
double-expire races are double-delete-or-planning-abort, never over-deletion). M1/M2 re-run
(9 and 3 tests respectively, matching the build record). TWO survivor mutations found and
pinned (+2 tests, 1 extension; 1849 total ×2 green): the cross-funnel SWEEP ORDER
(lists-before-content survived everything — pinned structurally via the recorder's invocation
sequence; the order is the crash-RESUME property: leaves before indexes keeps the expired
lists plannable until last) and the GC gate SIDE (`after` instead of `before` survived —
pinned with before=disabled/after=enabled must refuse, Java's ctor reads `base`). Also added
the re-run pin (second sweep of the same (before, after) aborts at planning with zero delete
calls — Java's `readManifests` throws identically). Doc corrections: the "staler `before` only
shrinks" claim was unsound (a concurrent expire GROWS the set — still safe, argued via
unreachability-from-`after`; module doc + lessons fixed), `BaseSnapshot.equals` citation
corrected (5 fields, id-diff equivalent by immutability), the inherited B1 window now stated
in the module docs, and the Rust-stricter retained-list read scope noted (Java's prune
early-exits; Rust always reads both sides — more pre-deletion `Err` cases only).

## ACTIVE (2026-06-11): A3 — ExpireSnapshots Java interop (worktree wt-orphan, BUILDER Opus, Group A)

Close the named interop deferral on the `Maintenance: ExpireSnapshots` GAP_MATRIX row: prove the
Rust `ExpireSnapshotsAction` (B1 retention) + `ExpireSnapshotsCleanup` (B2 ReachableFileCleanup file
GC) agree with Java 1.10.0 `RemoveSnapshots` + `cleanExpiredFiles(true)` on the SAME fixtures, judged
by Java where possible. Modify ONLY: dev/java-interop/** (oracle + new script + map.md),
crates/iceberg/tests/interop_expire.rs (new), GAP_MATRIX row, task/todo.md, task/lessons.md. NO
transaction/ production edits — a real Rust bug = STOP + report. No commit.

**Java strategy-selection (bytecode-verified `RemoveSnapshots.cleanExpiredSnapshots`, 1.10.0):** when
`incrementalCleanup==null` Java picks INCREMENTAL iff `!specifiedSnapshotId && !hasRemovedNonMainAncestors
&& !hasNonMainSnapshots(current)`, else REACHABLE. Rust ports ReachableFileCleanup ONLY, so EVERY fixture
must FORCE Java to Reachable: keep a surviving TAG (⇒ `hasNonMainSnapshots(current)` true). This doubles
as a judgment-surface requirement (a tag protecting an otherwise-expirable snapshot). `ReachableFileCleanup
.cleanFiles` is the 2-arg `(base, current)` core = the Rust 2-state `clean_expired_files`.

**Outcome (2026-06-11): A3 LANDED. Full chain GREEN both directions, 5 fixtures.** Files:
`dev/java-interop/src/.../InteropOracle.java` (new `ExpireOracle` + 2 dispatch cases +
`LocalTableOperations.continueVersioningFrom` + the `SnapshotMetaOracle.emit` null-parent fix),
`dev/java-interop/run-interop-expire.sh` (new), `dev/java-interop/map.md` (row + 3 Debug entries),
`crates/iceberg/tests/interop_expire.rs` (new, env-gated, local `expire_meta_view` with the
expired-parent fix), `GAP_MATRIX.md` (row, 5-pipe audit clean), todo, lessons. NO transaction/
production edits. Gate CLEAN: typos/fmt/clippy `-D warnings` (workspace ex-sqllogictest) clean,
`cargo test -p iceberg --lib` 1911 ×2 (no lib code added), interop_expire offline no-op (3 pass).
Sabotage BOTH fail-closed: (a) resurrect a snapshot id in rust final.metadata.json ⇒ Java byte-diff
fails (2 snapshots vs 1); (b) drop a token from rust_deleted.json ⇒ Java verify `1 failures` + Rust
descriptor test FAILED. **STOP-FINDING reported (separate increment):** Rust
`FastAppend::existing_manifest` (append.rs:148) drops all-tombstone manifests where Java
`FastAppend.apply` carries ALL prior manifests forward (`snapshot.allManifests()`, no filter) —
surfaced by an emptying-delete `rewrite` fixture, then fixtured away (delete leaves a file existing).
Deferred on the row: IncrementalFileCleanup, cleanExpiredMetadata, ref-age (max_ref_age_ms) interop.

- [x] **Oracle (`ExpireOracle` in InteropOracle.java + dispatch cases):**
  - `generate-interop-expire` (`-Dinterop.expire.dir`): build a REAL on-disk multi-snapshot table via
    `LocalTableOperations`+`newFastAppend` (metadata-only `DataFiles` — paths need not exist, the cleanup
    deletes by path via a collector), parameterized timestamps for a deterministic cut; run Java
    `table.expireSnapshots().expireOlderThan(cut).retainLast(n).cleanExpiredFiles(true).deleteWith(collector)
    .commit()`; emit `<dir>/<fixture>/java_deleted.json` (SORTED deleted-file list) + leave the expired
    table at `<dir>/<fixture>/table/metadata/final.metadata.json` (re-located by the LocalTableOps commit).
    Reuse `emit-snapshot-meta` (SnapshotMetaOracle, re-parsed base) for the canonical view.
  - `verify-interop-expire` (`-Dinterop.expire.dir`): Java re-reads the RUST-expired table at
    `<dir>/<fixture>/rust_table/metadata/final.metadata.json` (production read), emits its canonical view
    of it; the script byte-diffs Java-on-Rust vs Java-on-Java. Sentinel `verify-interop-expire: 0 failures`.
- [x] **Fixtures (each forces Reachable via a surviving tag):**
  - `linear`: linear main history s1..s4, a tag at the head; expire prunes a contiguous prefix (retainLast +
    expireOlderThan cut between s2/s3). Pins prefix retention + manifest-list/manifest/content file GC.
  - `tag_protected`: linear history + a tag on s2 (an otherwise-expirable mid-chain snapshot); the tag KEEPS
    s2 and its files (and forces Reachable). Pins ref-protection of the cleanup set.
  - `stats`: a `linear`-shape table with a statistics file (+ partition-statistics if cheap via
    `updateStatistics`) attached to an EXPIRED snapshot; pins stats-file cleanup. Tag at head.
  - `deletes`: a V2 table with a position-delete-bearing snapshot that expires, so a DELETE manifest walks
    through cleanup. Tag at head. (DV/V3 deferred — the cleanup walks delete manifests identically.)
  - ALL fixtures fixtured AWAY from shared manifest LISTS (each Java snapshot owns a unique list, so the
    Rust retained-shared list guard never fires ⇒ byte-equality). Ref aging (`max_ref_age_ms`) NOT exercised
    (Rust implements it but keep the cut age-only to isolate; flag if Java diverges).
- [x] **Direction 1 (Rust acts, Java judges):** the Rust GEN test commits the SAME chain on a copy, runs
  `ExpireSnapshotsCleanup::commit_and_clean` with a COLLECTING deleter (no physical delete — collect-only so
  Java can still read), lands `rust_table/.../final.metadata.json` + `rust_deleted.json`. Script: Java emits
  its view of the Rust table, byte-diff vs Java-on-Java; assert the two deleted sets equal as sorted sets.
- [x] **Direction 2 (Java acts, Rust verifies):** `interop_expire.rs` parses the Java-expired table, asserts
  its surviving snapshot-id/refs set matches; AND Rust's own cleanup-candidate set (`clean_expired_files` on
  the SAME pre-expire fixture, collect-only) equals Java's `java_deleted.json`.
- [x] **`run-interop-expire.sh`:** set -euo pipefail; per-fixture chain mirroring run-interop-write-actions.sh
  (reset → Java gen+deleted+meta → Rust gen+deleted → Java view-of-Rust + byte-diff + set-diff → Rust assert).
  Fail-closed: missing artifact fails the chain; mvn verdict from OUTPUT sentinel (`|| true` + grep), not exit
  code (the run-interop-dv.sh rule).
- [x] **interop_expire.rs:** env-gated (`ICEBERG_INTEROP_EXPIRE_*`), clean no-op when unset, hard-asserts when
  the script sets them. Mirrors interop_write_actions_meta.rs structure.
- [x] **GAP_MATRIX:** ExpireSnapshots row cell — interop deferral RESOLVED for retention + ReachableFileCleanup
  (both directions, the covered surface); stays 🟡 (IncrementalFileCleanup + cleanExpiredMetadata remain).
  5-pipe audit.
- [x] **Verify:** full chain GREEN (paste tail); Rust gate typos+fmt+clippy+`cargo test -p iceberg --lib` ×2
  (baseline 1911); 2 SABOTAGE checks (resurrect a snapshot id in rust JSON ⇒ byte-diff fails; drop a file from
  rust_deleted ⇒ set-diff fails); restore green.

- [x] **A3 REVIEWER (2026-06-11, wt-orphan, Opus, adversarial): VERDICT — PASS with TWO harness fixes
      (cross-strategy + descriptor injectivity) + one fixture-surface correction.** Read the chain, the
      oracle, both view builders, and re-derived `RemoveSnapshots.cleanExpiredSnapshots` /
      `hasNonMainSnapshots` / `hasRemovedNonMainAncestors` / `mainAncestors` / `FastAppend.apply` from
      1.10.0 bytecode. Ran all 3 required sentinel injections + 3 sabotages (D1 byte-diff, D2 descriptor,
      D2 summary) + the tag_protected flip + a strategy reflection probe.
  - **VACUITY #1 (headline #3) FOUND + FIXED — the central claim was FALSE.** A reflection probe on
        Java's `incrementalCleanup` field proved 4 of 5 fixtures (linear/stats/deletes/rewrite) ran Java's
        **IncrementalFileCleanup**, NOT ReachableFileCleanup (the strategy the Rust side ports): a tag on
        the HEAD leaves every survivor ON the post-expiry main ancestry ⇒ `hasNonMainSnapshots(current)`
        is false ⇒ Java auto-selects Incremental. Only `tag_protected` (mid-chain tag) auto-selected
        Reachable. The comparison passed only because the two strategies coincide on these shapes
        (measured by force-probe) — a coincidence of shape, NOT 1:1 evidence of the Reachable port. FIX:
        `((RemoveSnapshots) expireApi).withIncrementalCleanup(false)` (a real engine selector, package-
        private, bytecode-offset-verified) forces Java's Reachable path for EVERY fixture against the
        identical deleted sets ⇒ genuine Reachable-vs-Reachable. Surviving tag retained as the
        ref-protection surface.
  - **VACUITY #2 (headline #1) FOUND + FIXED — the descriptor was NON-INJECTIVE.** `<funnel>@ord<N>`
        keyed only on the owning-snapshot ordinal, so two DIFFERENT content files added by the SAME commit
        (a 2-file append) collapse to ONE token ⇒ a Rust-deletes-X / Java-deletes-Y swap passes set-equality
        vacuously. FIX: cross-language-stable per-file discriminators on BOTH sides (`#rc<record_count>` for
        content, `#a<>e<>d<>` file counts for manifests, `#sz<file_size>` for statistics; manifest lists
        need none — one per snapshot). Offline fail-before/pass-after pin
        `test_descriptor_token_is_injective_over_sibling_files_of_one_snapshot` (fails under the ordinal-only
        scheme, passes after). Chain still green; `rewrite` now shows `content@ord0#rc10`/`manifest@ord0#a2e0d0`.
  - **FIXTURE-SURFACE CORRECTION (headline #7): `deletes` overclaimed.** The delete manifest is carried
        forward into the head ⇒ `manifestsToDelete` is empty ⇒ NEITHER side's cleanup ever READS the delete
        manifest body (only manifest LISTS die). Corrected the oracle comment to scope the fixture to
        metadata+list GC on a delete-bearing table; delete-manifest CONTENT cleanup is covered by the B2
        unit tests, not here. `stats`/`tag_protected`/`rewrite`/`linear` surfaces all CONFIRMED genuine
        (stats puffins on disk + in deleted set; tag flip from 2→3 deletions; rewrite fires content+manifest
        funnels).
  - **CONFIRMED (no change needed): headline #2** all 3 sentinel injections fail-closed (verify-throw ⇒
        sentinel absent ⇒ fail; missing artifact ⇒ verify FAIL / diff exit 2 / emit mvn-exit-1 no `||true`;
        double-run ⇒ `rm -rf TMP` wipes leftovers); **#4** view-copy drift is minimal (ONLY parent_ordinal
        differs — mechanical diff), and the Java null-parent fix does NOT regress (run-interop-write-actions
        + run-interop-dv both GREEN); **#5** FastAppend STOP-finding is REAL (1.10.0 bytecode `apply` offset
        89-94 carries `allManifests` unfiltered vs Rust append.rs:148 `has_added/existing` filter), accurately
        + findably recorded, correctly fixtured-away, NOT fixed (production read-only); **#6** D2 is a DEEP
        `assert_eq!` on the full canonical view (summary-corruption sabotage fails it), D1 byte-diff catches a
        Rust-metadata corruption.
  - Files touched (harness/test only): `dev/java-interop/src/.../InteropOracle.java` (withIncrementalCleanup,
        descriptor discriminators, deletes comment), `crates/iceberg/tests/interop_expire.rs` (discriminators
        + injectivity pin), task/todo + task/lessons. transaction/ + common/snapshot_meta_view.rs +
        Cargo/pom byte-untouched (`git diff --stat` empty). Gate CLEAN: typos/fmt/clippy `-D warnings` clean,
        `cargo test -p iceberg --lib` **1911 ×2**, full chain GREEN. No commit.

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
- Index: [todo-archive/map.md](todo-archive/map.md).
