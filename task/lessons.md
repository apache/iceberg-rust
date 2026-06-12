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

> **Compaction log.** Last pass: 2026-06-12 (pass 4 — post-Wave-5 merge union, 1,238 lines;
> 4 KEEP / 25 ARCHIVE / 2 promoted) →
> [lessons-archive/2026-06_wave5.md](lessons-archive/2026-06_wave5.md). Promoted: 1 →
> [docs/testing.md](../docs/testing.md), 1 → `dev/java-interop/map.md#debug`. Prior passes:
> 2026-06-12 (pass 3 — 14/47/3) →
> [lessons-archive/2026-06_wave3-wave4-overnight.md](lessons-archive/2026-06_wave3-wave4-overnight.md);
> 2026-06-11 (pass 2 — 17/25/6); 2026-06-09 (pass 1 — 31 promoted). Archives are not read by
> default — see [skills/compaction.md](../skills/compaction.md).

---

<!-- Newest entries at the bottom. Example shape:

### YYYY-MM-DD
- **DO** carry context on every fallible Rust call (`.with_context(...)` / `.expect("msg")`).
  *Why:* a bare `.unwrap()` panic gives the operator no cause from logs alone.
- **DO NOT** edit upstream crate files to land a fork feature when an additive module would do.
  *Why:* it makes the next upstream merge conflict-prone. Prefer additive changes.
-->



### 2026-06-11 (Wave-4 F2 — variant shredding overlay + VariantVisitor, BUILDER Fable, wt-vschema)
- **1.10.0 `ShreddedObject` DIVERGES from MAIN twice — both found in bytecode and confirmed by live
  probe (`/tmp/variant-probe/ShredProbe.java`), and both are BUGS the port must not mirror:** (1)
  `remove()` lacks MAIN's `this.serializationState = null`, so `sizeInBytes()` → `remove(x)` →
  `writeTo` serializes the STALE cached state with x still present; (2) the `SerializationState`
  ctor's non-`SerializedObject` branch compiles to `aload_3` (the PARAMETER map = the overlay's
  LIVE `shreddedFields`) for the materializing merge while `this.shreddedFields` keeps the
  pre-merge COPY that `writeTo` iterates — the FIRST serialization over a constructed backing
  writes count/dataSize for the merged set but only the copy's fields ⇒ CORRUPT bytes (probe:
  re-read throws IndexOutOfBoundsException); a later state rebuild self-heals because the live map
  was polluted. MAIN fixed both (the param was renamed). The Rust port is STATELESS (plan computed
  fresh per call) and MAIN-consistent; the constructed-backing oracle is Java's SELF-HEALED second
  serialization (puts → sizeInBytes → re-put to reset the cache → serialize). LESSON: a
  source-vs-bytecode diff can hide in PARAMETER NAMING — `javap` shows `aload_<n>` vs `getfield`,
  which no source read reveals; for any stateful Java class, probe the mutate-after-size sequence.
- **The verbatim-slice contract is what makes the overlay safe over third-party data:**
  `SerializedObject.sliceValue(index)` spans are exactly B1's sorted-distinct-offsets field
  ranges, so `parse_object` grew an optional range recorder (one parser, no duplication) and the
  overlay copies untouched fields' ORIGINAL bytes — only ids/offsets/header recompute (ids
  re-resolve BY NAME at write time even for verbatim fields, Java `metadata.id` + "Invalid
  metadata, missing: %s"). The designated mutation (verbatim → canonical re-encode) is killed
  ONLY by the non-canonical fixtures (long-form string that fits short form; oversized offset
  width) — canonical-input fixtures pass under the mutation, so a fixture set without a
  non-canonical backing would NOT pin the contract at all.
- **Java's remove-then-put contract is deliberately inconsistent — mirror BOTH sides:** `put`
  does not clear `removedFields`, so after `remove(x)` + `put(x, v)` the views (`get` → null,
  `numFields`/`fieldNames` exclude x — removedFields filters them) DISAGREE with serialization
  (which includes x=v: the SerializationState's shredded map still carries it). Probe-pinned
  (`probe_remove_then_put_view numFields=2 get_b=null` + bytes containing b=9). Fixing either
  side to be "consistent" diverges from Java.
- **Duplicate backing-field names: the rejection is REPLACEMENT-SENSITIVE, so door at
  serialization time, not construction.** Java's ImmutableMap throws only when the duplicate
  SURVIVES the replaced/removed filter; putting or removing the duplicated name skips both
  occurrences and serializes fine (fixture-pinned both ways). An eager constructor-time door
  would over-reject vs Java. Constructed-backing duplicates collapse SILENTLY instead (HashMap
  put semantics) — two different dup behaviors in one class.
- **`VariantVisitor` facts (1.10.0 == MAIN, bytecode-verified):** drivers are `visit(Variant,
  visitor)` + `visit(VariantValue, visitor)` — the brief's `visit(VariantMetadata, VariantValue,
  VariantVisitor)` signature DOES NOT EXIST in 1.10.0 (brief ≠ spec, again); object traversal
  iterates `fieldNames()` (stored order) but recurses into `object.get(name)` — the NAME LOOKUP —
  so a non-name-sorted object NPEs in Java (Rust: named Err, after-hook still fired); after-hooks
  run in `finally` (pinned by asserting the hook fired on the error path); all defaults return
  null (Rust `Option::None`) and the result lists carry nulls. A Java-generated event log (the
  generator's LoggingVisitor) is a cheap, exact traversal-order oracle — pin the SEQUENCE, not
  properties of it.

### 2026-06-12 (Wave-5 Group U / U1 — view metadata + view ops + catalog CRUD, BUILDER Opus, wt-views)
- **The view SPEC builder was ALREADY a 1:1 Java port — the V2 "read before assuming unbuilt" lesson
  paid off; the actual gap was the entire CATALOG-facing surface.** `spec/view_metadata_builder.rs`
  (58 KB) is a faithful port of Java `ViewMetadata.Builder`: `reuse_or_create_new_view_version_id` ==
  `reuseOrCreateNewViewVersionId`/`sameViewVersion`, schema interning, version-log append + expiry
  (`version.history.num-entries`, default 10), the dialect-drop rules. What was MISSING and had to be
  built: `ViewMetadata::read_from`/`write_to`, a `View` type, `ViewCommit`/`ViewRequirement`, the
  `replace_version`/`update_properties` ops, and the `Catalog` trait view methods + a catalog impl.
  DO survey the spec layer in full before scoping a "views" increment — most of the hard metadata
  logic may already be ported from upstream.
- **The view REPLACE requirement set is `[AssertViewUUID]` ALONE — bytecode-pinned from
  `UpdateRequirements.forReplaceView`, the V1-lesson view sibling.** 1.10.0 bytecode: `forReplaceView`
  seeds ONE `AssertViewUUID(metadata.uuid())` then forEach-applies `Builder.update`. The `Builder` is
  constructed with `base = null` (offset 34 `aconst_null`), and the `update(AddSchema)` arm only adds
  `AssertLastAssignedFieldId` when `base != null` (offset 11 `ifnull 33`) — so it NO-OPS for views.
  There is NO `AddViewVersion`/`SetCurrentViewVersion` arm at all. So a view replace commit carries
  `[AssertViewUUID]` regardless of the update list. Pin it at the ActionCommit source (`take_requirements`
  == `[UuidMatch]`), not via concurrency — the retry machinery masks over-strict requirements (V1 lesson).
- **`ViewVersionReplace.internalApply` assigns `versionId = max(versionId)+1` then defers REUSE to the
  builder — the op's `+1` is a hint, not the final id.** 1.10.0 bytecode matches the /tmp source exactly:
  `Preconditions.checkState` on representations/schema/defaultNamespace, build version with
  `versionId(maxVersionId+1)` + `timestampMillis(now)` + `putAllSummary(EnvironmentContext.get())`, then
  `ViewMetadata.buildFrom(base).setCurrentVersion(newVersion, schema).build()` — and `setCurrentVersion`
  runs `reuseOrCreateNewViewVersionId`, so committing identical representations REUSES the existing id
  (version count unchanged). The Rust `ReplaceViewVersionAction::to_commit` mirrors this: feed the
  `max+1` candidate through `into_builder().set_current_version(...)` and let the builder dedup.
- **The view JSON wire format DIVERGES from Rust serde in FIELD ORDER but not field SET — round-trip is
  field-set-exact, NOT byte-exact.** Java `ViewMetadataParser.toJson` writes `view-uuid, format-version,
  location, [properties only if non-empty], schemas, current-version-id, versions, version-log`; Java
  `ViewVersionParser` writes `version-id, timestamp-ms, schema-id, summary, [default-catalog only if
  non-null], default-namespace, representations`. Rust's `_serde::ViewMetadataV1` orders
  `format-version` FIRST and `schemas` LAST, and always emits `properties` (Java omits when empty). Both
  parse each other's field set (serde is order-insensitive). DO pin the field SET (parse a Java-ordered
  doc + round-trip), and flag the order/empty-properties divergence — byte-level view interop is a
  next-wave item, not a U1 claim.
- **Mirror the table CRUD idiom EXACTLY for views in MemoryCatalog: a SEPARATE `view_metadata_locations`
  map in `NamespaceState`, so a view and a table of the same name in one namespace do not collide.** Java
  keeps views and tables in distinct catalog spaces; the Rust MemoryCatalog already keyed tables by name
  in `table_metadata_locations`, so views need their own parallel map (+ `insert_new_view`/
  `remove_existing_view`/`commit_view_update`/`get_existing_view_location` + `ViewNotFound`/
  `ViewAlreadyExists` ErrorKinds for Java `NoSuchViewException`/`AlreadyExistsException` parity).

#### U1 REVIEWER corrections (2026-06-12, wt-views) — adversarial pass against 1.10.0 bytecode
- **CORRECTS the "separate map ⇒ do not collide" lesson above: separate maps are right, but Java
  ENFORCES a shared name space across them — a view CANNOT be created/renamed onto a TABLE's name,
  and vice versa.** The builder shipped views+tables coexisting silently (probe: `create_view` over a
  table AND `create_table` over a view both SUCCEEDED). Java 1.10.0 `InMemoryViewOperations.doCommit`
  (offset 85-117) throws `AlreadyExistsException("Table with same name already exists")` when
  `tables.containsKey(ident)`; the view-catalog's table builder throws `"View with same name already
  exists"` (`viewExists` check); `renameView`/`renameTable` both cross-check the OTHER map at the
  destination. FIX (this pass): added a `tables.contains_key`/`views.contains_key` cross-guard to
  `insert_new_table` + `insert_new_view` in `namespace_state.rs` (symmetric — both helpers live in the
  modified file; the table-direction guard only rejects a previously-silent corruption, no legit test
  hits it). DO add the cross-type collision guard to BOTH `insert_new_*` helpers whenever a new
  catalog object class shares the table name space.
- **The in-tree MemoryCatalog `update_view` (and the pre-existing `update_table` it mirrors) has NO
  base-location CAS — a stale concurrent commit silently lands (last-write-win), diverging from Java
  `InMemoryViewOperations.doCommit`'s `views.compute` location-equality check.** Probe: two
  `ReplaceViewVersionAction` commits built from the SAME base, applied sequentially — the second
  (stale) one SUCCEEDED (versions 1→2→3, location 00001→00002) instead of failing. Java's `doCommit`
  lambda throws `CommitFailedException("...because it has been concurrently modified to %s")` when the
  stored location ≠ the expected base location. ROOT CAUSE: `ViewCommit::apply` re-loads the CURRENT
  view inside the lock and the only requirement is `AssertViewUUID` — but the UUID is INVARIANT across
  replaces, so it never detects staleness. This is consistent with the pre-existing `update_table`
  path (same gap), so it is NOT a view-specific regression and was NOT fixed here (fixing only the view
  side would diverge from the table side; belongs in a dedicated optimistic-concurrency-parity
  increment touching both). DO NOT claim "catalog view CRUD at parity" without the concurrency
  dimension — `[AssertViewUUID]` alone is correct for the REST commit protocol (server does the CAS)
  but insufficient for an in-process catalog with no location CAS.
- **VERIFIED interop-readiness (escalation NOT needed): Java `ViewMetadataParser.fromJson` reads
  Rust's emitted wire format TODAY.** 1.10.0 bytecode: all reads are by-key (`JsonUtil.getString`/
  `getInt`/`get` → order-insensitive Jackson), and `properties` is read under `if (node.has(...))`
  (offset 46-67) — so Rust always-emitting `"properties":{}` is tolerated, and field ORDER is
  irrelevant. The reverse holds too: Rust `_serde::ViewMetadataV1.properties` is `Option<..>` +
  `unwrap_or_default()`, so Rust tolerates Java's omit-when-empty. The wire divergence is COSMETIC
  (field order + always-emit-empty-properties), not blocking — byte-level view interop stays a
  next-wave item but bidirectional field-set reads work now. Mutation pinned: dropping the required
  `default-namespace` field makes the parse fail (`ViewVersionV1.default_namespace` is non-Option).

### 2026-06-12 (Wave-5 Group Y / Y2 — ComputeTableStats action, REVIEWER Opus, wt-tstats)
- **STOP-GRADE: Java's NDV pipeline builds an ALPHA-family sketch; the Y1 port is QUICKSELECT — do NOT
  dismiss the puffin spec's "Alpha" as a doc nit.** *Why:* three independent sources agree —
  `format/puffin-spec.md` ("constructing **Alpha family sketch**"), Spark `ThetaSketchAgg
  .createAggregationBuffer` MAIN source (`UpdateSketch.builder.setFamily(Family.ALPHA).build()`, all of
  spark v3.5/4.0/4.1 + the class doc), and `datasketches-java-3.3.0` bytecode (`UpdateSketchBuilder`'s
  DEFAULT family is QuickSelect, so the explicit `.setFamily(ALPHA)` is load-bearing — it overrides the
  default the Y1 port matches). Consequence (Java probe, lgK12/seed9001): **exact mode (≲ a few thousand
  distinct, theta==MAX) Alpha and QuickSelect are byte-identical + same ndv; estimation mode (≳7k) they
  DIVERGE** (n=1M → Alpha 1004032 vs QS 1002714, different retained set + bytes — Alpha switches to a
  sampling estimate `nominal*MAX/theta`). A test suite that feeds only ≤6 distinct values per column
  CANNOT see this — the divergence is SILENT. Lesson: when the headline is a sketch-FAMILY question, the
  crown-jewel hand-count test is necessary but NOT sufficient — add an explicit estimation-mode value pin
  (large distinct input) that documents the QuickSelect↔Alpha gap, so the next agent porting Alpha has a
  visible, citable pin (flip it to assert-equal-with-Alpha when the family lands). DON'T fix Y1's crate
  from Y2 (committed byte surface) — STOP-report the family verdict in the module doc + matrix cell.
- **The `footer_size <= file_size` invariant is too weak — a `footer_size == total` mutation SURVIVES it.**
  *Why:* a real Puffin stats file always has the leading MAGIC + the blob payloads BEFORE the footer, so
  `footer < total` STRICTLY; Java readers locate the blob region as `fileSize - footerSize`, so a footer
  that equals/exceeds the data is corrupt. Pin `<` not `<=`. (Verified the seam IS correct otherwise:
  Rust `footer_size()` == Java `PuffinWriter.footerSize()` byte-for-byte = MAGIC(4) + payload +
  FOOTER_STRUCT(12) = payload+16, the exact bytes `write_footer` appends.)
- **The 4 byte-form pins (long/date/string/decimal) skip the dangerous edges — add them.** *Why:*
  negative/zero decimals (Java `BigInteger.toByteArray` minimal two's-complement: -1→`ff`, 0→`00`,
  -300→`fe d4`), uuid (16B BE), float/double NaN + -0.0 (Java `putFloat`/`putDouble` write RAW bits, no
  NaN canonicalization — Rust `to_le_bytes` matches), and boolean (1 byte) all have distinct failure modes
  a long/string pin can't catch. `Datum::to_bytes` matches Java for ALL of them (verified vs a Java probe).
- **Spark agg/UDAF source IS available at `/tmp/iceberg-java-ref/spark/v{3.5,4.0,4.1}` even though no
  iceberg-spark JAR is in `~/.m2`.** *Why:* the orchestrator believed datasketches came in via a spark jar
  in m2 — it did NOT (only `datasketches-java`/`-memory` jars are there). For spark-action provenance read
  the MAIN source tree, not m2 bytecode; for the FAMILY question that source (`setFamily(ALPHA)`) is the
  authoritative oracle, cross-checked against the datasketches jar's builder-default bytecode.

### 2026-06-12 (Wave-5 Group Y / Y3 — Alpha-family update sketch, BUILDER Opus, wt-tstats)
- **THE ndv-source ruling: Iceberg's `ndv` reads the COMPACT sketch's `getEstimate`, NOT the Alpha
  update sketch's. The two genuinely DIFFER in estimation mode — derive which object from
  `NDVSketchUtil.toBlob`, never assume "the sketch's estimate".** *Why (decisive — spark v3.5/4.0/4.1
  `NDVSketchUtil.java` identical):* `Sketch sketch = CompactSketch.wrap(Memory.wrap(bytes)); ... ndv =
  String.valueOf((long) sketch.getEstimate())`. `sketch` is the COMPACT sketch reparsed from the
  serialized bytes — its `getEstimate` is the family-COMPACT STANDARD estimator `retained*(2^63/theta)`.
  The live Alpha UPDATE sketch's `getEstimate` (`HeapAlphaSketch.getEstimate` bytecode) is family-aware:
  `theta>split1` → standard, `theta<=split1` → SAMPLING `nominal*(2^63/theta)`. Java probe (lgK12/seed9001,
  n=1M): UPDATE sampling estimate = 1002319 but COMPACT estimate = 1004032 — and **1004032 is the prompt's
  pinned ndv**. So the action must do `CompactThetaSketch::deserialize(&payload).estimate()`, exactly
  Java's `CompactSketch.wrap(bytes).getEstimate()`. Pinning BOTH values (the compact one as the ndv, the
  update one as "NOT the ndv") makes the object-selection load-bearing. The prompt framed `nominal*MAX/theta`
  as "the Alpha estimator the Y2 probe saw" — true of the UPDATE sketch, but the ndv uses the COMPACT one;
  a builder who wired `alpha.estimate()` would emit 1002319 and silently diverge from every engine.
- **`HeapAlphaSketch.compact()` is family-COMPACT, not family-ALPHA — the on-disk form REUSES the
  QuickSelect serializer verbatim; only the UPDATE-side retention/theta differs.** *Why:* `toByteArray()`
  on the live Alpha sketch writes a family-ALPHA preamble, but Iceberg serializes via `UpdateSketch.compact()`
  → `componentsToCompact(thetaLong, getRetainedEntries(true), seedHash, isEmpty, ..., cache)` → `compactCache`
  keeps cache entries `0<h<theta`, `Arrays.sort` ascending, `loadCompactMemory` writes the family-COMPACT (id 3)
  preamble. So `AlphaSketch::serialize_compact()` = the Y1 `serialize_compact_from_parts(is_empty, theta,
  sorted_below_theta_hashes, seed)` UNCHANGED — one path, no fork. The reused retained count is
  `getRetainedEntries(true)` (the DIRTY-aware `countPart` = the below-theta cache count), NOT the raw
  `curCount_` (which over-counts dirty stale slots). Make the shared helpers `pub(crate)` rather than
  duplicating the serializer.
- **The Alpha dirty-phase insert (`enhancedHashInsert`) reuses stale (≥theta) slots WITHOUT a count
  bump and does NOT rebuild every insert — port it faithfully or the retained set drifts by an element.**
  *Why:* once `theta<=split1` the table accumulates above-theta entries; a new insert probes, and on the
  FIRST stale slot it reuses it in place (`InsertedCountNotIncremented`), decays theta, sets dirty; only a
  truly-empty slot bumps `curCount` and may trigger `rebuildDirty` (a same-size purge; if nothing purged,
  grow by 1). A naive "rebuild on every dirty insert" or "always land in an empty slot" port produced a
  retained SET that differed from Java by ONE borderline hash near theta — invisible to retained-count and
  theta pins (both matched) but caught by the BYTE-EXACT fixture. Lesson: for a stateful Java sketch, the
  byte-exact estimation fixture is the only pin that catches a single-element set drift; pin retained+theta
  AND the full bytes.
- **A byte-exact `*_HEX` const transcribed by hand into Rust is error-prone — generate it, then REPLACE
  the const programmatically from the verified-equal Rust/Java output, never retype a 8000-char hex.**
  *Why:* my first paste of the lgK9/520 fixture had 2 extra hex chars at byte 1880; the retained SET and
  theta were byte-identical (proven by dumping both sets — zero diff), so the bug was purely a transcription
  typo in the const, not the algorithm. A `python3 re.sub` replacing the const with the Java-generated hex
  (after confirming the live Rust `serialize_compact()` == the Java hex) fixed it in one shot. DO diff the
  retained SETS first when an estimation fixture fails — if the sets match, the bug is in the const, not the
  sketch.
- **`setHashTableThreshold(lgNom, lgArr)` uses the 0.5 fraction when `lgArr <= lgNom` (NOT 0.9375).** *Why:*
  the initial Alpha table at lgK12 has lgArr=7 (`startingSubMultiple(13,3,5)=7`), and 7<=12 ⇒ threshold =
  `floor(0.5*2^7)=64`, not 120. I twice wrongly asserted the 0.9375 branch; it only applies once the table
  has grown PAST nominal. Read the `if_icmpgt` direction in the bytecode, don't assume the resize-phase fraction.

### 2026-06-12 (I1 — theta-blob puffin interop, BUILDER Fable/Sonnet, wt-interop6)
- **`BlobMetadata` fields in the Rust iceberg crate are PRIVATE — use accessor methods, not field
  access.** `blob_type()`, `fields()`, `snapshot_id()`, `sequence_number()`, `properties()` are
  the correct calls; `blob_metadata.r#type` / `.fields` / etc. will not compile.
  *Why:* the struct fields are `pub(crate)` only. The `Blob` struct returned by `reader.blob()`
  has the same accessor names and IS public.
- **`FileIO::from_path(...)` does not exist — use `FileIO::new_with_fs()` for local filesystem
  access in tests.** The correct API for a no-config local FileIO is `FileIO::new_with_fs()`;
  the builder pattern is `FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build()`.
  *Why:* `from_path` is a pattern from other Iceberg crate versions; the fork's `FileIO` does not
  have it. Always `grep -n "pub fn"` the `FileIO` impl before assuming a constructor exists.
- **Puffin file structure (Iceberg spec): footer layout is `[data-blobs][footer-magic(4)][footer-json(N)][footer-struct: payload_len(4 LE u32)|flags(4)|trailing-magic(4)]`.** Blob offsets in the footer JSON are ABSOLUTE from the file start (blob[0].offset = 4, right after the leading magic). To read the footer from Python: `payload_len = struct.unpack('<I', data[file_len-12:file_len-8])[0]`; `footer_json_start = file_len - 12 - payload_len - 4` (skip the footer's own leading magic). DO NOT search for the sketch preamble bytes by pattern — parse the footer JSON for blob offsets and corrupt those bytes directly.
  *Why:* a pattern search for compact-sketch family byte `0x03` in the raw file hit a byte in the
  Puffin wire framing, not the sketch payload, so the corrupt byte had no effect on Java's
  `CompactSketch.wrap()`. The footer-parse approach is deterministic and works for any blob count
  or order.
- **When the sabotage battery's `|| true` catch-all pattern is used on a Rust test failure, verify
  the check logic doesn't produce false-greens.** The partition-stats template's `7c` pattern
  checks `grep -q "^test.*ok$"` combined with `grep -qiE "error|panicked|FAILED"` — a truncated
  puffin that panics at the file-metadata read will not emit `^test.*ok$` at all, so the pattern
  needs a second pass. A simpler invariant: the truncated file MUST NOT allow the Rust test to
  exit 0 normally. For Rust `cargo test`, the process exit code is non-zero on FAILED/error, so
  `|| true` plus absence of "ok" is sufficient.

#### I1 REVIEWER corrections (2026-06-12, wt-interop6) — adversarial pass
- **A theta sketch's `getEstimate()` is INSENSITIVE to a single-byte flip in the hash-entry region —
  it depends ONLY on `theta` and the retained ENTRY COUNT, not on the entry values. So a "corrupt a
  payload data byte" sabotage is a SILENT NO-OP on the ndv-vs-estimate cross-check.** *Why (probed):*
  the builder's 6b zeroed blob0's first 8 bytes — that corrupts the compact-sketch PREAMBLE
  (preLongs/serVer/family), which makes `CompactSketch.wrap()` THROW, so 6b "passed" only as a PARSE
  CRASH (the same failure class as a truncation), NOT via the estimate cross-check the increment's
  headline depends on. I probed flipping a byte deep inside the sorted-hash region (offset+1000): the
  file parsed AND `getEstimate()` returned the UNCHANGED 1004032 — the verify PASSED on a corrupted
  artifact. The genuinely SEMANTIC mutation is to corrupt `theta` itself (the LE long at compact-
  estimation payload offset +16): the preamble stays valid so the file PARSES, but `getEstimate() =
  retained * 2^63 / theta` changes (halving theta → estimate doubles 1004032→2008064), which ONLY the
  cross-check catches. FIX (this pass): rewrote 6b to halve `theta` via the footer-parsed SOURCE
  offset, with an estimation-mode precondition guard (exact-mode theta==MAX is inert) and a belt that
  asserts the FAIL came from the `getEstimate() as long expected=` cross-check line (not a parse
  crash) — so a future degeneration of the semantic mutation into a structural one fails closed. DO,
  for any statistic-bearing blob, pin a sabotage that mutates the STATISTIC (theta/count) while
  keeping the container parseable — a payload-data-byte flip proves nothing about the estimator.
- **A `SKIP` branch in a sabotage step is a false-green: it lets the chain continue without the
  corruption ever landing.** *Why:* the builder's 6b printed "6b SKIP" and continued on a magic-detect
  failure (exit 42). A sabotage that cannot be applied has proven nothing and MUST hard-fail. FIX:
  converted the 6b skip-exits (42 framing / 43 not-estimation-mode) into `exit 1` with a restore.
- **CONFIRMED the crown-jewel family pin is load-bearing both ways (mutation-tested).** Swapping the
  Java oracle's `setFamily(Family.ALPHA)` → `QUICKSELECT` in `buildSketchPayload` makes the Java D2
  generate THROW immediately at its own `ESTIMATION_NDV_PIN` sanity check (`estimation ndv pin check
  FAILED: expected 1004032 got 1002714`) — the n=1M estimation blob is genuinely large-n, the family
  is unpinned-detecting, and the chain fails closed. The Rust D2 estimation pin (`expected_ndv ==
  ESTIMATION_NDV_PIN` for field_id=3) is an independent second guard. Reverted after.
- **Integer-exactness CONFIRMED both directions:** Rust compares `sketch.estimate() as i64` via
  `assert_eq!` (no tolerance/abs/epsilon anywhere in `interop_theta.rs`); Java compares `(long)
  compact.getEstimate()` via `!=`. The Puffin footer-layout lesson (trailing magic, payload_len u32
  LE, absolute blob offsets) is accurate against `puffin/writer.rs::write_footer`.
- **COVERAGE NOTE (not fixed — named):** the interop layer proves the blob byte round-trip but does
  NOT assert the stats file's REGISTRATION entry (snapshot-id/statistics-path) on the committed table
  metadata — the Java verify reads the loose copied `rust_stats.puffin`, not `final.metadata.json`.
  Registration IS unit-covered (`compute_table_stats.rs` re-parses the committed `StatisticsFile`), so
  this is a minor interop-completeness gap, not a correctness hole. DO consider a `statisticsFiles()`
  read on the Java side in a follow-up to close the registration dimension at the interop level.

### 2026-06-12 (I2 — view metadata interop, BUILDER Fable/Sonnet, wt-interop6)
- **Java's `View` interface does NOT expose `operations()` — cast to `BaseView` to access the
  committed `ViewMetadata`.** `InMemoryCatalog.loadView(ident)` returns `View`; `View` has no
  `operations()`. Cast: `((org.apache.iceberg.view.BaseView) loadedView).operations().current()`.
  Same pattern applies after `buildView(...).create()` or `.replace()` — store the returned `View`
  reference and cast. *Why:* `BaseView` is the concrete abstract class that wires the operations
  handle; the interface intentionally hides it. `grep 'extends BaseView'` in `iceberg-core` to find
  other concrete view impls that may need the same cast.
- **Java's `reuseOrCreateNewViewVersionId` deduplicates VIEW VERSIONS with identical SQL — the
  two SQL strings MUST be DIFFERENT (not just syntactically but character-exact) for version count
  to grow to 2.** *Why:* `BaseViewVersionReplace` (bytecode-verified) compares representations via
  `sameViewVersion` — if the replacement representations are character-identical to the current
  version's, it reuses the existing version-id and the version-log count stays at 1. Use
  `SQL_V1 = "... WHERE id > 0"` and `SQL_V2 = "... WHERE id > 100"` — any string difference
  works, but the constants must be agreed between Java oracle and Rust test (anti-circular).
- **View metadata wire format FIELD-ORDER DIVERGENCE is tolerated at parse time on BOTH sides;
  do NOT attempt byte-exact equality for view metadata JSON.** Java `ViewMetadataParser.toJson`
  writes `view-uuid` first; Rust's `_serde::ViewMetadataV1` writes `format-version` first. Java
  omits `"properties"` when empty; Rust always emits `"properties":{}`. Both serde parsers
  (Jackson and serde_json) are key-order-insensitive; Rust's `properties` field is
  `Option<HashMap<...>>` deserialized with `unwrap_or_default()`. Pin the FIELD SET, not bytes.
  Nail this with a dedicated `test_view_tolerance_controls` that feeds a Java-ordered no-properties
  JSON into Rust's `read_from` and confirms the parse succeeds + properties map is empty.
- **For D2 sabotage (Rust reads Java-written metadata), verify via the RUST TEST's exit code + the
  expected stdout pattern, NOT the Java oracle.** The D2 test (`test_view_d2_rust_reads_java`)
  runs via `cargo test --exact --nocapture`; on an injected mismatch it panics with `assert_eq!` 
  and `cargo test` exits non-zero. The sabotage check pattern: `|| true` catch + `grep -q
  "test test_view_d2_rust_reads_java ... ok"` absent → PASS (failure confirmed). Do NOT look for
  `'^FAIL '` patterns on the Rust side — those are Java sentinel patterns.
- **Dropping a REQUIRED field from a version JSON (`default-namespace`) causes Rust `read_from`
  to fail with a deserialization error — no need for an explicit guard.** Rust's
  `_serde::ViewVersionV1.default_namespace` is non-`Option` (direct `NamespaceIdent`), so serde
  returns an error immediately if the field is absent. This makes 6b a clean structural-failure
  sabotage, not a semantic one. Java `ViewVersionParser` likewise throws on a missing
  `default-namespace`. Pin both via the chain script: a modified `java_view_metadata.json` with
  one version's `default-namespace` removed must make Rust's D2 test fail.
- **A dangling `current-version-id` (pointing to a non-existent version) causes Rust
  `ViewMetadata::read_from` to return an error at METADATA BUILD TIME, not at access time.**
  *Why:* the view metadata builder validates that `current_version_id` refers to a version in
  the `versions` map; if the id has no corresponding version, it returns
  `Err(ErrorKind::DataInvalid)`. This is clean fail-closed behavior (no delayed panic). Pin it
  as 6c in the sabotage battery: `current-version-id: 99` with no version 99 in `versions`.
- **`clippy::never_loop` fires on `for repr in iter { irrefutable-pattern; return ... }` — use
  `iter.next()` with a `let Some(...) else { panic! }` guard instead.** The `for`-loop form looks
  right but clippy sees it as "loop body always executes at most once" because the return exits
  before the next iteration. The idiomatic fix: `let Some(repr) = iter.next() else { panic!(...) };`
  followed by the irrefutable destructure. This applies to any helper that extracts the first
  element from a known-non-empty iterator via a pattern.
- **An `irrefutable if let` or irrefutable single-arm `match` on an enum with ONE variant triggers
  both a clippy lint and a compiler warning — use a bare destructure `let Pattern = value;`.**
  *Why:* `if let ViewRepresentation::Sql(r) = repr { ... }` on a value that IS always
  `ViewRepresentation::Sql` compiles but warns. Use `let ViewRepresentation::Sql(r) = repr;`
  directly (a refutable pattern is only needed when the enum has multiple arms).

#### I2 REVIEWER corrections (2026-06-12, wt-interop6) — adversarial kill-list against the comparators
- **A field-by-field interop comparator that checks schema field NAMES but not field TYPE or the
  required flag has a real PROJECTION GAP — corrupting `id` from long→string passed BOTH the Java
  D1 oracle and the Rust D2 test silently.** The kill-list (corrupt one field of the SOURCE
  metadata, re-run the production reader, confirm it fails) found four unpinned fields on EACH
  side: (1) non-current version `timestamp-ms`, (2) version-log entry `timestamp-ms`, (3) schema
  field `type`, (4) schema field `required`. SQL/dialect/field-names/version-log-id-order WERE
  pinned. FIX (this pass): both emitters now write `schema_fields` (name/type/required),
  `version_timestamps`, and `version_log` (id+timestamp); both comparators assert them. Post-fix
  kill-list: all four PINNED on both directions. DO run the corrupt-one-field kill-list against
  EVERY field a "field-by-field" oracle claims — names-only schema checks are the classic gap.
- **The view metadata `type` string is cross-language byte-identical for primitives — Rust
  `Type::Display` and Java `Type.toString()` both emit `long`/`string`/`int`/… so the comparator
  can compare them as plain strings.** (Decimal is `decimal(p,s)` on both; fixed is `fixed(n)`.)
  No normalization needed for the fixture's `long`/`string`.
- **`ViewMetadata::read_from` does NOT call `validate()` directly — validation runs inside
  `TryFrom<ViewMetadataV1>` (`view_metadata.validate()?` at the end of the conversion), which serde
  invokes during `from_slice`.** So a dangling `current-version-id` is rejected at DESERIALIZE
  time (parse error wraps `No version exists with the current version id N`), not at a later
  access. This is why sabotage 6c fails cleanly at the `read_from` `.expect`, and 6d (valid parse,
  wrong SQL value) fails LATER at the `assert_eq!` — distinct failure sites prove distinct
  provenance.
- **Interop test env-dir paths MUST be absolute when invoking `cargo test` by hand — a relative
  `ICEBERG_INTEROP_VIEW_DIR` resolves against the test process CWD (workspace root), not your
  shell CWD, so the `metadata_path.exists()` guard fires and every sabotage "passes" for the WRONG
  reason (missing file, not the injected corruption).** The chain script is correct (it builds an
  absolute `${TMP}` from `${SCRIPT_DIR}`); only ad-hoc reviewer commands hit this. Always echo the
  resolved path and confirm a CLEAN run passes before trusting a sabotage's failure.

### 2026-06-12 (I3 — data-level WAP interop, BUILDER Sonnet)

- **`updateProperties().set(...).commit()` in BOTH Java and Rust does NOT create a snapshot — only
  emits a `SetProperties`/`RemoveProperties` table update with no `AddSnapshot`.** Using it to
  "bump" main in a REPLAY-shape chain means the table's `current-snapshot-id` is UNCHANGED after
  the "bump", so `staged.parent == current` → the cherry-pick takes the FAST-FORWARD path (no new
  snapshot, no `source-snapshot-id`/`published-wap-id` tags). DO use a REAL data fast-append for
  the bump (write an actual parquet file); even one row with a known id (e.g. id=99 category=a
  data="bump") is sufficient and the row becomes part of the expected fixture.
- **S-replay order: stage FIRST while `current = base`, THEN advance main with the bump commit.**
  The REPLAY shape requires `staged.parent ≠ current head at cherry-pick time`. Stage the WAP
  append BEFORE the bump so `staged.parent = base`. After the bump, `current = bump ≠ base =
  staged.parent` → REPLAY guaranteed. If you stage AFTER the bump, `staged.parent = bump = head`
  → FAST-FORWARD → no `source-snapshot-id`/`published-wap-id`. The sequence is: (1) fast-append
  base, (2) `stage_only()` WAP append, (3) verify `current_snapshot_id == base_snapshot_id`,
  (4) fast-append bump. Confirm REPLAY by asserting the staged snapshot is NOT reachable from the
  current ancestry (`staged_id ∉ walk_from(current_snapshot)`).
- **`LocalTableOperations.commit(null, metadata)` always writes `v0.metadata.json` to its metadata
  directory — repeated calls to `verifyRustTable` fail with "File already exists".** Fix: use
  `Files.createTempDirectory(parent, prefix)` per verify call to create a fresh directory per run.
  Rebuild the `TableMetadata` with the temp dir as location (via `TableMetadata.buildFrom(meta)
  .discardChanges().setLocation(tempDir.toString()).build()`), then seed `LocalTableOperations`
  with the temp dir. Data files referenced by the manifests use absolute paths so they resolve to
  the original parquet files regardless of the temp dir location.
- **`TableMetadata.Builder.withLocation()` does NOT exist in iceberg-core 1.10.0; the correct
  method is `setLocation(String)`.** Always verify Java API names by running `javap` on the target
  class in `~/.m2` before writing oracle code that calls an API method.
- **Semantic sabotage for WAP interop must target the WAP-ID chain, not file paths or partition
  directories.** Swapping file paths or directory names in manifests (even in-place binary edits)
  does not corrupt the `category` column values stored in the parquet data — IcebergGenerics reads
  the actual Arrow column values from the parquet files, which are immutable. The correct semantic
  sabotage: corrupt the staged snapshot's `wap.id` in the metadata JSON (change `"w1"` →
  `"w1-CORRUPTED"`). The metadata still parses, the staged snapshot is still present, Java
  cherry-picks it and produces a snapshot with `published-wap-id="w1-CORRUPTED"` → the
  `published-wap-id == "w1"` assertion fires. This is the WAP chain semantic pin, not a partition
  routing pin.
  _Partially corrected 2026-06-12 (I3 REVIEWER): the claim "IcebergGenerics reads the actual column
  values, which are immutable" is RIGHT for NON-partition columns but WRONG for the identity-PARTITION
  column — that value is PROJECTED from the manifest partition STAMP, so even a writer that bakes the
  wrong category into the parquet column reads back as the stamp. See the I3 REVIEWER block below._

#### I3 REVIEWER corrections (2026-06-12, wt-interop6) — adversarial data-move kill-list
- **A "row-content pin" whose EXPECTED set is loaded from the OTHER side's own read of the SAME
  table is CIRCULAR — it catches only a Rust-vs-Java READER disagreement, never a wrong VALUE.**
  *Why (proven):* the D2 test compared `actual_rows` (Rust read) against `expected_rows` loaded from
  `java_cherrypick_rows.json` (Java's read of the same table). A `data`-value move injected at the
  Java WRITER (id=50/60 `data` "e"/"f"→"X") flowed into BOTH `java_cherrypick_rows.json` AND the
  parquet, so Rust-reads-X == Java-reads-X and the pin PASSED on a corrupted artifact. FIX (this
  pass): added a hand-declared `ground_truth_rows` vec (10→a … 99→bump) asserted independently of the
  Java-derived expected — the probe now FAILS at the anti-circular `assert_eq!` (fail-before/pass-
  after confirmed). DO pin at least ONE side of an interop row-content comparator against a
  hardcoded fixture, never derive BOTH the actual and the expected from the same physical table.
- **An identity-partition column read back through a scan is PROJECTED from the manifest partition
  STAMP, not read from the parquet column — so an `id→category` "routing pin" canNOT detect a
  rows-in-wrong-partition DATA move where the stored column disagrees with the stamp.** *Why
  (proven both legs' probes):* a Java writer that put `category="b"` rows into the partition-`a` file
  (stamp stays "a") was read back by Rust `to_arrow()` as `category="a"` — the projection masked the
  "b" column entirely; the routing pin reported `a={...50,60...}` and PASSED. This is INHERENT Iceberg
  behavior (neither Java `IcebergGenerics` nor Rust validates stored-column == partition-stamp; a
  "garbage in" gap), so it is NAMED RESIDUE, not a fixable assertion. The data-VALUE move is caught
  by the anti-circular ground-truth pin (on the non-partition `data` column); the column-vs-stamp case
  is documented in the test + matrix.
- **A literal parquet file SWAP and an in-place same-length byte patch BOTH trip a STRUCTURAL belt
  before any semantic pin — do NOT rely on a file-swap to exercise a row-content pin.** *Why
  (proven):* swapping two staged files of different sizes crashes the reader on `file_size_in_bytes`
  buffer-fill (manifest records the size); a same-length category-byte flip corrupts the gzip page
  checksum (`corrupt gzip stream does not have a matching checksum`). Both are parse crashes (the
  fail-closed structural belt), NOT the routing/content pin. To exercise a row-content pin with a
  PARSE-CLEAN data move you must drive the WRITER to emit a valid-but-wrong file — a post-hoc file
  edit cannot. The I1 critic's "swap two staged files" sabotage does NOT translate to a partitioned
  identity table: the swap is structurally rejected, and even a clean move is projection-masked.
- **CONFIRMED both lesson-7 bytecode claims against 1.10.0 jars:** `TableMetadata$Builder` has
  `setLocation(String)` + `withMetadataLocation(String)` but NO `withLocation` (javap);
  `PropertiesUpdate` (impl of `UpdateProperties`) calls `TableMetadata.replaceProperties` and contains
  NO `addSnapshot`/`newSnapshot` — so `updateProperties().commit()` creates no snapshot. Both builder
  lessons are correct.
- **CRITIC PROCESS SCAR: `git checkout <file>` on an UNCOMMITTED working-tree file reverts it to
  HEAD, DESTROYING the uncommitted work — it does not "undo my probe edit."** *Why:* I `git checkout`ed
  `InteropOracle.java` to drop a temporary probe, but I3 was uncommitted on top of HEAD, so the
  checkout wiped all 669 lines of the builder's WapDataOracle. Recovered by reconstructing from the
  in-context Reads + the builder transcript (verified: 669 insertions, compiles, chain ×2 green). DO
  revert a temporary probe with the INVERSE edit (or a `.bak` copy), NEVER `git checkout` a file that
  carries uncommitted work.
