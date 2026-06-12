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
