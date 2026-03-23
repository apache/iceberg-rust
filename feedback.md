# Code Review: Iceberg Rewrite & Manifest Compaction APIs

**Reviewer**: Senior Engineer
**Date**: 2026-03-23
**Files reviewed**: `rewrite.rs` (328 lines prod, ~1060 lines tests), `rewrite_manifest.rs` (537 lines prod, ~1250 lines tests), `mod.rs` (shared helpers)

---

## Test Results

```
transaction::rewrite::tests              — 24 passed, 0 failed ✅
transaction::rewrite_manifest::tests     — 31 passed, 0 failed ✅
All transaction:: tests                  — 74 passed, 0 failed ✅
Full iceberg crate                       — 1163 passed, 41 failed (pre-existing, unrelated)
Compiler warnings in our code            — 0
```

The 41 pre-existing failures are Windows-specific path-escaping issues in `catalog::memory`, `inspect`, `io::object_cache`, and `scan`. Confirmed unrelated.

---

## Strengths (Pros)

### 1. Correct Iceberg Spec Semantics
- `RewriteOperation::operation()` correctly returns `Operation::Replace`, matching the Iceberg spec for data compaction.
- Dead entries (already-deleted) are dropped during manifest rewrite (`rewrite.rs:323-326`), with a clear comment explaining that the old snapshot preserves the delete record for time-travel.
- `file_sequence_number` is correctly passed to `add_delete_file()` (not `sequence_number`), preserving the GC-critical distinction between data sequence and file sequence numbers.

### 2. Single Commit UUID Shared Across Components
- `commit_uuid` is generated once (`rewrite.rs:103`) and passed to both `SnapshotProducer` and `RewriteOperation`. All manifest paths from a single commit share the same UUID for traceability.
- Rewritten manifests use `-m-rewrite{N}` suffix (`rewrite.rs:272-278`) to avoid path collision with the `SnapshotProducer`'s added-file manifests which use `-m{N}`. The comment at line 272-273 explains why.

### 3. Proper Partition Spec Lookup
- `rewrite.rs:258-268` looks up the partition spec by ID from the manifest file metadata, not the table's current default. Correctly handles partition evolution.
- `rewrite_manifest.rs:406-418` does the same in `RollingManifestWriter::new_writer()`. Both are consistent.

### 4. Delete Validation
- `rewrite.rs:225-240` validates that every requested delete file was actually found in a manifest. Missing paths are sorted for deterministic error messages.
- Prevents silent data loss where a caller thinks a file was deleted but it wasn't.

### 5. Graceful Error Handling Over Panics
- `sequence_number` extracted via `ok_or_else` with descriptive error messages (`rewrite.rs:304-312`) instead of `unwrap()`. Prevents panics on V1 tables or entries with unresolved sequence numbers.
- Same pattern used consistently in `rewrite_manifest.rs:330-347` for both `snapshot_id` and `sequence_number`.
- `manifest_length` cast uses `u64::try_from().unwrap_or(0)` (`rewrite_manifest.rs:145`) instead of unsafe `as u64`.

### 6. Rolling Writer Design
- `RollingManifestWriter` (`rewrite_manifest.rs:364-501`) cleanly encapsulates manifest splitting with `should_roll()` / `flush()` / `finish()` API.
- Size estimation via `avg_entry_bytes` is pragmatic and avoids serialization overhead.

### 7. Comprehensive Test Coverage (74 tests total)
- **Format versions**: V1, V2, V3 all tested for both `RewriteAction` and `RewriteManifestsAction`.
- **Negative tests**: delete-not-found, duplicate detection, already-deleted file, delete-only (no added files), no-snapshot table.
- **Edge cases**: sequential rewrites, partial deletes, all-entries-dead, mixed large/small manifests, contradictory config (`min > target`), zero target size.
- **Metadata preservation**: snapshot_id, sequence_number, file_sequence_number, and summary totals verified through compaction.
- **Unit tests**: `compare_literal` tested with float, double, NaN, and Infinity values to guard against future `OrderedFloat` removal.

### 8. Clean Shared Test Infrastructure
- Unified `test_data_file(path, spec_id, record_count, partition_val)` and `collect_entries()` in `mod.rs` — no duplication across test modules.
- Each test module has thin convenience wrappers (`file()`, `file_with_records()`) for its default parameters.
- `make_v1/v2/v3_minimal_table_in_catalog()` backed by single `make_table_in_catalog(format_version)`.
- Module-level imports are minimal; test-specific imports (e.g., `DataFileBuilder`) are scoped locally.

### 9. Good Documentation
- Doc comments on `RewriteAction`, `RewriteManifestsAction`, `RewriteOperation`, and `RollingManifestWriter`.
- TODO comment on `delete_entries()` no-op (`rewrite.rs:157-161`) explains the design choice and flags future work.
- `compare_literal` doc (`rewrite_manifest.rs:525-529`) explains `OrderedFloat` safety.

---

## Weaknesses (Cons)

### Should Fix (non-blocking)

#### SF-1 (Medium): Summary Doesn't Track Deleted Records Explicitly
`SnapshotProducer::summary()` only counts `added_data_files`. Deleted-file and deleted-record counts in the snapshot summary are inferred by `update_snapshot_summaries()` from previous snapshot totals, not explicitly counted. The test at `rewrite.rs` ~line 1005 acknowledges this with a comment.

Not a bug (inference works today), but fragile — if a previous summary was wrong, the error propagates.

**Recommendation**: File a tracking issue to add explicit delete counting to `SnapshotSummaryCollector`.

#### SF-2 (Low): `compact_group` Has 8 Parameters
```rust
#[allow(clippy::too_many_arguments)]
async fn compact_group(table, group_key, group_files, snapshot_id,
    commit_uuid, manifest_counter, key_metadata, target_manifest_size_bytes)
```
Suppresses the clippy lint. The function is internal and has clear parameter names, so this is tolerable.

**Recommendation**: Extract a `CompactContext` struct in a follow-up.

#### SF-3 (Low): Two Different Memory Catalog Constructors
`rewrite.rs` tests use `new_memory_catalog()` from `crate::memory::tests`, while `rewrite_manifest.rs` uses a local `new_test_catalog()` built with `MemoryCatalogBuilder`. Both produce memory catalogs but via different code paths. The `rewrite_manifest.rs` version was created to work around Windows path issues.

**Recommendation**: Unify or document the reason in a follow-up.

### Nice to Have (follow-up PRs)

| # | Item | Notes |
|---|------|-------|
| N-1 | Concurrent rewrite conflict test | Exercise the `Transaction::do_commit` retry path with competing rewrites |
| N-2 | Parallel manifest loading | `futures::stream::iter(...).buffered(N)` for tables with hundreds of manifests |
| N-3 | Explicit delete counting in summary | Track via issue, implement in `SnapshotSummaryCollector` |

---

## File-by-File Summary

### `rewrite.rs` — Data File Rewrite (Compaction)

| Aspect | Rating | Notes |
|--------|--------|-------|
| Correctness | ✅ Strong | Single UUID, correct partition spec, file_sequence_number, delete validation |
| Error handling | ✅ Strong | `ok_or_else` everywhere, sorted missing paths, descriptive messages |
| API design | ✅ Good | Builder pattern, `with_check_duplicate`, clear method names |
| Test coverage | ✅ Excellent | 24 tests: V1/V2/V3, edge cases, negatives |
| Code hygiene | ✅ Good | Minimal imports, thin wrappers, shared helpers, TODO documented |

### `rewrite_manifest.rs` — Manifest Compaction

| Aspect | Rating | Notes |
|--------|--------|-------|
| Correctness | ✅ Strong | Rolling writer, partition sort, metadata preservation, safe casts |
| Error handling | ✅ Strong | Consistent `ok_or_else`, `u64::try_from` |
| API design | ✅ Good | Configurable thresholds, clear doc comments |
| Test coverage | ✅ Excellent | 31 tests: rolling writer, dead entries, summary preservation, V1/V2/V3, float comparison |
| Code hygiene | ✅ Good | Clean imports, shared helpers, 8-param fn is only remaining nit |

### `mod.rs` — Shared Test Infrastructure

| Aspect | Rating | Notes |
|--------|--------|-------|
| Design | ✅ Good | Single `make_table_in_catalog`, unified `test_data_file`, `collect_entries` |
| Quality | ✅ Good | `pub(crate)` visibility, doc comments, V2 template compatibility note |

---

## Action Items

| Priority | # | Item | File |
|----------|---|------|------|
| Should Fix | SF-1 | File tracking issue for delete summary counting | `snapshot.rs` |
| Should Fix | SF-2 | Extract `CompactContext` struct | `rewrite_manifest.rs` |
| Should Fix | SF-3 | Unify test catalog constructors | Both test modules |
| Nice to Have | N-1 | Concurrent rewrite conflict test | `rewrite.rs` |
| Nice to Have | N-2 | Parallel manifest loading | Both files |
| Nice to Have | N-3 | Explicit delete counting in summary | `snapshot.rs` |

---

## Conclusion

The implementation is **correct, well-tested, and ready to merge**. All 74 tests pass with zero warnings in the changed code.

Production code demonstrates solid understanding of the Iceberg spec — partition evolution handling, sequence number semantics, delete validation, and proper manifest lifecycle. The shared test infrastructure eliminates duplication and the V1/V2/V3 coverage catches format-specific bugs.

The remaining items (SF-1 through SF-3, N-1 through N-3) are all non-blocking improvements suitable for follow-up PRs.

**Verdict: Approved ✅**
