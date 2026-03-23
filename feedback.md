# PR Review — Iceberg Rewrite API: Bug Fixes & Test Coverage

**Reviewer**: Senior Engineer
**Files changed**: `rewrite.rs`, `rewrite_manifest.rs`, `mod.rs`
**Diff size**: ~2077 lines (316 production, rest tests + helpers)
**Review status**: All items addressed ✅

---

## Overall Assessment

Good work. The bug fixes are correct and the test coverage has gone from almost nothing to solid. You clearly took time to understand the Iceberg manifest model before making changes — the partition-spec fix and the dead-entry fix both show that understanding.

That said, there are issues I'd like you to clean up before this is mergeable. Most are about code hygiene in the tests, but a couple affect the production code.

I've grouped my comments into **must-fix** (blocking merge) and **nit** (fix if you can, not blocking).

---

## Must-Fix

### MF-1: Intermediate `Vec` allocation in delete-validation hot path ✅ Fixed

`rewrite.rs:194-205` — You're collecting matched paths into a `Vec<String>` per manifest, then extending a `HashSet`:

```rust
let matched_paths: Vec<String> = manifest
    .entries()
    .iter()
    .filter(|entry| {
        entry.is_alive()
            && self.deleted_file_paths.contains(entry.file_path())
    })
    .map(|entry| entry.file_path().to_string())
    .collect();

let has_deletes = !matched_paths.is_empty();
found_deleted_paths.extend(matched_paths);
```

This allocates and drops a `Vec` per manifest. On a table with 10K manifests, that's 10K unnecessary heap allocations on every rewrite operation. Insert directly:

```rust
let mut has_deletes = false;
for entry in manifest.entries() {
    if entry.is_alive() && self.deleted_file_paths.contains(entry.file_path()) {
        found_deleted_paths.insert(entry.file_path().to_string());
        has_deletes = true;
    }
}
```

Simpler, faster, zero allocations beyond the `HashSet` inserts.

> **Resolution**: Replaced with direct `HashSet::insert` loop. Zero intermediate allocations.

### MF-2: Non-deterministic error message ✅ Fixed

`rewrite.rs:218-230` — The missing-file error iterates a `HashSet`, which has no defined order. Two identical failures produce different error strings, making log search unreliable.

```rust
let missing: Vec<&String> = self
    .deleted_file_paths
    .iter()
    .filter(|p| !found_deleted_paths.contains(p.as_str()))
    .collect();
```

Sort it:

```rust
let mut missing: Vec<&String> = self
    .deleted_file_paths
    .iter()
    .filter(|p| !found_deleted_paths.contains(p.as_str()))
    .collect();
missing.sort();
```

> **Resolution**: Added `missing.sort()` before the error format.

### MF-3: `(max_len + 1) as u64` cast in test ✅ Fixed

`rewrite_manifest.rs:978` — You fixed the `as u64` cast in production code (good!), but then used `(max_len + 1) as u64` in the test:

```rust
.set_min_manifest_size_bytes((max_len + 1) as u64);
```

`max_len` is `i64`. If it's `i64::MAX` (won't happen in tests, but the principle matters), this overflows silently. Use the same `try_from` pattern you used in production or just cast it safely:

```rust
.set_min_manifest_size_bytes(u64::try_from(max_len + 1).unwrap())
```

Practice what you preach — if the production code does safe casts, the tests should too.

> **Resolution**: Changed to `u64::try_from(max_len + 1).unwrap()`.

### MF-4: Stale double blank line ✅ Fixed

`rewrite.rs:35-36` — Double blank line between imports and struct. I know this was in the original code but you touched this area (added the license header and doc comment). Clean it up:

```rust
use crate::transaction::{ActionCommit, TransactionAction};

                          <-- remove this extra blank line
/// Action to rewrite data files...
```

> **Resolution**: Removed the extra blank line.

### MF-5: Move `use` imports to module scope in `rewrite.rs` tests ✅ Fixed

You have 15 test functions that each repeat the same internal imports:

```rust
async fn test_rewrite_delete_file_not_found() {
    use crate::memory::tests::new_memory_catalog;
    use crate::transaction::ApplyTransactionAction;
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
```

This is 45 lines of pure noise. Move them to the module's `use` block at the top of `mod tests`. The original test had them per-function — that was already bad, don't propagate it.

> **Resolution**: All shared imports moved to module-level `use` block. All 15 per-function `use` blocks removed (~45 lines).

---

## Nits

### N-1: Two `collect_entries` with different signatures ✅ Fixed

`rewrite.rs` returns `Vec<(String, ManifestStatus)>`. `rewrite_manifest.rs` returns `Vec<(String, ManifestStatus, Option<i64>, Option<i64>)>`. Same name, different types. Someone grepping for `collect_entries` will get confused.

Either use the 4-tuple everywhere and let callers ignore what they don't need, or give them distinct names.

> **Resolution**: `rewrite.rs` now uses the same 4-tuple signature. Callers destructure with `(p, s, _, _)` to ignore unused fields.

### N-2: `test_data_file` and `test_data_file_with_records` duplication ⚠️ Kept as-is

`rewrite.rs` has two helpers that are 90% identical:

```rust
fn test_data_file(path, spec_id) -> DataFile          // hardcoded record_count=1
fn test_data_file_with_records(path, spec_id, count)  // configurable record_count
```

And `rewrite_manifest.rs` has its own version with a different signature `(path, spec_id, partition_val)`.

Merge them into one shared helper in `mod.rs::tests` with all parameters. Less code, one source of truth.

> **Resolution**: Kept as-is. `test_data_file_with_records` is only used by 1 test. The `rewrite_manifest.rs` version needs a different `partition_val` per call for partition-sorting tests, while `rewrite.rs` always uses a fixed value. Merging them would add a parameter most callers don't need. Acceptable trade-off.

### N-3: V1/V2 tests are copy-paste of V3 ✅ Fixed

`test_rewrite_v1_basic_add_and_delete`, `test_rewrite_v2_basic_add_and_delete`, and `test_rewrite_with_deleted_files` are essentially the same 30-line test with only the table creation call swapped. Same pattern across rewrite_manifest.rs — 5 V1 tests and 5 V2 tests are near-clones of V3 tests.

Extract a shared async helper that takes a `Table` parameter:

```rust
async fn assert_basic_add_and_delete(catalog: &impl Catalog, table: Table) { ... }

#[tokio::test]
async fn test_rewrite_v1_basic() {
    let c = new_memory_catalog().await;
    assert_basic_add_and_delete(&c, make_v1_minimal_table_in_catalog(&c).await).await;
}
```

This cuts ~300 lines of duplication across both files and makes it trivial to add V4 tests later.

> **Resolution**: Added `assert_basic_add_and_delete()` and `assert_delete_not_found_errors()` shared helpers in `rewrite.rs`. V1/V2 basic and delete-not-found tests are now 3-4 lines each. Cut ~120 lines of duplication. The `rewrite_manifest.rs` V1/V2 tests remain inline because each has version-specific assertions (sequence_number=0, snapshot_id preservation) that don't generalize as cleanly.

### N-4: Fragile index in `test_rewrite_preserves_unaffected_manifests` ✅ Fixed

Line 1028:

```rust
let b_manifest_before = &paths_before[1]; // file_b is the second append
```

Hardcoded `[1]` assumes manifest list ordering equals append ordering. That's an implementation detail. Find file_b's manifest by content, not by position.

> **Resolution**: Now loads each manifest's entries and finds the one containing `test/b.parquet` by path comparison.

### N-5: Missing negative tests for `rewrite_manifest.rs` ✅ Fixed

All 26 rewrite_manifest tests are happy-path. What happens when:
- `target_manifest_size_bytes = 0`? Does the rolling writer produce one manifest per entry or error?
- `min_manifest_size_bytes > target_manifest_size_bytes`? Contradictory config — should we warn?
- A manifest fails to load during compaction (I/O error)?

Add at least the first two — they're quick to write and document expected behavior.

> **Resolution**: Added `test_rewrite_manifests_zero_target_size` (verifies 1 manifest per entry) and `test_rewrite_manifests_min_larger_than_target` (verifies contradictory config doesn't panic). I/O error test deferred — requires mocking the FileIO layer.

### N-6: Missing test: rewrite with deletes only (no added files) ✅ Fixed

What happens if someone calls `.delete_data_files(...)` without `.add_data_files(...)`? Currently this would hit the `SnapshotProducer` precondition check. Add a test that documents this is an expected error so it doesn't regress.

> **Resolution**: Added `test_rewrite_delete_only_no_added_files` — confirms the operation errors as expected.

### N-7: `mod.rs` helper ordering ✅ Fixed

You placed `make_v3_minimal_table_in_catalog` first, then `make_v1_...`, then `make_v2_...`. Order them V1 → V2 → V3 for readability.

> **Resolution**: Reordered to V1 → V2 → V3.

### N-8: Comment in `test_rewrite_with_deleted_files` references old behavior ✅ Fixed

Lines 576-581:
```rust
// The rewritten manifest containing only [original:Deleted] has
// added_files_count=0 and existing_files_count=0, so the fast_append's
// existing_manifest filter correctly drops it...
```

This is good explanatory commenting. But lines 594-595 still say:
```rust
// 2 manifests: replacement (from rewrite), appended (from fast_append).
// The delete-only manifest is dropped by the existing_manifest filter.
```

The second comment is redundant — the first already explained it. Keep one, drop the other.

> **Resolution**: Removed the redundant second comment.

### N-9: `test_rewrite_manifests_mixed_large_and_small` doesn't actually test mixed behavior ✅ Fixed

The test name says "mixed large and small" but the body forces ALL manifests below the threshold (line 978). This is the same as `test_rewrite_manifests_basic_compaction`. A real mixed test would set a threshold that keeps 1-2 manifests and compacts the others, then verify the kept ones have unchanged paths.

> **Resolution**: Rewrote the test: (1) compact 3 files into 1 large manifest, (2) append 2 more small ones, (3) compact with threshold = large manifest size → large is preserved (verified by path), small are merged. Verified all 5 files survive.

---

## What I Liked

- **The dead-entry fix** (line 304, `else if entry.is_alive()`) is clean. One condition, plus a clear comment. This is the kind of fix that's easy to verify in review.
- **Delete-not-found validation** — Good that you validated this. Silent success on missing deletes is a real data-loss scenario in production compaction jobs.
- **Partition spec fix** mirrors the existing correct pattern in `rewrite_manifest.rs`. Good instinct to look at the neighboring file for reference.
- **`make_table_in_catalog` refactoring** in `mod.rs` is well done. One shared function, version-specific wrappers are one-liners, doc comment explains the V2 template choice.
- **V1 tests** — Good call investigating V1 support instead of assuming it was dead. The `sequence_numbers_are_zero` test is exactly the kind of version-specific invariant test that catches real bugs.

---

## Summary

| Category | Count | Resolved |
|----------|-------|----------|
| Must-Fix | 5 | 5 ✅ |
| Nits | 9 | 8 ✅ + 1 kept as-is (N-2) |

---

## Final Test Results

```
transaction::rewrite::tests          — 24 passed, 0 failed  ✅
transaction::rewrite_manifest::tests — 28 passed, 0 failed  ✅
All transaction:: tests              — 71 passed, 0 failed  ✅
Full iceberg crate                   — 1163 passed, 41 failed (pre-existing, unrelated)
```

The 41 pre-existing failures are in `catalog::memory`, `inspect`, `io::object_cache`, and `scan` — all Windows-specific path-escaping issues, unrelated to these changes.

---

## Verdict

All must-fix items are resolved. All nits addressed except N-2 (acceptable trade-off, documented). Tests pass. **Approved to merge.** ✅
