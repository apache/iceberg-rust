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

---

<!-- Newest entries at the bottom. Example shape:

### YYYY-MM-DD
- **DO** carry context on every fallible Rust call (`.with_context(...)` / `.expect("msg")`).
  *Why:* a bare `.unwrap()` panic gives the operator no cause from logs alone.
- **DO NOT** edit upstream crate files to land a fork feature when an additive module would do.
  *Why:* it makes the next upstream merge conflict-prone. Prefer additive changes.
-->

### 2026-06-07
- **DO** run a new test module against the *full* parallel lib suite (`cargo test -p iceberg --lib`),
  not just its own filter, before declaring green. *Why:* adding ManageSnapshots' 12 tests increased
  parallel load and surfaced a latent flaky assertion in an unrelated test
  (`catalog/memory/catalog.rs::test_update_table`). The new code was correct; a pre-existing race only
  became visible under load. A filtered run (`... transaction::manage_snapshots`) hid it.
- **DO** treat strict `<` comparisons on millisecond wall-clock timestamps as flaky, and assert `<=`
  (plus a structural check that the change happened, e.g. metadata-log growth). *Why:* two metadata
  versions can legitimately share a `last_updated_ms`. Fixed `test_update_table` accordingly — a
  legitimate fix since we own the fork, and upstreamable.
- **Pattern: adding a transaction action.** Mirror `transaction/sort_order.rs` — builder struct that
  records intent, `#[async_trait] impl TransactionAction { commit(self: Arc<Self>, &Table) ->
  Result<ActionCommit> }` resolving against `table.metadata()` and returning
  `ActionCommit::new(updates, requirements)`; add `mod x;` + `use ...XAction;` + a `pub fn x()` ctor in
  `transaction/mod.rs`. The `TransactionAction` trait must be `use`d in tests to call `.commit()`.

### 2026-06-07 (multi-agent review remediation)
- **DO** verify a parity contract against the Java *source*, not intuition, before implementing the
  Rust side. *Why:* the first `fast_forward` cut wrongly required `to` to be a branch and rejected an
  absent `from`; Java `UpdateSnapshotReferencesOperation.replaceBranch` only requires `from` to be a
  branch and **auto-creates** an absent `from`. Read `/tmp/iceberg-java-ref` for the exact precondition
  checks (`Preconditions.checkArgument(...)`) and the early-return no-op cases.
- **DO** emit only refs whose final state differs from their original (no-op suppression) in
  metadata-mutating commit actions. *Why:* create-then-remove, fast-forward-to-same, replace-to-same
  should produce zero `TableUpdate`s — matches Java's "only changed refs" and keeps the commit
  idempotent at the catalog layer. Compare `metadata.refs.get(name) == working.get(name)` (both
  `SnapshotReference: PartialEq`).
- **DO** flag an agent finding you could NOT confirm rather than acting on it. *Why:* a reviewer
  claimed Java rejects non-positive retention (`> 0`); a grep of `SnapshotRef.java` showed no such
  `checkArgument`. Left unimplemented and tracked in `task/todo.md` as a follow-up — don't add
  validation that may diverge from Java on an unverified claim.
- **DO** test pre-existing-ref paths with a fixture that actually contains those refs. *Why:* the
  straight-line `make_v2_table()` has only `main`, so remove/replace/rollback-non-ancestor were
  untestable; build a forked fixture (`add_snapshot` a sibling + `set_ref` a branch/tag). `add_snapshot`
  validates the timestamp against the metadata's `last-updated-ms` (not just snapshot timestamps) — set
  the grafted snapshot's `timestamp_ms` after it.
- **DO** keep summary/headline sections in sync with the detail table when flipping a status. *Why:*
  the GAP_MATRIX + Roadmap "Headline gaps" kept listing `timestamp_ns`/column-defaults as missing after
  the matrix body flipped them to ✅. When you change a row's status, grep for the capability name and
  reconcile every mention.
