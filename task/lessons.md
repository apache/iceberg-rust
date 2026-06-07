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
