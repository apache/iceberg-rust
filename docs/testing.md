# Testing Discipline

The load-bearing gate referenced by the operating manuals ([skills/](../skills/)) and by
[CLAUDE.md](../CLAUDE.md). **Tests ship with the change, in the same commit/PR.** Behavior added
without tests is reverted, not patched. This document is the contract; read it before any code
change.

## The hard rules

- **Tests in the same commit/PR as the behavior they cover.** No "tests later," no follow-up PR.
- **No disabled tests as a substitute for passing ones** — no `#[ignore]`, no `--skip`, no
  `--no-verify`, no commented-out tests, no `// TODO: add test`.
- **A test must be able to fail.** `assert!(result.is_ok())` as the entire body is not a test.
  Assert on the *value*, not merely that nothing panicked.
- **The test fails without the change.** If it passes against the unmodified code, it pins nothing.
  Verify this for any non-trivial change.

## Names are specifications

A test name states the behavior it pins, not the function it calls.

- DO: `test_overwrite_replaces_only_matching_partitions`,
  `test_schema_evolution_rejects_incompatible_type_widening`,
  `test_bucket_transform_is_stable_across_releases`.
- DO NOT: `test_overwrite`, `test_schema`, `test_transform`, `test_works`.

If you cannot name the behavior crisply, you do not yet understand what you are testing.

## Each test names the risk it pins

This is the [Risk-First](../skills/Opus.md) discipline applied to the test surface. For every test,
you should be able to answer "what failure mode does this catch?" If you can't, the test is weak —
sharpen it or delete it.

- For every happy-path test, write **at least one** negative / edge / error-path test for the same
  code path: empty, malformed, boundary, concurrent, partial-failure.
- For **guards** (anything that must reject a forbidden input), test that the prohibited shape
  **fails** — not just that the allowed shape succeeds. A guard that lets the bad case through
  silently is worse than no guard.
- For **concurrency**, exercise the race window directly (a `Barrier`, a contention loop), not just
  the sequential happy path.

## Numeric / format-sensitive code

For code where bit-level drift matters silently — partition transforms, statistical aggregations,
encoders/decoders, serialization round-trips, anything producing `f64`/`f32` — pin a **fixture-based
regression at `f64::to_bits()` precision** (or an exact byte/encoding comparison), and name the
drift you are guarding against. A vague "matches expected" assertion hides exactly the bug you care
about.

```rust
// Pins the exact bits, not an approximate match — catches silent recalculation drift.
assert_eq!(computed.to_bits(), 0x4009_21fb_5444_2d18, "pi transform drifted");
```

For format/serialization code, round-trip and assert byte-equality where the spec requires a stable
encoding.

## The Done gate

A change is **not done** until every box is checked (mirrored from the manuals' §4):

- [ ] Tests for the change exist in the same commit/PR.
- [ ] Test names describe the behavior pinned, not the function tested.
- [ ] Each test names the risk it pins.
- [ ] At least one happy-path AND one negative/edge/error test per code path.
- [ ] Tests fail without the change applied.
- [ ] Numeric/format-sensitive code has bit/byte-precise fixture regression with the drift named.
- [ ] Code compiles / interprets without errors (run it — do not assume).
- [ ] Tests pass with no ignored/skipped tests.
- [ ] Output matches the expected schema or contract.
- [ ] Null / empty / edge cases handled AND tested.
- [ ] No new warnings; no unintended changes outside the target files.
- [ ] Imports and dependencies are correct and actually used — no orphaned imports.
- [ ] Verification commands clean (see the manuals' Language-Specific Rules and
      [CLAUDE.md](../CLAUDE.md) Build & test).

Ask: "Would a staff engineer reviewing this approve it — including the tests?" **Never mark a task
complete without proving it works.**
