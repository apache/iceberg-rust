# Testing contract

The authoritative testing rules for this fork. The operating manuals ([skills/](../skills/)) and
[CLAUDE.md](../CLAUDE.md) point here; read this **before any code change**. Where this file and
[CLAUDE.md](../CLAUDE.md) disagree, CLAUDE.md wins.

The north star is **1:1 capability parity with Java `iceberg-core` / `iceberg-api`** (see
[Roadmap.md](../Roadmap.md)). Tests exist to prove that parity and to keep it from regressing.

## The rules (non-negotiable)

1. **Tests ship with the change — same commit/PR.** Behavior added without a test gets reverted, not
   patched. No `#[ignore]`, no commented-out tests, no `// TODO: add test`.
2. **Every code path gets a happy-path test AND at least one negative / edge / error-path test.** A
   guard that rejects bad input is only "tested" when a test proves the rejection *fires*.
3. **Each test names the risk it pins.** If you cannot state the failure mode the test catches, the
   test is weak — rewrite or delete it. (Risk-First, see the manuals.)
4. **Test names describe the behavior pinned, not the function called.** Prefer
   `test_rollback_to_non_ancestor_fails` over `test_rollback`.
5. **A test must fail without the change applied.** If it passes against the unmodified code, it pins
   nothing.

## What to test, by risk surface

- **Spec / on-disk format** (`spec/`): round-trip serialization (serde → struct → serde) and, for
  format-sensitive encodings, an **exact-byte fixture** regression. A silent encoding change corrupts
  already-written tables — name that risk in the test.
- **Numeric / transform code** (partition transforms, aggregations, bucketing): assert exact values,
  using `f64::to_bits` where float drift is possible. Name the drift you are guarding against.
- **Transactions / commit actions** (`transaction/`): assert the produced `TableUpdate`s **and** the
  `TableRequirement` optimistic-concurrency guards (not just the updates). Test the conflict/guard
  path, not only the happy commit. Cover the validation errors each action raises.
- **Catalogs** (`crates/catalog/*`): logic that needs no live service goes in unit tests over the
  in-memory catalog; anything needing a real service is an integration test (see below).
- **Boundaries / untrusted input**: parse-at-the-door tests for malformed, empty, out-of-range input.
- **Concurrency**: exercise the race window directly (e.g. a `Barrier`/contention loop), not just a
  sequential happy path. Avoid asserting on wall-clock timing — millisecond comparisons are flaky
  under parallel load (assert `<=` plus a structural change, not strict `<`).

## Interop tests — the only true 1:1 evidence

Unit tests prove *our* code is internally consistent; they do **not** prove parity with Java. A
GAP_MATRIX row flips to ✅ only when an **interop test** proves byte-level table compatibility with
Java **in both directions where applicable**:

- **Read what Java wrote** — point the Rust reader at table metadata/data produced by Apache
  Iceberg (Java/Spark) and assert it reads back correctly. The Spark provisioner under
  [dev/spark/](../dev/spark/) and the suites in [crates/integration_tests/](../crates/integration_tests/)
  are the harness for this.
- **Prove Java can read what we write** — produce a table with the Rust write path and verify Java
  reads it without loss.

Until an interop test exists for a capability, record it as 🟡 (partial), not ✅, in
[docs/parity/GAP_MATRIX.md](parity/GAP_MATRIX.md) — even if the unit tests are green.

## Verification commands (the Done gate)

Run before declaring any change complete (canonical list in [CLAUDE.md](../CLAUDE.md) and the
[skills/](../skills/) manuals):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --workspace -- -D warnings
cargo test --workspace --no-fail-fast        # or: make check && make test
```

Or the `Makefile` targets: `make check` (fmt + clippy + TOML + unused-deps) and `make test`.

### Environmental prerequisites (offline vs full)

- The lint gate uses the pinned nightly in [rust-toolchain.toml](../rust-toolchain.toml) (rustup
  fetches it automatically); downstream only needs MSRV **1.92**.
- **`protoc`** is required by `crates/sqllogictest` (transitively via `datafusion-substrait`). Without
  it, the core surface still builds/tests via `cargo test --workspace --exclude iceberg-sqllogictest`.
- **Docker** is required by `make test` (`docker-up`) for the service-bound integration suites
  (Hive Metastore, REST fixture, MinIO/S3). AWS Glue / S3 Tables suites need real credentials. These
  are **not** part of the offline gate — run the offline lib/unit suite for fast iteration, and the
  full suite (with Docker) before claiming an integration capability done.

When the offline suite is green but service-bound suites were skipped, say so explicitly — a skip is
not a pass.

## Definition of done (per capability)

A [GAP_MATRIX.md](parity/GAP_MATRIX.md) row is ✅ only when **(1)** the Rust API matches the Java
contract's behavior, **(2)** unit tests ship with it in the same change, and **(3)** an interop test
proves byte-level table compatibility with Java in both directions where applicable. Anything short of
all three is 🟡.
