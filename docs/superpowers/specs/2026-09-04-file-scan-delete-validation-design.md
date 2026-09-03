# FileScanTaskDeleteFile Validation Design

## Context

Apache Iceberg Rust issue #3135 asks for deletion-vector validation to happen when a
`FileScanTaskDeleteFile` is built. The current implementation validates required
deletion-vector metadata in the delete-file index and caching loader, after an invalid
task can already exist and cross planning boundaries.

## Goals

- Make every builder-created deletion-vector task structurally valid.
- Preserve the behavior of equality-delete and ordinary position-delete tasks.
- Keep decoded bitmap cardinality validation in the loader because it depends on I/O.
- Return `DataInvalid` errors with the delete-file path and invalid field.

## Builder Contract

`FileScanTaskDeleteFile` will use the same typed-builder conversion pattern as
`FileScanTask`: `build()` returns `Result<FileScanTaskDeleteFile>`, and conversion calls
a private `validate()` method.

A task is a deletion vector exactly when:

```text
file_type == PositionDeletes && file_format == Puffin
```

Deletion-vector tasks must contain:

- `referenced_data_file`
- `content_offset`
- `content_size_in_bytes`
- `record_count`

`content_offset` and `content_size_in_bytes` must be non-negative so they can safely
be converted to the unsigned values required by range reads. Non-deletion-vector tasks
will not acquire new restrictions.

## Call-Site Migration

Builder call sites will propagate `Result` where possible and use `expect` or `unwrap`
only in tests that intentionally construct valid fixtures. Direct struct literals will
remain unchanged unless making fields private is required by the builder invariant; field
visibility changes are outside this issue.

The loader will stop repeating checks that the builder now guarantees. It will continue
to validate decoded bitmap cardinality against `record_count`, because that check can only
happen after reading the Puffin blob.

The delete-file index will stop repeating required-field checks after task construction
becomes the invariant boundary. Duplicate deletion vectors for one data file remain an
index-level validation because they involve multiple tasks.

## Error Handling

All builder validation failures use `ErrorKind::DataInvalid`. Messages identify the
deletion-vector path and the missing or negative field. Validation runs before any I/O,
so malformed manifest metadata fails during scan planning rather than during execution.

## Testing

TDD coverage will first demonstrate failures on current `main`, then verify:

- a complete deletion-vector task builds successfully;
- each required field is rejected when absent;
- negative offset and size are rejected;
- ordinary Parquet position-delete tasks remain valid without DV metadata;
- equality-delete tasks remain valid without DV metadata.

Verification will include the focused task tests, the `iceberg` library test suite,
`cargo fmt --all -- --check`, targeted Clippy with warnings denied, and
`git diff --check`.

## Scope

This change does not alter Puffin decoding, bitmap cardinality rules, duplicate-DV
detection, transaction semantics, or public deletion-vector formats.
