// Fork-only utilities. Kept separate from upstream's `utils.rs` so merges
// from apache/iceberg-rust don't pull in our additions there.

pub(crate) mod bin_packing;
