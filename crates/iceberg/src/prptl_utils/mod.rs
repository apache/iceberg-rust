// Perpetual fork-only utility modules.
//
// Lives in a separate `prptl_utils/` directory rather than alongside
// `utils.rs` so that future merges from apache/iceberg-rust upstream don't
// have to navigate around fork-specific additions in `utils.rs`. Add new
// fork-only helpers here unless there is a specific reason to integrate
// them with upstream's `utils` namespace.

pub(crate) mod bin_packing;
