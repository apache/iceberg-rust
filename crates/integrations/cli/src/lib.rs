
#![doc = include_str!("../README.md")]
pub const ICEBERG_CLI_VERSION: &str = env!("CARGO_PKG_VERSION");

mod catalog;
pub use catalog::*;