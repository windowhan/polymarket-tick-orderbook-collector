//! Greenfield v2 library surface for the Polymarket orderbook collector.
//!
//! The current implementation begins with shared contract types.  Runtime
//! modules will be added after the architecture contracts are stable.

pub mod common;
pub mod node;

/// Return the crate version compiled into the binary.
///
/// # Detailed Description
/// This helper gives future binaries and smoke tests a tiny stable API while
/// the greenfield v2 contracts are being built out.  The value is read from
/// Cargo package metadata at compile time and therefore always matches the
/// artifact being tested.
///
/// # Arguments
/// This function does not accept arguments.
///
/// # Returns
/// The semantic version string from `Cargo.toml`.
///
/// # Example — Input / Output
/// ```rust
/// let version = polymarket_collector::library_version();
/// assert!(!version.is_empty());
/// ```
///
/// # Related
/// - [`common::contracts`] for the v2 architecture contracts.
pub fn library_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
