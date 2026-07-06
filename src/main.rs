//! Minimal greenfield v2 binary entrypoint.
//!
//! Runtime commands are intentionally not implemented yet.  The first build
//! milestone is contract/type validation from architecture section 22.1.

/// Print the greenfield contract build marker.
///
/// # Detailed Description
/// The previous implementation has been intentionally retired.  This entrypoint
/// keeps `cargo build`, `cargo run`, and CI smoke checks working while the new
/// orchestrator/collector contracts are introduced in small verified steps.
///
/// # Arguments
/// This function does not accept command-line arguments yet.
///
/// # Returns
/// The process exits successfully after printing a short marker.
///
/// # Example — Input / Output
/// ```text
/// $ cargo run --quiet
/// polymarket-collector v0.1.0 greenfield contracts
/// ```
///
/// # Related
/// - `src/common/contracts.rs` for architecture 22.1 domain types.
fn main() {
    println!(
        "polymarket-collector v{} greenfield contracts",
        polymarket_collector::library_version()
    );
}
