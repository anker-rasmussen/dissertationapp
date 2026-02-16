//! End-to-end tests using real Veilid devnets.
//!
//! These tests are marked `#[ignore]` by default since they require:
//! - Docker installed and running
//! - Veilid repository at expected path (../../veilid relative to market)
//! - libipspoof.so built (for IP translation in devnet)
//! - Sufficient time for node attachment (~30s per node)
//!
//! Run with:
//! ```bash
//! LD_PRELOAD=/path/to/veilid/.devcontainer/scripts/libipspoof.so \
//!   cargo test --test integration_tests -- --ignored e2e_smoke_
//! ```
//! For full MPC/decryption flows (slower):
//! ```bash
//! LD_PRELOAD=/path/to/veilid/.devcontainer/scripts/libipspoof.so \
//!   cargo test --test integration_tests -- --ignored e2e_full_
//! ```
//!
//! The tests will automatically:
//! 1. Stop any existing devnet
//! 2. Start fresh devnet containers
//! 3. Run the tests serially
//! 4. Clean up the devnet after each test

mod helpers;

mod edge_cases;
mod smoke;
