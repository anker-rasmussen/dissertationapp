//! End-to-end smoke tests using real Veilid devnets.
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
//!   cargo nextest run --profile e2e --ignored
//! ```
//!
//! The tests will automatically:
//! 1. Stop any existing devnet
//! 2. Start fresh devnet containers
//! 3. Run the tests serially
//! 4. Clean up the devnet after each test

mod smoke;
