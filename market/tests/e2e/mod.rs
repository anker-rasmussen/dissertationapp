//! End-to-end tests using real Veilid devnets.
//!
//! These tests are marked `#[ignore]` by default since they require:
//! - Veilid repository at expected path (../../veilid relative to market)
//! - ipspoof library built (`cargo build -p ipspoof --release`)
//! - Preload env var set (LD_PRELOAD on Linux, DYLD_INSERT_LIBRARIES on macOS)
//! - Sufficient time for node attachment (~30s per node)
//!
//! Run with:
//! ```bash
//! # Linux
//! LD_PRELOAD=/path/to/veilid/target/release/libveilid_ipspoof.so \
//!   cargo test --test integration_tests -- --ignored e2e_smoke_
//! # macOS
//! DYLD_INSERT_LIBRARIES=/path/to/veilid/target/release/libveilid_ipspoof.dylib \
//!   cargo test --test integration_tests -- --ignored e2e_smoke_
//! ```
//!
//! The tests will automatically:
//! 1. Stop any existing devnet
//! 2. Start fresh devnet via veilid-playground
//! 3. Run the tests serially
//! 4. Clean up the devnet after each test

mod helpers;

mod edge_cases;
mod smoke;
