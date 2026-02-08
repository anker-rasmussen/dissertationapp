#![recursion_limit = "256"]
//! Integration tests for multi-party sealed-bid auctions.
//!
//! These tests use the DI-based test harness to simulate multi-party
//! auction scenarios without requiring Docker or real network infrastructure.
//!
//! E2E tests (in `e2e` module) require Docker and real Veilid devnet.
//! Run them with: `cargo nextest run --ignored`

mod common;
mod e2e;
mod integration;
