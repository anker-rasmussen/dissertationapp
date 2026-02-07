//! Trait abstractions for dependency injection and testability.
//!
//! This module provides trait-based abstractions for external dependencies,
//! enabling unit testing without requiring actual network connections or
//! external processes.

pub mod dht;
pub mod process;
pub mod random;
pub mod time;
pub mod transport;

// Re-export all traits for crate-internal use.
// The public API surface is controlled by lib.rs re-exports.
pub use dht::DhtStore;
pub use process::{MpcResult, MpcRunner};
pub use random::RandomSource;
pub use time::TimeProvider;
pub use transport::{MessageTransport, TransportTarget};

// Re-export default implementations
pub use random::ThreadRng;
pub use time::SystemTimeProvider;
