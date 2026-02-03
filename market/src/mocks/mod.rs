//! Mock implementations for testing.
//!
//! This module provides mock implementations of the trait abstractions
//! that allow unit testing without external dependencies.

pub mod dht;
pub mod process;
pub mod random;
pub mod time;
pub mod transport;

pub use dht::{make_test_public_key, make_test_record_key, MockDht, MockDhtFailure, SharedDhtHandle, SharedMockDht};
pub use process::{MockMpcRunner, SharedBidRegistry, WinnerStrategy};
pub use random::MockRandom;
pub use time::MockTime;
pub use transport::MockTransport;
