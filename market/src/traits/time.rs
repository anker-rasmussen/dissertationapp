//! Time provider abstraction for testable time-dependent code.

use std::time::{SystemTime, UNIX_EPOCH};

/// Trait for providing the current Unix timestamp.
///
/// This abstraction allows code that depends on the current time to be
/// tested with deterministic, controllable time values.
pub trait TimeProvider: Send + Sync {
    /// Returns the current Unix timestamp in seconds.
    fn now_unix(&self) -> u64;
}

/// Production implementation that uses the system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemTimeProvider;

impl TimeProvider for SystemTimeProvider {
    fn now_unix(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

impl SystemTimeProvider {
    pub const fn new() -> Self {
        Self
    }
}

