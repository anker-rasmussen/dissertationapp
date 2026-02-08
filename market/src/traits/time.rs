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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_time_provider_returns_reasonable_value() {
        let provider = SystemTimeProvider::new();
        let now = provider.now_unix();

        // Should be after 2020 (1577836800) and before 2100 (4102444800)
        assert!(now > 1577836800, "Timestamp should be after 2020");
        assert!(now < 4102444800, "Timestamp should be before 2100");
    }

    #[test]
    fn test_system_time_provider_is_monotonic() {
        let provider = SystemTimeProvider::new();
        let t1 = provider.now_unix();
        let t2 = provider.now_unix();

        assert!(t2 >= t1, "Time should not go backwards");
    }
}
