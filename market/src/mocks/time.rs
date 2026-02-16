//! Mock time provider for testing.

use crate::traits::TimeProvider;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Mock time provider with controllable time value.
#[derive(Debug, Clone)]
pub struct MockTime {
    current_time: Arc<AtomicU64>,
}

impl MockTime {
    /// Create a new mock time provider starting at the specified timestamp.
    pub fn new(initial_time: u64) -> Self {
        Self {
            current_time: Arc::new(AtomicU64::new(initial_time)),
        }
    }

    /// Create a mock time provider starting at a reasonable default (2024-01-01).
    pub fn default_time() -> Self {
        Self::new(1_704_067_200) // 2024-01-01 00:00:00 UTC
    }

    /// Set the current time to a specific value.
    pub fn set(&self, timestamp: u64) {
        self.current_time.store(timestamp, Ordering::SeqCst);
    }

    /// Advance time by the specified number of seconds.
    pub fn advance(&self, seconds: u64) {
        self.current_time.fetch_add(seconds, Ordering::SeqCst);
    }

    /// Get the current mock time value.
    pub fn get(&self) -> u64 {
        self.current_time.load(Ordering::SeqCst)
    }
}

impl Default for MockTime {
    fn default() -> Self {
        Self::default_time()
    }
}

impl TimeProvider for MockTime {
    fn now_unix(&self) -> u64 {
        self.current_time.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_time_initial_value() {
        let time = MockTime::new(1000);
        assert_eq!(time.now_unix(), 1000);
    }

    #[test]
    fn test_mock_time_set() {
        let time = MockTime::new(1000);
        time.set(2000);
        assert_eq!(time.now_unix(), 2000);
    }

    #[test]
    fn test_mock_time_advance() {
        let time = MockTime::new(1000);
        time.advance(500);
        assert_eq!(time.now_unix(), 1500);
    }

    #[test]
    fn test_mock_time_clone_shares_state() {
        let time1 = MockTime::new(1000);
        let time2 = time1.clone();

        time1.advance(500);
        assert_eq!(time2.now_unix(), 1500);
    }

    #[test]
    fn test_default_time() {
        let time = MockTime::default();
        // Should be 2024-01-01
        assert_eq!(time.now_unix(), 1704067200);
    }
}
