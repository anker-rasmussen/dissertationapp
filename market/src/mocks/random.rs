//! Mock random source for deterministic testing.

use crate::traits::RandomSource;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Mock random source that produces deterministic values.
#[derive(Debug, Clone)]
pub struct MockRandom {
    /// Counter used to generate deterministic "random" values.
    counter: Arc<AtomicU64>,
    /// Fixed seed for reproducible sequences.
    seed: u64,
}

impl MockRandom {
    /// Create a new mock random source with the specified seed.
    pub fn new(seed: u64) -> Self {
        Self {
            counter: Arc::new(AtomicU64::new(0)),
            seed,
        }
    }

    /// Create a mock random source with a default seed.
    pub fn default_seed() -> Self {
        Self::new(0x12345678_9ABCDEF0)
    }

    /// Reset the counter to generate the same sequence again.
    pub fn reset(&self) {
        self.counter.store(0, Ordering::SeqCst);
    }

    /// Get the current counter value.
    pub fn counter(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
    }

    /// Simple deterministic mixing function.
    fn mix(&self, counter: u64) -> u64 {
        let mut x = self.seed.wrapping_add(counter);
        x = x.wrapping_mul(0x517CC1B727220A95);
        x ^= x >> 32;
        x = x.wrapping_mul(0x517CC1B727220A95);
        x ^= x >> 32;
        x
    }
}

impl Default for MockRandom {
    fn default() -> Self {
        Self::default_seed()
    }
}

impl RandomSource for MockRandom {
    fn fill_bytes(&self, dest: &mut [u8]) {
        let mut offset = 0;
        while offset < dest.len() {
            let counter = self.counter.fetch_add(1, Ordering::SeqCst);
            let value = self.mix(counter);
            let bytes = value.to_le_bytes();

            let remaining = dest.len() - offset;
            let to_copy = remaining.min(8);
            dest[offset..offset + to_copy].copy_from_slice(&bytes[..to_copy]);
            offset += to_copy;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_random_deterministic() {
        let rng1 = MockRandom::new(42);
        let rng2 = MockRandom::new(42);

        let a = rng1.random_bytes_32();
        let b = rng2.random_bytes_32();

        assert_eq!(a, b, "Same seed should produce same values");
    }

    #[test]
    fn test_mock_random_different_seeds() {
        let rng1 = MockRandom::new(1);
        let rng2 = MockRandom::new(2);

        let a = rng1.random_bytes_32();
        let b = rng2.random_bytes_32();

        assert_ne!(a, b, "Different seeds should produce different values");
    }

    #[test]
    fn test_mock_random_reset() {
        let rng = MockRandom::new(42);

        let a = rng.random_bytes_32();
        rng.reset();
        let b = rng.random_bytes_32();

        assert_eq!(a, b, "Reset should replay the sequence");
    }

    #[test]
    fn test_mock_random_fill_partial() {
        let rng = MockRandom::new(42);

        let mut buf = [0u8; 5];
        rng.fill_bytes(&mut buf);

        assert!(buf.iter().any(|&b| b != 0), "Should have non-zero bytes");
    }

    #[test]
    fn test_mock_random_clone_independent() {
        let rng1 = MockRandom::new(42);
        let _ = rng1.random_bytes_32(); // Advance counter

        let rng2 = rng1.clone();

        // Both should produce the same value since they share counter
        let a = rng1.random_bytes_32();
        // rng2's next call will get a different value since counter advanced
        let b = rng2.random_bytes_32();

        assert_ne!(
            a, b,
            "Clone shares counter, so values differ after first call"
        );
    }
}
