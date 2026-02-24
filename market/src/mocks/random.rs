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
        Self::new(0x1234_5678_9ABC_DEF0)
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
    const fn mix(&self, counter: u64) -> u64 {
        let mut x = self.seed.wrapping_add(counter);
        x = x.wrapping_mul(0x517C_C1B7_2722_0A95);
        x ^= x >> 32;
        x = x.wrapping_mul(0x517C_C1B7_2722_0A95);
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
