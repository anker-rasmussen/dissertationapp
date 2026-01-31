//! Random source abstraction for testable random number generation.

use rand::RngCore;

/// Trait for providing random bytes.
///
/// This abstraction allows code that depends on random numbers to be
/// tested with deterministic, controllable values.
pub trait RandomSource: Send + Sync {
    /// Fill the destination buffer with random bytes.
    fn fill_bytes(&self, dest: &mut [u8]);

    /// Generate a random 32-byte array (useful for nonces and keys).
    fn random_bytes_32(&self) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        self.fill_bytes(&mut bytes);
        bytes
    }

    /// Generate a random 12-byte array (useful for GCM nonces).
    fn random_bytes_12(&self) -> [u8; 12] {
        let mut bytes = [0u8; 12];
        self.fill_bytes(&mut bytes);
        bytes
    }
}

/// Production implementation using the thread-local RNG.
#[derive(Debug, Clone, Copy, Default)]
pub struct ThreadRng;

impl RandomSource for ThreadRng {
    fn fill_bytes(&self, dest: &mut [u8]) {
        rand::thread_rng().fill_bytes(dest);
    }
}

impl ThreadRng {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_rng_fills_bytes() {
        let rng = ThreadRng::new();
        let mut buf = [0u8; 32];

        rng.fill_bytes(&mut buf);

        // Very unlikely to be all zeros after random fill
        assert!(buf.iter().any(|&b| b != 0), "Buffer should have non-zero bytes");
    }

    #[test]
    fn test_thread_rng_produces_different_values() {
        let rng = ThreadRng::new();

        let a = rng.random_bytes_32();
        let b = rng.random_bytes_32();

        // Extremely unlikely to be equal
        assert_ne!(a, b, "Two random values should differ");
    }

    #[test]
    fn test_random_bytes_12() {
        let rng = ThreadRng::new();
        let bytes = rng.random_bytes_12();

        assert_eq!(bytes.len(), 12);
        assert!(bytes.iter().any(|&b| b != 0), "Should have non-zero bytes");
    }
}
