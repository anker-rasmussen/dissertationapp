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
    pub const fn new() -> Self {
        Self
    }
}
