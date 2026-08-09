use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use sha2::{Digest, Sha256};

/// A deterministic RNG with a stable seed-expansion scheme.
#[derive(Clone, Debug)]
pub struct DeterministicRng {
    seed: u64,
    inner: ChaCha20Rng,
}

impl DeterministicRng {
    /// Constructs a stream from a portable integer seed.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"agentforge-test-seed-v1\0");
        hasher.update(seed.to_be_bytes());
        let expanded: [u8; 32] = hasher.finalize().into();
        Self {
            seed,
            inner: ChaCha20Rng::from_seed(expanded),
        }
    }

    /// Returns the integer seed recorded in evidence and failure artifacts.
    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the next `u64` from the stream.
    pub fn next_u64(&mut self) -> u64 {
        self.inner.next_u64()
    }

    /// Fills a byte slice from the deterministic stream.
    pub fn fill_bytes(&mut self, destination: &mut [u8]) {
        self.inner.fill_bytes(destination);
    }

    /// Samples uniformly from `[start, end)`.
    ///
    /// # Panics
    ///
    /// Panics when `start >= end`.
    pub fn range_u64(&mut self, start: u64, end: u64) -> u64 {
        self.inner.random_range(start..end)
    }

    /// Samples uniformly from `[0, upper)`.
    ///
    /// # Panics
    ///
    /// Panics when `upper` is zero.
    pub fn index(&mut self, upper: usize) -> usize {
        self.inner.random_range(0..upper)
    }

    /// Returns true with probability `numerator / denominator`.
    ///
    /// # Panics
    ///
    /// Panics for a zero denominator or a numerator larger than it.
    pub fn ratio(&mut self, numerator: u32, denominator: u32) -> bool {
        assert!(denominator > 0, "ratio denominator must be non-zero");
        assert!(
            numerator <= denominator,
            "ratio numerator must not exceed denominator"
        );
        self.inner.random_ratio(numerator, denominator)
    }

    /// Shuffles a slice using this stream.
    pub fn shuffle<T>(&mut self, values: &mut [T]) {
        for upper in (1..values.len()).rev() {
            let selected = self.inner.random_range(0..=upper);
            values.swap(upper, selected);
        }
    }

    /// Derives a new stream without consuming the parent stream.
    #[must_use]
    pub fn fork(&self, label: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"agentforge-test-fork-v1\0");
        hasher.update(self.seed.to_be_bytes());
        hasher.update(label.as_bytes());
        let digest: [u8; 32] = hasher.finalize().into();
        let mut seed_bytes = [0_u8; 8];
        seed_bytes.copy_from_slice(&digest[..8]);
        Self::new(u64::from_be_bytes(seed_bytes))
    }
}
