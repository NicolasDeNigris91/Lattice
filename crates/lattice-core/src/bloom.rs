//! Bloom filter for `SSTable` existence pruning.
//!
//! INVARIANT: a key inserted via [`BloomFilter::insert`] is reported by
//! [`BloomFilter::might_contain`] as `true`. The converse is the whole
//! point: a key never inserted is reported as `true` only with the
//! configured false-positive probability (about one percent at the
//! default sizing of ten bits per key, seven hashes).
//!
//! Hashing uses one 128-bit `xxh3` digest per key, split into two
//! 64-bit halves `(h1, h2)`. Position `i` is `h1 + i * h2 mod m`. This
//! double-hashing scheme matches Kirsch and Mitzenmacher's analysis,
//! producing the same false-positive rate as `k` independent hashes
//! while doing only one digest per key.

use xxhash_rust::xxh3::xxh3_128;

use crate::error::{Error, Result};

const BITS_PER_KEY: usize = 10;
const NUM_HASHES: u32 = 7;

/// Counting-free Bloom filter.
#[derive(Debug, Clone)]
pub(crate) struct BloomFilter {
    bits: Vec<u64>,
    num_bits: u64,
    num_hashes: u32,
}

impl BloomFilter {
    /// Empty filter sized for `expected_keys` keys at the default
    /// false-positive rate of about one percent.
    pub(crate) fn with_capacity(expected_keys: usize) -> Self {
        let raw_bits = expected_keys.max(1).saturating_mul(BITS_PER_KEY);
        let num_words = raw_bits.div_ceil(64);
        let num_bits = u64::try_from(num_words.saturating_mul(64)).unwrap_or(u64::MAX);
        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes: NUM_HASHES,
        }
    }

    pub(crate) fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = double_hash(key);
        for i in 0..self.num_hashes {
            let pos = h1.wrapping_add(u64::from(i).wrapping_mul(h2)) % self.num_bits;
            let word = (pos / 64) as usize;
            let bit = pos % 64;
            self.bits[word] |= 1u64 << bit;
        }
    }

    pub(crate) fn might_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = double_hash(key);
        for i in 0..self.num_hashes {
            let pos = h1.wrapping_add(u64::from(i).wrapping_mul(h2)) % self.num_bits;
            let word = (pos / 64) as usize;
            let bit = pos % 64;
            if self.bits[word] & (1u64 << bit) == 0 {
                return false;
            }
        }
        true
    }

    /// Encode to a byte vector, framed with `num_bits` and `num_hashes`.
    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + 4 + self.bits.len() * 8);
        out.extend_from_slice(&self.num_bits.to_le_bytes());
        out.extend_from_slice(&self.num_hashes.to_le_bytes());
        for word in &self.bits {
            out.extend_from_slice(&word.to_le_bytes());
        }
        out
    }

    /// Decode from bytes produced by [`BloomFilter::serialize`].
    pub(crate) fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 12 {
            return Err(Error::MalformedFormat("bloom header truncated"));
        }
        let num_bits = u64::from_le_bytes(bytes[0..8].try_into().expect("8"));
        let num_hashes = u32::from_le_bytes(bytes[8..12].try_into().expect("4"));
        let num_words = (num_bits / 64) as usize;
        let words_bytes = &bytes[12..];
        if words_bytes.len() != num_words * 8 {
            return Err(Error::MalformedFormat("bloom bits truncated"));
        }
        let mut bits = Vec::with_capacity(num_words);
        for chunk in words_bytes.chunks_exact(8) {
            bits.push(u64::from_le_bytes(chunk.try_into().expect("8")));
        }
        Ok(Self {
            bits,
            num_bits,
            num_hashes,
        })
    }
}

/// One 128-bit `xxh3` digest split into two 64-bit halves, used for the
/// Kirsch-Mitzenmacher double-hashing scheme. Truncation is intentional:
/// we want the low 64 bits of each half, not a value-preserving narrow.
#[inline]
#[allow(clippy::cast_possible_truncation)]
fn double_hash(key: &[u8]) -> (u64, u64) {
    let h = xxh3_128(key);
    ((h >> 64) as u64, h as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let mut bloom = BloomFilter::with_capacity(1000);
        for i in 0u32..1000 {
            bloom.insert(&i.to_be_bytes());
        }
        for i in 0u32..1000 {
            assert!(
                bloom.might_contain(&i.to_be_bytes()),
                "key {i} reported absent after insertion"
            );
        }
    }

    #[test]
    fn empty_filter_rejects_everything() {
        let bloom = BloomFilter::with_capacity(100);
        for i in 0u32..200 {
            assert!(!bloom.might_contain(&i.to_be_bytes()));
        }
    }

    #[test]
    fn false_positive_rate_under_two_percent() {
        let mut bloom = BloomFilter::with_capacity(10_000);
        for i in 0u32..10_000 {
            bloom.insert(&i.to_be_bytes());
        }
        // Probe keys that were never inserted.
        let mut false_positives = 0;
        for i in 100_000u32..110_000 {
            if bloom.might_contain(&i.to_be_bytes()) {
                false_positives += 1;
            }
        }
        // Theoretical FP at 10 bits per key, 7 hashes is ~0.82%. We assert
        // a generous 2% to absorb hash variance.
        assert!(
            false_positives < 200,
            "{false_positives} false positives in 10000 probes"
        );
    }

    #[test]
    fn round_trip_serialize_deserialize() {
        let mut bloom = BloomFilter::with_capacity(50);
        for i in 0u32..50 {
            bloom.insert(&i.to_be_bytes());
        }
        let bytes = bloom.serialize();
        let restored = BloomFilter::deserialize(&bytes).unwrap();
        for i in 0u32..50 {
            assert!(restored.might_contain(&i.to_be_bytes()));
        }
    }

    #[test]
    fn deserialize_rejects_truncated_input() {
        assert!(BloomFilter::deserialize(&[]).is_err());
        assert!(BloomFilter::deserialize(&[0u8; 4]).is_err());
    }
}
