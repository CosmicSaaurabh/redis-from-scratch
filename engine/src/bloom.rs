//! A Bloom filter over SSTable keys.
//!
//! Every SSTable carries one. Without it, a point lookup for a key that is not
//! present must read an index block and a data block from every file whose key
//! range could contain it, which for a read-heavy workload over a deep tree is
//! the dominant cost. With it, the overwhelming majority of those files are
//! eliminated with one in-memory probe.
//!
//! The filter can say "definitely not here" and "probably here". It can never
//! say "definitely here", and it can never produce a false negative, which is
//! what makes it safe to use as a skip decision: a false positive costs one
//! wasted block read, while a false negative would lose data.

/// A Bloom filter sized for a known number of keys.
#[derive(Debug, Clone)]
pub struct Bloom {
    bits: Vec<u8>,
    /// Number of hash probes per key.
    hashes: u32,
}

/// Default bits per key.
///
/// Ten bits per key gives roughly a 1% false positive rate at the optimal
/// number of probes, which is the point where halving the error again starts
/// costing more memory than the saved I/O is worth.
pub const DEFAULT_BITS_PER_KEY: usize = 10;

impl Bloom {
    /// Builds a filter for `keys`, sized at `bits_per_key`.
    pub fn build<'a, I>(keys: I, count: usize, bits_per_key: usize) -> Bloom
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let hashes: Vec<u64> = keys.into_iter().map(hash_key).collect();
        let _ = count;
        Bloom::from_hashes(&hashes, bits_per_key)
    }

    /// Builds a filter from precomputed key hashes.
    ///
    /// An SSTable writer streams keys out to disk and cannot hold them all, but
    /// a Bloom filter has to be sized from the final key count. Keeping eight
    /// bytes per key instead of the key itself is what makes that affordable.
    pub fn from_hashes(hashes_of_keys: &[u64], bits_per_key: usize) -> Bloom {
        let bits_per_key = bits_per_key.max(1);
        let count = hashes_of_keys.len();
        // A filter with no keys still needs a byte, so that a reader never has
        // to special-case an empty vector.
        let num_bits = (count * bits_per_key).max(64);
        let num_bytes = num_bits.div_ceil(8);
        let num_bits = num_bytes * 8;

        // The error-minimising probe count is ln(2) * bits/key. Clamping keeps
        // a pathological configuration from spending all its time hashing.
        let hashes = ((bits_per_key as f64 * 0.69) as u32).clamp(1, 30);

        let mut bits = vec![0u8; num_bytes];
        for &h in hashes_of_keys {
            let (mut probe, delta) = seed_from_hash(h);
            for _ in 0..hashes {
                let bit = (probe as usize) % num_bits;
                bits[bit / 8] |= 1u8 << (bit % 8);
                probe = probe.wrapping_add(delta);
            }
        }
        Bloom { bits, hashes }
    }

    /// Reconstructs a filter from its encoded bytes.
    pub fn from_parts(bits: Vec<u8>, hashes: u32) -> Bloom {
        Bloom { bits, hashes }
    }

    /// Returns the raw bit array.
    pub fn bits(&self) -> &[u8] {
        &self.bits
    }

    /// Returns the probe count.
    pub fn hashes(&self) -> u32 {
        self.hashes
    }

    /// Reports whether `key` might be present.
    ///
    /// A `false` result is certain. A `true` result is a hint.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.bits.is_empty() || self.hashes == 0 {
            // A filter that was never built must not filter anything out.
            return true;
        }
        let num_bits = self.bits.len() * 8;
        let (mut h, delta) = Self::probe_seed(key);
        for _ in 0..self.hashes {
            let bit = (h as usize) % num_bits;
            if self.bits[bit / 8] & (1u8 << (bit % 8)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }

    fn probe_seed(key: &[u8]) -> (u32, u32) {
        seed_from_hash(hash_key(key))
    }
}

/// Hashes a key for filter construction.
pub fn hash_key(key: &[u8]) -> u64 {
    fnv1a64(key)
}

/// Derives the probe sequence from one hash.
///
/// This is Kirsch and Mitzenmacher's double hashing: k independent hashes are
/// simulated as `h1 + i*h2`, which has the same asymptotic false positive rate
/// as k real hashes while computing only one. For a filter probed on every
/// read that difference is the whole point.
fn seed_from_hash(h: u64) -> (u32, u32) {
    let h1 = h as u32;
    // Rotating rather than taking the high word keeps the delta from being
    // correlated with h1 for short keys.
    let h2 = (h >> 32) as u32;
    (h1, h2.rotate_left(15) | 1)
}

/// FNV-1a, chosen for having no dependencies and being fast on the short keys
/// a key-value store sees. The filter's error rate depends on the hash being
/// well distributed, not on it being cryptographic.
fn fnv1a64(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    // A final avalanche step, because FNV alone leaves the low bits weak and
    // the probe sequence takes both halves of the word.
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(n: usize) -> Vec<Vec<u8>> {
        (0..n).map(|i| format!("key:{i:08}").into_bytes()).collect()
    }

    #[test]
    fn never_produces_a_false_negative() {
        // This is the property the whole design rests on: if the filter ever
        // said "not here" about a key that is here, the engine would lose data.
        for n in [0usize, 1, 10, 1000, 10_000] {
            let ks = keys(n);
            let f = Bloom::build(ks.iter().map(|k| k.as_slice()), n, DEFAULT_BITS_PER_KEY);
            for k in &ks {
                assert!(
                    f.may_contain(k),
                    "false negative for {:?} with n={n}",
                    String::from_utf8_lossy(k)
                );
            }
        }
    }

    #[test]
    fn false_positive_rate_is_near_one_percent() {
        let n = 20_000;
        let ks = keys(n);
        let f = Bloom::build(ks.iter().map(|k| k.as_slice()), n, DEFAULT_BITS_PER_KEY);

        let mut fp = 0;
        let trials = 50_000;
        for i in 0..trials {
            let probe = format!("absent:{i:08}").into_bytes();
            if f.may_contain(&probe) {
                fp += 1;
            }
        }
        let rate = fp as f64 / trials as f64;
        assert!(
            rate < 0.03,
            "false positive rate {rate:.4} is far above the 1% design target"
        );
    }

    #[test]
    fn more_bits_per_key_lowers_the_error_rate() {
        let n = 10_000;
        let ks = keys(n);
        let measure = |bpk: usize| {
            let f = Bloom::build(ks.iter().map(|k| k.as_slice()), n, bpk);
            let trials = 20_000;
            let fp = (0..trials)
                .filter(|i| f.may_contain(format!("nope:{i:08}").as_bytes()))
                .count();
            fp as f64 / trials as f64
        };
        let sparse = measure(4);
        let dense = measure(16);
        assert!(
            dense < sparse,
            "16 bits/key ({dense:.4}) should beat 4 bits/key ({sparse:.4})"
        );
    }

    #[test]
    fn round_trips_through_its_encoded_form() {
        let ks = keys(500);
        let f = Bloom::build(ks.iter().map(|k| k.as_slice()), 500, DEFAULT_BITS_PER_KEY);
        let copy = Bloom::from_parts(f.bits().to_vec(), f.hashes());
        for k in &ks {
            assert!(copy.may_contain(k));
        }
    }

    #[test]
    fn an_empty_filter_admits_everything() {
        // A file written before filters existed, or one whose filter block
        // failed to load, must degrade to "check the file" rather than
        // "the key is absent".
        let f = Bloom::from_parts(Vec::new(), 0);
        assert!(f.may_contain(b"anything"));
    }

    #[test]
    fn handles_binary_and_empty_keys() {
        let ks: Vec<Vec<u8>> = vec![vec![], vec![0], vec![0xff; 300], b"\r\n\0".to_vec()];
        let f = Bloom::build(
            ks.iter().map(|k| k.as_slice()),
            ks.len(),
            DEFAULT_BITS_PER_KEY,
        );
        for k in &ks {
            assert!(f.may_contain(k));
        }
    }
}
