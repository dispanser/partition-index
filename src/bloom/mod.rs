use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

/// Basic Filter trait, not constrained to bloom filters
pub trait Filter {
    fn insert(self: &mut Self, key: u64);

    fn contains(self: &Self, key: u64) -> bool;

    fn fp_rate(self: &Self) -> f64;
}

/// Basic implementation of a bloom filter following the paper as closely as I can.
pub struct PaperBloom {
    bits: Vec<u8>,
    m: u64,
    d: u64,
}

impl PaperBloom {
    pub fn new(d: u64, m: u64) -> Self {
        let size = if m / 8 * 8 == m { m / 8 } else { m / 8 + 1 };
        PaperBloom {
            bits: vec![0; size as usize],
            d,
            m,
        }
    }
}

impl Filter for PaperBloom {
    fn insert(self: &mut Self, key: u64) {
        let mut hasher = DefaultHasher::new();
        let mut key_rot = key;
        for _iteration in 0..self.d {
            hasher.write_u64(key_rot);
            key_rot = hasher.finish();
            let bit_to_set = key_rot % self.m;
            self.bits[(bit_to_set >> 3) as usize] |= 1 << (bit_to_set & 7) as u8;
        }
    }

    fn contains(self: &Self, key: u64) -> bool {
        let mut hasher = DefaultHasher::new();
        let mut key_rot = key;
        for _iteration in 0..self.d {
            hasher.write_u64(key_rot);
            key_rot = hasher.finish();
            let bit_to_set = key_rot % self.m;
            if (self.bits[(bit_to_set >> 3) as usize] >> (bit_to_set & 7) as u8) & 1 == 0 {
                return false;
            }
        }
        true
    }

    fn fp_rate(self: &Self) -> f64 {
        0.1
    }
}

#[cfg(test)]
mod tests {
    use super::{Filter, PaperBloom};
    #[test]
    fn no_false_negatives() {
        const SAMPLE_SIZE: u64 = 1_000;
        // const SAMPLE_SIZE: usize = 10_000_000;
        const SAMPLES: u64 = 1_000_000;
        let mut pb = PaperBloom::new(3, 5000);
        (0..SAMPLE_SIZE).for_each(|key| pb.insert(key));
        for i in 0..SAMPLE_SIZE {
            assert!(pb.contains(i));
        }
        let mut pos = 0;
        for i in SAMPLE_SIZE..(SAMPLE_SIZE + SAMPLES) {
            if pb.contains(i) {
                pos += 1;
            }
        }
        eprintln!(
            "tp;false positive rate: {:.2}%",
            pos as f64 / SAMPLES as f64 * 100.0
        );
        assert!(false);
    }
    // use rand::prelude::thread_rng;
    // let mut rng = thread_rng();
}
