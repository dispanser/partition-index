use crate::filter::Filter;
use std::{collections::hash_map::DefaultHasher, hash::Hasher};

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
        let mut iteration = self.d;
        let bits_per_slot = (u64::BITS - self.m.leading_zeros() + 2) as u64;
        let slots_per_hash = 64 / bits_per_slot as u64;
        for _hash in 0..(self.d + slots_per_hash - 1) / slots_per_hash {
            hasher.write_u64(key_rot);
            key_rot = hasher.finish();
            for from in 0..std::cmp::min(slots_per_hash, iteration) {
                let bits = (key_rot >> (from * bits_per_slot)) & ((1 << bits_per_slot) - 1);
                let bit_to_set = bits % self.m;
                self.bits[(bit_to_set >> 3) as usize] |= 1 << (bit_to_set & 7) as u8;
                iteration -= 1;
            }
        }
    }

    fn contains(self: &Self, key: u64) -> bool {
        let mut hasher = DefaultHasher::new();
        let mut key_rot = key;
        let mut iteration = self.d;
        let bits_per_slot = (u64::BITS - self.m.leading_zeros() + 2) as u64;
        let slots_per_hash = 64 / bits_per_slot as u64;
        for _hash in 0..(self.d + slots_per_hash - 1) / slots_per_hash {
            hasher.write_u64(key_rot);
            key_rot = hasher.finish();
            for from in 0..std::cmp::min(slots_per_hash, iteration) {
                let bits = (key_rot >> (from * bits_per_slot)) & ((1 << bits_per_slot) - 1);
                let bit_to_read = bits % self.m;
                if (self.bits[(bit_to_read >> 3) as usize] >> (bit_to_read & 7) as u8) & 1 == 0 {
                    return false;
                }
                iteration -= 1;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::PaperBloom;
    use crate::filter::correctness_tests::*;

    const INPUTS: u64 = 10_000;

    #[test]
    fn no_false_negatives() {
        // 10 bits, 7 functions --> < 1% fp
        let mut pb = PaperBloom::new(7, 100000);

        fill_from_range(&mut pb, 0..INPUTS);
        check_false_negatives(&mut pb, 0..INPUTS);
    }

    #[test]
    fn verify_false_positive_rate() {
        const SAMPLE: u64 = 100_000;

        // 10 bits, 7 functions --> < 1% fp
        let mut pb = PaperBloom::new(7, 100000);
        fill_from_range(&mut pb, 0..INPUTS);

        let fp_rate = estimate_false_positive_rate(&mut pb, INPUTS..INPUTS + SAMPLE);
        assert!(
            fp_rate < 0.01,
            "false positive rate: {:.3}% >= {:.3}",
            fp_rate * 100.0,
            0.01
        );
    }
}
