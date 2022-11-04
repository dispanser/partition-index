use crate::filter::Filter;
use rand::{thread_rng, Rng};
use std::{collections::hash_map::DefaultHasher, hash::Hasher};

pub struct CuckooFilter {
    data: Vec<u16>, // 16 bit fingerprints, 0 marks invalid entry
    slots: u64,
    buckets_per_entry: u64,
}

impl CuckooFilter {
    pub fn new(slots: u64, buckets_per_entry: u64) -> Self {
        CuckooFilter {
            data: vec![0; (slots * buckets_per_entry).try_into().unwrap()],
            slots,
            buckets_per_entry,
        }
    }

    // TODO: recursively call ourselves with some evicted key when all buckets are occupied
    fn try_insert(self: &mut Self, fingerprint: u16, slot: u64, tries_left: u8) -> bool {
        let start_slot = ((slot % self.slots) * self.buckets_per_entry) as usize;
        for b in start_slot..(start_slot + self.buckets_per_entry as usize) {
            if self.data[b] == 0 || self.data[b] == fingerprint {
                self.data[b] = fingerprint;
                return true;
            }
        }
        if tries_left == 0 {
            return false;
        }
        // Evicting the first entry. Determined by a fair dice roll.
        let bucket = start_slot + rand::thread_rng().gen_range(0..self.buckets_per_entry) as usize;
        let evicted = self.data[bucket];
        // replace already, otherwise we immediately find our fingerprint-to-evict
        self.data[bucket] = fingerprint;
        if self.try_insert(evicted, slot ^ hash(evicted as u64), tries_left - 1) {
            true
        } else {
            self.data[bucket] = evicted; // restore previous entry
            false
        }
    }

    fn find_in_slot(self: &Self, fingerprint: u16, slot: u64) -> bool {
        let start_slot = ((slot % self.slots) * self.buckets_per_entry) as usize;
        for b in start_slot..(start_slot + self.buckets_per_entry as usize) {
            if self.data[b] == fingerprint {
                return true;
            }
        }
        false
    }
}

impl Filter for CuckooFilter {
    fn insert(self: &mut Self, key: u64) {
        let fingerprint = fingerprint(key);
        if !self.try_insert(fingerprint, hash(key), u8::MAX) {
            panic!("failed to insert: implement eviction!");
        }
    }

    fn contains(self: &Self, key: u64) -> bool {
        let fingerprint = fingerprint(key);
        let b1 = hash(key);
        self.find_in_slot(fingerprint, b1)
            || self.find_in_slot(fingerprint, b1 ^ hash(fingerprint.into()))
    }
}

#[inline]
fn hash(key: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write_u64(key);
    hasher.finish()
}

#[inline]
/// Create a 16-bit fingerprint for the given key_rot
/// 0 is an invalid fingerprint as it demarks an empty entry, so another
/// round of hashing is done until a valid fingerprint is created.
/// Valid fingerprints have a range of [1, 65536)
fn fingerprint(key: u64) -> u16 {
    let mut hasher = DefaultHasher::new();
    let mut key_rot = key;
    loop {
        hasher.write_u64(key_rot);
        key_rot = hasher.finish();
        if key_rot & 0xFFFF != 0 {
            break;
        }
        eprintln!("tp;repeatedly calling hash b/c we saw 0 at {}", key_rot);
    }
    (key_rot & 0xFFFF) as u16
}

#[cfg(test)]
mod tests {
    use super::{CuckooFilter, Filter};

    #[test]
    fn no_false_negatives() {
        const SAMPLE_SIZE: u64 = 10_000;
        const SAMPLES: u64 = 100_000;
        let mut pb = CuckooFilter::new(50000, 4);
        (0..SAMPLE_SIZE).for_each(|key| pb.insert(key));
        for i in 0..SAMPLE_SIZE {
            assert!(pb.contains(i));
        }
        let mut false_positives = 0;
        for i in SAMPLE_SIZE..(SAMPLE_SIZE + SAMPLES) {
            if pb.contains(i) {
                false_positives += 1;
            }
        }
        let fp_rate = false_positives as f64 / SAMPLES as f64;
        eprintln!(
            "tp;false positive rate: {:.3}% from {} false positives",
            fp_rate * 100.0,
            false_positives
        );
        // we should see 2 * b / 2^16 == 0.006%
        assert!(fp_rate < 0.0001);
    }
}

#[cfg(test)]
mod occupancy_tests {
    use crate::filter::cuckoo::{fingerprint, hash};

    use super::CuckooFilter;

    /// insert values into a cuckoo filter until it fails
    fn data_density(slots: u64, entries_per_slot: u64) -> (u64, f64) {
        let mut pb = CuckooFilter::new(slots, entries_per_slot);
        let mut inserted = 0;
        for i in 0..(slots * entries_per_slot + 1) {
            if !pb.try_insert(fingerprint(i), hash(i), u8::MAX) {
                break;
            }
            inserted += 1;
        }
        (
            inserted,
            inserted as f64 / (slots * entries_per_slot) as f64,
        )
    }

    #[test]
    /// according to the paper, occupancy should be 0.84
    fn two_entries() {
        // this has space for 2048 fingerprints
        let (inserted, occupancy) = data_density(1 << 10, 2);
        eprintln!("tp;inserted: {}, occupancy {}", inserted, occupancy);
        // 84% is what the paper says
        assert!(occupancy > 0.84);
    }

    #[test]
    fn four_buckets() {
        // this has space for 2048 fingerprints
        let (inserted, occupancy) = data_density(1 << 10, 4);
        eprintln!("tp;inserted: {}, occupancy {}", inserted, occupancy);
        // 95% is what the paper says
        assert!(occupancy > 0.94);
    }

    #[test]
    fn eight_buckets() {
        // this has space for 2048 fingerprints
        let (inserted, occupancy) = data_density(1 << 10, 8);
        eprintln!("tp;inserted: {}, occupancy {}", inserted, occupancy);
        // 98% is what the paper says
        assert!(occupancy > 0.97);
    }
}
