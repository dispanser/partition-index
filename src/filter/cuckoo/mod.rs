pub mod growable;

use crate::filter::Filter;
use rand::Rng;
use std::{collections::hash_map::DefaultHasher, hash::Hasher};

use super::InsertResult;

#[derive(Debug)]
pub struct CuckooFilter {
    data: Vec<u16>, // 16 bit fingerprints, 0 marks invalid entry
    buckets: u64,
    entries_per_bucket: u64,
    items: u64, // number of fingerprints stored in the filter
}

// lingo:
// - bucket: as in the cuckoo paper, a list of entries. A value can be in one of two buckets.
// - entry: entries form a bucket. The number of entries per bucket is fixed via constructor arg.
// - slot: a single place in the array of fingerprints, containing one or zero fingerprints.
impl CuckooFilter {
    pub fn new(buckets: u64, buckets_per_entry: u64) -> Self {
        CuckooFilter {
            data: vec![0; (buckets * buckets_per_entry).try_into().unwrap()],
            buckets,
            entries_per_bucket: buckets_per_entry,
            items: 0,
        }
    }

    fn try_insert(self: &mut Self, fingerprint: u16, bucket: u64, tries_left: u8) -> InsertResult {
        assert!(bucket < self.buckets, "{} < {}", bucket, self.buckets);
        let start_slot = (bucket * self.entries_per_bucket) as usize;
        for b in start_slot..(start_slot + self.entries_per_bucket as usize) {
            if self.data[b] == fingerprint {
                return InsertResult::Duplicate;
            }
            if self.data[b] == 0 {
                self.data[b] = fingerprint;
                self.items += 1;
                return InsertResult::Success;
            }
        }
        if tries_left == 0 {
            return InsertResult::Rejected;
        }
        // Evicting the first entry. Determined by a fair dice roll.
        let slot = start_slot + rand::thread_rng().gen_range(0..self.entries_per_bucket) as usize;
        let evicted = self.data[slot];
        // replace already, otherwise we immediately find our fingerprint-to-evict
        self.data[slot] = fingerprint;
        let result = self.try_insert(
            evicted,
            flip_bucket(evicted, bucket, self.buckets),
            tries_left - 1,
        );
        if result == InsertResult::Rejected {
            self.data[slot] = evicted; // restore previous entry
            InsertResult::Rejected
        } else {
            result
        }
    }

    fn find_in_bucket(self: &Self, fingerprint: u16, bucket: u64) -> bool {
        assert!(bucket < self.buckets);
        let start_slot = (bucket * self.entries_per_bucket) as usize;
        for b in start_slot..(start_slot + self.entries_per_bucket as usize) {
            if self.data[b] == fingerprint {
                return true;
            }
        }
        false
    }
}

pub fn bucket(key: u64, buckets: u64) -> u64 {
    // let fp_hash = hash(fingerprint.into()) % buckets;
    // TODO: can we make this do without all the xor?
    // using the all-new "fp_hash - b" approach should obsolete that computation
    // ((hash(key) % buckets) ^ fp_hash) % buckets
    hash(key) % buckets
}

pub fn flip_bucket(fingerprint: u16, bucket: u64, buckets: u64) -> u64 {
    assert!(bucket < buckets, "bucket {} >= max of {}", bucket, buckets);
    let fp_hash = hash(fingerprint.into());
    fp_hash.wrapping_sub(bucket) % buckets
}

impl Filter for CuckooFilter {
    fn insert(self: &mut Self, key: u64) -> InsertResult {
        let fingerprint = fingerprint(key);
        let bucket = bucket(key, self.buckets);
        let other = flip_bucket(fingerprint, bucket, self.buckets);
        if self.find_in_bucket(fingerprint, bucket) || self.find_in_bucket(fingerprint, other) {
            InsertResult::Duplicate
        } else if self.find_in_bucket(0, other) {
            self.try_insert(fingerprint, other, u8::MAX)
        } else {
            self.try_insert(fingerprint, bucket, u8::MAX)
        }
    }

    fn contains(self: &Self, key: u64) -> bool {
        let fingerprint = fingerprint(key);
        let bucket = bucket(key, self.buckets);
        let alt = flip_bucket(fingerprint, bucket, self.buckets);
        self.find_in_bucket(fingerprint, bucket) || self.find_in_bucket(fingerprint, alt)
    }
}

#[inline]
pub fn hash(key: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write_u64(key);
    hasher.finish()
}

#[inline]
/// Create a 16-bit fingerprint for the given key_rot
/// 0 is an invalid fingerprint as it demarks an empty entry, so another
/// round of hashing is done until a valid fingerprint is created.
/// Valid fingerprints have a range of [1, 65536)
pub fn fingerprint(key: u64) -> u16 {
    let mut hasher = DefaultHasher::new();
    let mut key_rot = key;
    loop {
        hasher.write_u64(key_rot);
        key_rot = hasher.finish();
        if key_rot & 0xFFFF != 0 {
            break;
        }
    }
    (key_rot & 0xFFFF) as u16
}

#[cfg(test)]
mod tests {
    use super::CuckooFilter;
    use crate::filter::{correctness_tests::*, Filter, InsertResult};

    const INPUTS: u64 = 10_000;

    #[test]
    fn no_duplicate_inserts() {
        let mut cuckoo = CuckooFilter::new(10, 1);
        // 0 goes to buckets 0 and 9, occupying bucket 0 after insert
        assert_eq!(cuckoo.insert(0), InsertResult::Success);
        // 2 goes to buckets 0 and 2, evicting key 0 to bucket 9 on insert
        assert_eq!(cuckoo.insert(2), InsertResult::Success);
        // 0 inserted again, should not be inserted but identify the duplicate insert
        assert_eq!(cuckoo.insert(0), InsertResult::Duplicate);
    }

    #[test]
    fn no_false_negatives() {
        // 10 bits, 7 functions --> < 1% fp
        let mut pb = CuckooFilter::new(50000, 4);

        fill_from_range(&mut pb, 0..INPUTS);
        check_false_negatives(&mut pb, 0..INPUTS);
    }

    #[test]
    fn verify_false_positive_rate() {
        const SAMPLE: u64 = 100_000;

        // 10 bits, 7 functions --> < 1% fp
        let mut pb = CuckooFilter::new(50000, 4);
        fill_from_range(&mut pb, 0..INPUTS);

        let fp_rate = estimate_false_positive_rate(&mut pb, INPUTS..INPUTS + SAMPLE);
        assert!(
            fp_rate < 0.0001,
            "false positive rate: {:.3}% >= {:.3}",
            fp_rate * 100.0,
            0.0001
        );
    }
}

#[cfg(test)]
mod occupancy_tests {
    use crate::filter::{cuckoo::fingerprint, InsertResult};

    use super::{bucket, CuckooFilter};

    /// insert values into a cuckoo filter until it fails
    fn data_density(buckets: u64, entries_per_bucket: u64) -> f64 {
        let mut pb = CuckooFilter::new(buckets, entries_per_bucket);
        let mut inserted = 0;
        for i in 0..(buckets * entries_per_bucket + 1) {
            let fingerprint = fingerprint(i);
            let bucket = bucket(i, buckets);
            if pb.try_insert(fingerprint, bucket, u8::MAX) == InsertResult::Rejected {
                break;
            }
            inserted += 1;
        }
        inserted as f64 / (buckets * entries_per_bucket) as f64
    }

    #[test]
    fn one_entry() {
        // 2^16, 2^17, ... gives a lot of fingerprint clashes. I think that's because our
        // `fingerprint(key)` and `hash(key)` are basically the same function + % 2^16
        // leads to same fingerprints consistenly hitting the same buckets.
        let occupancy = data_density(1 << 5 - 1, 1);
        // 50% is what the paper says, not sure how I got 70 ;)
        assert!(occupancy > 0.70, "occupancy == {}, !> 0.70", occupancy);
    }

    #[test]
    /// according to the paper, occupancy should be 0.84
    fn two_entries() {
        // this has space for 2048 fingerprints
        let occupancy = data_density(1 << 10, 2);
        // 84% is what the paper says
        assert!(occupancy > 0.84, "occupancy == {}, !> 0.84", occupancy);
    }

    #[test]
    fn four_buckets() {
        // this has space for 4096 fingerprints
        let occupancy = data_density(1 << 10, 4);
        // 95% is what the paper says
        assert!(occupancy > 0.94, "occupancy == {}, !> 0.94", occupancy);
    }

    #[test]
    fn eight_buckets() {
        // this has space for 8192 fingerprints
        let occupancy = data_density(1 << 10, 8);
        // 98% is what the paper says
        assert!(occupancy > 0.97, "occupancy == {}, !> 0.97", occupancy);
    }
}

#[cfg(test)]
mod prop_tests {

    use crate::filter::cuckoo::{bucket, fingerprint, flip_bucket};
    use proptest::prelude::*;

    fn bucket_roundtrip(key: u64, buckets: u64) {
        let fingerprint = fingerprint(key);
        let b0 = bucket(key, buckets);
        let b1 = flip_bucket(fingerprint, b0, buckets);
        let b2 = flip_bucket(fingerprint, b1, buckets);
        assert_eq!(b0, b2, "b0 != b2: {} != {}", b0, b2);
    }

    proptest! {
        #[test]
        fn bucket_roundtrip_prop(key in 0 .. u64::MAX, buckets in 3u64..u32::MAX.into()) {
            bucket_roundtrip(key, buckets);
        }

        #[test]
        fn bucket_roundtrip_prop_pow_of_2(key in 0 .. u64::MAX, buckets in 3u32..30) {
            let buckets = 2u64.pow(buckets);
            bucket_roundtrip(key, buckets);
        }
    }
}
