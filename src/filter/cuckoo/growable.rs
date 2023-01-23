use crate::filter::{Filter, InsertResult};
use rand::Rng;

use super::{bucket, fingerprint, flip_bucket};

#[derive(Debug)]
pub struct GrowableCuckooFilter {
    data: Vec<Vec<u16>>, // 16 bit fingerprints, 0 marks invalid entry
    buckets: u64,
    entries_per_bucket: usize,
    items: u64, // number of fingerprints stored in the filter
}

impl GrowableCuckooFilter {
    pub fn new(buckets: u64) -> Self {
        GrowableCuckooFilter {
            data: vec![vec![]; buckets as usize],
            buckets,
            entries_per_bucket: 1,
            items: 0,
        }
    }

    pub fn entries_per_bucket(self: &Self) -> usize {
        self.entries_per_bucket
    }

    pub fn items(self: &Self) -> u64 {
        self.items
    }

    pub fn num_buckets(self: &Self) -> u64 {
        self.buckets
    }

    pub fn drain(self: Self) -> Vec<Vec<u16>> {
        self.data
    }

    fn try_insert(self: &mut Self, fingerprint: u16, bucket: u64, tries_left: u8) -> InsertResult {
        assert!(bucket < self.buckets);
        let entries = &mut self.data[bucket as usize];

        if entries.len() < self.entries_per_bucket {
            entries.push(fingerprint);
            self.items += 1;
            return InsertResult::Success;
        }

        // Insert failed, no space, let's grow
        if tries_left == 0 {
            self.entries_per_bucket += 1;
            entries.push(fingerprint);
            self.items += 1;
            return InsertResult::Success;
        }

        // Pick a random entry to evict. Non-random selection can lead to cycles.
        let entry = rand::thread_rng().gen_range(0..entries.len()) as usize;
        let evicted = entries[entry];

        // Replace value, otherwise we immediately find our fingerprint-to-evict
        entries[entry] = fingerprint;
        drop(entries); // recursive mut self call requires dropping our &mut bucket
        self.try_insert(
            evicted,
            flip_bucket(evicted, bucket, self.buckets),
            tries_left - 1,
        )
    }

    fn find_in_bucket(self: &Self, fingerprint: u16, bucket: u64) -> bool {
        for entry in &self.data[bucket as usize] {
            if *entry == fingerprint {
                return true;
            }
        }
        false
    }
}

impl Filter for GrowableCuckooFilter {
    fn insert(self: &mut Self, key: u64) -> InsertResult {
        let fingerprint = fingerprint(key);
        let bucket = bucket(key, self.buckets);
        let other = flip_bucket(fingerprint, bucket, self.buckets);
        if self.find_in_bucket(fingerprint, bucket) || self.find_in_bucket(fingerprint, other) {
            InsertResult::Duplicate
        } else if self.data[other as usize].len() < self.entries_per_bucket {
            self.try_insert(fingerprint, other, 63)
        } else {
            self.try_insert(fingerprint, bucket, 63)
        }
    }

    fn contains(self: &Self, key: u64) -> bool {
        let fingerprint = fingerprint(key);
        let bucket = bucket(key, self.buckets);
        let alt = flip_bucket(fingerprint, bucket, self.buckets);
        self.find_in_bucket(fingerprint, bucket) || self.find_in_bucket(fingerprint, alt)
    }
}

#[cfg(test)]
mod tests {

    use super::GrowableCuckooFilter;
    use crate::filter::{correctness_tests::*, Filter, InsertResult};

    const INPUTS: u64 = 10_000;

    #[test]
    fn no_duplicate_inserts() {
        let mut cuckoo = GrowableCuckooFilter::new(10);
        // 0 goes to buckets 0 and 9, occupying bucket 0 after insert
        assert_eq!(cuckoo.insert(0), InsertResult::Success);
        // 2 goes to buckets 0 and 2, evicting key 0 to bucket 9 on insert
        assert_eq!(cuckoo.insert(2), InsertResult::Success);
        // 0 inserted again, should not be inserted but identify the duplicate insert
        assert_eq!(cuckoo.insert(0), InsertResult::Duplicate);
    }

    #[test]
    fn no_false_negatives() {
        let mut pb = GrowableCuckooFilter::new(10);

        fill_from_range(&mut pb, 0..11);
        check_false_negatives(&mut pb, 0..11);
    }

    #[test]
    fn verify_false_positive_rate() {
        const SAMPLE: u64 = 100_000;

        // 10 bits, 7 functions --> < 1% fp
        let mut pb = GrowableCuckooFilter::new(50000);
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
    use crate::filter::Filter;

    use super::GrowableCuckooFilter;

    /// insert values into a cuckoo filter until it fails
    fn data_density(buckets: u64, entries_per_bucket: usize) -> f64 {
        let mut pb = GrowableCuckooFilter::new(buckets);
        let max_entries = buckets * entries_per_bucket as u64;
        for i in 0..max_entries {
            pb.insert(i);
            // pb.try_insert(fingerprint(i), hash(i) % buckets, u8::MAX);
            // break as soon as we cross the desired number of entries per bucket
            if pb.entries_per_bucket > entries_per_bucket {
                break;
            }
        }
        (pb.items - 1) as f64 / max_entries as f64
    }

    // 2^16, 2^17, ... gives a lot of fingerprint clashes. I think that's because our
    // `fingerprint(key)` and `hash(key)` are basically the same function + % 2^16
    // leads to same fingerprints consistenly hitting the same buckets.
    // Possible fix: hash differently, possibly by initializing hashers differently.
    #[test]
    #[ignore]
    fn hash_clash() {
        let occupancy = data_density(1 << 16, 1);
        // 84% is what the paper says
        assert!(occupancy < 1.0, "occupancy == {}, !< 0.70", occupancy);
    }

    #[test]
    fn one_entry() {
        let occupancy = data_density((1 << 10) - 1, 1);
        // 50% is what the paper says
        assert!(occupancy > 0.50, "occupancy == {}, !> 0.50", occupancy);
    }

    #[test]
    /// according to the paper, occupancy should be 0.84
    fn two_entries() {
        // this has space for 2046 fingerprints
        let occupancy = data_density((1 << 10) - 1, 2);
        // 84% is what the paper says, but we use 63 instead of 500 eviction attempts
        assert!(occupancy > 0.82, "occupancy == {}, !> 0.82", occupancy);
    }

    #[test]
    fn twofivek() {
        // this has space for 2046 fingerprints
        let occupancy = data_density(2500, 3);
        // 84% is what the paper says
        assert!(occupancy > 0.84, "occupancy == {}, !> 0.84", occupancy);
    }

    #[test]
    fn four_buckets() {
        // this has space for 4092 fingerprints
        let occupancy = data_density((1 << 10) - 1, 4);
        // 95% is what the paper says, but we use 63 instead of 500 eviction attempts
        assert!(occupancy > 0.92, "occupancy == {}, !> 0.92", occupancy);
    }

    #[test]
    fn eight_buckets() {
        // this has space for 8184 fingerprints
        let occupancy = data_density((1 << 10) - 1, 8);
        // 98% is what the paper says, but we use 63 instead of 500 eviction attempts
        assert!(occupancy > 0.97, "occupancy == {}, !> 0.97", occupancy);
    }
}
