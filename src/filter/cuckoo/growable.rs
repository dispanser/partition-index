use crate::filter::Filter;
use rand::Rng;

use super::{fingerprint, hash};

#[derive(Debug)]
pub struct GrowableCuckooFilter {
    data: Vec<Vec<u16>>, // 16 bit fingerprints, 0 marks invalid entry
    buckets: u64,
    entries_per_bucket: u64,
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

    pub fn entries_per_bucket(self: &Self) -> u64 {
        self.entries_per_bucket
    }

    pub fn items(self: &Self) -> u64 {
        self.items
    }

    pub fn buckets(self: &Self) -> u64 {
        self.buckets
    }

    fn try_insert(self: &mut Self, fingerprint: u16, bucket: u64, tries_left: u8) {
        assert!(bucket < self.buckets);
        let entries = &mut self.data[bucket as usize];

        if let Some(_) = entries.iter().find(|entry| **entry == fingerprint) {
            return;
        }

        if entries.len() < self.entries_per_bucket as usize {
            entries.push(fingerprint);
            self.items += 1;
            return;
        }

        // Insert failed, no space, let's grow
        if tries_left == 0 {
            self.entries_per_bucket += 1;
            entries.push(fingerprint);
            self.items += 1;
            return;
        }

        // Pick a random entry to evict. Non-random selection can lead to cycles.
        let entry = rand::thread_rng().gen_range(0..entries.len()) as usize;
        let evicted = entries[entry];

        // Replace value, otherwise we immediately find our fingerprint-to-evict
        entries[entry] = fingerprint;
        drop(entries); // recursive mut self call requires dropping our &mut bucket
        self.try_insert(evicted, self.flip_bucket(evicted, bucket), tries_left - 1);
    }

    fn find_in_bucket(self: &Self, fingerprint: u16, bucket: u64) -> bool {
        for entry in &self.data[bucket as usize] {
            if *entry == fingerprint {
                return true;
            }
        }
        false
    }

    fn bucket(self: &Self, key: u64, fingerprint: u16) -> u64 {
        let fp_hash = hash(fingerprint.into()) % self.buckets;
        ((hash(key) % self.buckets) ^ fp_hash) % self.buckets
    }

    fn flip_bucket(self: &Self, fingerprint: u16, bucket: u64) -> u64 {
        assert!(bucket < self.buckets, "bucket {}<{}", bucket, self.buckets);
        let fp_hash = hash(fingerprint.into()) % self.buckets;
        (bucket ^ fp_hash) % self.buckets
    }
}

impl Filter for GrowableCuckooFilter {
    fn insert(self: &mut Self, key: u64) {
        let fingerprint = fingerprint(key);
        self.try_insert(fingerprint, self.bucket(key, fingerprint), 5)
    }

    fn contains(self: &Self, key: u64) -> bool {
        let fingerprint = fingerprint(key);
        let bucket = self.bucket(key, fingerprint);
        let alt = self.flip_bucket(fingerprint, bucket);
        self.find_in_bucket(fingerprint, bucket) || self.find_in_bucket(fingerprint, alt)
    }
}

#[cfg(test)]
mod tests {
    use rand::{Rng, SeedableRng};

    use super::GrowableCuckooFilter;
    use crate::filter::{
        correctness_tests::*,
        cuckoo::{fingerprint, hash},
    };

    const INPUTS: u64 = 10_000;

    fn bucket(key: u64, buckets: u64) -> u64 {
        let fingerprint = fingerprint(key);
        let fp_hash = hash(fingerprint.into()) % buckets;
        ((hash(key) % buckets) ^ fp_hash) % buckets
    }

    fn flip_bucket(fingerprint: u16, bucket: u64, buckets: u64) -> u64 {
        assert!(bucket < buckets, "bucket {} >= max of {}", bucket, buckets);
        let fp_hash = hash(fingerprint.into()) % buckets;
        (bucket ^ fp_hash) % buckets
    }

    fn bucket_roundtrip(key: u64, buckets: u64) {
        let fingerprint = fingerprint(key);
        let b0 = bucket(key, buckets);
        let b1 = flip_bucket(fingerprint, b0, buckets);
        let b2 = flip_bucket(fingerprint, b1, buckets);
        let b3 = flip_bucket(fingerprint, b2, buckets);
        let b4 = flip_bucket(fingerprint, b3, buckets);
        let b5 = flip_bucket(fingerprint, b4, buckets);
        assert_eq!(b0, b2, "b0 != b2: {} != {}", b0, b2);
        assert_eq!(b1, b3, "b1 != b3: {} != {}", b1, b3);
        assert_eq!(b2, b4, "b2 != b4: {} != {}", b2, b4);
        assert_eq!(b3, b5, "b3 != b5: {} != {}", b3, b5);
    }

    #[test]
    fn bucket_roundtrips() {
        bucket_roundtrip(0, 10);
        bucket_roundtrip(13, 999);
        let mut data_rng = rand_xoshiro::Xoshiro256PlusPlus::seed_from_u64(13);
        for _ in 0..100 {
            bucket_roundtrip(data_rng.gen(), data_rng.gen::<u32>().into());
        }
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
    use crate::filter::cuckoo::{fingerprint, hash};

    use super::GrowableCuckooFilter;

    /// insert values into a cuckoo filter until it fails
    fn data_density(buckets: u64, entries_per_bucket: u64) -> (u64, f64) {
        let mut pb = GrowableCuckooFilter::new(buckets);
        for i in 0..(buckets * entries_per_bucket + 1) {
            pb.try_insert(fingerprint(i), hash(i) % buckets, u8::MAX);
            // break as soon as we cross the desired number of entries per bucket
            if pb.entries_per_bucket > entries_per_bucket {
                break;
            }
        }
        (
            pb.items - 1,
            (pb.items - 1) as f64 / (buckets * entries_per_bucket) as f64,
        )
    }

    // 2^16, 2^17, ... gives a lot of fingerprint clashes. I think that's because our
    // `fingerprint(key)` and `hash(key)` are basically the same function + % 2^16
    // leads to same fingerprints consistenly hitting the same buckets.
    // Possible fix: hash differently, possibly by initializing hashers differently.
    #[test]
    #[ignore]
    fn hash_clash() {
        let (inserted, occupancy) = data_density(1 << 16, 1);
        eprintln!("tp;inserted: {}, occupancy {}", inserted, occupancy);
        // 84% is what the paper says
        assert!(occupancy < 1.0, "occupancy == {}, !< 0.70", occupancy);
    }

    #[test]
    fn one_entry() {
        let (inserted, occupancy) = data_density((1 << 10) - 1, 1);
        eprintln!("tp;inserted: {}, occupancy {}", inserted, occupancy);
        // 50% is what the paper says
        assert!(occupancy > 0.50, "occupancy == {}, !> 0.50", occupancy);
    }

    #[test]
    /// according to the paper, occupancy should be 0.84
    fn two_entries() {
        // this has space for 2046 fingerprints
        let (inserted, occupancy) = data_density((1 << 10) - 1, 2);
        eprintln!("tp;inserted: {}, occupancy {}", inserted, occupancy);
        // 84% is what the paper says
        assert!(occupancy > 0.84, "occupancy == {}, !> 0.84", occupancy);
    }

    #[test]
    fn four_buckets() {
        // this has space for 4092 fingerprints
        let (inserted, occupancy) = data_density((1 << 10) - 1, 4);
        eprintln!("tp;inserted: {}, occupancy {}", inserted, occupancy);
        // 95% is what the paper says
        assert!(occupancy > 0.95, "occupancy == {}, !> 0.95", occupancy);
    }

    #[test]
    fn eight_buckets() {
        // this has space for 8184 fingerprints
        let (inserted, occupancy) = data_density((1 << 10) - 1, 8);
        eprintln!("tp;inserted: {}, occupancy {}", inserted, occupancy);
        // 98% is what the paper says
        assert!(occupancy > 0.98, "occupancy == {}, !> 0.98", occupancy);
    }
}
