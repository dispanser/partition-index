use crate::filter::cuckoo::{bucket, fingerprint, flip_bucket, growable};
use crate::filter::Filter;
use crate::index::{PartitionFilter, PartitionIndex};

pub struct CuckooIndex<P>
where
    P: Clone,
{
    partitions: Vec<(P, usize)>,
    buckets: Vec<Vec<u16>>,
    slots: usize,
}

impl<P> CuckooIndex<P>
where
    P: std::clone::Clone,
{
    pub fn new(buckets: u64) -> Self {
        Self {
            partitions: vec![],
            buckets: vec![vec![]; buckets as usize],
            slots: 0,
        }
    }
}

impl<P> PartitionFilter<P> for CuckooIndex<P>
where
    P: std::clone::Clone,
{
    fn query(self: &Self, key: u64) -> Vec<P> {
        let fingerprint = fingerprint(key);
        let bucket1 = bucket(key, self.buckets.len() as u64);
        let bucket2 = flip_bucket(fingerprint, bucket1, self.buckets.len() as u64) as usize;
        eprintln!(
            "tp;query({}, {}, {}, {}",
            key, fingerprint, bucket1, bucket2
        );
        let mut pos = 0;
        let mut result = vec![];
        for (p, len) in &self.partitions {
            for l in 0..*len {
                if self.buckets[bucket1 as usize][pos + l] == fingerprint
                    || self.buckets[bucket2][pos + l] == fingerprint
                {
                    result.push(p.clone());
                }
            }
            pos += len;
        }

        result
    }
}

impl<P> PartitionIndex<P> for CuckooIndex<P>
where
    P: std::clone::Clone,
{
    fn add(self: &mut Self, values: impl Iterator<Item = u64>, partition: &P) {
        let mut f = growable::GrowableCuckooFilter::new(self.buckets.len() as u64);
        for v in values.into_iter() {
            f.insert(v);
        }
        self.partitions
            .push((partition.clone(), f.entries_per_bucket()));
        self.slots += f.entries_per_bucket();
        for (partition_values, bucket) in f.drain().iter_mut().zip(self.buckets.iter_mut()) {
            bucket.append(partition_values);
            if bucket.len() < self.slots {
                // resize underfull buckets from the partition filter
                bucket.resize(self.slots, 0);
            }
        }
    }

    fn remove(self: &mut Self, _partition: P) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::index::{
        cuckoo::CuckooIndex,
        tests::{self, TestPartition},
    };

    static SEED: u64 = 1337;

    #[test]
    fn fill_index() {
        let partitions = tests::create_test_data(100, (1000, 10000), SEED);
        let mut index: CuckooIndex<TestPartition> = CuckooIndex::new(2500);
        tests::fill_index(&mut index, &partitions);
        index.buckets.iter().for_each(|b| {
            assert_eq!(b.len(), index.buckets[0].len());
            assert!(b.len() >= partitions.len());
        });
    }

    #[test]
    fn query_index() {
        let partitions = &tests::create_test_data(10, (5, 17), SEED)[9..];
        let mut index: CuckooIndex<TestPartition> = CuckooIndex::new(5);
        tests::fill_index(&mut index, &partitions);

        for b in &index.buckets {
            eprintln!("tp;b[]: {:?}", b);
        }

        for p in partitions {
            if let Some(first_val) = tests::create_partition_data(&p).next() {
                assert!(
                    tests::query_result_contains(&index, first_val, &p),
                    "querying partitions for '{}' does not yield expected {:?}",
                    first_val,
                    &p.id
                );
            } else {
                panic!("could not create value for partition");
            }
        }
    }
}
