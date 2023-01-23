use crate::filter::cuckoo::{bucket, fingerprint, flip_bucket, growable};
use crate::filter::Filter;
use crate::index::{PartitionFilter, PartitionIndex};

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionInfo<P> {
    pub(crate) partition: P,
    pub(crate) entries: usize,
    pub(crate) active: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CuckooIndex<P> {
    pub(crate) partitions: Vec<PartitionInfo<P>>,
    pub(crate) buckets: Vec<Vec<u16>>,
    pub(crate) slots: usize,
}

impl<P> CuckooIndex<P> {
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
    P: Clone,
{
    fn query(self: &Self, key: u64) -> anyhow::Result<Vec<P>> {
        let fingerprint = fingerprint(key);
        let bucket1 = bucket(key, self.buckets.len() as u64);
        let bucket2 = flip_bucket(fingerprint, bucket1, self.buckets.len() as u64) as usize;
        eprintln!(
            "tp;query_mem({}): {}@[{}, {}]",
            key, fingerprint, bucket1, bucket2
        );
        let mut pos = 0;
        let mut result = vec![];
        for p in &self.partitions {
            if p.active {
                for l in 0..p.entries {
                    if self.buckets[bucket1 as usize][pos + l] == fingerprint
                        || self.buckets[bucket2][pos + l] == fingerprint
                    {
                        result.push(p.partition.clone());
                    }
                }
            }
            pos += p.entries;
        }
        Ok(result)
    }
}

impl<P> PartitionIndex<P> for CuckooIndex<P>
where
    P: PartialEq,
{
    fn add(self: &mut Self, values: impl Iterator<Item = u64>, partition: P) {
        let mut f = growable::GrowableCuckooFilter::new(self.buckets.len() as u64);
        for v in values.into_iter() {
            f.insert(v);
        }
        self.partitions.push(PartitionInfo {
            partition,
            entries: f.entries_per_bucket(),
            active: true,
        });
        self.slots += f.entries_per_bucket();
        for (partition_values, bucket) in f.drain().iter_mut().zip(self.buckets.iter_mut()) {
            bucket.append(partition_values);
            if bucket.len() < self.slots {
                // resize underfull buckets from the partition filter
                bucket.resize(self.slots, 0);
            }
        }
    }

    fn remove(self: &mut Self, to_be_removed: &P) {
        for p in self.partitions.iter_mut() {
            if &p.partition == to_be_removed {
                p.active = false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::index::{
        in_memory::CuckooIndex,
        tests::{self, TestPartition},
        PartitionFilter, PartitionIndex,
    };

    static SEED: u64 = 1337;

    #[test]
    fn fill_index() {
        let partitions = tests::create_test_data(100, (1000, 10000), SEED);
        let mut index: CuckooIndex<TestPartition> = CuckooIndex::new(2500);
        tests::fill_index(&mut index, &partitions);
        index.buckets.iter().for_each(|b| {
            assert_eq!(b.len(), index.slots);
            assert!(b.len() >= partitions.len());
        });
    }

    #[test]
    fn query_index() -> anyhow::Result<()> {
        // let partitions = &tests::create_test_data(10, (5, 17), SEED)[9..];
        let partitions = &tests::create_test_data(100, (999, 4999), SEED);
        let mut index: CuckooIndex<TestPartition> = CuckooIndex::new(800);
        tests::fill_index(&mut index, &partitions);

        for p in partitions {
            if let Some(first_val) = tests::create_partition_data(&p).next() {
                assert!(
                    index.query(first_val)?.contains(&p),
                    "querying partitions for '{}' does not yield expected {:?}",
                    first_val,
                    &p.id
                );
            } else {
                panic!("could not create value for partition");
            }
        }
        Ok(())
    }

    #[test]
    fn dont_yield_removed_partitions() -> anyhow::Result<()> {
        let partitions = &tests::create_test_data(10, (99, 499), SEED);
        let mut index: CuckooIndex<TestPartition> = CuckooIndex::new(80);
        tests::fill_index(&mut index, &partitions);
        index.remove(&partitions[3]);
        if let Some(first_val) = tests::create_partition_data(&partitions[3]).next() {
            assert!(
                !index.query(first_val)?.contains(&partitions[3]),
                "querying partitions for '{}' should not yield deleted partition {:?}",
                first_val,
                &partitions[3].id
            );
        }
        Ok(())
    }
}
