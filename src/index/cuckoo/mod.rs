use crate::filter::cuckoo::growable;
use crate::filter::Filter;
use crate::index::{PartitionFilter, PartitionIndex};

pub struct CuckooIndex<P>
where
    P: Clone,
{
    partitions: Vec<P>,
    buckets: Vec<Vec<u16>>,
}

impl<P> CuckooIndex<P>
where
    P: std::clone::Clone,
{
    pub fn new(buckets: u64) -> Self {
        Self {
            partitions: vec![],
            buckets: vec![vec![]; buckets as usize],
        }
    }
}

impl<P> PartitionFilter<P> for CuckooIndex<P>
where
    P: std::clone::Clone,
{
    fn query(_value: u64) -> Vec<P> {
        todo!()
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
        self.partitions.push(partition.clone());
        // values.for_each(|v| f.insert(v));
        todo!()
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

    #[test]
    fn first() {
        static SEED: u64 = 1337;
        let partitions = tests::create_test_data(100, (1000, 10000), SEED);
        let mut index: CuckooIndex<TestPartition> = CuckooIndex::new(2500);
        tests::fill_index(&mut index, &partitions);
    }
}
