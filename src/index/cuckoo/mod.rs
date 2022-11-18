use crate::index::{PartitionFilter, PartitionIndex};

pub struct CuckooIndex<P>
where
    P: Clone,
{
    pub partitions: Vec<P>,
    slots: Vec<Vec<u16>>,
    offset: usize,
}

impl<P> CuckooIndex<P>
where
    P: std::clone::Clone,
{
    pub fn new(num_slots: u64) -> Self {
        // let mut slots = Vec::new();
        // slots.resize_with(num_slots as usize, || vec![]);
        Self {
            partitions: vec![],
            slots: vec![vec![]; num_slots as usize],
            offset: 0,
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
    fn add(self: &mut Self, values: &dyn Iterator<Item = u64>, partition: &P) {
        // the width (number of entries per slot) we're currently working with
        let mut width = 1usize;
        let mut items = 0usize;
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
