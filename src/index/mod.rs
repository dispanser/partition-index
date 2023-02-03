pub mod in_memory;
pub mod poc;

use async_trait::async_trait;
/// The underlying assumption here is that we're indexing "partitions"
/// on an unknown stream of data. The only representation we can retrieve
/// is a hashed representation of all values to be indexed.
/// This allows some flexibility down the road by allowing different types
/// of partitions (parquet files, row groups, hive partitions, ...) or even
/// heterogeneous data that has different shape or form.
/// It is also possible to index multiple columns into the same index,
/// even if the columns have different types.

#[async_trait]
pub trait PartitionFilter<P>
{
    /// Query matching partitions for a given value
    /// TODO: what's the result type here? It's either the partition, or
    /// some kind of partition ID.
    async fn query(&self, value: u64) -> anyhow::Result<Vec<P>>;
}

pub trait PartitionIndex<P>
{
    /// Add a partition to the index.
    /// @param values an iterator of the values stored in the partition
    /// @param partition the partition identifier to associate the values with
    fn add(&mut self, values: impl Iterator<Item = u64>, partition: P);

    /// Remove a partition from the index.
    /// @param partition to remove
    fn remove(&mut self, partition: &P);
}

#[cfg(test)]
pub mod tests {
    use rand::distributions::Uniform;
    use rand::{Rng, SeedableRng};

    use super::PartitionIndex;

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub struct TestPartition {
        pub id: usize, // counting upwards from zero, the simplest possible ID
        pub size: u32,
        pub seed: u64, // we don't store actual sequence of values, but a seed.
    }

    pub fn fill_index(index: &mut impl PartitionIndex<TestPartition>, ps: &[TestPartition]) {
        for partition in ps {
            index.add(create_partition_data(&partition), partition.clone());
        }
    }

    pub fn create_partition_data(partition: &TestPartition) -> impl Iterator<Item = u64> {
        let data_rng = rand_xoshiro::Xoshiro256PlusPlus::seed_from_u64(partition.seed);
        data_rng
            .sample_iter(Uniform::new_inclusive(u64::MIN, u64::MAX))
            .take(partition.size as usize)
    }

    pub fn create_test_data(
        num_partitions: usize,
        size_range: (u32, u32),
        seed: u64,
    ) -> Vec<TestPartition> {
        let partition_size_distribution = Uniform::new(size_range.0, size_range.1);

        // Uses a random generator starting from a fixed seed, enabling the test to
        // reproduce the same results for the test / lookup phase without storing them.
        let mut data_rng = rand_xoshiro::Xoshiro256PlusPlus::seed_from_u64(seed);
        // let partition_sizes = (&mut data_rng).sample_iter(partition_size_distribution);
        (0..num_partitions)
            .map(|id| {
                let size = data_rng.sample(partition_size_distribution);
                let seed = data_rng.gen();
                TestPartition { id, size, seed }
            })
            .collect::<Vec<_>>()
    }
}
