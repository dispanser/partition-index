use partition_index::{
    self,
    benchmarks::BenchmarkPartition,
    index::{poc::PersistentIndex, PartitionIndex},
};
use std::time::SystemTime;

fn index_partition(
    index: &mut impl PartitionIndex<BenchmarkPartition>,
    partition: BenchmarkPartition,
) {
    index.add(
        partition.start..(partition.start + partition.length),
        partition,
    )
}

fn main() -> anyhow::Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let num_partitions: u64 = args[2].parse()?;
    let partition_size: u64 = args[3].parse()?;
    let buckets: u64 = args[4].parse()?;
    let mut partitions = vec![];
    partitions.reserve(num_partitions as usize);

    // Note that we don't store actual values, we only store what's effectively a range
    // that allows us to generate all the values. This enables us to index data much larger
    // than our actual disk by pretending we have data.
    for i in 0..num_partitions {
        partitions.push(BenchmarkPartition {
            start: i * partition_size,
            length: partition_size,
        });
    }

    let mut index = PersistentIndex::try_new(buckets, file_path.to_string())?;
    let start_indexing = SystemTime::now();
    for p in partitions {
        index_partition(&mut index, p);
        if index.estimate_size() > (1 << 27) {
            eprintln!(
                "tp;bench01::persist: {} bytes in memory",
                index.estimate_size()
            );
            index.persist()?;
        }
    }
    index.persist()?;
    let insert_duration = start_indexing.elapsed()?;
    let index_size = num_partitions * partition_size;
    eprintln!(
        "tp;bench01: inserted {} elems in {:?} ({:?} ops)",
        index_size,
        insert_duration,
        index_size as u128 * 1000 / insert_duration.as_millis()
    );
    Ok(())
}
