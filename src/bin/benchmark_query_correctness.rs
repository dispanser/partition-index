use partition_index::{
    self, benchmarks::BenchmarkPartition, index::poc::PersistentIndex, index::PartitionFilter,
};
use rand::{distributions::Uniform, Rng, SeedableRng};
use std::time::SystemTime;

fn main() -> anyhow::Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let num_queries: u64 = args[2].parse()?;

    let index = PersistentIndex::<BenchmarkPartition>::try_load_from_disk(file_path.to_string())?;
    let p0 = index.partitions().next().expect("invalid: empty index");
    let max_value = p0.elements() * index.num_partitions() as u64;
    let start_querying = SystemTime::now();

    let mut false_positives = 0u64;
    let mut false_negatives = 0u64;
    let value_distribution = Uniform::new(0, max_value);
    let mut data_rng = rand_xoshiro::Xoshiro256PlusPlus::seed_from_u64(1337);
    for _i in 0..num_queries {
        let value = data_rng.sample(value_distribution);
        let query_result = index.query(value)?;
        if query_result
            .iter()
            .any(|p| value >= p.start && value < p.start + p.length)
        {
            false_positives += query_result.len() as u64 - 1;
        } else {
            false_positives += query_result.len() as u64;
            false_negatives += 1;
        }
    }
    let query_duration = start_querying.elapsed()?;
    let fp_rate =
        false_positives as f64 / (num_queries as u128 * index.num_partitions() as u128) as f64;
    eprintln!(
        "tp;correctness: {} false positives, {} false negatives, fp-rate {}",
        false_positives, false_negatives, fp_rate,
    );
    eprintln!(
        "tp;bench query: queried {} elems in {:?} ({:?} ops)",
        num_queries,
        query_duration,
        num_queries as u128 * 1000 / query_duration.as_millis()
    );
    Ok(())
}
