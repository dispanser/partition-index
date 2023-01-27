use partition_index::{
    self,
    benchmarks::BenchmarkPartition,
    index::poc::PersistentIndex,
    index::PartitionFilter,
};
use std::time::SystemTime;

fn main() -> anyhow::Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let num_queries: u64 = args[2].parse()?;

    let mut index = PersistentIndex::<BenchmarkPartition>::try_load_from_disk(file_path.to_string())?;
    let start_querying = SystemTime::now();
    for i in 0..num_queries {
        index.query(i)?;
    }
    index.persist()?;
    let query_duration = start_querying.elapsed()?;
    eprintln!(
        "tp;bench query: queried {} elems in {:?} ({:?} ops)",
        num_queries,
        query_duration,
        num_queries as u128 * 1000 / query_duration.as_millis()
    );
    Ok(())
}
