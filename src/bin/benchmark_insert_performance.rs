use partition_index::{
    self,
    benchmarks::create_index,
};
use std::time::SystemTime;

fn main() -> anyhow::Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let num_partitions: u64 = args[2].parse()?;
    let partition_size: u64 = args[3].parse()?;
    let buckets: u64 = args[4].parse()?;
    let start_indexing = SystemTime::now();
    create_index(file_path, num_partitions, partition_size, buckets)?;
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
