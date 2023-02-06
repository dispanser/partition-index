use partition_index::{
    self, benchmarks::{read_throughput, BenchmarkResult}, benchmarks::{BenchmarkPartition, result_csv_line},
    index::poc::PersistentIndex, index::PartitionFilter,
};
use rayon::prelude::*;
use rstats::{noop, Median, Stats};
use std::time::{Duration, SystemTime};

fn run_query(index: &PersistentIndex<BenchmarkPartition>, i: u64) -> anyhow::Result<Duration> {
    let s = SystemTime::now();
    index.query(i)?;
    Ok(s.elapsed()?)
}

fn run_benchmark(index_root: &str, num_queries: u64, parallelism: usize) -> anyhow::Result<BenchmarkResult> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(parallelism)
        .build_global()
        .unwrap();
    let index = PersistentIndex::<BenchmarkPartition>::try_load_from_disk(index_root.to_string())?;
    let partition_size = index
        .partitions()
        .next()
        .expect("invalid: empty index")
        .elements();
    let queries = Vec::from_iter(0..num_queries);
    // let queries = 0..num_queries;
    let start_querying = SystemTime::now();
    let results: Vec<f64> = queries
        .par_iter()
        .map(|i| run_query(&index, *i).unwrap().as_micros() as f64)
        .collect();
    let query_duration = start_querying.elapsed()?;
    println!(
        "tp;bench query: queried {} elems in {:?} ({:?} ops)",
        num_queries,
        query_duration,
        num_queries as u128 * 1000 / query_duration.as_millis()
    );
    let med = results.medinfo(&mut noop)?;
    let ameanstats = results.ameanstd()?;
    Ok(BenchmarkResult {
        partitions: index.num_partitions(),
        partition_size,
        num_buckets: index.num_buckes(),
        parallelism,
        qps: num_queries as u128 * 1000 / query_duration.as_millis(),
        ameanstats,
        medianstats: med,
        read_throughput: read_throughput(&query_duration, index.num_slots(), num_queries),
    })
}

fn main() -> anyhow::Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let index_root = &args[1];
    let num_queries = args[2].parse()?;
    let parallelism = args[3].parse()?;
    let benchmark_result = run_benchmark(index_root, num_queries, parallelism)?;
    eprintln!("{}", result_csv_line(&benchmark_result));
    println!("Median     {}", benchmark_result.medianstats);
    println!("Arithmetic {}", benchmark_result.ameanstats);
    Ok(())
}
