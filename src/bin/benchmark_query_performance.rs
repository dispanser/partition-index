use std::time::Duration;

use partition_index::{
    self,
    benchmarks::{result_csv_line, run_benchmark, BenchmarkPartition},
    index::poc::PersistentIndex,
};

fn main() -> anyhow::Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let index_root = &args[1];
    let time_limit = args[2].parse()?;
    let parallelism = args[3].parse()?;
    let index = PersistentIndex::<BenchmarkPartition>::try_load_from_disk(index_root.clone())?;
    let benchmark_result = run_benchmark(&index, Duration::from_secs(time_limit), parallelism)?;
    eprintln!("{}", result_csv_line(&benchmark_result));
    println!("Median     {}", benchmark_result.medianstats);
    println!("Arithmetic {}", benchmark_result.ameanstats);
    Ok(())
}
