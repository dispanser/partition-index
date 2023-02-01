use partition_index::{
    self,
    benchmarks::BenchmarkPartition,
    index::poc::PersistentIndex,
    index::PartitionFilter,
};
use rstats::{Stats, Median, noop};
use std::time::{SystemTime, Duration};
use rayon::prelude::*;

fn run_query(index: &PersistentIndex<BenchmarkPartition>, i: u64) -> anyhow::Result<Duration>{
    let s = SystemTime::now();
    index.query(i)?;
    Ok(s.elapsed()?)
}

fn main() -> anyhow::Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let num_queries: u64 = args[2].parse()?;

    let index = PersistentIndex::<BenchmarkPartition>::try_load_from_disk(file_path.to_string())?;
    let queries = Vec::from_iter(0..num_queries);
    // let queries = 0..num_queries;
    let start_querying = SystemTime::now();
    let results: Vec<f64> = queries.par_iter().map(|i| { 
        run_query(&index, *i).unwrap().as_micros() as f64
    }).collect();
    let query_duration = start_querying.elapsed()?;
    eprintln!(
        "tp;bench query: queried {} elems in {:?} ({:?} ops)",
        num_queries,
        query_duration,
        num_queries as u128 * 1000 / query_duration.as_millis()
    );
    eprintln!("Median     {}", results.medstats(&mut noop)?);
    eprintln!("Arithmetic {}", results.ameanstd()?);
    eprintln!("{}", results.medinfo(&mut noop)?);
    Ok(())
}
