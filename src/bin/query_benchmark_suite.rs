use std::{fs, path::PathBuf};

use partition_index::{
    benchmarks::{
        create_index, result_csv_header, result_csv_line, run_benchmark, BenchmarkPartition,
        BenchmarkResult,
    },
    index::poc::PersistentIndex,
};

// this creates a dataset based on
fn run_single(
    partitions: u64,
    elements: u64,
    buckets: u64,
) -> anyhow::Result<Vec<BenchmarkResult>> {
    let num_queries = 500000;
    let parallelism = [1, 2, 3, 4, 6, 8, 12, 16]; //, 24, 32, 48, 64];
    let index_root = format!(
        "/home/data/tmp/partition_index/query_benchmarks/scratch/p={}/e={}/b={}",
        partitions, elements, buckets
    );
    if PathBuf::from(&index_root).exists() {
        let _ = fs::remove_dir_all(&index_root)?;
    }
    eprintln!(
        "[query benchmark]: creating p = {}, e = {}, b = {}",
        partitions, elements, buckets
    );
    create_index(&index_root, partitions, elements, buckets)?;
    let index = PersistentIndex::<BenchmarkPartition>::try_load_from_disk(index_root.to_string())?;
    // using the same index to run queries with different levels of parallelism
    let results = parallelism
        .iter()
        .map(|p| run_benchmark(&index, num_queries, *p).expect("waddabadda"))
        .collect();
    let _ = fs::remove_dir_all(index_root)?;
    Ok(results)
}

fn main() -> anyhow::Result<()> {
    let partitions = [1000, 10000, 100000, 1000000];
    let elements = [1000, 10000, 100000, 1000000];
    let buckets = [2200, 22000, 220000];

    let mut benchmark_results = vec![];
    for p in partitions {
        for e in elements {
            for b in buckets {
                if p * e <= 100 * 1000 * 1000 * 1000 && e > b {
                    let mut run_results = run_single(p, e, b)?;
                    benchmark_results.append(&mut run_results);
                }
            }
        }
    }
    println!("{}", result_csv_header());
    benchmark_results
        .iter()
        .for_each(|line| println!("{}", result_csv_line(line)));

    Ok(())
}
