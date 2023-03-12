use std::{fs, path::PathBuf, time::Duration};

use partition_index::{
    benchmarks::{
        create_index, result_csv_header, result_csv_line, run_benchmark, BenchmarkPartition,
        BenchmarkResult,
    },
    index::poc::PersistentIndex,
};

struct BenchmarkConfig {
    partitions: Vec<u64>,
    elements: Vec<u64>,
    buckets: Vec<u64>,
    parallelism: Vec<usize>,
    time_limit: Duration,
}

// this creates a dataset based on
fn run_single(
    partitions: u64,
    elements: u64,
    buckets: u64,
    parallelism: &Vec<usize>,
    time_limit: Duration,
) -> anyhow::Result<Vec<BenchmarkResult>> {
    let index_root = format!(
        "/home/data/tmp/partition_index/query_benchmarks/scratch/p={}/e={}/b={}",
        partitions, elements, buckets
    );
    if PathBuf::from(&index_root).exists() {
        let _ = fs::remove_dir_all(&index_root)?;
    }
    eprintln!(
        "[query benchmark]: creating p = {}, e = {}, b = {} at {}",
        partitions, elements, buckets, index_root
    );
    create_index(&index_root, partitions, elements, buckets)?;
    let index = PersistentIndex::<BenchmarkPartition>::try_load_from_disk(index_root.to_string())?;
    // using the same index to run queries with different levels of parallelism
    let results = parallelism
        .into_iter()
        .map(|p| run_benchmark(&index, time_limit, *p).expect("waddabadda"))
        .collect();
    let _ = fs::remove_dir_all(index_root)?;
    Ok(results)
}

fn main() -> anyhow::Result<()> {
    let _bucket_conf = BenchmarkConfig {
        partitions: vec![100000],
        elements: vec![100000],
        buckets: vec![11, 12, 13, 16, 40, 20, 24, 30, 40, 68]
            .into_iter()
            .map(|x| x * 1000)
            .collect(),
        parallelism: vec![1],
        time_limit: Duration::from_secs(30),
    };
    let _performance_conf = BenchmarkConfig {
        partitions: vec![1]
        // partitions: vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100,]
            .into_iter()
            .map(|x| x * 10000)
            .collect(),
        elements: vec![100000],
        buckets: vec![11].into_iter().map(|x| x * 1000).collect(),
        parallelism: vec![1, 2, 3, 4, 6, 8],
        time_limit: Duration::from_secs(30),
    };
    let _occupancy_conf = BenchmarkConfig {
        partitions: vec![1],
        elements: vec![100000],
        buckets: (1..1000).into_iter().map(|x| x * 500).collect(),
        parallelism: vec![1],
        time_limit: Duration::from_millis(1),
    };

    let conf = _bucket_conf;

    let mut benchmark_results = vec![];
    for p in conf.partitions {
        for e in conf.elements.iter() {
            for b in conf.buckets.iter() {
                // benchmark with more then 100 * 10^9 elements does not fit on our disk
                if p * e <= 100 * 1000 * 1000 * 1000 && e > &b {
                    let mut run_results =
                        run_single(p, *e, *b, &conf.parallelism, conf.time_limit)?;
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
