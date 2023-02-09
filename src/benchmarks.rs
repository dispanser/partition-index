use std::time::{Duration, SystemTime};

use rstats::{noop, MStats, Median, Stats};

use crate::index::{poc::PersistentIndex, PartitionFilter, PartitionIndex};

// Simple partition that has a start value and a size.
// It covers the values in range [start, start + length).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BenchmarkPartition {
    pub start: u64,
    pub length: u64,
}

impl BenchmarkPartition {
    pub fn elements(&self) -> u64 {
        self.length
    }
}

pub struct BenchmarkResult {
    pub num_queries: usize,
    pub partitions: usize,
    pub partition_size: u64,
    pub num_buckets: u64,
    pub parallelism: usize,
    pub qps: u128,
    pub ameanstats: MStats,
    pub medianstats: MStats,
    pub read_throughput: f64,
}

pub fn result_csv_header() -> String {
    "partitions,elements per partition,buckets,parallelism,\
    queries per second,mean latency (μs),std dev latency,\
    median (μs),mad,standard error,\
    read throughput (MB/s)"
        .to_string()
}

pub fn result_csv_line(benchmark_result: &BenchmarkResult) -> String {
    // partitions,elements per partition,buckets,parallelism,
    // queries per second,mean latency (μs),std dev latency,
    // median (μs),mad,read throughput (MB/s)
    format!(
        "{},{},{},{},{},{},{},{},{},{}",
        benchmark_result.partitions,
        benchmark_result.partition_size,
        benchmark_result.num_buckets,
        benchmark_result.parallelism,
        benchmark_result.qps,
        benchmark_result.ameanstats.centre,
        benchmark_result.ameanstats.dispersion,
        benchmark_result.medianstats.centre,
        benchmark_result.medianstats.dispersion,
        benchmark_result.read_throughput,
    )
}

fn index_partition(
    index: &mut impl PartitionIndex<BenchmarkPartition>,
    partition: BenchmarkPartition,
) {
    index.add(
        partition.start..(partition.start + partition.length),
        partition,
    )
}

pub fn create_index(
    index_root: &str,
    num_partitions: u64,
    partition_size: u64,
    buckets: u64,
) -> anyhow::Result<()> {
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

    let mut index = PersistentIndex::try_new(buckets, index_root.to_string())?;
    for p in partitions {
        index_partition(&mut index, p);
        if index.estimate_mem_size() > (1 << 30) {
            eprintln!(
                "tp;bench01::persist: {} bytes in memory",
                index.estimate_mem_size()
            );
            index.persist()?;
        }
    }
    index.persist()?;
    Ok(())
}

fn run_query(index: &PersistentIndex<BenchmarkPartition>, i: u64) -> anyhow::Result<Duration> {
    let s = SystemTime::now();
    index.query(i)?;
    Ok(s.elapsed()?)
}

pub fn run_benchmark(
    index: &PersistentIndex<BenchmarkPartition>,
    duration: Duration,
    parallelism: usize,
) -> anyhow::Result<BenchmarkResult> {
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(parallelism)
        .build()?;
    let partition_size = index
        .partitions()
        .next()
        .expect("invalid: empty index")
        .elements();
    let batch_size = usize::MAX / parallelism;
    let mut thread_results: Vec<Vec<Duration>> = vec![vec![]; parallelism];
    let start_querying = SystemTime::now();
    thread_pool.scope(|s| {
        for (task, res_ref) in thread_results.iter_mut().enumerate() {
            let start = batch_size * task;
            let end = start + batch_size;
            s.spawn(move |_| {
                *res_ref = (start..end)
                    .into_iter()
                    .map(|i| {
                        if start_querying.elapsed().unwrap() < duration {
                            Some(run_query(&index, i as u64).unwrap())
                        } else {
                            None
                        }
                    })
                    .take_while(|e| e.is_some())
                    .map(|e| e.unwrap())
                    .collect();
            });
        }
    });
    let query_duration = start_querying.elapsed()?;
    let results: Vec<_> = thread_results
        .iter()
        .flatten()
        .map(|d| d.as_micros() as f64)
        .collect();

    let num_queries: usize = results.len();
    println!(
        "tp;bench query: queried {} elems in {:?} ({:?} ops) using {} threads",
        num_queries,
        query_duration,
        num_queries as u128 * 1000 / query_duration.as_millis(),
        parallelism,
    );
    let ameanstats = results.ameanstd()?;
    let med = results.medstats(&mut noop)?;
    Ok(BenchmarkResult {
        num_queries,
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

/// compute the MB/s read performance
pub fn read_throughput(d: &Duration, num_slots: usize, num_queries: usize) -> f64 {
    // read b buckets times two bytes times two buckets times num queries
    ((num_slots * num_queries * 2 * 2 * 1000) / (1 << 20)) as f64 / d.as_millis() as f64
}
