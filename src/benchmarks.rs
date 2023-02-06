use std::time::Duration;

use rstats::{MStats, Med};

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
    pub partitions: usize,
    pub partition_size: u64,
    pub num_buckets: u64,
    pub parallelism: usize,
    pub qps: u128,
    pub ameanstats: MStats,
    pub medianstats: Med,
    pub read_throughput: f64,
}

pub fn result_csv_line(benchmark_result: &BenchmarkResult) -> String {
    // partitions, elements per partition, buckets, parallelism,
    // throughput (MB/s), mean latency (μs), std dev latency,
    // median (μs), 1st quartile, 3rd quartile, med, standard error,
    // read throughput (MB)
    format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{}",
        benchmark_result.partitions,
        benchmark_result.partition_size,
        benchmark_result.num_buckets,
        benchmark_result.parallelism,
        benchmark_result.qps,
        benchmark_result.ameanstats.centre,
        benchmark_result.ameanstats.dispersion,
        benchmark_result.medianstats.median,
        benchmark_result.medianstats.lq,
        benchmark_result.medianstats.uq,
        benchmark_result.medianstats.mad,
        benchmark_result.medianstats.ste,
        benchmark_result.read_throughput,
    )
}

/// compute the MB/s read performance
pub fn read_throughput(d: &Duration, num_slots: usize, num_queries: u64) -> f64 {
    // read b buckets times two bytes times two buckets times num queries
    ((num_slots as u64 * num_queries * 2 * 2 * 1000) / (1 << 20)) as f64 / d.as_millis() as f64
}
