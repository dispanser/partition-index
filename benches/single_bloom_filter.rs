extern crate partition_index;

use partition_index::filter::bloom::basic_bloom::PaperBloom;
use partition_index::filter::Filter;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

fn insert_n(n: u64, d: u64, m: u64) -> PaperBloom {
    let mut filter = PaperBloom::new(d, m);
    (0..n).for_each(|key| {
        filter.insert(key);
    });
    filter
}

fn contains(f: &dyn Filter) -> bool {
    f.contains(0)
}

fn insert_bench_vary_n(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom::insert_varying size");
    for n in [10_000, 100_000, 1_000_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| insert_n(n, 10, 14 * n))
        });
    }
}

fn insert_bench_vary_d(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom::insert_varying_d");
    for d in [2, 4, 8, 16] {
        group.bench_with_input(BenchmarkId::from_parameter(d), &d, |b, &d| {
            b.iter(|| insert_n(100_000, d, 1_400_000))
        });
    }
}

fn contains_bench_vary_d(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom::contains_varying_d");
    for d in [2, 4, 8, 16] {
        // precompute filter outside of the contains benchmark
        let filter = insert_n(10_000, d, 1 << 16);
        group.bench_with_input(BenchmarkId::from_parameter(d), &d, |b, &_| {
            b.iter(|| contains(black_box(&filter)))
        });
    }
}

fn contains_bench_vary_n(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom::contains_varying_n");
    for n in [10_000, 100_000, 1_000_000] {
        // precompute filter outside of the contains benchmark
        let filter = insert_n(n, 10, 1 << 16);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &_| {
            b.iter(|| contains(black_box(&filter)))
        });
    }
}

criterion_group!(
    benches,
    insert_bench_vary_d,
    insert_bench_vary_n,
    contains_bench_vary_d,
    contains_bench_vary_n
);
criterion_main!(benches);
