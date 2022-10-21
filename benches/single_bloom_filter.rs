extern crate bloom_lake;

use bloom_lake::bloom::{Filter, PaperBloom};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn insert_n(n: u64, d: u64, m: u64) -> PaperBloom {
    let mut filter = PaperBloom::new(d, m);
    (0..n).for_each(|key| filter.insert(key));
    filter
}

fn contains(f: &dyn Filter) -> bool {
    f.contains(0)
}

fn insert_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_varying size");
    for n in [10_000, 100_000, 1_000_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| insert_n(n, 10, 14 * n))
        });
    }
}

fn contains_bench_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains_small_set");
    for d in 2..=10 {
        // precompute filter outside of the contains benchmark
        let filter = insert_n(10_000, 10, 1 << 16);
        group.bench_with_input(BenchmarkId::from_parameter(d), &d, |b, &d| {
            b.iter(|| contains(black_box(&filter)))
        });
    }
}

criterion_group!(benches, contains_bench_small, insert_bench);
criterion_main!(benches);
