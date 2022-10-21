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

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("insert_n 10_000, 10, 1 << 16", |b| {
        b.iter(|| insert_n(black_box(10_000), black_box(10), black_box(1 << 16)))
    });
    c.bench_function("insert_n 2_000_000, 10, 1 << 24", |b| {
        b.iter(|| insert_n(black_box(2_000_000), black_box(10), black_box(1 << 24)))
    });
    let small_filter = insert_n(10_000, 10, 1 << 16);
    c.bench_function("contains 0 on 10k (small filter)", |b| {
        b.iter(|| contains(black_box(&small_filter)))
    });
    let big_filter = insert_n(2_000_000, 10, 1 << 24);
    c.bench_function("contains 0 on 2m (big filter)", |b| {
        b.iter(|| contains(black_box(&big_filter)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
