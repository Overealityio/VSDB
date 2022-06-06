use criterion::{criterion_group, Criterion};
use rand::Rng;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use vsdb::basic_multi_key::mapx_triple_key::MapxTk;

fn read_write(c: &mut Criterion) {
    let mut group =
        c.benchmark_group("** vsdb::basic_multi_key::mapx_triple_key::MapxTk **");
    group
        .measurement_time(Duration::from_secs(9))
        .sample_size(100);

    let i = AtomicUsize::new(0);
    let db = MapxTk::new();
    group.bench_function(" write ", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::SeqCst);
            db.insert(&(&n, &n, &n), &n);
        })
    });

    group.bench_function(" read ", |b| {
        b.iter(|| {
            let n = i.fetch_sub(1, Ordering::SeqCst);
            db.get(&(&n, &n, &n));
        })
    });
    group.finish();
}

fn random_read_write(c: &mut Criterion) {
    let mut group =
        c.benchmark_group("** vsdb::basic_multi_key::mapx_triple_key::MapxTk **");
    group
        .measurement_time(Duration::from_secs(9))
        .sample_size(100);

    let mut rng = rand::thread_rng();
    let db = MapxTk::new();
    let mut keys = vec![];
    group.bench_function(" random write ", |b| {
        b.iter(|| {
            let n: usize = rng.gen();
            db.insert(&(&n, &n, &n), &n);
            keys.push(n);
        })
    });

    group.bench_function(" random read ", |b| {
        b.iter(|| {
            let index: usize = rng.gen_range(0..keys.len());
            keys.get(index).map(|key| db.get(&(&key, &key, &key)));
        })
    });
    group.finish();
}

criterion_group!(benches, read_write, random_read_write);
