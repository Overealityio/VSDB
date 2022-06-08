use criterion::{criterion_group, Criterion};
use rand::Rng;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use vsdb::{basic::mapx_ord::MapxOrd, ValueEnDe};

fn read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::basic::mapx_ord::MapxOrd **");
    group
        .measurement_time(Duration::from_secs(9))
        .sample_size(100);

    let i = AtomicUsize::new(0);
    let mut db = MapxOrd::new();

    group.bench_function(" write ", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::SeqCst);
            let key = <usize as ValueEnDe>::encode(&n);
            db.set_value(key, n);
        })
    });

    group.bench_function(" read ", |b| {
        b.iter(|| {
            let n = i.fetch_sub(1, Ordering::SeqCst);
            let key = <usize as ValueEnDe>::encode(&n);
            db.get(&key);
        })
    });
    group.finish();
}

fn random_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::basic::mapx_ord::MapxOrd **");
    group
        .measurement_time(Duration::from_secs(9))
        .sample_size(100);

    let mut rng = rand::thread_rng();
    let mut db = MapxOrd::new();
    let mut keys = vec![];
    group.bench_function(" random write ", |b| {
        b.iter(|| {
            let n = rng.gen::<usize>();
            let key = <usize as ValueEnDe>::encode(&n);
            db.set_value(key.clone(), n);
            keys.push(key);
        })
    });

    group.bench_function(" random read ", |b| {
        b.iter(|| {
            let index: usize = rng.gen_range(0..keys.len());
            keys.get(index).map(|key| db.get(key));
        })
    });

    group.finish();
}

criterion_group!(benches, read_write, random_read_write);
