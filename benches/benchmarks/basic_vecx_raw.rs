use criterion::{criterion_group, Criterion};
use rand::Rng;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use vsdb::{basic::vecx_raw::VecxRaw, ValueEnDe};

fn read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::basic::vecx_raw::VecxRaw **");
    group
        .measurement_time(Duration::from_secs(9))
        .sample_size(100);

    let i = AtomicUsize::new(0);
    let mut db = VecxRaw::new();

    group.bench_function(" write ", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::SeqCst);
            let val = <usize as ValueEnDe>::encode(&n);
            db.push(val);
        })
    });

    group.bench_function(" read ", |b| {
        b.iter(|| {
            let n = i.fetch_sub(1, Ordering::SeqCst);
            db.get(n);
        })
    });
    group.finish();
}

fn random_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::basic::vecx_raw::VecxRaw **");
    group
        .measurement_time(Duration::from_secs(9))
        .sample_size(100);

    let mut rng = rand::thread_rng();
    let mut db = VecxRaw::new();
    group.bench_function(" random write ", |b| {
        b.iter(|| {
            let n = rng.gen::<usize>();
            let val = <usize as ValueEnDe>::encode(&n);
            db.push(val);
        })
    });

    group.bench_function(" random read ", |b| {
        b.iter(|| {
            let idx: usize = rng.gen_range(0..db.len());
            db.get(idx);
        })
    });
    group.finish();
}

criterion_group!(benches, read_write, random_read_write);
