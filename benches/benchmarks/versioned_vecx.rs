use criterion::{criterion_group, Criterion};
use rand::Rng;
use std::time::Duration;
use vsdb::{versioned::vecx::VecxVs, VersionName, VsMgmt};

fn read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::versioned::vecx::VecxVs **");
    group
        .measurement_time(Duration::from_secs(9))
        .sample_size(100);

    let mut rng = rand::thread_rng();
    let db = VecxVs::new();
    db.version_create(VersionName(b"version0")).unwrap();

    group.bench_function(" write ", |b| {
        b.iter(|| {
            let n = rng.gen::<usize>();
            db.push(n);
        })
    });

    group.bench_function(" read ", |b| {
        b.iter(|| {
            let index: usize = rng.gen_range(0..db.len());
            db.get(index);
        })
    });

    group.bench_function(" random read ", |b| {
        b.iter(|| {
            let index: usize = rng.gen_range(0..db.len());
            db.get(index);
        })
    });

    group.finish();
}

criterion_group!(benches, read_write);
