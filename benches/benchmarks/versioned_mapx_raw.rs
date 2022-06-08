use criterion::{criterion_group, Criterion};
use rand::Rng;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use vsdb::{versioned::mapx_raw::MapxRawVs, BranchName, VersionName, VsMgmt};

fn read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::versioned::mapx_raw::MapxRawVs **");
    group
        .measurement_time(Duration::from_secs(9))
        .sample_size(100);

    let i = AtomicUsize::new(0);
    let mut db = MapxRawVs::new();
    db.version_create(VersionName(b"version0")).unwrap();

    group.bench_function(" write ", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::SeqCst);
            db.insert(&n.to_be_bytes(), &[1; 128]).unwrap();
        })
    });

    group.bench_function(" read ", |b| {
        b.iter(|| {
            let n = i.fetch_sub(1, Ordering::SeqCst);
            db.get(&n.to_be_bytes());
        })
    });
    group.finish();
}

fn random_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::versioned::mapx_raw::MapxRawVs **");
    group
        .measurement_time(Duration::from_secs(9))
        .sample_size(100);

    let mut keys = vec![];
    let mut rng = rand::thread_rng();
    let mut db = MapxRawVs::new();
    db.version_create(VersionName(b"version0")).unwrap();

    group.bench_function(" random write ", |b| {
        b.iter(|| {
            let n = rng.gen::<usize>();
            let key = n.to_be_bytes();
            db.insert(&key, &[1; 128]).unwrap();
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

fn version_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::versioned::mapx_raw::MapxRawVs **");
    group
        .measurement_time(Duration::from_secs(9))
        .sample_size(100);

    let i = AtomicUsize::new(0);
    let mut db = MapxRawVs::new();

    group.bench_function(" version write ", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::SeqCst);
            db.version_create(VersionName(&n.to_be_bytes())).unwrap();
            db.insert(&n.to_be_bytes(), &[1; 128]).unwrap();
        })
    });

    group.bench_function(" version read ", |b| {
        b.iter(|| {
            let n = i.fetch_sub(1, Ordering::SeqCst);
            db.get_by_branch_version(
                &n.to_be_bytes(),
                db.branch_get_default().as_deref(),
                VersionName(&n.to_be_bytes()),
            );
        })
    });
    group.finish();
}

fn version_random_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::versioned::mapx_raw::MapxRawVs **");
    group
        .measurement_time(Duration::from_secs(9))
        .sample_size(100);

    let mut keys = vec![];
    let mut rng = rand::thread_rng();
    let mut db = MapxRawVs::new();

    group.bench_function(" version random write ", |b| {
        b.iter(|| {
            let n = rng.gen::<usize>();
            let key = n.to_be_bytes();
            db.version_create(VersionName(&key)).unwrap();
            db.insert(&key, &[1; 128]).unwrap();
            keys.push(key);
        })
    });

    group.bench_function(" version random read ", |b| {
        b.iter(|| {
            let index: usize = rng.gen_range(0..keys.len());
            keys.get(index).map(|key| {
                db.get_by_branch_version(
                    key,
                    db.branch_get_default().as_deref(),
                    VersionName(key),
                );
            });
        })
    });
    group.finish();
}

fn branch_version_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::versioned::mapx_raw::MapxRawVs **");
    group
        .measurement_time(Duration::from_secs(9))
        .sample_size(100);

    let i = AtomicUsize::new(0);
    let mut db = MapxRawVs::new();
    db.version_create(VersionName(b"version0")).unwrap();

    group.bench_function(" branch version write ", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::SeqCst);
            let key = &n.to_be_bytes();
            db.branch_create(BranchName(key), VersionName(key), false)
                .unwrap();
            db.insert(key, &[1; 128]).unwrap();
        })
    });

    group.bench_function(" branch version read ", |b| {
        b.iter(|| {
            let n = i.fetch_sub(1, Ordering::SeqCst);
            let key = &n.to_be_bytes();
            db.get_by_branch_version(key, BranchName(key), VersionName(key));
        })
    });
    group.finish();
}

fn branch_version_random_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::versioned::mapx_raw::MapxRawVs **");
    group
        .measurement_time(Duration::from_secs(9))
        .sample_size(100);

    let mut keys = vec![];
    let mut rng = rand::thread_rng();
    let mut db = MapxRawVs::new();
    db.version_create(VersionName(b"version0")).unwrap();

    group.bench_function(" branch version random write ", |b| {
        b.iter(|| {
            let n = rng.gen::<usize>();
            let key = n.to_be_bytes();
            db.branch_create(BranchName(&key), VersionName(&key), false)
                .unwrap();
            db.insert(&key, &[1; 128]).unwrap();
            keys.push(key);
        })
    });

    group.bench_function(" branch version random read ", |b| {
        b.iter(|| {
            let index: usize = rng.gen_range(0..keys.len());
            keys.get(index).map(|key| {
                db.get_by_branch_version(key, BranchName(key), VersionName(key));
            });
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    read_write,
    random_read_write,
    version_read_write,
    version_random_read_write,
    branch_version_read_write,
    branch_version_random_read_write
);
