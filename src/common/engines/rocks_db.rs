// #![allow(warnings)]

use crate::common::{
    vsdb_get_base_dir, vsdb_set_base_dir, BranchID, Engine, Pre, PreBytes, RawBytes,
    RawKey, RawValue, VersionID, INITIAL_BRANCH_ID, MB, PREFIX_SIZ, RESERVED_ID_CNT,
};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DBCompressionType, DBIterator, Direction,
    FlushOptions, IteratorMode, Options, ReadOptions, SliceTransform, WriteBatch, DB,
};
use ruc::*;
use std::{
    collections::{
        btree_map::{Entry as BEntry, IntoIter as BIntoIter},
        BTreeMap,
    },
    mem::{self, size_of},
    ops::{Bound, RangeBounds},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    thread::available_parallelism,
};

const DATA_SET_NUM: usize = 32;

const META_KEY_MAX_KEYLEN: [u8; 1] = [u8::MAX];
const META_KEY_BRANCH_ID: [u8; 1] = [u8::MAX - 1];
const META_KEY_VERSION_ID: [u8; 1] = [u8::MAX - 2];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

static HDR: Lazy<(DB, Vec<String>)> = Lazy::new(|| rocksdb_open().unwrap());

static FLUSH_INDICATOR: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| (0..=DATA_SET_NUM).map(|_| AtomicU64::new(0)).collect());

pub(crate) struct RocksEngine {
    meta: &'static DB,
    areas: Vec<&'static str>,
    prefix_allocator: PreAllocator,
    max_keylen: AtomicUsize,
    cache: Cache,
}

impl RocksEngine {
    #[inline(always)]
    fn cf_hdr(&self, area_idx: usize) -> &ColumnFamily {
        self.meta.cf_handle(self.areas[area_idx]).unwrap()
    }

    #[inline(always)]
    fn get_max_keylen(&self) -> usize {
        self.max_keylen.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn set_max_key_len(&self, len: usize) {
        self.max_keylen.store(len, Ordering::Relaxed);

        self.cache.buf0[DATA_SET_NUM].write().insert(
            META_KEY_MAX_KEYLEN.to_vec().into(),
            Some(len.to_be_bytes().into()),
        );
    }

    #[inline(always)]
    fn get_upper_bound_value(&self, meta_prefix: PreBytes) -> Vec<u8> {
        static BUF: Lazy<RawBytes> = Lazy::new(|| vec![u8::MAX; 512].into_boxed_slice());

        let mut max_guard = meta_prefix.to_vec();

        let l = self.get_max_keylen();
        if l < 513 {
            max_guard.extend_from_slice(&BUF[..l]);
        } else {
            max_guard.extend_from_slice(&vec![u8::MAX; l]);
        }

        max_guard
    }

    fn update_batch(&self, data: &CacheMap, area_idx: usize) -> Result<()> {
        alt!(data.is_empty(), return Ok(()));

        let mut batch = WriteBatch::default();
        for (extended_key, value) in data.iter() {
            if let Some(v) = value {
                if DATA_SET_NUM == area_idx {
                    // update meta
                    batch.put(extended_key, v);
                } else {
                    batch.put_cf(self.cf_hdr(area_idx), extended_key, v);
                }
            } else {
                batch.delete_cf(self.cf_hdr(area_idx), extended_key);
            }
        }
        self.meta.write(batch).c(d!())
    }

    #[inline(always)]
    #[allow(unused_variables)]
    fn flush_area_cache(&self, area_idx: usize) {
        static LK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
        let x = LK.lock();

        if let Some(mut buf0) = self.cache.buf0[area_idx].try_write() {
            if 0 == FLUSH_INDICATOR[area_idx].load(Ordering::Relaxed) {
                let mut buf1 = self.cache.buf1[area_idx].write();
                buf1.clear();
                mem::swap(&mut *buf0, &mut *buf1);
                drop(buf0);
                pnk!(self.update_batch(&buf1, area_idx));
            }
        }
    }

    #[inline(always)]
    fn flush_engine(&self) {
        let mut opts = FlushOptions::default();
        opts.set_wait(true);
        self.meta.flush_opt(&opts).unwrap();

        (0..DATA_SET_NUM).for_each(|i| {
            self.meta.flush_cf_opt(self.cf_hdr(i), &opts).unwrap();
        });
    }
}

impl Engine for RocksEngine {
    fn new() -> Result<Self> {
        let (meta, areas) =
            (&HDR.0, HDR.1.iter().map(|i| i.as_str()).collect::<Vec<_>>());

        let (prefix_allocator, initial_value) = PreAllocator::init();

        if meta.get(&META_KEY_MAX_KEYLEN).c(d!())?.is_none() {
            meta.put(META_KEY_MAX_KEYLEN, 0_usize.to_be_bytes())
                .c(d!())?;
        }

        if meta.get(&META_KEY_BRANCH_ID).c(d!())?.is_none() {
            meta.put(
                META_KEY_BRANCH_ID,
                (1 + INITIAL_BRANCH_ID as usize).to_be_bytes(),
            )
            .c(d!())?;
        }

        if meta.get(&META_KEY_VERSION_ID).c(d!())?.is_none() {
            meta.put(META_KEY_VERSION_ID, 0_usize.to_be_bytes())
                .c(d!())?;
        }

        if meta.get(prefix_allocator.key).c(d!())?.is_none() {
            meta.put(prefix_allocator.key, initial_value).c(d!())?;
        }

        let max_keylen = AtomicUsize::new(crate::parse_int!(
            meta.get(&META_KEY_MAX_KEYLEN).unwrap().unwrap(),
            usize
        ));

        let mut opts = FlushOptions::default();
        opts.set_wait(true);
        meta.flush().c(d!())?;

        Ok(RocksEngine {
            meta,
            areas,
            prefix_allocator,
            // length of the raw key, exclude the meta prefix
            max_keylen,
            cache: Cache::new(),
        })
    }

    // 'step 1' and 'step 2' is not atomic in multi-threads scene,
    // so we should get the 'write' lock for thread safe.
    #[allow(unused_variables)]
    fn alloc_prefix(&self) -> Pre {
        // step 1
        let mut buf0 = self.cache.buf0[DATA_SET_NUM].write();
        let buf1 = self.cache.buf1[DATA_SET_NUM].read();

        let ret = if let Some(v) = buf0
            .get(&self.prefix_allocator.key[..])
            .or_else(|| buf1.get(&self.prefix_allocator.key[..]))
        {
            let ret = crate::parse_prefix!(v.as_ref().unwrap());
            drop(buf1);
            ret
        } else {
            drop(buf1);
            crate::parse_prefix!(
                self.meta.get(self.prefix_allocator.key).unwrap().unwrap()
            )
        };

        // step 2
        buf0.insert(
            self.prefix_allocator.key.to_vec().into(),
            Some((1 + ret).to_be_bytes().into()),
        );

        ret
    }

    // 'step 1' and 'step 2' is not atomic in multi-threads scene,
    // so we should get the 'write' lock for thread safe.
    #[allow(unused_variables)]
    fn alloc_branch_id(&self) -> BranchID {
        // step 1
        let mut buf0 = self.cache.buf0[DATA_SET_NUM].write();
        let buf1 = self.cache.buf1[DATA_SET_NUM].read();

        let ret = if let Some(v) = buf0
            .get(&META_KEY_BRANCH_ID[..])
            .or_else(|| buf1.get(&META_KEY_BRANCH_ID[..]))
        {
            let ret = crate::parse_int!(v.as_ref().unwrap(), BranchID);
            drop(buf1);
            ret
        } else {
            drop(buf1);
            crate::parse_int!(
                self.meta.get(META_KEY_BRANCH_ID).unwrap().unwrap(),
                BranchID
            )
        };

        // step 2
        buf0.insert(
            META_KEY_BRANCH_ID.to_vec().into(),
            Some((1 + ret).to_be_bytes().into()),
        );

        ret
    }

    // 'step 1' and 'step 2' is not atomic in multi-threads scene,
    // so we should get the 'write' lock for thread safe.
    #[allow(unused_variables)]
    fn alloc_version_id(&self) -> VersionID {
        // step 1
        let mut buf0 = self.cache.buf0[DATA_SET_NUM].write();
        let buf1 = self.cache.buf1[DATA_SET_NUM].read();

        let ret = if let Some(v) = buf0
            .get(&META_KEY_VERSION_ID[..])
            .or_else(|| buf1.get(&META_KEY_VERSION_ID[..]))
        {
            let ret = crate::parse_int!(v.as_ref().unwrap(), VersionID);
            drop(buf1);
            ret
        } else {
            drop(buf1);
            crate::parse_int!(
                self.meta.get(META_KEY_VERSION_ID).unwrap().unwrap(),
                VersionID
            )
        };

        // step 2
        buf0.insert(
            META_KEY_VERSION_ID.to_vec().into(),
            Some((1 + ret).to_be_bytes().into()),
        );

        ret
    }

    #[inline(always)]
    fn area_count(&self) -> usize {
        DATA_SET_NUM
    }

    #[inline(always)]
    fn flush(&self) {
        self.flush_cache();
        self.flush_engine();
    }

    // flush cache every N seconds
    #[inline(always)]
    #[allow(unused_variables)]
    fn flush_cache(&self) {
        for i in 0..=DATA_SET_NUM {
            self.flush_area_cache(i);
        }
    }

    fn iter(&self, area_idx: usize, meta_prefix: PreBytes) -> RocksIter {
        let inner = self
            .meta
            .prefix_iterator_cf(self.cf_hdr(area_idx), meta_prefix);

        let mut opt = ReadOptions::default();
        opt.set_prefix_same_as_start(true);

        let inner_rev = self.meta.iterator_cf_opt(
            self.cf_hdr(area_idx),
            opt,
            IteratorMode::From(
                &self.get_upper_bound_value(meta_prefix),
                Direction::Reverse,
            ),
        );

        let buf0 = self.cache.buf0[area_idx].read();
        let buf1 = self.cache.buf1[area_idx].read();
        FLUSH_INDICATOR[area_idx].fetch_add(1, Ordering::Relaxed);
        let buf_head = buf0.clone();
        drop(buf0);

        let (mut cache_updated, mut cache_deleted) = buf_head
            .into_iter()
            .filter(|(k, _)| k[..PREFIX_SIZ] == meta_prefix[..])
            .map(|(k, v)| (k[PREFIX_SIZ..].into(), v))
            .fold((BTreeMap::new(), HashSet::new()), |mut acc, (k, v)| {
                if let Some(v) = v {
                    acc.0.insert(k, v);
                } else {
                    acc.1.insert(k);
                }
                acc
            });

        buf1.iter()
            .filter(|(k, _)| k[..PREFIX_SIZ] == meta_prefix[..])
            .for_each(|(k, v)| {
                if !cache_updated.contains_key(&k[PREFIX_SIZ..])
                    && !cache_deleted.contains(&k[PREFIX_SIZ..])
                {
                    let k = k[PREFIX_SIZ..].to_vec().into();
                    if let Some(v) = v {
                        cache_updated.insert(k, v.clone());
                    } else {
                        cache_deleted.insert(k);
                    }
                }
            });

        let cache_iter = cache_updated.into_iter();

        RocksIter {
            inner,
            inner_rev,

            cache_iter,
            cache_deleted,
            candidate_values: BTreeMap::new(),
        }
    }

    fn range<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        area_idx: usize,
        meta_prefix: PreBytes,
        bounds: R,
    ) -> RocksIter {
        let mut opt = ReadOptions::default();
        let mut opt_rev = ReadOptions::default();

        let mut b_lo = meta_prefix.to_vec();
        let l = match bounds.start_bound() {
            Bound::Included(lo) => {
                b_lo.extend_from_slice(lo);
                opt.set_iterate_lower_bound(b_lo.as_slice());
                opt_rev.set_iterate_lower_bound(b_lo.as_slice());
                b_lo.as_slice()
            }
            Bound::Excluded(lo) => {
                b_lo.extend_from_slice(lo);
                b_lo.push(0u8);
                opt.set_iterate_lower_bound(b_lo.as_slice());
                opt_rev.set_iterate_lower_bound(b_lo.as_slice());
                b_lo.as_slice()
            }
            _ => meta_prefix.as_slice(),
        };

        let mut b_hi = meta_prefix.to_vec();
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                b_hi.extend_from_slice(hi);
                b_hi.push(0u8);
                opt.set_iterate_upper_bound(b_hi.as_slice());
                opt_rev.set_iterate_upper_bound(b_hi.as_slice());
                b_hi
            }
            Bound::Excluded(hi) => {
                b_hi.extend_from_slice(hi);
                opt.set_iterate_upper_bound(b_hi.as_slice());
                opt_rev.set_iterate_upper_bound(b_hi.as_slice());
                b_hi
            }
            _ => self.get_upper_bound_value(meta_prefix),
        };

        opt.set_prefix_same_as_start(true);
        opt_rev.set_prefix_same_as_start(true);

        let inner = self.meta.iterator_cf_opt(
            self.cf_hdr(area_idx),
            opt,
            IteratorMode::From(l, Direction::Forward),
        );

        let inner_rev = self.meta.iterator_cf_opt(
            self.cf_hdr(area_idx),
            opt_rev,
            IteratorMode::From(&h, Direction::Reverse),
        );

        let buf0 = self.cache.buf0[area_idx].read();
        let buf1 = self.cache.buf1[area_idx].read();
        FLUSH_INDICATOR[area_idx].fetch_add(1, Ordering::Relaxed);
        let buf_head = buf0.clone();
        drop(buf0);

        let (mut cache_updated, mut cache_deleted) = buf_head
            .into_iter()
            .filter(|(k, _)| {
                k[..PREFIX_SIZ] == meta_prefix[..] && bounds.contains(&&k[PREFIX_SIZ..])
            })
            .map(|(k, v)| (k[PREFIX_SIZ..].into(), v))
            .fold((BTreeMap::new(), HashSet::new()), |mut acc, (k, v)| {
                if let Some(v) = v {
                    acc.0.insert(k, v);
                } else {
                    acc.1.insert(k);
                }
                acc
            });

        buf1.iter()
            .filter(|(k, _)| {
                k[..PREFIX_SIZ] == meta_prefix[..] && bounds.contains(&&k[PREFIX_SIZ..])
            })
            .for_each(|(k, v)| {
                if !cache_updated.contains_key(&k[PREFIX_SIZ..])
                    && !cache_deleted.contains(&k[PREFIX_SIZ..])
                {
                    let k = k[PREFIX_SIZ..].to_vec().into();
                    if let Some(v) = v {
                        cache_updated.insert(k, v.clone());
                    } else {
                        cache_deleted.insert(k);
                    }
                }
            });

        let cache_iter = cache_updated.into_iter();

        RocksIter {
            inner,
            inner_rev,

            cache_iter,
            cache_deleted,
            candidate_values: BTreeMap::new(),
        }
    }

    #[inline(always)]
    fn get(
        &self,
        area_idx: usize,
        meta_prefix: PreBytes,
        key: &[u8],
    ) -> Option<RawValue> {
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);

        let buf0 = self.cache.buf0[area_idx].read();
        let buf1 = self.cache.buf1[area_idx].read();

        if let Some(v) = buf0.get(&k[..]).or_else(|| buf1.get(&k[..])) {
            let res = v.clone();
            drop(buf0);
            res
        } else {
            drop(buf0);
            self.meta
                .get_cf(self.cf_hdr(area_idx), &k[..])
                .unwrap()
                .map(|v| v.to_vec().into())
        }
    }

    fn insert(
        &self,
        area_idx: usize,
        meta_prefix: PreBytes,
        key: &[u8],
        value: &[u8],
    ) -> Option<RawValue> {
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        let k = k.into_boxed_slice();

        if key.len() > self.get_max_keylen() {
            self.set_max_key_len(key.len());
        }

        let mut buf0 = self.cache.buf0[area_idx].write();
        let buf1 = self.cache.buf1[area_idx].read();

        let old_v = match buf0.get(&k[..]).or_else(|| buf1.get(&k[..])) {
            Some(v) => v.clone(),
            None => self
                .meta
                .get_cf(self.cf_hdr(area_idx), &k)
                .unwrap()
                .map(|v| v.to_vec().into()),
        };

        if let Some(v) = old_v.as_ref() {
            if value == &v[..] {
                return old_v;
            }
        }

        buf0.insert(k, Some(value.to_vec().into()));
        old_v
    }

    #[inline(always)]
    fn remove(
        &self,
        area_idx: usize,
        meta_prefix: PreBytes,
        key: &[u8],
    ) -> Option<RawValue> {
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        let k = k.into_boxed_slice();

        let mut buf0 = self.cache.buf0[area_idx].write();
        let buf1 = self.cache.buf1[area_idx].write();

        let old_v = match buf0.get(&k[..]).or_else(|| buf1.get(&k[..])) {
            Some(v) => v.clone(),
            None => self
                .meta
                .get_cf(self.cf_hdr(area_idx), &k[..])
                .unwrap()
                .map(|v| v.to_vec().into()),
        };

        if old_v.is_some() {
            buf0.insert(k, None);
        }

        old_v
    }

    #[inline(always)]
    fn get_instance_len(&self, instance_prefix: PreBytes) -> u64 {
        let buf0 = self.cache.buf0[DATA_SET_NUM].read();
        let buf1 = self.cache.buf1[DATA_SET_NUM].read();

        if let Some(v) = buf0
            .get(&instance_prefix[..])
            .or_else(|| buf1.get(&instance_prefix[..]))
        {
            let ret = crate::parse_int!(v.as_ref().unwrap(), u64);
            drop(buf0);
            ret
        } else {
            drop(buf0);
            crate::parse_int!(self.meta.get(instance_prefix).unwrap().unwrap(), u64)
        }
    }

    #[inline(always)]
    fn set_instance_len(&self, instance_prefix: PreBytes, new_len: u64) {
        self.cache.buf0[DATA_SET_NUM].write().insert(
            instance_prefix.to_vec().into(),
            Some(new_len.to_be_bytes().into()),
        );
    }

    #[inline(always)]
    fn increase_instance_len(&self, instance_prefix: PreBytes) {
        let mut buf0 = self.cache.buf0[DATA_SET_NUM].write();
        let buf1 = self.cache.buf1[DATA_SET_NUM].read();

        let l = if let Some(v) = buf0
            .get(&instance_prefix[..])
            .or_else(|| buf1.get(&instance_prefix[..]))
        {
            crate::parse_int!(v.as_ref().unwrap(), u64)
        } else {
            crate::parse_int!(self.meta.get(instance_prefix).unwrap().unwrap(), u64)
        };

        buf0.insert(
            instance_prefix.to_vec().into(),
            Some((l + 1).to_be_bytes().into()),
        );
    }

    #[inline(always)]
    fn decrease_instance_len(&self, instance_prefix: PreBytes) {
        let mut buf0 = self.cache.buf0[DATA_SET_NUM].write();
        let buf1 = self.cache.buf1[DATA_SET_NUM].write();

        let l = if let Some(v) = buf0
            .get(&instance_prefix[..])
            .or_else(|| buf1.get(&instance_prefix[..]))
        {
            crate::parse_int!(v.as_ref().unwrap(), u64)
        } else {
            crate::parse_int!(self.meta.get(instance_prefix).unwrap().unwrap(), u64)
        };

        buf0.insert(
            instance_prefix.to_vec().into(),
            Some((l - 1).to_be_bytes().into()),
        );
    }
}

pub struct RocksIter {
    inner: DBIterator<'static>,
    inner_rev: DBIterator<'static>,

    cache_iter: BIntoIter<RawKey, RawValue>,
    cache_deleted: HashSet<RawKey>,
    candidate_values: BTreeMap<RawKey, RawValue>,
}

impl Iterator for RocksIter {
    type Item = (RawKey, RawValue);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((k, v)) = self
            .inner
            .next()
            .map(|(ik, iv)| (ik[PREFIX_SIZ..].into(), iv))
        {
            if self.cache_deleted.contains(&k) {
                continue;
            } else if let BEntry::Vacant(e) = self.candidate_values.entry(k) {
                e.insert(v);
                break;
            } else {
                break;
            }
        }

        if let Some((k, v)) = self.cache_iter.next() {
            self.candidate_values.insert(k, v);
        }

        if let Some(k) = self.candidate_values.keys().next() {
            let k = k.clone();
            self.candidate_values.remove_entry(&k)
        } else {
            None
        }
    }
}

impl DoubleEndedIterator for RocksIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        while let Some((k, v)) = self
            .inner_rev
            .next()
            .map(|(ik, iv)| (ik[PREFIX_SIZ..].into(), iv))
        {
            if self.cache_deleted.contains(&k) {
                continue;
            } else if let BEntry::Vacant(e) = self.candidate_values.entry(k) {
                e.insert(v);
                break;
            } else {
                break;
            }
        }

        if let Some((k, v)) = self.cache_iter.next_back() {
            self.candidate_values.insert(k, v);
        }

        if let Some(k) = self.candidate_values.keys().next_back() {
            let k = k.clone();
            self.candidate_values.remove_entry(&k)
        } else {
            None
        }
    }
}

// key of the prefix allocator in the 'meta'
struct PreAllocator {
    key: [u8; 1],
}

impl PreAllocator {
    const fn init() -> (Self, PreBytes) {
        (
            Self {
                key: META_KEY_PREFIX_ALLOCATOR,
            },
            (RESERVED_ID_CNT + Pre::MIN).to_be_bytes(),
        )
    }

    // fn next(base: &[u8]) -> [u8; PREFIX_SIZ] {
    //     (crate::parse_prefix!(base) + 1).to_be_bytes()
    // }
}

type CacheMap = HashMap<RawKey, Option<RawValue>>;

#[derive(Default)]
struct Cache {
    // area idx => kv set
    // NOTE:
    // the last item is the cache of meta data,
    // aka `cache_map[DATA_SET_NUM]`
    buf0: Vec<Arc<RwLock<CacheMap>>>,
    buf1: Vec<Arc<RwLock<CacheMap>>>,
}

impl Cache {
    fn new() -> Self {
        Self {
            buf0: (0..=DATA_SET_NUM)
                .map(|_| Arc::new(RwLock::new(HashMap::new())))
                .collect(),
            buf1: (0..=DATA_SET_NUM)
                .map(|_| Arc::new(RwLock::new(HashMap::new())))
                .collect(),
        }
    }
}

fn rocksdb_open() -> Result<(DB, Vec<String>)> {
    let dir = vsdb_get_base_dir();

    // avoid setting again on an opened DB
    info_omit!(vsdb_set_base_dir(&dir));

    let cpunum = available_parallelism().map(usize::from).unwrap_or(8);
    let cpunum = max!(cpunum, 16) as i32;

    let mut cfg = Options::default();
    cfg.create_if_missing(true);
    cfg.create_missing_column_families(true);
    cfg.set_prefix_extractor(SliceTransform::create_fixed_prefix(size_of::<Pre>()));
    cfg.increase_parallelism(cpunum);
    cfg.set_num_levels(7);
    cfg.set_max_open_files(1000_0000);
    cfg.set_allow_mmap_writes(true);
    cfg.set_allow_mmap_reads(true);
    // cfg.set_use_direct_reads(true);
    // cfg.set_use_direct_io_for_flush_and_compaction(true);
    cfg.set_write_buffer_size(512 * MB as usize);
    cfg.set_max_write_buffer_number(3);

    #[cfg(feature = "compress")]
    {
        cfg.set_compression_type(DBCompressionType::Lz4);
    }

    #[cfg(not(feature = "compress"))]
    {
        cfg.set_compression_type(DBCompressionType::None);
    }

    let cfhdrs = (0..DATA_SET_NUM).map(|i| i.to_string()).collect::<Vec<_>>();

    let cfs = cfhdrs
        .iter()
        .map(|i| ColumnFamilyDescriptor::new(i, cfg.clone()))
        .collect::<Vec<_>>();

    let db = DB::open_cf_descriptors(&cfg, &dir, cfs).c(d!())?;

    Ok((db, cfhdrs))
}
