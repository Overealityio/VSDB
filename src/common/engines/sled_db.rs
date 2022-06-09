use crate::common::{
    vsdb_get_base_dir, vsdb_set_base_dir, BranchID, Engine, Pre, PreBytes, RawKey,
    RawValue, VersionID, GB, INITIAL_BRANCH_ID, PREFIX_SIZ, RESERVED_ID_CNT,
};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use ruc::*;
use sled::{Batch, Config, Db, IVec, Iter, Mode, Tree};
use std::{
    collections::{
        btree_map::{Entry as BEntry, IntoIter as BIntoIter},
        BTreeMap,
    },
    mem,
    ops::{Bound, RangeBounds},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

// the 'prefix search' in sled is just a global scaning,
// use a relative larger number to sharding the `Tree` pressure.
const DATA_SET_NUM: usize = 1023;

const META_KEY_BRANCH_ID: [u8; 1] = [u8::MAX - 1];
const META_KEY_VERSION_ID: [u8; 1] = [u8::MAX - 2];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

static FLUSH_INDICATOR: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| (0..=DATA_SET_NUM).map(|_| AtomicU64::new(0)).collect());

pub(crate) struct SledEngine {
    meta: Db,
    areas: Vec<Tree>,
    prefix_allocator: PreAllocator,
    cache: Cache,
}

impl SledEngine {
    fn update_batch(&self, data: CacheMap, area_idx: usize) -> Result<()> {
        alt!(data.is_empty(), return Ok(()));

        let mut batch = Batch::default();
        for (extended_key, value) in data.into_iter() {
            if let Some(v) = value {
                batch.insert(extended_key, v);
            } else {
                batch.remove(extended_key);
            }
        }

        if DATA_SET_NUM == area_idx {
            // update meta
            self.meta.apply_batch(batch).c(d!())
        } else {
            self.areas[area_idx].apply_batch(batch).c(d!())
        }
    }

    #[inline(always)]
    #[allow(unused_variables)]
    fn flush_area_cache(&self, area_idx: usize) {
        static LK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
        let x = LK.lock();

        if let Some(mut buf) = self.cache.buf[area_idx].try_write() {
            if 0 == FLUSH_INDICATOR[area_idx].load(Ordering::Relaxed) {
                let data = mem::take(&mut buf[0]);
                buf[1] = data.clone();
                drop(buf);
                pnk!(self.update_batch(data, area_idx));
            }
        }
    }

    #[inline(always)]
    fn flush_engine(&self) {
        self.meta.flush().unwrap();
        (0..self.areas.len()).for_each(|i| {
            self.areas[i].flush().unwrap();
        });
    }
}

impl Engine for SledEngine {
    fn new() -> Result<Self> {
        let meta = sled_open().c(d!())?;

        let areas = (0..DATA_SET_NUM)
            .map(|idx| meta.open_tree(idx.to_be_bytes()).c(d!()))
            .collect::<Result<Vec<_>>>()?;

        let (prefix_allocator, initial_value) = PreAllocator::init();

        if meta.get(&META_KEY_BRANCH_ID).c(d!())?.is_none() {
            meta.insert(
                META_KEY_BRANCH_ID,
                (1 + INITIAL_BRANCH_ID as usize).to_be_bytes(),
            )
            .c(d!())?;
        }

        if meta.get(&META_KEY_VERSION_ID).c(d!())?.is_none() {
            meta.insert(META_KEY_VERSION_ID, 0_usize.to_be_bytes())
                .c(d!())?;
        }

        if meta.get(prefix_allocator.key).c(d!())?.is_none() {
            meta.insert(prefix_allocator.key, initial_value).c(d!())?;
        }

        Ok(SledEngine {
            meta,
            areas,
            prefix_allocator,
            cache: Cache::new(),
        })
    }

    // 'step 1' and 'step 2' is not atomic in multi-threads scene,
    // so we should get the 'write' lock for thread safe.
    #[allow(unused_variables)]
    fn alloc_prefix(&self) -> Pre {
        // step 1
        let mut buf = self.cache.buf[DATA_SET_NUM].write();

        let ret = if let Some(v) = buf[0]
            .get(&self.prefix_allocator.key[..])
            .or_else(|| buf[1].get(&self.prefix_allocator.key[..]))
        {
            let ret = crate::parse_prefix!(v.as_ref().unwrap());
            ret
        } else {
            crate::parse_prefix!(
                self.meta.get(self.prefix_allocator.key).unwrap().unwrap()
            )
        };

        // step 2
        buf[0].insert(
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
        let mut buf = self.cache.buf[DATA_SET_NUM].write();

        let ret = if let Some(v) = buf[0]
            .get(&META_KEY_BRANCH_ID[..])
            .or_else(|| buf[1].get(&META_KEY_BRANCH_ID[..]))
        {
            let ret = crate::parse_int!(v.as_ref().unwrap(), BranchID);
            ret
        } else {
            crate::parse_int!(
                self.meta.get(META_KEY_BRANCH_ID).unwrap().unwrap(),
                BranchID
            )
        };

        // step 2
        buf[0].insert(
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
        let mut buf = self.cache.buf[DATA_SET_NUM].write();

        let ret = if let Some(v) = buf[0]
            .get(&META_KEY_VERSION_ID[..])
            .or_else(|| buf[1].get(&META_KEY_VERSION_ID[..]))
        {
            let ret = crate::parse_int!(v.as_ref().unwrap(), VersionID);
            ret
        } else {
            crate::parse_int!(
                self.meta.get(META_KEY_VERSION_ID).unwrap().unwrap(),
                VersionID
            )
        };

        // step 2
        buf[0].insert(
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

    #[inline(always)]
    fn iter(&self, area_idx: usize, meta_prefix: PreBytes) -> SledIter {
        let (buf_head, buf_middle) = {
            let buf = self.cache.buf[area_idx].read();
            FLUSH_INDICATOR[area_idx].fetch_add(1, Ordering::Relaxed);
            (buf[0].clone(), buf[1].clone())
        };

        let (cache_updated, cache_deleted) = buf_middle
            .into_iter()
            .chain(buf_head.into_iter())
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
        let cache_iter = cache_updated.into_iter();

        SledIter {
            inner: self.areas[area_idx].scan_prefix(meta_prefix.as_slice()),
            bounds: (Bound::Unbounded, Bound::Unbounded),
            cache_iter,
            cache_deleted,
            candidate_values: BTreeMap::new(),
            area_idx,
        }
    }

    fn range<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        area_idx: usize,
        meta_prefix: PreBytes,
        bounds: R,
    ) -> SledIter {
        let mut b_lo = meta_prefix.to_vec();
        let l = match bounds.start_bound() {
            Bound::Included(lo) => {
                b_lo.extend_from_slice(lo);
                Bound::Included(IVec::from(b_lo))
            }
            Bound::Excluded(lo) => {
                b_lo.extend_from_slice(lo);
                Bound::Excluded(IVec::from(b_lo))
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let mut b_hi = meta_prefix.to_vec();
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                b_hi.extend_from_slice(hi);
                Bound::Included(IVec::from(b_hi))
            }
            Bound::Excluded(hi) => {
                b_hi.extend_from_slice(hi);
                Bound::Excluded(IVec::from(b_hi))
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let (buf_head, buf_middle) = {
            let buf = self.cache.buf[area_idx].read();
            FLUSH_INDICATOR[area_idx].fetch_add(1, Ordering::Relaxed);
            (buf[0].clone(), buf[1].clone())
        };

        let (cache_updated, cache_deleted) = buf_middle
            .into_iter()
            .chain(buf_head.into_iter())
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
        let cache_iter = cache_updated.into_iter();

        SledIter {
            inner: self.areas[area_idx].scan_prefix(meta_prefix.as_slice()),
            bounds: (l, h),
            cache_iter,
            cache_deleted,
            candidate_values: BTreeMap::new(),
            area_idx,
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

        let buf = self.cache.buf[area_idx].read();

        if let Some(v) = buf[0].get(&k[..]).or_else(|| buf[1].get(&k[..])) {
            let res = v.clone();
            drop(buf);
            res
        } else {
            drop(buf);
            self.areas[area_idx]
                .get(&k)
                .unwrap()
                .map(|v| v.to_vec().into())
        }
    }

    #[inline(always)]
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

        let mut buf = self.cache.buf[area_idx].write();

        let old_v = match buf[0].get(&k[..]).or_else(|| buf[1].get(&k[..])) {
            Some(v) => v.clone(),
            None => self.areas[area_idx]
                .get(&k)
                .unwrap()
                .map(|v| v.to_vec().into()),
        };

        if let Some(v) = old_v.as_ref() {
            if value == &v[..] {
                return old_v;
            }
        }

        buf[0].insert(k, Some(value.to_vec().into()));
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

        let mut buf = self.cache.buf[area_idx].write();

        let old_v = match buf[0].get(&k[..]).or_else(|| buf[1].get(&k[..])) {
            Some(v) => v.clone(),
            None => self.areas[area_idx]
                .get(&k)
                .unwrap()
                .map(|v| v.to_vec().into()),
        };

        if old_v.is_some() {
            buf[0].insert(k, None);
        }

        old_v
    }

    #[inline(always)]
    fn get_instance_len(&self, instance_prefix: PreBytes) -> u64 {
        let buf = self.cache.buf[DATA_SET_NUM].read();

        if let Some(v) = buf[0]
            .get(&instance_prefix[..])
            .or_else(|| buf[1].get(&instance_prefix[..]))
        {
            let ret = crate::parse_int!(v.as_ref().unwrap(), u64);
            drop(buf);
            ret
        } else {
            drop(buf);
            crate::parse_int!(self.meta.get(instance_prefix).unwrap().unwrap(), u64)
        }
    }

    #[inline(always)]
    fn set_instance_len(&self, instance_prefix: PreBytes, new_len: u64) {
        self.cache.buf[DATA_SET_NUM].write()[0].insert(
            instance_prefix.to_vec().into(),
            Some(new_len.to_be_bytes().into()),
        );
    }

    #[inline(always)]
    fn increase_instance_len(&self, instance_prefix: PreBytes) {
        let mut buf = self.cache.buf[DATA_SET_NUM].write();

        let l = if let Some(v) = buf[0]
            .get(&instance_prefix[..])
            .or_else(|| buf[1].get(&instance_prefix[..]))
        {
            crate::parse_int!(v.as_ref().unwrap(), u64)
        } else {
            crate::parse_int!(self.meta.get(instance_prefix).unwrap().unwrap(), u64)
        };

        buf[0].insert(
            instance_prefix.to_vec().into(),
            Some((l + 1).to_be_bytes().into()),
        );
    }

    #[inline(always)]
    fn decrease_instance_len(&self, instance_prefix: PreBytes) {
        let mut buf = self.cache.buf[DATA_SET_NUM].write();

        let l = if let Some(v) = buf[0]
            .get(&instance_prefix[..])
            .or_else(|| buf[1].get(&instance_prefix[..]))
        {
            crate::parse_int!(v.as_ref().unwrap(), u64)
        } else {
            crate::parse_int!(self.meta.get(instance_prefix).unwrap().unwrap(), u64)
        };

        buf[0].insert(
            instance_prefix.to_vec().into(),
            Some((l - 1).to_be_bytes().into()),
        );
    }
}

pub struct SledIter {
    inner: Iter,
    bounds: (Bound<IVec>, Bound<IVec>),
    cache_iter: BIntoIter<RawKey, RawValue>,
    cache_deleted: HashSet<RawKey>,
    candidate_values: BTreeMap<RawKey, RawValue>,
    area_idx: usize,
}

impl Drop for SledIter {
    fn drop(&mut self) {
        FLUSH_INDICATOR[self.area_idx].fetch_sub(1, Ordering::Relaxed);
    }
}

impl Iterator for SledIter {
    type Item = (RawKey, RawValue);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((k, v)) = self.inner.next().map(|i| i.unwrap()) {
            match self.bounds.start_bound() {
                Bound::Included(l) => alt!(&k < l, continue),
                Bound::Unbounded => {}
                Bound::Excluded(l) => alt!(&k <= l, continue),
            }

            match self.bounds.end_bound() {
                Bound::Excluded(u) => alt!(&k >= u, break),
                Bound::Included(u) => alt!(&k > u, break),
                Bound::Unbounded => {}
            }

            if self.cache_deleted.contains(&k[PREFIX_SIZ..]) {
                continue;
            } else if let BEntry::Vacant(e) =
                self.candidate_values.entry(k[PREFIX_SIZ..].into())
            {
                e.insert(v.to_vec().into());
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

impl DoubleEndedIterator for SledIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        while let Some((k, v)) = self.inner.next_back().map(|i| i.unwrap()) {
            match self.bounds.start_bound() {
                Bound::Included(l) => alt!(&k < l, break),
                Bound::Unbounded => {}
                Bound::Excluded(l) => alt!(&k <= l, break),
            }

            match self.bounds.end_bound() {
                Bound::Excluded(u) => alt!(&k >= u, continue),
                Bound::Included(u) => alt!(&k > u, continue),
                Bound::Unbounded => {}
            }

            if self.cache_deleted.contains(&k[PREFIX_SIZ..]) {
                continue;
            } else if let BEntry::Vacant(e) =
                self.candidate_values.entry(k[PREFIX_SIZ..].into())
            {
                e.insert(v.to_vec().into());
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
}
type CacheMap = HashMap<RawKey, Option<RawValue>>;

#[derive(Default)]
struct Cache {
    // area idx => kv set
    // NOTE:
    // the last item is the cache of meta data,
    // aka `cache_map[DATA_SET_NUM]`
    buf: Vec<Arc<RwLock<[CacheMap; 2]>>>,
}

impl Cache {
    fn new() -> Self {
        Self {
            buf: (0..=DATA_SET_NUM)
                .map(|_| Arc::new(RwLock::new([HashMap::new(), HashMap::new()])))
                .collect(),
        }
    }
}

fn sled_open() -> Result<Db> {
    let dir = vsdb_get_base_dir();

    // avoid setting again on an opened DB
    info_omit!(vsdb_set_base_dir(&dir));

    let mut cfg = Config::new()
        .path(&dir)
        .mode(Mode::HighThroughput)
        .cache_capacity(10 * GB);

    #[cfg(feature = "compress")]
    {
        cfg = cfg.use_compression(true).compression_factor(1);
    }

    #[cfg(not(feature = "compress"))]
    {
        cfg = cfg.use_compression(false);
    }

    cfg.open().c(d!())
}
