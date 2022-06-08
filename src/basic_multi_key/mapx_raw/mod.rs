//!
//! A multi-key style of `MapxRaw`.
//!
//! NOTE:
//! - Both keys and values will **NOT** be encoded in this structure
//!

#[cfg(test)]
mod test;

use crate::{
    basic::mapx_raw::MapxRaw,
    common::{ende::ValueEnDe, RawValue},
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxRawMk {
    // Will never be changed once created
    key_size: usize,
    // A nested map-structure, looks like:
    // map { key => map { key => map { key => value } } }
    inner: MapxRaw,
}

impl MapxRawMk {
    /// # Panic
    /// Will panic if `0 == key_size`.
    #[inline(always)]
    pub fn new(key_size: usize) -> Self {
        assert!(0 < key_size);
        Self {
            key_size,
            inner: MapxRaw::new(),
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &[&[u8]]) -> Option<RawValue> {
        if key.len() != self.key_size {
            return None;
        }

        let mut hdr = self.inner;
        for (idx, k) in key.iter().enumerate() {
            if let Some(v) = hdr.get(k) {
                if 1 + idx == self.key_size {
                    return Some(v);
                } else {
                    hdr = pnk!(ValueEnDe::decode(&v));
                }
            } else {
                return None;
            }
        }

        None // empty key
    }

    #[inline(always)]
    pub fn get_mut<'a>(&'a mut self, key: &'a [&'a [u8]]) -> Option<ValueMut<'a>> {
        self.get(key).map(move |v| ValueMut::new(self, key, v))
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &[&[u8]]) -> bool {
        self.get(key).is_some()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline(always)]
    pub fn entry_ref<'a>(&'a mut self, key: &'a [&'a [u8]]) -> Entry<'a> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn insert(&mut self, key: &[&[u8]], value: &[u8]) -> Result<Option<RawValue>> {
        if key.len() != self.key_size {
            return Err(eg!("Incorrect key size"));
        }

        let mut ret = None;

        let mut hdr = self.inner;
        for (idx, k) in key.iter().enumerate() {
            if 1 + idx == self.key_size {
                ret = hdr.insert(k, value);
                break;
            } else {
                let mut new_hdr = None;
                let f = || {
                    new_hdr.replace(MapxRaw::new());
                    new_hdr.as_ref().unwrap().encode()
                };
                let mutv = hdr.entry_ref(k).or_insert_ref_with(f);
                let h = if let Some(h) = new_hdr {
                    h
                } else {
                    pnk!(ValueEnDe::decode(mutv.as_ref()))
                };
                drop(mutv);
                hdr = h;
            }
        }

        Ok(ret)
    }

    /// Support batch removal.
    #[inline(always)]
    pub fn remove(&mut self, key: &[&[u8]]) -> Result<Option<RawValue>> {
        // Support batch removal from key path.
        if key.len() > self.key_size {
            return Err(eg!("Incorrect key size"));
        }

        let mut hdr = self.inner;
        for (idx, k) in key.iter().enumerate() {
            if let Some(v) = hdr.get(k) {
                // NOTE: use `key.len()` instead of `self.key_size`
                if 1 + idx == key.len() {
                    let ret = hdr.remove(k);
                    // NOTE: use `self.key_size` instead of `key.len()`
                    if 1 + idx == self.key_size {
                        return Ok(ret);
                    } else {
                        return Ok(None);
                    }
                } else {
                    hdr = pnk!(ValueEnDe::decode(&v));
                }
            } else {
                return Ok(None);
            }
        }

        Ok(None) // empty key
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline(always)]
    pub fn iter_op<F>(&self, op: &mut F) -> Result<()>
    where
        F: FnMut(&[&[u8]], &[u8]) -> Result<()>,
    {
        self.iter_op_with_key_prefix(op, &[]).c(d!())
    }

    #[inline(always)]
    pub fn iter_op_with_key_prefix<F>(
        &self,
        op: &mut F,
        key_prefix: &[&[u8]],
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], &[u8]) -> Result<()>,
    {
        let mut key_buf = vec![Default::default(); self.key_size()];
        let mut hdr = self.inner;
        let mut depth = self.key_size();

        if self.key_size < key_prefix.len() {
            return Err(eg!("Invalid key size"));
        } else {
            for (idx, k) in key_prefix.iter().enumerate() {
                if let Some(v) = hdr.get(k) {
                    key_buf[idx] = k.to_vec().into_boxed_slice();
                    if 1 + idx == self.key_size {
                        let key = key_buf
                            .iter()
                            .map(|sub_k| sub_k.as_ref())
                            .collect::<Vec<_>>();
                        return op(key.as_slice(), &v).c(d!());
                    } else {
                        hdr = pnk!(ValueEnDe::decode(&v));
                        depth -= 1;
                    }
                } else {
                    // key-prefix does not exist
                    return Ok(());
                }
            }
        };

        self.recursive_walk(hdr, &mut key_buf, depth, op).c(d!())
    }

    fn recursive_walk<F>(
        &self,
        hdr: MapxRaw,
        key_buf: &mut [RawValue],
        depth: usize,
        op: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], &[u8]) -> Result<()>,
    {
        let idx = self.key_size() - depth;
        if 1 == depth {
            for (k, v) in hdr.iter() {
                key_buf[idx] = k;
                let key = key_buf
                    .iter()
                    .map(|sub_k| sub_k.as_ref())
                    .collect::<Vec<_>>();
                op(key.as_slice(), &v[..]).c(d!())?;
            }
        } else {
            for (k, v) in hdr.iter() {
                key_buf[idx] = k;
                let hdr = pnk!(ValueEnDe::decode(&v));
                self.recursive_walk(hdr, key_buf, depth - 1, op).c(d!())?;
            }
        }

        Ok(())
    }

    #[inline(always)]
    pub(super) fn iter_op_typed_value<V, F>(&self, op: &mut F) -> Result<()>
    where
        F: FnMut(&[&[u8]], &V) -> Result<()>,
        V: ValueEnDe,
    {
        self.iter_op_typed_value_with_key_prefix(op, &[]).c(d!())
    }

    #[inline(always)]
    pub fn iter_op_typed_value_with_key_prefix<V, F>(
        &self,
        op: &mut F,
        key_prefix: &[&[u8]],
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], &V) -> Result<()>,
        V: ValueEnDe,
    {
        let mut key_buf = vec![Default::default(); self.key_size()];
        let mut hdr = self.inner;
        let mut depth = self.key_size();

        if self.key_size < key_prefix.len() {
            return Err(eg!("Invalid key size"));
        } else {
            for (idx, k) in key_prefix.iter().enumerate() {
                if let Some(v) = hdr.get(k) {
                    key_buf[idx] = k.to_vec().into_boxed_slice();
                    if 1 + idx == self.key_size {
                        let key = key_buf
                            .iter()
                            .map(|sub_k| sub_k.as_ref())
                            .collect::<Vec<_>>();
                        return op(key.as_slice(), &pnk!(ValueEnDe::decode(&v))).c(d!());
                    } else {
                        hdr = pnk!(ValueEnDe::decode(&v));
                        depth -= 1;
                    }
                } else {
                    // key-prefix does not exist
                    return Ok(());
                }
            }
        };

        self.recursive_walk_typed_value(hdr, &mut key_buf, depth, op)
            .c(d!())
    }

    fn recursive_walk_typed_value<V, F>(
        &self,
        hdr: MapxRaw,
        key_buf: &mut [RawValue],
        depth: usize,
        op: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], &V) -> Result<()>,
        V: ValueEnDe,
    {
        let idx = self.key_size() - depth;
        if 1 == depth {
            for (k, v) in hdr.iter() {
                key_buf[idx] = k;
                let key = key_buf
                    .iter()
                    .map(|sub_k| sub_k.as_ref())
                    .collect::<Vec<_>>();
                op(key.as_slice(), &pnk!(ValueEnDe::decode(&v))).c(d!())?;
            }
        } else {
            for (k, v) in hdr.iter() {
                key_buf[idx] = k;
                let hdr = pnk!(ValueEnDe::decode(&v));
                self.recursive_walk_typed_value(hdr, key_buf, depth - 1, op)
                    .c(d!())?;
            }
        }

        Ok(())
    }

    #[inline(always)]
    pub fn key_size(&self) -> usize {
        self.key_size
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct ValueMut<'a> {
    hdr: &'a mut MapxRawMk,
    key: &'a [&'a [u8]],
    value: RawValue,
}

impl<'a> ValueMut<'a> {
    fn new(hdr: &'a mut MapxRawMk, key: &'a [&'a [u8]], value: RawValue) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<'a> Drop for ValueMut<'a> {
    fn drop(&mut self) {
        pnk!(self.hdr.insert(self.key, &self.value));
    }
}

impl<'a> Deref for ValueMut<'a> {
    type Target = RawValue;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a> DerefMut for ValueMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

pub struct Entry<'a> {
    hdr: &'a mut MapxRawMk,
    key: &'a [&'a [u8]],
}

impl<'a> Entry<'a> {
    pub fn or_insert_ref(self, default: &'a [u8]) -> Result<ValueMut<'a>> {
        if !self.hdr.contains_key(self.key) {
            self.hdr.insert(self.key, default).c(d!())?;
        }
        self.hdr.get_mut(self.key).c(d!())
    }
}
