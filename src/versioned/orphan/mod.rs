//!
//! Documents => [MapxRawVs](crate::versioned::mapx_raw)
//!

#[cfg(test)]
mod test;

use crate::{
    versioned::mapx_ord_rawkey::MapxOrdRawKeyVs, BranchName, ValueEnDe, VersionName,
    VsMgmt,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

/// Used to express some 'non-collection' types,
/// such as any type of integer, an enum value, etc..
///
/// Documents => [MapxRawVs](crate::versioned::mapx_raw::MapxRawVs)
#[derive(Serialize, Deserialize, Debug)]
#[serde(bound = "")]
pub struct OrphanVs<T> {
    inner: MapxOrdRawKeyVs<T>,
}

impl<T> Clone for OrphanVs<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: ValueEnDe> OrphanVs<T> {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            inner: MapxOrdRawKeyVs::new(),
        }
    }

    #[inline(always)]
    pub fn get_value(&self) -> Option<T> {
        // value of the default branch must exists
        self.inner.get(&[])
    }

    /// Get the mutable handler of the value.
    ///
    /// NOTE:
    /// - Always use this method to change value
    ///     - `*(<Orphan>).get_mut() = ...`
    /// - **NEVER** do this:
    ///     - `*(&mut <Orphan>) = Orphan::new(...)`
    ///     - OR you will loss the 'versioned' ability of this object
    pub fn get_mut(&mut self) -> Option<ValueMut<'_, T>> {
        self.get_value().map(|value| ValueMut { hdr: self, value })
    }

    #[inline(always)]
    pub fn set_value(&mut self, v: T) -> Result<Option<T>> {
        self.set_value_ref(&v).c(d!())
    }

    #[inline(always)]
    pub fn set_value_ref(&mut self, v: &T) -> Result<Option<T>> {
        self.inner.insert_ref(&[], v).c(d!())
    }

    #[inline(always)]
    pub fn get_value_by_branch(&self, branch_name: BranchName) -> Option<T> {
        self.inner.get_by_branch(&[], branch_name)
    }

    #[inline(always)]
    pub fn set_value_by_branch(
        &mut self,
        v: T,
        branch_name: BranchName,
    ) -> Result<Option<T>> {
        self.set_value_ref_by_branch(&v, branch_name).c(d!())
    }

    #[inline(always)]
    pub fn set_value_ref_by_branch(
        &mut self,
        v: &T,
        branch_name: BranchName,
    ) -> Result<Option<T>> {
        self.inner.insert_ref_by_branch(&[], v, branch_name).c(d!())
    }

    #[inline(always)]
    pub fn get_value_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<T> {
        self.inner
            .get_by_branch_version(&[], branch_name, version_name)
    }
}

impl<T: ValueEnDe> Default for OrphanVs<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> VsMgmt for OrphanVs<T>
where
    T: ValueEnDe,
{
    crate::impl_vs_methods!();
}

/// A type returned by `get_mut()`.
pub struct ValueMut<'a, T>
where
    T: ValueEnDe,
{
    hdr: &'a mut OrphanVs<T>,
    value: T,
}

impl<'a, T> Drop for ValueMut<'a, T>
where
    T: ValueEnDe,
{
    fn drop(&mut self) {
        pnk!(self.hdr.set_value_ref(&self.value));
    }
}

impl<'a, T> Deref for ValueMut<'a, T>
where
    T: ValueEnDe,
{
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, T> DerefMut for ValueMut<'a, T>
where
    T: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
