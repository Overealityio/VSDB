//!
//! # Common components
//!

pub(crate) mod ende;
pub(crate) mod engines;

#[cfg(feature = "hash")]
pub(crate) mod utils;

use engines::Engine;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use ruc::*;
use std::{
    env, fs,
    mem::size_of,
    sync::atomic::{AtomicBool, Ordering},
    thread,
};

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub(crate) type RawBytes = Box<[u8]>;
pub(crate) type RawKey = RawBytes;
pub(crate) type RawValue = RawBytes;

pub(crate) type Pre = u64;
pub(crate) type PreBytes = [u8; PREFIX_SIZ];
pub(crate) const PREFIX_SIZ: usize = size_of::<Pre>();

pub(crate) type BranchID = u64;
pub(crate) type VersionID = u64;

/// Avoid making mistakes between branch name and version name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BranchName<'a>(pub &'a [u8]);
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct BranchNameOwned(pub Vec<u8>);
/// Avoid making mistakes between branch name and version name.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub struct ParentBranchName<'a>(pub &'a [u8]);
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ParentBranchNameOwned(pub Vec<u8>);
/// Avoid making mistakes between branch name and version name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VersionName<'a>(pub &'a [u8]);
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct VersionNameOwned(pub Vec<u8>);

pub const KB: u64 = 1 << 10;
pub const MB: u64 = 1 << 20;
pub const GB: u64 = 1 << 30;

const RESERVED_ID_CNT: Pre = 4096_0000;
pub(crate) const BIGGEST_RESERVED_ID: Pre = RESERVED_ID_CNT - 1;
pub(crate) const NULL: BranchID = BIGGEST_RESERVED_ID as BranchID;

pub(crate) const INITIAL_BRANCH_ID: BranchID = 0;
pub(crate) const INITIAL_BRANCH_NAME: BranchName<'static> = BranchName(b"master");

// default value for reserved number when pruning old data
pub(crate) const RESERVED_VERSION_NUM_DEFAULT: usize = 10;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

const BASE_DIR_VAR: &str = "VSDB_BASE_DIR";

static VSDB_BASE_DIR: Lazy<Mutex<String>> = Lazy::new(|| Mutex::new(gen_data_dir()));

static VSDB_CUSTOM_DIR: Lazy<String> = Lazy::new(|| {
    let d = VSDB_BASE_DIR.lock().clone() + "/__CUSTOM__";
    fs::create_dir_all(&d).unwrap();
    env::set_var("VSDB_CUSTOM_DIR", &d);
    d
});

// flush in-memory cache to disk every N millisseconds
const CACHE_FLUSH_ITV_MS: u64 = 1;

#[cfg(any(
    feature = "rocks_engine",
    all(feature = "rocks_engine", feature = "sled_engine"),
    all(not(feature = "rocks_engine"), not(feature = "sled_engine")),
))]
pub(crate) static VSDB: Lazy<VsDB<engines::RocksDB>> = Lazy::new(|| {
    let res = pnk!(VsDB::new());
    thread::spawn(|| {
        sleep_ms!(1);
        loop {
            sleep_ms!(CACHE_FLUSH_ITV_MS);
            VSDB.db.flush_cache();
        }
    });
    res
});

#[cfg(all(feature = "sled_engine", not(feature = "rocks_engine")))]
pub(crate) static VSDB: Lazy<VsDB<engines::Sled>> = Lazy::new(|| {
    let res = pnk!(VsDB::new());
    thread::spawn(|| {
        sleep_ms!(1);
        loop {
            sleep_ms!(CACHE_FLUSH_ITV_MS);
            VSDB.db.flush_cache();
        }
    });
    res
});

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// Parse bytes to a specified integer type.
#[macro_export(crate)]
macro_rules! parse_int {
    ($bytes: expr, $ty: ty) => {{
        let array: [u8; std::mem::size_of::<$ty>()] = $bytes[..].try_into().unwrap();
        <$ty>::from_be_bytes(array)
    }};
}

/// Parse bytes to a `Pre` type.
#[macro_export(crate)]
macro_rules! parse_prefix {
    ($bytes: expr) => {
        $crate::parse_int!($bytes, $crate::common::Pre)
    };
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub(crate) struct VsDB<T: Engine> {
    db: T,
}

impl<T: Engine> VsDB<T> {
    #[inline(always)]
    fn new() -> Result<Self> {
        Ok(Self {
            db: T::new().c(d!())?,
        })
    }

    #[inline(always)]
    pub(crate) fn alloc_branch_id(&self) -> BranchID {
        self.db.alloc_branch_id()
    }

    #[inline(always)]
    pub(crate) fn alloc_version_id(&self) -> VersionID {
        self.db.alloc_version_id()
    }

    #[inline(always)]
    fn flush(&self) {
        self.db.flush()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[inline(always)]
fn gen_data_dir() -> String {
    // Compatible with Windows OS?
    let d = env::var(BASE_DIR_VAR)
        .or_else(|_| env::var("HOME").map(|h| format!("{}/.vsdb", h)))
        .unwrap_or_else(|_| "/tmp/.vsdb".to_owned());
    fs::create_dir_all(&d).unwrap();
    d
}

/// ${VSDB_CUSTOM_DIR}
#[inline(always)]
pub fn vsdb_get_custom_dir() -> String {
    VSDB_CUSTOM_DIR.clone()
}

/// ${VSDB_BASE_DIR}
#[inline(always)]
pub fn vsdb_get_base_dir() -> String {
    VSDB_BASE_DIR.lock().clone()
}

/// Set ${VSDB_BASE_DIR} manually.
#[inline(always)]
pub fn vsdb_set_base_dir(dir: &str) -> Result<()> {
    static HAS_INITED: AtomicBool = AtomicBool::new(false);

    if HAS_INITED.swap(true, Ordering::Relaxed) {
        Err(eg!("VSDB has been initialized !!"))
    } else {
        env::set_var(BASE_DIR_VAR, dir);
        *VSDB_BASE_DIR.lock() = dir.to_owned();
        Ok(())
    }
}

/// Flush data to disk, may take a long time.
#[inline(always)]
pub fn vsdb_flush() {
    VSDB.flush();
}

macro_rules! impl_from_for_name {
    ($target: tt) => {
        impl<'a> From<&'a [u8]> for $target<'a> {
            fn from(t: &'a [u8]) -> Self {
                $target(t)
            }
        }
        impl<'a> From<&'a Vec<u8>> for $target<'a> {
            fn from(t: &'a Vec<u8>) -> Self {
                $target(t.as_slice())
            }
        }
        impl<'a> From<&'a str> for $target<'a> {
            fn from(t: &'a str) -> Self {
                $target(t.as_bytes())
            }
        }
        impl<'a> From<&'a String> for $target<'a> {
            fn from(t: &'a String) -> Self {
                $target(t.as_bytes())
            }
        }
    };
    ($target: tt, $($t: tt),+) => {
        impl_from_for_name!($target);
        impl_from_for_name!($($t), +);
    };
}

impl_from_for_name!(BranchName, ParentBranchName, VersionName);

impl Default for BranchName<'static> {
    fn default() -> Self {
        INITIAL_BRANCH_NAME
    }
}

impl BranchNameOwned {
    #[inline(always)]
    pub fn as_deref(&self) -> BranchName {
        BranchName(&self.0)
    }
}

impl ParentBranchNameOwned {
    #[inline(always)]
    pub fn as_deref(&self) -> ParentBranchName {
        ParentBranchName(&self.0)
    }
}

impl VersionNameOwned {
    #[inline(always)]
    pub fn as_deref(&self) -> VersionName {
        VersionName(&self.0)
    }
}
