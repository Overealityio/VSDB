use super::*;
use crate::{
    common::{BranchName, ParentBranchName, INITIAL_BRANCH_NAME},
    ValueEnDe, VsMgmt,
};

#[test]
fn test_master_branch_exists() {
    let mut hdr = MapxRawVs::new();
    assert!(hdr.branch_exists(INITIAL_BRANCH_NAME));
}

#[test]
fn test_master_branch_has_versions() {
    let mut hdr = MapxRawVs::new();
    assert_eq!(false, hdr.branch_has_versions(INITIAL_BRANCH_NAME));
}

#[test]
fn test_branch_create_no_version() {
    let mut hdr = MapxRawVs::new();
    let bn = BranchName(b"test");
    let vn = VersionName(b"test1");
    unsafe {
        pnk!(hdr.branch_create_without_new_version(bn, false));
    }
    assert!(hdr.branch_exists(INITIAL_BRANCH_NAME));
    assert!(hdr.branch_exists(bn));
    assert_eq!(false, hdr.branch_has_versions(bn));

    pnk!(hdr.version_create_by_branch(vn, bn));
    assert!(hdr.branch_has_versions(bn));
}

#[test]
fn test_branch_create_by_base_branch() {
    let mut hdr = MapxRawVs::new();
    pnk!(hdr.version_create(VersionName(b"manster0")));
    let bn1 = BranchName(b"test1");
    let vn11 = VersionName(b"testversion11");
    pnk!(hdr.branch_create(bn1, vn11, false));

    let key = b"testkey";
    let value = b"testvalue";
    pnk!(hdr.insert(key, value));

    let bn2 = BranchName(b"test2");
    let vn21 = VersionName(b"testversion21");

    pnk!(hdr.branch_create_by_base_branch(bn2, vn21, ParentBranchName(b"test1"), false));
    let key = b"testkey1";
    let value = b"testvalue1";
    pnk!(hdr.insert(key, value));
}

#[test]
fn test_branch_remove() {
    let mut hdr = MapxRawVs::new();
    let bn = BranchName(b"test");
    let vn = VersionName(b"test1");
    pnk!(hdr.branch_create(bn, vn, false));
    assert!(hdr.branch_exists(INITIAL_BRANCH_NAME));
    assert!(hdr.branch_exists(bn));
    pnk!(hdr.branch_remove(bn));
    assert_eq!(false, hdr.branch_exists(bn));
}

#[test]
fn test_branch_merge() {
    let mut hdr = MapxRawVs::new();
    let mvn = VersionName(b"manster0");
    pnk!(hdr.version_create(mvn));
    pnk!(hdr.insert(b"mansterkey", b"manstervalue"));

    let bn = BranchName(b"test");
    let vn = VersionName(b"test0");

    pnk!(hdr.branch_create(bn, vn, false));
    let key = b"testkey";
    let value = b"testvalue";
    pnk!(hdr.insert(key, value));
    pnk!(hdr.branch_merge_to(bn, INITIAL_BRANCH_NAME));
    pnk!(hdr.branch_set_default(INITIAL_BRANCH_NAME));
    let val = pnk!(hdr.get_by_branch(key, INITIAL_BRANCH_NAME));
    assert_eq!(val.as_ref(), value);
}

#[test]
fn test_branch_pop_version() {
    let mut hdr = MapxRawVs::new();
    assert_eq!(false, hdr.branch_has_versions(INITIAL_BRANCH_NAME));
    pnk!(hdr.version_create(VersionName(b"manster0")));
    assert!(hdr.branch_has_versions(INITIAL_BRANCH_NAME));
    pnk!(hdr.branch_pop_version(INITIAL_BRANCH_NAME));
    assert_eq!(false, hdr.branch_has_versions(INITIAL_BRANCH_NAME));
}

#[test]
fn test_branch_swap() {
    let mut hdr = MapxRawVs::new();
    pnk!(hdr.version_create(VersionName(b"manster0")));

    let mkey = b"mansterkey";
    let mvalue = b"manstervalue";
    pnk!(hdr.insert(mkey, mvalue));

    let bn = BranchName(b"test");
    let vn = VersionName(b"test0");
    pnk!(hdr.branch_create(bn, vn, false));

    let tkey = b"testkey";
    let tvalue = b"testvalue";
    pnk!(hdr.insert(tkey, tvalue));

    unsafe {
        pnk!(hdr.branch_swap(INITIAL_BRANCH_NAME, bn));
    }
    let val = pnk!(hdr.get_by_branch(tkey, INITIAL_BRANCH_NAME));
    assert_eq!(val.as_ref(), tvalue);
    let val = pnk!(hdr.get_by_branch(mkey, bn));
    assert_eq!(val.as_ref(), mvalue);
}
#[test]
fn test_branch_truncate() {
    let mut hdr = MapxRawVs::new();
    pnk!(hdr.version_create(VersionName(b"manster0")));

    let mkey0 = b"mansterkey0";
    let mvalue0 = b"manstervalue0";
    pnk!(hdr.insert(mkey0, mvalue0));

    let mkey1 = b"mansterkey1";
    let mvalue1 = b"manstervalue1";
    pnk!(hdr.insert(mkey1, mvalue1));

    pnk!(hdr.branch_truncate(INITIAL_BRANCH_NAME));
    assert!(hdr.get(mkey0).is_none());
    assert!(hdr.get(mkey1).is_none());
}
#[test]
fn test_branch_truncate_to() {
    let mut hdr = MapxRawVs::new();
    let vn = VersionName(b"manster0");
    pnk!(hdr.version_create(vn));

    let mkey0 = b"mansterkey0";
    let mvalue0 = b"manstervalue0";
    pnk!(hdr.insert(mkey0, mvalue0));

    pnk!(hdr.version_create(VersionName(b"manster1")));

    let mkey1 = b"mansterkey1";
    let mvalue1 = b"manstervalue1";
    pnk!(hdr.insert(mkey1, mvalue1));

    pnk!(hdr.branch_truncate_to(INITIAL_BRANCH_NAME, vn));

    assert!(hdr.get(mkey1).is_none());
}
#[test]
fn test_insert() {
    let mut hdr = MapxRawVs::new();
    pnk!(hdr.version_create(VersionName(b"manster0")));
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), (max + i).to_be_bytes()))
        .for_each(|(key, value)| {
            assert!(hdr.get(&key).is_none());
            assert!(pnk!(hdr.insert(&key, &value)).is_none());
            assert!(pnk!(hdr.insert(&key, &value)).is_some());
            assert!(hdr.contains_key(&key));
            assert_eq!(&pnk!(hdr.get(&key))[..], &value);
            assert_eq!(&pnk!(hdr.remove(&key)).unwrap()[..], &value);
            assert!(hdr.get(&key).is_none());
        });
}

#[test]
fn test_len() {
    let mut hdr = MapxRawVs::new();
    pnk!(hdr.version_create(VersionName(b"manster0")));
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), (max + i).to_be_bytes()))
        .for_each(|(key, value)| {
            assert!(pnk!(hdr.insert(&key, &value)).is_none());
        });
    assert_eq!(500, hdr.len());

    for key in 0..max {
        assert!(pnk!(hdr.remove(&key.to_be_bytes())).is_some());
    }
    assert_eq!(0, hdr.len());
}

#[test]
fn test_valueende() {
    let cnt = 500;
    let dehdr = {
        let mut hdr = MapxRawVs::new();
        pnk!(hdr.version_create(VersionName(b"manster0")));
        (0..cnt)
            .map(|i: usize| (i.to_be_bytes(), i.to_be_bytes()))
            .for_each(|(key, value)| {
                assert!(pnk!(hdr.insert(&key, &value)).is_none());
            });
        crate::vsdb_flush();
        <MapxRawVs as ValueEnDe>::encode(&hdr)
    };
    let mut reloaded = pnk!(<MapxRawVs as ValueEnDe>::decode(&dehdr));
    assert_eq!(cnt, reloaded.len());
    (0..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        assert_eq!(i.to_vec().into_boxed_slice(), reloaded.get(&i).unwrap());
    });
}

#[test]
fn test_emptystr_version() {
    let mut hdr = MapxRawVs::new();
    pnk!(hdr.version_create(VersionName(b"")));

    let key = b"key";
    let value = b"value";

    assert!(hdr.get(key).is_none());
    assert!(pnk!(hdr.insert(key, value)).is_none());
    assert!(hdr.contains_key(key));
    assert_eq!(&pnk!(hdr.get(key))[..], value);
    assert_eq!(&pnk!(hdr.remove(key)).unwrap()[..], value);
    assert!(hdr.get(key).is_none());
}

#[test]
fn test_version_create() {
    let mut hdr = MapxRawVs::new();
    pnk!(hdr.version_create(VersionName(b"v-001")));
    assert!(hdr.version_create(VersionName(b"v-001")).is_err());
    assert!(hdr.is_empty());
    pnk!(hdr.version_create(VersionName(b"v-002")));
    assert!(hdr.is_empty());
}

#[test]
fn test_version_empty() {
    let mut hdr = MapxRawVs::new();
    pnk!(hdr.version_create(VersionName(b"v-001")));
    assert!(hdr.is_empty());
    pnk!(hdr.version_create(VersionName(b"v-002")));
    assert!(hdr.is_empty());
    assert!(hdr.is_empty_by_branch(INITIAL_BRANCH_NAME));
    assert!(hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-001")));
    assert!(hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-002")));

    pnk!(hdr.insert(b"v-002/key-01", b"v-002/value-01"));
    pnk!(hdr.insert(b"v-002/key-02", b"v-002/value-02"));
    pnk!(hdr.insert(b"v-002/key-02", b"v-002/value-02"));

    assert!(!hdr.is_empty());
    assert!(!hdr.is_empty_by_branch(INITIAL_BRANCH_NAME));
    assert!(hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-001")));
    assert!(!hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-002")));
}

#[test]
fn test_version_get() {
    let mut hdr = MapxRawVs::new();
    let vn = VersionName(b"master0");
    pnk!(hdr.version_create(vn));
    let key = b"masterkey";
    let value = b"mastervalue";

    pnk!(hdr.insert(key, value));
    assert_eq!(&hdr.get(key).unwrap()[..], value);

    assert_eq!(hdr.remove(b"fake key").unwrap(), None);

    assert_eq!(&hdr.remove(key).unwrap().unwrap()[..], value);
    assert!(hdr.get(key).is_none());
    assert!(
        hdr.get_by_branch_version(key, INITIAL_BRANCH_NAME, vn)
            .is_none()
    );
}

#[test]
fn test_version_rebase() {
    let mut hdr = MapxRawVs::new();

    pnk!(hdr.version_create(VersionName(&[0])));
    pnk!(hdr.insert(&[0], &[0]));
    pnk!(hdr.version_create(VersionName(&[1])));
    pnk!(hdr.insert(&[0], &[1]));
    pnk!(hdr.version_create(VersionName(&[2])));
    pnk!(hdr.insert(&[0], &[2]));
    pnk!(hdr.version_create(VersionName(&[3])));
    pnk!(hdr.insert(&[0], &[3]));

    assert!(hdr.version_exists(VersionName(&[0])));
    assert!(hdr.version_exists(VersionName(&[1])));
    assert!(hdr.version_exists(VersionName(&[2])));
    assert!(hdr.version_exists(VersionName(&[3])));

    unsafe {
        pnk!(hdr.version_rebase(VersionName(&[1])));
    }
    assert!(hdr.version_exists(VersionName(&[0])));
    assert!(hdr.version_exists(VersionName(&[1])));
    assert!(!hdr.version_exists(VersionName(&[2])));
    assert!(!hdr.version_exists(VersionName(&[3])));
}

#[test]
fn test_version_rebase_by_branch() {
    let mut hdr = MapxRawVs::new();

    let bn = BranchName(b"test");
    let vn = VersionName(b"test1");
    pnk!(hdr.branch_create(bn, vn, false));
    pnk!(hdr.insert_by_branch(&[0], &[0], bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[0]), bn));
    pnk!(hdr.insert_by_branch(&[0], &[1], bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[1]), bn));
    pnk!(hdr.insert_by_branch(&[0], &[2], bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[2]), bn));
    pnk!(hdr.insert_by_branch(&[0], &[3], bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[3]), bn));
    pnk!(hdr.insert_by_branch(&[0], &[4], bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[4]), bn));
    pnk!(hdr.insert_by_branch(&[0], &[5], bn));

    assert_eq!(&[0], &pnk!(hdr.get_by_branch_version(&[0], bn, vn))[..]);
    assert_eq!(
        &[1],
        &pnk!(hdr.get_by_branch_version(&[0], bn, VersionName(&[0])))[..]
    );
    assert_eq!(
        &[2],
        &pnk!(hdr.get_by_branch_version(&[0], bn, VersionName(&[1])))[..]
    );
    assert_eq!(
        &[3],
        &pnk!(hdr.get_by_branch_version(&[0], bn, VersionName(&[2])))[..]
    );
    assert_eq!(
        &[4],
        &pnk!(hdr.get_by_branch_version(&[0], bn, VersionName(&[3])))[..]
    );
    assert_eq!(
        &[5],
        &pnk!(hdr.get_by_branch_version(&[0], bn, VersionName(&[4])))[..]
    );

    unsafe {
        pnk!(hdr.version_rebase_by_branch(VersionName(&[1]), bn));
    }

    assert!(hdr.version_exists_on_branch(VersionName(&[0]), bn));
    assert!(hdr.version_exists_on_branch(VersionName(&[1]), bn));
    assert!(!hdr.version_exists_on_branch(VersionName(&[2]), bn));
    assert!(!hdr.version_exists_on_branch(VersionName(&[3]), bn));
    assert!(!hdr.version_exists_on_branch(VersionName(&[4]), bn));

    assert_eq!(
        &[1],
        &pnk!(hdr.get_by_branch_version(&[0], bn, VersionName(&[0])))[..]
    );
    assert_eq!(
        &[5],
        &pnk!(hdr.get_by_branch_version(&[0], bn, VersionName(&[1])))[..]
    );

    assert!(
        hdr.get_by_branch_version(&[0], bn, VersionName(&[2]))
            .is_none()
    );
    assert!(
        hdr.get_by_branch_version(&[0], bn, VersionName(&[3]))
            .is_none()
    );
    assert!(
        hdr.get_by_branch_version(&[0], bn, VersionName(&[4]))
            .is_none()
    );
    assert_eq!(&[5], &pnk!(hdr.get_by_branch(&[0], bn))[..]);
}

#[test]
fn test_prune() {
    let mut hdr = MapxRawVs::new();

    pnk!(hdr.prune(None));
    pnk!(hdr.prune(Some(1000000000)));

    pnk!(hdr.version_create(VersionName(b"")));
    pnk!(hdr.insert(&[0], &[0]));
    pnk!(hdr.version_create(VersionName(b"a")));
    pnk!(hdr.insert(&[1], &[1]));
    pnk!(hdr.insert(&[2], &[2]));
    pnk!(hdr.version_create(VersionName(b"b")));
    pnk!(hdr.insert(&[3], &[3]));
    pnk!(hdr.insert(&[4], &[4]));
    pnk!(hdr.insert(&[5], &[5]));
    pnk!(hdr.version_create(VersionName(b"c")));
    pnk!(hdr.insert(&[6], &[6]));
    pnk!(hdr.insert(&[7], &[7]));

    assert!(hdr.version_exists(VersionName(b"a")));
    assert!(hdr.version_exists(VersionName(b"b")));
    assert!(hdr.version_exists(VersionName(b"c")));

    pnk!(hdr.prune(Some(1)));

    assert!(!hdr.version_exists(VersionName(b"a")));
    assert!(!hdr.version_exists(VersionName(b"b")));
    assert!(hdr.version_exists(VersionName(b"c")));
}
