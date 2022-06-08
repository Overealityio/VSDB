use ruc::*;
use std::{sync::mpsc::channel, thread};
use vsdb::{
    vsdb_set_base_dir, BranchName, MapxRawMkVs, ParentBranchName, ValueEnDe,
    VersionName, VersionNameOwned, VsMgmt,
};

const BRANCH_LIMITS: usize = 128;
const INITIAL_BRANCH_NAME: BranchName<'static> = BranchName(b"master");

#[test]
fn basic_cases() {
    info_omit!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    let cnt = 200;
    let hdr = {
        let hdr_i = MapxRawMkVs::new(2);
        hdr_i.version_create(VersionName(b"test")).unwrap();

        (0..cnt).for_each(|i: usize| {
            assert!(hdr_i.get(&[&[], &i.to_be_bytes()]).is_none());
        });

        (0..cnt)
            .map(|i: usize| (i.to_be_bytes(), i.to_be_bytes()))
            .for_each(|(i, b)| {
                let k = &[&[], &i[..]];
                hdr_i.insert(k, &b).unwrap();
                assert_eq!(&hdr_i.get(k).unwrap()[..], &b);
                assert_eq!(&hdr_i.remove(k).unwrap().unwrap()[..], &b);
                assert!(hdr_i.get(k).is_none());
                assert!(hdr_i.insert(k, &b).unwrap().is_none());
                assert!(hdr_i.insert(k, &b).unwrap().is_some());
            });

        <MapxRawMkVs as ValueEnDe>::encode(&hdr_i)
    };

    let reloaded = pnk!(<MapxRawMkVs as ValueEnDe>::decode(&hdr));

    (0..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        assert_eq!(
            i.to_vec().into_boxed_slice(),
            reloaded.get(&[&[], &i]).unwrap()
        );
    });

    (1..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        let k = &[&[], &i[..]];
        reloaded.insert(k, &i).unwrap();
        assert_eq!(&reloaded.get(k).unwrap()[..], &i);
        assert!(reloaded.contains_key(k));
        assert!(reloaded.remove(k).unwrap().is_some());
        assert!(!reloaded.contains_key(k));
    });

    reloaded.version_create(VersionName(b"test2")).unwrap();

    reloaded.insert(&[&[], &[1]], &[1]).unwrap();
    reloaded.insert(&[&[], &[4]], &[4]).unwrap();
    reloaded.insert(&[&[], &[6]], &[6]).unwrap();
    reloaded.insert(&[&[], &[80]], &[80]).unwrap();

    assert_eq!(&[1], &reloaded.get(&[&[], &[1]]).unwrap()[..]);
    assert_eq!(&[4], &reloaded.get(&[&[], &[4]]).unwrap()[..]);
    assert_eq!(&[6], &reloaded.get(&[&[], &[6]]).unwrap()[..]);
    assert_eq!(&[80], &reloaded.get(&[&[], &[80]]).unwrap()[..]);
}

#[test]
#[allow(non_snake_case)]
fn vcs_mgmt() {
    info_omit!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    let mut hdr = MapxRawMkVs::new(2);
    pnk!(hdr.version_create(VersionName(b"")));

    version_operations(&mut hdr);
    branch_operations(&mut hdr);
    default_branch(&mut hdr);
}

fn version_operations(hdr: &mut MapxRawMkVs) {
    // No version is created manually,
    // but an empty version is automatically created
    // each time an instance is initialized,
    // so this operation will success.
    assert!(hdr.insert(&[&[], b"key"], b"value").is_ok());
    assert!(hdr.remove(&[&[], b"key"]).is_ok());

    hdr.version_create(VersionName(b"v-001")).unwrap();

    // use existing version name
    assert!(hdr.version_create(VersionName(b"v-001")).is_err());

    // assert!(hdr.is_empty());

    hdr.version_create(VersionName(b"v-002")).unwrap();

    // assert!(hdr.is_empty());
    // assert!(hdr.is_empty_by_branch(INITIAL_BRANCH_NAME));
    // assert!(hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-001")));
    // assert!(hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-002")));

    pnk!(hdr.insert(&[&[], b"v-002/key-01"], b"v-002/value-01"));
    pnk!(hdr.insert(&[&[], b"v-002/key-02"], b"v-002/value-02"));
    pnk!(hdr.insert(&[&[], b"v-002/key-02"], b"v-002/value-02"));

    assert_eq!(
        hdr.get(&[&[], b"v-002/key-01"]),
        Some(b"v-002/value-01".to_vec().into_boxed_slice())
    );
    assert!(
        hdr.get_by_branch(&[&[], b"v-002/key-01"], BranchName(b"fake branch"))
            .is_none()
    );
    assert!(
        hdr.get_by_branch_version(
            &[&[], b"v-002/key-01"],
            INITIAL_BRANCH_NAME,
            VersionName(b"v-001")
        )
        .is_none()
    );
    assert_eq!(
        hdr.get(&[&[], b"v-002/key-02"]),
        Some(b"v-002/value-02".to_vec().into_boxed_slice())
    );
    assert!(
        hdr.get_by_branch(&[&[], b"v-002/key-02"], BranchName(b"fake branch"))
            .is_none()
    );
    assert!(
        hdr.get_by_branch_version(
            &[&[], b"v-002/key-02"],
            INITIAL_BRANCH_NAME,
            VersionName(b"v-001")
        )
        .is_none()
    );

    // assert!(!hdr.is_empty());
    // assert!(!hdr.is_empty_by_branch(INITIAL_BRANCH_NAME));
    // assert!(hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-001")));
    // assert!(!hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-002")));

    hdr.version_create(VersionName(b"v-003")).unwrap();
    pnk!(hdr.insert(&[&[], b"v-003/key-01"], b"v-003/value-01"));
    assert_eq!(
        &hdr.get(&[&[], b"v-003/key-01"]).unwrap()[..],
        b"v-003/value-01"
    );

    assert_eq!(hdr.remove(&[&[], b"fake key"]).unwrap(), None);

    assert_eq!(
        &hdr.remove(&[&[], b"v-003/key-01"]).unwrap().unwrap()[..],
        b"v-003/value-01"
    );
    assert!(hdr.get(&[&[], b"v-003/key-01"]).is_none());
    assert!(
        hdr.get_by_branch_version(
            &[&[], b"v-003/key-01"],
            INITIAL_BRANCH_NAME,
            VersionName(b"v-003")
        )
        .is_none()
    );

    assert_eq!(
        &pnk!(hdr.remove(&[&[], b"v-002/key-01"])).unwrap()[..],
        b"v-002/value-01"
    );
    assert!(hdr.get(&[&[], b"v-002/key-01"]).is_none());
    // still available in a old version
    assert_eq!(
        &hdr.get_by_branch_version(
            &[&[], b"v-002/key-01"],
            INITIAL_BRANCH_NAME,
            VersionName(b"v-002")
        )
        .unwrap()[..],
        b"v-002/value-01"
    );

    // assert!(!hdr.is_empty());
    // assert!(!hdr.is_empty_by_branch(INITIAL_BRANCH_NAME));
    // assert!(hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-001")));
    // assert!(!hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-002")));
    // assert!(!hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-003")));

    pnk!(hdr.insert(&[&[], b"v-003/key-02"], b"v-003/value-02"));
    assert!(
        hdr.get_by_branch_version(
            &[&[], b"v-003/key-02"],
            INITIAL_BRANCH_NAME,
            VersionName(b"v-001")
        )
        .is_none()
    );
    assert!(
        hdr.get_by_branch_version(
            &[&[], b"v-003/key-02"],
            INITIAL_BRANCH_NAME,
            VersionName(b"v-002")
        )
        .is_none()
    );
    assert_eq!(
        &hdr.get_by_branch_version(
            &[&[], b"v-003/key-02"],
            INITIAL_BRANCH_NAME,
            VersionName(b"v-003")
        )
        .unwrap()[..],
        b"v-003/value-02"
    );
    assert_eq!(
        &hdr.get(&[&[], b"v-003/key-02"]).unwrap()[..],
        b"v-003/value-02"
    );

    assert_eq!(
        &hdr.get_by_branch_version(
            &[&[], b"v-002/key-02"],
            INITIAL_BRANCH_NAME,
            VersionName(b"v-003")
        )
        .unwrap()[..],
        b"v-002/value-02"
    );

    // update a existing key
    pnk!(hdr.insert(&[&[], b"v-002/key-02"], b"v-002/value-03"));
    // get the new view of this value
    assert_eq!(
        &hdr.get_by_branch_version(
            &[&[], b"v-002/key-02"],
            INITIAL_BRANCH_NAME,
            VersionName(b"v-003")
        )
        .unwrap()[..],
        b"v-002/value-03"
    );

    pnk!(hdr.version_pop());
    assert!(
        hdr.get_by_branch_version(
            &[&[], b"v-003/key-02"],
            INITIAL_BRANCH_NAME,
            VersionName(b"v-003")
        )
        .is_none()
    );

    // the head version is v-002 after the `pop`, so will get the v2 value
    assert_eq!(
        &hdr.get(&[&[], b"v-002/key-02"]).unwrap()[..],
        b"v-002/value-02"
    );
    assert_eq!(
        &hdr.get_by_branch_version(
            &[&[], b"v-002/key-02"],
            INITIAL_BRANCH_NAME,
            VersionName(b"v-002")
        )
        .unwrap()[..],
        b"v-002/value-02"
    );
}

fn branch_operations(hdr: &mut MapxRawMkVs) {
    hdr.branch_create(BranchName(b"b-1"), random_version().as_deref(), false)
        .unwrap();
    hdr.branch_create(BranchName(b"b-2"), random_version().as_deref(), false)
        .unwrap();

    assert!(
        hdr.branch_create(INITIAL_BRANCH_NAME, random_version().as_deref(), false)
            .is_err()
    );
    assert!(
        hdr.branch_create(BranchName(b"b-1"), random_version().as_deref(), false)
            .is_err()
    );
    assert!(
        hdr.branch_create(BranchName(b"b-2"), random_version().as_deref(), false)
            .is_err()
    );

    assert_eq!(
        &hdr.get_by_branch(&[&[], b"v-002/key-02"], BranchName(b"b-1"))
            .unwrap()[..],
        b"v-002/value-02"
    );
    assert_eq!(
        &hdr.get_by_branch(&[&[], b"v-002/key-02"], BranchName(b"b-2"))
            .unwrap()[..],
        b"v-002/value-02"
    );

    // Version ID can not be repeated within the global view.
    assert!(
        hdr.version_create_by_branch(VersionName(b"v-001"), BranchName(b"b-1"))
            .is_err()
    );

    hdr.version_create_by_branch(VersionName(b"v-004"), BranchName(b"b-1"))
        .unwrap();
    pnk!(hdr.insert_by_branch(
        &[&[], b"v-001/key-01"],
        b"v-001/value-01",
        BranchName(b"b-1")
    ));

    // multi-layers view
    hdr.branch_create_by_base_branch(
        BranchName(b"b-1-child"),
        random_version().as_deref(),
        ParentBranchName(b"b-1"),
        false,
    )
    .unwrap();
    assert_eq!(
        &hdr.get_by_branch(&[&[], b"v-002/key-02"], BranchName(b"b-1-child"))
            .unwrap()[..],
        b"v-002/value-02"
    );
    assert_eq!(
        &hdr.get_by_branch(&[&[], b"v-001/key-01"], BranchName(b"b-1"))
            .unwrap()[..],
        b"v-001/value-01"
    );

    // insert to a non-existing branch
    assert!(
        hdr.insert_by_branch(&[&[], b"k"], b"v", BranchName(b"fake branch"))
            .is_err()
    );

    // try go get versions either on self nor on parents
    assert!(
        hdr.get_by_branch_version(
            &[&[], b"v-001/key-01"],
            BranchName(b"b-2"),
            VersionName(b"v-004")
        )
        .is_none()
    );

    // total number of branch has no limits
    (4..2 * BRANCH_LIMITS).for_each(|i| {
        pnk!(hdr.branch_create(
            BranchName(&i.to_be_bytes()),
            random_version().as_deref(),
            false
        ));
    });
    (4..2 * BRANCH_LIMITS).for_each(|i| {
        pnk!(hdr.branch_remove(BranchName(&i.to_be_bytes())));
    });

    pnk!(hdr.branch_create_by_base_branch_version(
        BranchName(&3usize.to_be_bytes()),
        random_version().as_deref(),
        ParentBranchName(INITIAL_BRANCH_NAME.0),
        VersionName(b"v-002"),
        false
    ));
    pnk!(hdr.version_create_by_branch(
        VersionName(b"verN"),
        BranchName(&3usize.to_be_bytes())
    ));

    // total number of branch ancestor has limits
    (4..(BRANCH_LIMITS - 2)).for_each(|i| {
        pnk!(hdr.branch_create_by_base_branch(
            BranchName(&i.to_be_bytes()),
            random_version().as_deref(),
            ParentBranchName(&(i - 1).to_be_bytes()),
            false
        ));
        pnk!(hdr.version_create_by_branch(
            VersionName(format!("verN_{}", i).as_bytes()),
            BranchName(&i.to_be_bytes())
        ));
    });
    pnk!(hdr.branch_create_by_base_branch(
        BranchName(&(BRANCH_LIMITS - 2).to_be_bytes()),
        random_version().as_deref(),
        ParentBranchName(&(BRANCH_LIMITS - 3).to_be_bytes()),
        false,
    ));
    (3..(BRANCH_LIMITS - 1)).rev().for_each(|i| {
        pnk!(hdr.branch_remove(BranchName(&i.to_be_bytes())));
    });

    pnk!(hdr.branch_create(
        BranchName(&1usize.to_be_bytes()),
        random_version().as_deref(),
        false
    ));
    pnk!(hdr.branch_remove(BranchName(&1usize.to_be_bytes()),));

    // not exist
    assert!(hdr.branch_remove(BranchName(b"fake branch")).is_err());

    pnk!(hdr.branch_remove(BranchName(b"b-1-child")));
    pnk!(hdr.branch_remove(BranchName(b"b-1")));
    assert!(
        hdr.get_by_branch(&[&[], b"v-001/key-01"], BranchName(b"b-1"))
            .is_none()
    );

    // create some versions to be truncated
    (0..100u64).for_each(|i| {
        hdr.version_create_by_branch(VersionName(&i.to_be_bytes()), BranchName(b"b-2"))
            .unwrap();
        pnk!(hdr.insert_by_branch(
            &[&[], &i.to_be_bytes()],
            &i.to_be_bytes(),
            BranchName(b"b-2")
        ));
    });

    // verion names already exist
    (0..100u64).for_each(|i| {
        assert!(
            hdr.version_create_by_branch(
                VersionName(&i.to_be_bytes()),
                BranchName(b"b-2")
            )
            .is_err()
        );
    });

    // all values are correct
    (0..100u64).for_each(|i| {
        assert_eq!(
            &hdr.get_by_branch_version(
                &[&[], &i.to_be_bytes()],
                BranchName(b"b-2"),
                VersionName(&i.to_be_bytes())
            )
            .unwrap()[..],
            &i.to_be_bytes()
        );
    });

    // clear up versions on "b-2" branch
    hdr.branch_truncate_to(BranchName(b"b-2"), VersionName(&0u64.to_be_bytes()))
        .unwrap();

    // orphraned versions will exist until a `prune`
    pnk!(hdr.prune(Some(100000)));

    // now we can use these version names again
    (11..=100u64).for_each(|i| {
        pnk!(hdr.version_create_by_branch(
            VersionName(&i.to_be_bytes()),
            BranchName(b"b-2")
        ));
    });

    // get very old value after passing through many verions
    assert_eq!(
        &hdr.get_by_branch_version(
            &[&[], b"v-002/key-02"],
            BranchName(b"b-2"),
            VersionName(&100u64.to_be_bytes())
        )
        .unwrap()[..],
        b"v-002/value-02"
    );

    // ensure the view of main branch is not affected
    assert_eq!(
        &hdr.get(&[&[], b"v-002/key-02"]).unwrap()[..],
        b"v-002/value-02"
    );
    assert_eq!(
        &hdr.get_by_branch(&[&[], b"v-002/key-02"], INITIAL_BRANCH_NAME)
            .unwrap()[..],
        b"v-002/value-02"
    );

    // keep one version to support other branches forked from it
    hdr.branch_truncate_to(BranchName(b"b-2"), VersionName(&11u64.to_be_bytes()))
        .unwrap();

    // created data to be merged
    (0..10u64).for_each(|i| {
        hdr.branch_create_by_base_branch(
            BranchName(&i.to_be_bytes()),
            random_version().as_deref(),
            ParentBranchName(b"b-2"),
            false,
        )
        .unwrap();
        (1000..1010u64).for_each(|j| {
            hdr.version_create_by_branch(
                VersionName(&((1 + i) * j).to_be_bytes()),
                BranchName(&i.to_be_bytes()),
            )
            .unwrap();
            pnk!(hdr.insert_by_branch(
                &[&[], &((1 + i) * j).to_be_bytes()],
                &((1 + i) * j).to_be_bytes(),
                BranchName(&i.to_be_bytes())
            ));
        });
    });

    (0..10u64).for_each(|i| {
        if 0 == i {
            pnk!(hdr.branch_merge_to(BranchName(&i.to_be_bytes()), BranchName(b"b-2")));
        } else {
            assert!(
                hdr.branch_merge_to(BranchName(&i.to_be_bytes()), BranchName(b"b-2"))
                    .is_err()
            );
            unsafe {
                pnk!(hdr.branch_merge_to_force(
                    BranchName(&i.to_be_bytes()),
                    BranchName(b"b-2")
                ));
            }
        }
        assert!(hdr.branch_exists(BranchName(&i.to_be_bytes())));
    });

    // All versions and their chanegs are belong to the base branch now
    (0..10u64).for_each(|i| {
        (1000..1010u64).for_each(|j| {
            assert!(
                hdr.get_by_branch_version(
                    &[&[], &((1 + i) * j).to_be_bytes()],
                    BranchName(&i.to_be_bytes()),
                    VersionName(&((1 + i) * j).to_be_bytes())
                )
                .is_some()
            );
            // all changes have been move to the parent branch
            assert_eq!(
                &hdr.get_by_branch_version(
                    &[&[], &((1 + i) * j).to_be_bytes()],
                    BranchName(b"b-2"),
                    VersionName(&((1 + i) * j).to_be_bytes())
                )
                .unwrap()[..],
                &((1 + i) * j).to_be_bytes()
            );
        });
    });
}

fn default_branch(hdr: &mut MapxRawMkVs) {
    hdr.branch_create(BranchName(b"fork"), random_version().as_deref(), false)
        .unwrap();

    hdr.branch_set_default(BranchName(b"fork")).unwrap();
    hdr.version_create(VersionName(b"ver-on-fork")).unwrap();
    hdr.insert(&[&[], b"key"], b"value").unwrap();
    assert_eq!(&hdr.get(&[&[], b"key"]).unwrap()[..], b"value");
    assert!(
        hdr.get_by_branch(&[&[], b"key"], INITIAL_BRANCH_NAME)
            .is_none()
    );

    hdr.branch_set_default(INITIAL_BRANCH_NAME).unwrap();
    assert!(hdr.get(&[&[], b"key"]).is_none());
    assert_eq!(
        &hdr.get_by_branch(&[&[], b"key"], BranchName(b"fork"))
            .unwrap()[..],
        b"value"
    );

    let (s, r) = channel();
    for i in (u64::MAX - 10)..u64::MAX {
        let ss = s.clone();
        let mut h = hdr.clone();
        thread::spawn(move || {
            pnk!(h.branch_create(
                BranchName(&i.to_be_bytes()),
                random_version().as_deref(),
                false
            ));
            pnk!(h.branch_set_default(BranchName(&i.to_be_bytes())));
            pnk!(h.version_create(VersionName(format!("ver-on-fork—{}", i).as_bytes())));
            pnk!(h.insert(&[&[], b"key"], &i.to_be_bytes()));
            ss.send("done").unwrap();
        });
    }

    for _ in 0..10 {
        r.recv().unwrap();
    }

    for i in (u64::MAX - 10)..u64::MAX {
        assert_eq!(
            &hdr.get_by_branch_version(
                &[&[], b"key"],
                BranchName(&i.to_be_bytes()),
                VersionName(format!("ver-on-fork—{}", i).as_bytes())
            )
            .unwrap()[..],
            &i.to_be_bytes()
        );
    }
}

#[test]
fn prune() {
    info_omit!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    let mut hdr = MapxRawMkVs::new(2);

    // noop operation is ok
    pnk!(hdr.prune(None));
    pnk!(hdr.prune(Some(1000000000)));

    pnk!(hdr.version_create(VersionName(b"")));
    pnk!(hdr.insert(&[&[0], &[255]], &[0]));
    pnk!(hdr.version_create(VersionName(b"a")));
    pnk!(hdr.insert(&[&[1], &[255]], &[1]));
    pnk!(hdr.insert(&[&[2], &[255]], &[2]));
    pnk!(hdr.version_create(VersionName(b"b")));
    pnk!(hdr.insert(&[&[3], &[255]], &[3]));
    pnk!(hdr.insert(&[&[4], &[255]], &[4]));
    pnk!(hdr.insert(&[&[5], &[255]], &[5]));
    pnk!(hdr.version_create(VersionName(b"c")));
    pnk!(hdr.insert(&[&[6], &[255]], &[6]));
    pnk!(hdr.insert(&[&[7], &[255]], &[7]));

    assert!(hdr.version_exists(VersionName(b"a")));
    assert!(hdr.version_exists(VersionName(b"b")));
    assert!(hdr.version_exists(VersionName(b"c")));

    pnk!(hdr.prune(Some(1)));

    assert!(!hdr.version_exists(VersionName(b"a")));
    assert!(!hdr.version_exists(VersionName(b"b")));
    assert!(hdr.version_exists(VersionName(b"c")));

    assert_eq!(&[0], &hdr.get(&[&[0], &[255]]).unwrap()[..]);
    assert_eq!(&[1], &hdr.get(&[&[1], &[255]]).unwrap()[..]);
    assert_eq!(&[2], &hdr.get(&[&[2], &[255]]).unwrap()[..]);
    assert_eq!(&[3], &hdr.get(&[&[3], &[255]]).unwrap()[..]);
    assert_eq!(&[4], &hdr.get(&[&[4], &[255]]).unwrap()[..]);
    assert_eq!(&[5], &hdr.get(&[&[5], &[255]]).unwrap()[..]);
    assert_eq!(&[6], &hdr.get(&[&[6], &[255]]).unwrap()[..]);
    assert_eq!(&[7], &hdr.get(&[&[7], &[255]]).unwrap()[..]);

    hdr.clear();

    pnk!(hdr.version_create(VersionName(b"")));
    pnk!(hdr.insert(&[&[0], &[255]], &[0]));
    pnk!(hdr.version_create(VersionName(b"a")));
    pnk!(hdr.insert(&[&[1], &[255]], &[1]));
    pnk!(hdr.insert(&[&[2], &[255]], &[2]));
    pnk!(hdr.version_create(VersionName(b"b")));
    pnk!(hdr.insert(&[&[3], &[255]], &[3]));
    pnk!(hdr.insert(&[&[4], &[255]], &[4]));
    pnk!(hdr.insert(&[&[5], &[255]], &[5]));
    pnk!(hdr.version_create(VersionName(b"c")));
    pnk!(hdr.insert(&[&[6], &[255]], &[6]));
    pnk!(hdr.insert(&[&[7], &[255]], &[7]));

    pnk!(hdr.branch_create(BranchName(b"A"), random_version().as_deref(), false));
    pnk!(hdr.branch_set_default(BranchName(b"A")));

    pnk!(hdr.version_create(VersionName(b"d")));
    pnk!(hdr.insert(&[&[0], &[255]], &[8]));
    pnk!(hdr.version_create(VersionName(b"e")));
    pnk!(hdr.insert(&[&[1], &[255]], &[9]));
    pnk!(hdr.version_create(VersionName(b"f")));
    pnk!(hdr.insert(&[&[2], &[255]], &[10]));

    pnk!(hdr.branch_create_by_base_branch_version(
        BranchName(b"B"),
        random_version().as_deref(),
        ParentBranchName(INITIAL_BRANCH_NAME.0),
        VersionName(b"c"),
        false
    ));
    pnk!(hdr.branch_set_default(BranchName(b"B")));

    pnk!(hdr.version_create(VersionName(b"g")));
    pnk!(hdr.insert(&[&[0], &[255]], &[11]));
    pnk!(hdr.version_create(VersionName(b"h")));
    pnk!(hdr.insert(&[&[1], &[255]], &[12]));

    pnk!(hdr.branch_set_default(INITIAL_BRANCH_NAME));

    assert!(hdr.version_exists(VersionName(b"a")));
    assert!(hdr.version_exists(VersionName(b"b")));
    assert!(hdr.version_exists(VersionName(b"c")));
    assert!(hdr.version_exists_on_branch(VersionName(b"d"), BranchName(b"A")));
    assert!(hdr.version_exists_on_branch(VersionName(b"e"), BranchName(b"A")));
    assert!(hdr.version_exists_on_branch(VersionName(b"f"), BranchName(b"A")));
    assert!(!hdr.version_exists(VersionName(b"d")));
    assert!(!hdr.version_exists(VersionName(b"e")));
    assert!(!hdr.version_exists(VersionName(b"f")));
    assert!(hdr.version_exists_on_branch(VersionName(b"g"), BranchName(b"B")));
    assert!(hdr.version_exists_on_branch(VersionName(b"h"), BranchName(b"B")));
    assert!(!hdr.version_exists_on_branch(VersionName(b"g"), BranchName(b"A")));
    assert!(!hdr.version_exists_on_branch(VersionName(b"h"), BranchName(b"A")));
    assert!(!hdr.version_exists(VersionName(b"g")));
    assert!(!hdr.version_exists(VersionName(b"h")));

    pnk!(hdr.prune(Some(2)));

    assert!(!hdr.version_exists(VersionName(b"a")));
    assert!(hdr.version_exists(VersionName(b"b")));
    assert!(hdr.version_exists(VersionName(b"c")));

    assert_eq!(&[0], &pnk!(hdr.get(&[&[0], &[255]]))[..]);
    assert_eq!(&[1], &pnk!(hdr.get(&[&[1], &[255]]))[..]);
    assert_eq!(&[2], &pnk!(hdr.get(&[&[2], &[255]]))[..]);
    assert_eq!(&[3], &pnk!(hdr.get(&[&[3], &[255]]))[..]);
    assert_eq!(&[4], &pnk!(hdr.get(&[&[4], &[255]]))[..]);
    assert_eq!(&[5], &pnk!(hdr.get(&[&[5], &[255]]))[..]);
    assert_eq!(&[6], &pnk!(hdr.get(&[&[6], &[255]]))[..]);
    assert_eq!(&[7], &pnk!(hdr.get(&[&[7], &[255]]))[..]);

    pnk!(hdr.branch_set_default(BranchName(b"A")));

    assert!(hdr.version_exists(VersionName(b"d")));
    assert!(hdr.version_exists(VersionName(b"e")));
    assert!(hdr.version_exists(VersionName(b"f")));

    assert_eq!(&[8], &hdr.get(&[&[0], &[255]]).unwrap()[..]);
    assert_eq!(&[9], &hdr.get(&[&[1], &[255]]).unwrap()[..]);
    assert_eq!(&[10], &hdr.get(&[&[2], &[255]]).unwrap()[..]);

    pnk!(hdr.branch_set_default(BranchName(b"B")));

    assert!(hdr.version_exists(VersionName(b"g")));
    assert!(hdr.version_exists(VersionName(b"h")));

    assert_eq!(&[11], &hdr.get(&[&[0], &[255]]).unwrap()[..]);
    assert_eq!(&[12], &hdr.get(&[&[1], &[255]]).unwrap()[..]);
}

#[test]
fn version_rebase() {
    info_omit!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    let hdr = MapxRawMkVs::new(2);

    pnk!(hdr.version_create(VersionName(&[0])));
    pnk!(hdr.insert(&[&[0], &[0]], &[0]));
    pnk!(hdr.version_create(VersionName(&[1])));
    pnk!(hdr.insert(&[&[0], &[0]], &[1]));
    pnk!(hdr.version_create(VersionName(&[2])));
    pnk!(hdr.insert(&[&[0], &[0]], &[2]));
    pnk!(hdr.version_create(VersionName(&[3])));
    pnk!(hdr.insert(&[&[0], &[0]], &[3]));
    pnk!(hdr.version_create(VersionName(&[4])));
    pnk!(hdr.insert(&[&[0], &[0]], &[4]));

    assert_eq!(
        &[0],
        &pnk!(hdr.get_by_branch_version(
            &[&[0], &[0]],
            INITIAL_BRANCH_NAME,
            VersionName(&[0])
        ))[..]
    );
    assert_eq!(
        &[1],
        &pnk!(hdr.get_by_branch_version(
            &[&[0], &[0]],
            INITIAL_BRANCH_NAME,
            VersionName(&[1])
        ))[..]
    );
    assert_eq!(
        &[2],
        &pnk!(hdr.get_by_branch_version(
            &[&[0], &[0]],
            INITIAL_BRANCH_NAME,
            VersionName(&[2])
        ))[..]
    );
    assert_eq!(
        &[3],
        &pnk!(hdr.get_by_branch_version(
            &[&[0], &[0]],
            INITIAL_BRANCH_NAME,
            VersionName(&[3])
        ))[..]
    );
    assert_eq!(
        &[4],
        &pnk!(hdr.get_by_branch_version(
            &[&[0], &[0]],
            INITIAL_BRANCH_NAME,
            VersionName(&[4])
        ))[..]
    );

    assert!(hdr.version_exists(VersionName(&[1])));
    assert!(hdr.version_exists(VersionName(&[2])));
    assert!(hdr.version_exists(VersionName(&[3])));
    assert!(hdr.version_exists(VersionName(&[4])));

    unsafe {
        pnk!(hdr.version_rebase(VersionName(&[2])));
    }

    assert!(hdr.version_exists(VersionName(&[1])));
    assert!(hdr.version_exists(VersionName(&[2])));
    assert!(!hdr.version_exists(VersionName(&[3])));
    assert!(!hdr.version_exists(VersionName(&[4])));

    assert!(
        hdr.get_by_branch_version(&[&[0], &[0]], INITIAL_BRANCH_NAME, VersionName(&[3]))
            .is_none()
    );
    assert!(
        hdr.get_by_branch_version(&[&[0], &[0]], INITIAL_BRANCH_NAME, VersionName(&[4]))
            .is_none()
    );

    assert_eq!(
        &[0],
        &pnk!(hdr.get_by_branch_version(
            &[&[0], &[0]],
            INITIAL_BRANCH_NAME,
            VersionName(&[0])
        ))[..]
    );
    assert_eq!(
        &[1],
        &pnk!(hdr.get_by_branch_version(
            &[&[0], &[0]],
            INITIAL_BRANCH_NAME,
            VersionName(&[1])
        ))[..]
    );
    assert_eq!(
        &[4],
        &pnk!(hdr.get_by_branch_version(
            &[&[0], &[0]],
            INITIAL_BRANCH_NAME,
            VersionName(&[2])
        ))[..]
    );

    // current header is version 2
    assert_eq!(&[4], &pnk!(hdr.get(&[&[0], &[0]]))[..]);

    let br = BranchName(&[1]);
    pnk!(hdr.branch_create(br, random_version().as_deref(), false));

    pnk!(hdr.version_create_by_branch(VersionName(&[10]), br));
    pnk!(hdr.insert_by_branch(&[&[0], &[0]], &[0], br));
    pnk!(hdr.version_create_by_branch(VersionName(&[11]), br));
    pnk!(hdr.insert_by_branch(&[&[0], &[0]], &[1], br));
    pnk!(hdr.version_create_by_branch(VersionName(&[22]), br));
    pnk!(hdr.insert_by_branch(&[&[0], &[0]], &[2], br));
    pnk!(hdr.version_create_by_branch(VersionName(&[33]), br));
    pnk!(hdr.insert_by_branch(&[&[0], &[0]], &[3], br));
    pnk!(hdr.version_create_by_branch(VersionName(&[44]), br));
    pnk!(hdr.insert_by_branch(&[&[0], &[0]], &[4], br));

    assert_eq!(
        &[0],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], br, VersionName(&[10])))[..]
    );
    assert_eq!(
        &[1],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], br, VersionName(&[11])))[..]
    );
    assert_eq!(
        &[2],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], br, VersionName(&[22])))[..]
    );
    assert_eq!(
        &[3],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], br, VersionName(&[33])))[..]
    );
    assert_eq!(
        &[4],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], br, VersionName(&[44])))[..]
    );

    unsafe {
        pnk!(hdr.version_rebase_by_branch(VersionName(&[22]), br));
    }

    assert!(hdr.version_exists_on_branch(VersionName(&[11]), br));
    assert!(hdr.version_exists_on_branch(VersionName(&[22]), br));
    assert!(!hdr.version_exists_on_branch(VersionName(&[33]), br));
    assert!(!hdr.version_exists_on_branch(VersionName(&[44]), br));

    assert_eq!(
        &[1],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], br, VersionName(&[11])))[..]
    );
    assert_eq!(
        &[4],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], br, VersionName(&[22])))[..]
    );

    assert!(
        hdr.get_by_branch_version(&[&[0], &[0]], br, VersionName(&[33]))
            .is_none()
    );
    assert!(
        hdr.get_by_branch_version(&[&[0], &[0]], br, VersionName(&[44]))
            .is_none()
    );

    // current header is version 22
    assert_eq!(&[4], &pnk!(hdr.get_by_branch(&[&[0], &[0]], br))[..]);

    ////////////////////////////////////
    // recheck data on default branch //
    ////////////////////////////////////

    assert_eq!(
        &[1],
        &pnk!(hdr.get_by_branch_version(
            &[&[0], &[0]],
            INITIAL_BRANCH_NAME,
            VersionName(&[1])
        ))[..]
    );
    assert_eq!(
        &[4],
        &pnk!(hdr.get_by_branch_version(
            &[&[0], &[0]],
            INITIAL_BRANCH_NAME,
            VersionName(&[2])
        ))[..]
    );

    assert!(
        hdr.get_by_branch_version(&[&[0], &[0]], INITIAL_BRANCH_NAME, VersionName(&[3]))
            .is_none()
    );
    assert!(
        hdr.get_by_branch_version(&[&[0], &[0]], INITIAL_BRANCH_NAME, VersionName(&[4]))
            .is_none()
    );

    assert!(hdr.version_exists(VersionName(&[1])));
    assert!(hdr.version_exists(VersionName(&[2])));
    assert!(!hdr.version_exists(VersionName(&[3])));
    assert!(!hdr.version_exists(VersionName(&[4])));

    assert!(!hdr.version_exists(VersionName(&[11])));
    assert!(!hdr.version_exists(VersionName(&[22])));
    assert!(!hdr.version_exists(VersionName(&[33])));
    assert!(!hdr.version_exists(VersionName(&[44])));
}

fn random_version() -> VersionNameOwned {
    VersionNameOwned(
        (1_0000_0000 + rand::random::<u64>() / 2)
            .to_be_bytes()
            .to_vec(),
    )
}
