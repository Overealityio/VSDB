use ruc::*;
use vsdb::{vsdb_set_base_dir, MapxTkVs, VersionName, VsMgmt};

#[test]
fn basic_cases() {
    info_omit!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    let mut map = MapxTkVs::new();
    pnk!(map.version_create(VersionName(b"")));

    assert!(pnk!(map.insert((1u8, 1u8, 1u8), 9u8)).is_none());
    assert!(pnk!(map.insert((1, 1, 2), 8)).is_none());
    assert!(pnk!(map.insert((1, 1, 3), 7)).is_none());

    assert_eq!(map.get(&(&1, &1, &1)).unwrap(), 9);
    assert_eq!(map.get(&(&1, &1, &2)).unwrap(), 8);
    assert_eq!(map.get(&(&1, &1, &3)).unwrap(), 7);

    // does not exist
    assert!(pnk!(map.remove(&(&1, Some((&1, Some(&4)))))).is_none());

    assert!(pnk!(map.remove(&(&1, Some((&1, Some(&1)))))).is_some());
    assert!(map.get(&(&1, &1, &1)).is_none());

    // partial-path remove
    assert!(pnk!(map.remove(&(&1, Some((&1, None))))).is_none()); // yes, is none
    assert!(map.get(&(&1, &1, &2)).is_none());
    assert!(map.get(&(&1, &1, &3)).is_none());

    map.entry_ref(&(&1, &1, &99)).or_insert_ref(&100);
    assert_eq!(map.get(&(&1, &1, &99)).unwrap(), 100);
}
