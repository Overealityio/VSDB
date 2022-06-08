use crate::ValueEnDe;

use super::*;
use ruc::*;

#[test]
fn test_insert() {
    let mut hdr = MapxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), <usize as ValueEnDe>::encode(&(max + i))))
        .for_each(|(key, value)| {
            assert!(hdr.get(&key).is_none());
            hdr.entry_ref(&key).or_insert_ref(&value);
            assert!(hdr.insert(&key, &value).is_some());
            assert!(hdr.contains_key(&key));
            assert_eq!(pnk!(hdr.get(&key)), value);
            assert_eq!(pnk!(hdr.remove(&key)), value);
            assert!(hdr.get(&key).is_none());
            assert!(hdr.insert(&key, &value).is_none());
        });
    hdr.clear();
    (0..max).map(|i: usize| i.to_be_bytes()).for_each(|key| {
        assert!(hdr.get(&key).is_none());
    });
    assert!(hdr.is_empty());
}
#[test]
fn test_len() {
    let mut hdr = MapxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), (max + i).to_be_bytes()))
        .for_each(|(key, value)| {
            assert!(hdr.insert(&key, &value).is_none());
        });
    assert_eq!(500, hdr.len());

    for key in 0..max {
        assert!(hdr.remove(&key.to_be_bytes()).is_some());
    }
    assert_eq!(0, hdr.len());
}

#[test]
fn test_valueende() {
    let cnt = 500;
    let dehdr = {
        let mut hdr = MapxRaw::new();
        (0..cnt)
            .map(|i: usize| (i.to_be_bytes(), <usize as ValueEnDe>::encode(&i)))
            .for_each(|(key, value)| {
                assert!(hdr.insert(&key, &value).is_none());
            });
        crate::vsdb_flush();
        <MapxRaw as ValueEnDe>::encode(&hdr)
    };
    let mut reloaded = pnk!(<MapxRaw as ValueEnDe>::decode(&dehdr));
    assert_eq!(cnt, reloaded.len());
    (0..cnt).map(|i: usize| i).for_each(|i| {
        let val = pnk!(<usize as ValueEnDe>::decode(&pnk!(
            reloaded.get(&i.to_be_bytes())
        )));
        assert_eq!(i, val);
    });
}

#[test]
fn test_iter() {
    let mut hdr = MapxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), i.to_be_bytes()))
        .for_each(|(key, value)| {
            assert!(hdr.insert(&key, &value).is_none());
        });
    for (key, _) in hdr.iter() {
        assert!(hdr.remove(&key).is_some());
    }
    assert_eq!(0, hdr.len());
}

#[test]
fn test_first_last() {
    let mut hdr = MapxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), <usize as ValueEnDe>::encode(&i)))
        .for_each(|(key, value)| {
            assert!(hdr.insert(&key, &value).is_none());
        });

    crate::vsdb_flush();

    let (_, value) = pnk!(hdr.iter().next());
    let val = pnk!(<usize as ValueEnDe>::decode(&value));
    assert_eq!(0, val);

    let (_, value) = pnk!(hdr.iter().next_back());
    let val = pnk!(<usize as ValueEnDe>::decode(&value));
    assert_eq!(max - 1, val);
}
