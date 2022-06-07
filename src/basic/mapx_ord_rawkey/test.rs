use super::*;
use ruc::*;

#[test]
fn test_insert() {
    let hdr = MapxOrdRawKey::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), (max + i).to_be_bytes()))
        .for_each(|(key, value)| {
            assert!(hdr.get(&key).is_none());
            hdr.entry_ref(&key).or_insert_ref(&value);
            hdr.set_value(Box::new(key), value);
            assert!(hdr.insert(Box::new(key), value).is_some());
            assert!(hdr.contains_key(&key));
            assert_eq!(pnk!(hdr.get(&key)), value);
            assert_eq!(pnk!(hdr.remove(&key)), value);
            assert!(hdr.get(&key).is_none());
            assert!(hdr.insert(Box::new(key), value).is_none());
        });
    hdr.clear();
    (0..max).map(|i: usize| i.to_be_bytes()).for_each(|key| {
        assert!(hdr.get(&key).is_none());
    });
    assert!(hdr.is_empty());
}
#[test]
fn test_len() {
    let hdr = MapxOrdRawKey::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), (max + i).to_be_bytes()))
        .for_each(|(key, value)| {
            assert!(hdr.insert(Box::new(key), value).is_none());
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
        let hdr = MapxOrdRawKey::new();
        (0..cnt)
            .map(|i: usize| (i.to_be_bytes(), i))
            .for_each(|(key, value)| {
                assert!(hdr.insert(Box::new(key), value).is_none());
            });
        crate::vsdb_flush();
        <MapxOrdRawKey<usize> as ValueEnDe>::encode(&hdr)
    };
    let reloaded = pnk!(<MapxOrdRawKey<usize> as ValueEnDe>::decode(&dehdr));
    assert_eq!(cnt, reloaded.len());
    (0..cnt).map(|i: usize| i).for_each(|i| {
        assert_eq!(i, reloaded.get(&i.to_be_bytes()).unwrap());
    });
}

#[test]
fn test_iter() {
    let hdr = MapxOrdRawKey::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), i))
        .for_each(|(key, value)| {
            assert!(hdr.insert(Box::new(key), value).is_none());
        });
    for (key, _) in hdr.iter() {
        hdr.unset_value(&key);
    }
    assert_eq!(0, hdr.len());
}

#[test]
fn test_first_last() {
    let hdr = MapxOrdRawKey::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), i))
        .for_each(|(key, value)| {
            assert!(hdr.insert(Box::new(key), value).is_none());
        });
    let (_, value) = pnk!(hdr.first());
    assert_eq!(0, value);

    let (_, value) = pnk!(hdr.last());
    assert_eq!(max - 1, value);
}

#[test]
fn test_values() {
    let hdr = MapxOrdRawKey::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), i))
        .for_each(|(key, value)| {
            assert!(hdr.insert(Box::new(key), value).is_none());
        });
    let mut i = 0;
    for it in hdr.values() {
        assert_eq!(i, it);
        i = i + 1;
    }
}

#[test]
fn test_values_first_last() {
    let hdr = MapxOrdRawKey::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), i))
        .for_each(|(key, value)| {
            assert!(hdr.insert(Box::new(key), value).is_none());
        });
    let value = pnk!(hdr.values().next());
    assert_eq!(0, value);

    let value = pnk!(hdr.values().next_back());
    assert_eq!(max - 1, value);
}
