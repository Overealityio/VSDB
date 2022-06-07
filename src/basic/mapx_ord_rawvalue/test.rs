use crate::ValueEnDe;

use super::*;
use ruc::*;

#[test]
fn test_insert() {
    let hdr: MapxOrdRawValue<usize> = MapxOrdRawValue::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i, (max + i).to_be_bytes()))
        .for_each(|(key, value)| {
            assert!(hdr.get(&key).is_none());
            hdr.entry_ref(&key).or_insert_ref(&value);
            hdr.set_value(key, Box::new(value));
            assert!(hdr.insert(key, Box::new(value)).is_some());
            assert!(hdr.contains_key(&key));

            assert_eq!(*pnk!(hdr.get(&key)), value);
            assert_eq!(*pnk!(hdr.remove(&key)), value);

            assert!(hdr.get(&key).is_none());
            assert!(hdr.insert(key, Box::new(value)).is_none());
        });
    hdr.clear();
    (0..max).map(|i: usize| i).for_each(|key| {
        assert!(hdr.get(&key).is_none());
    });
    assert!(hdr.is_empty());
}
#[test]
fn test_len() {
    let hdr: MapxOrdRawValue<usize> = MapxOrdRawValue::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i, (max + i).to_be_bytes()))
        .for_each(|(key, value)| {
            assert!(hdr.insert(key, Box::new(value)).is_none());
        });
    assert_eq!(500, hdr.len());

    for key in 0..max {
        assert!(hdr.remove(&key).is_some());
    }
    assert_eq!(0, hdr.len());
}

#[test]
fn test_valueende() {
    let cnt = 500;
    let dehdr = {
        let hdr: MapxOrdRawValue<usize> = MapxOrdRawValue::new();
        (0..cnt)
            .map(|i: usize| (i, <usize as ValueEnDe>::encode(&i)))
            .for_each(|(key, value)| {
                assert!(hdr.insert(key, value).is_none());
            });
        crate::vsdb_flush();
        <MapxOrdRawValue<usize> as ValueEnDe>::encode(&hdr)
    };
    let reloaded = pnk!(<MapxOrdRawValue<usize> as ValueEnDe>::decode(&dehdr));
    assert_eq!(cnt, reloaded.len());
    (0..cnt).map(|i: usize| i).for_each(|i| {
        let val = pnk!(<usize as ValueEnDe>::decode(&pnk!(reloaded.get(&i))));
        assert_eq!(i, val);
    });
}

#[test]
fn test_iter() {
    let hdr: MapxOrdRawValue<usize> = MapxOrdRawValue::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i, i.to_be_bytes()))
        .for_each(|(key, value)| {
            assert!(hdr.insert(key, Box::new(value)).is_none());
        });
    for (key, _) in hdr.iter() {
        hdr.unset_value(&key);
    }
    assert_eq!(0, hdr.len());
}

#[test]
fn test_first_last() {
    let hdr: MapxOrdRawValue<usize> = MapxOrdRawValue::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i, <usize as ValueEnDe>::encode(&i)))
        .for_each(|(key, value)| {
            assert!(hdr.insert(key, value).is_none());
        });

    let (_, value) = pnk!(hdr.first());
    let val = pnk!(<usize as ValueEnDe>::decode(&value));
    assert_eq!(0, val);

    let (_, value) = pnk!(hdr.last());
    let val = pnk!(<usize as ValueEnDe>::decode(&value));
    assert_eq!(max - 1, val);
}
