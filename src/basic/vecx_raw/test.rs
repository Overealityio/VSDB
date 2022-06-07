use crate::ValueEnDe;

use super::*;
use ruc::*;

#[test]
fn test_insert() {
    let hdr = VecxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i, <usize as ValueEnDe>::encode(&(max + i))))
        .for_each(|(key, value)| {
            assert!(hdr.get(key).is_none());
            hdr.insert(key, value);
            let val = pnk!(<usize as ValueEnDe>::decode(&pnk!(hdr.get(key))));
            assert_eq!(max + key, val);
        });
    hdr.clear();
    (0..max).map(|i: usize| i).for_each(|key| {
        assert!(hdr.get(key).is_none());
    });
    assert!(hdr.is_empty());
}
#[test]
fn test_len() {
    let hdr = VecxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i, (max + i).to_be_bytes()))
        .for_each(|(key, value)| {
            hdr.insert(key, Box::new(value));
        });
    assert_eq!(500, hdr.len());
    hdr.clear();
    assert_eq!(0, hdr.len());
}

#[test]
fn test_valueende() {
    let cnt = 500;
    let dehdr = {
        let hdr = VecxRaw::new();
        (0..cnt)
            .map(|i: usize| (i, <usize as ValueEnDe>::encode(&i)))
            .for_each(|(key, value)| {
                hdr.insert(key, value);
            });
        crate::vsdb_flush();
        <VecxRaw as ValueEnDe>::encode(&hdr)
    };
    let reloaded = pnk!(<VecxRaw as ValueEnDe>::decode(&dehdr));
    assert_eq!(cnt, reloaded.len());
    (0..cnt).map(|i: usize| i).for_each(|i| {
        let val = pnk!(<usize as ValueEnDe>::decode(&pnk!(reloaded.get(i))));
        assert_eq!(i, val);
    });
}

#[test]
fn test_remove() {
    let hdr = VecxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i, <usize as ValueEnDe>::encode(&(max + i))))
        .for_each(|(key, value)| {
            hdr.insert(key, value);
        });
    assert_eq!(max, hdr.len());

    let idx = 400;
    let val = pnk!(<usize as ValueEnDe>::decode(&hdr.remove(idx)));
    assert_eq!(max + idx, val);
    hdr.clear();
    assert_eq!(0, hdr.len());
}

#[test]
fn test_iter_next() {
    let hdr = VecxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i, <usize as ValueEnDe>::encode(&i)))
        .for_each(|(key, value)| {
            hdr.insert(key, value);
        });
    let value = pnk!(hdr.iter().next());
    let val = pnk!(<usize as ValueEnDe>::decode(&value));
    assert_eq!(0, val);

    let value = pnk!(hdr.iter().next_back());
    let val = pnk!(<usize as ValueEnDe>::decode(&value));
    assert_eq!(max - 1, val);
}

#[test]
fn test_push_pop() {
    let hdr = VecxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: usize| <usize as ValueEnDe>::encode(&i))
        .for_each(|value| {
            hdr.push(value);
        });
    for idx in (0..max).rev() {
        let val = pnk!(<usize as ValueEnDe>::decode(&pnk!(hdr.pop())));
        assert_eq!(idx, val);
    }
}

#[test]
fn test_swap_remove() {
    let hdr = VecxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: usize| <usize as ValueEnDe>::encode(&i))
        .for_each(|value| {
            hdr.push(value);
        });
    for idx in (0..max - 1).rev() {
        let val = pnk!(<usize as ValueEnDe>::decode(&hdr.swap_remove(idx)));
        assert_eq!(val, idx);
    }
    assert_eq!(1, hdr.len());
    let value = pnk!(hdr.last());
    let val = pnk!(<usize as ValueEnDe>::decode(&value));
    assert_eq!(max - 1, val);
}

#[test]
fn test_last() {
    let hdr = VecxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i, <usize as ValueEnDe>::encode(&i)))
        .for_each(|(key, value)| {
            hdr.insert(key, value);
        });
    let value = pnk!(hdr.last());
    let val = pnk!(<usize as ValueEnDe>::decode(&value));
    assert_eq!(max - 1, val);
}
