use crate::ValueEnDe;

use super::*;
use ruc::*;

#[test]
fn test_insert() {
    let hdr = MapxRawMk::new(2);
    assert_eq!(2, hdr.key_size());
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), <usize as ValueEnDe>::encode(&(max + i))))
        .for_each(|(subkey, value)| {
            let key: &[&[u8]] = &[&subkey, &subkey];
            assert!(hdr.get(&key).is_none());
            pnk!(hdr.entry_ref(&key).or_insert_ref(&value));
            assert!(pnk!(hdr.insert(&key, &value)).is_some());
            assert!(hdr.contains_key(&key));
            assert_eq!(pnk!(hdr.get(&key)), value);
            assert_eq!(pnk!(pnk!(hdr.remove(&key))), value);
            assert!(hdr.get(&key).is_none());
            assert!(pnk!(hdr.insert(&key, &value)).is_none());
        });
    hdr.clear();
    (0..max).map(|i: usize| i.to_be_bytes()).for_each(|subkey| {
        assert!(hdr.get(&[&subkey, &subkey]).is_none());
    });
    assert!(hdr.is_empty());
}

#[test]
fn test_valueende() {
    let cnt = 500;
    let dehdr = {
        let hdr = MapxRawMk::new(2);
        let max = 500;
        (0..max)
            .map(|i: usize| (i.to_be_bytes(), <usize as ValueEnDe>::encode(&i)))
            .for_each(|(subkey, value)| {
                let key: &[&[u8]] = &[&subkey, &subkey];
                assert!(pnk!(hdr.insert(&key, &value)).is_none());
            });
        crate::vsdb_flush();
        <MapxRawMk as ValueEnDe>::encode(&hdr)
    };
    let reloaded = pnk!(<MapxRawMk as ValueEnDe>::decode(&dehdr));

    (0..cnt)
        .map(|i: usize| (i, i.to_be_bytes()))
        .for_each(|(i, subkey)| {
            let val = pnk!(<usize as ValueEnDe>::decode(&pnk!(
                reloaded.get(&[&subkey, &subkey])
            )));
            assert_eq!(i, val);
        });
}

#[test]
fn test_iter_op() {
    let map = MapxRawMk::new(4);
    assert!(
        map.entry_ref(&[&[1], &[2], &[3], &[4]])
            .or_insert_ref(&[0])
            .is_ok()
    );
    assert_eq!(map.get(&[&[1], &[2], &[3], &[4]]).unwrap().as_ref(), &[0]);

    let mut cnt = 0;
    pnk!(map.iter_op(&mut |k: &[&[u8]], v: &[u8]| {
        cnt += 1;
        assert_eq!(k, &[&[1], &[2], &[3], &[4]]);
        assert_eq!(v, &[0]);
        Ok(())
    }));
    assert_eq!(cnt, 1);
}

#[test]
fn test_iter_op_with_key_prefix() {
    let map = MapxRawMk::new(4);
    assert!(
        map.entry_ref(&[&[11], &[12], &[13], &[14]])
            .or_insert_ref(&[])
            .is_ok()
    );
    assert_eq!(
        map.get(&[&[11], &[12], &[13], &[14]]).unwrap().as_ref(),
        &[]
    );
    assert!(
        map.entry_ref(&[&[11], &[12], &[13], &[15]])
            .or_insert_ref(&[0])
            .is_ok()
    );
    assert_eq!(
        map.get(&[&[11], &[12], &[13], &[15]]).unwrap().as_ref(),
        &[0]
    );

    let mut cnt = 0;
    let mut op = |k: &[&[u8]], v: &[u8]| {
        cnt += 1;
        println!("cnt = {} v = {:?}", cnt, v);
        if v == &[] {
            assert_eq!(k, &[&[11], &[12], &[13], &[14]]);
        } else {
            assert_eq!(k, &[&[11], &[12], &[13], &[15]]);
        }
        Ok(())
    };

    // cnt += 2
    pnk!(map.iter_op(&mut op));
    // cnt += 2
    pnk!(map.iter_op_with_key_prefix(&mut op, &[&[11]]));
    // // cnt += 2
    pnk!(map.iter_op_with_key_prefix(&mut op, &[&[11], &[12]]));
    // // cnt += 2
    pnk!(map.iter_op_with_key_prefix(&mut op, &[&[11], &[12], &[13]]));
    // // cnt += 1
    pnk!(map.iter_op_with_key_prefix(&mut op, &[&[11], &[12], &[13], &[14]]));
    // // cnt += 1
    pnk!(map.iter_op_with_key_prefix(&mut op, &[&[11], &[12], &[13], &[15]]));

    // cnt += 0
    pnk!(map.iter_op_with_key_prefix(&mut op, &[&[111]]));
    // cnt += 0
    pnk!(map.iter_op_with_key_prefix(&mut op, &[&[111], &[12]]));
    // cnt += 0
    pnk!(map.iter_op_with_key_prefix(&mut op, &[&[111], &[12], &[13]]));
    // cnt += 0
    pnk!(map.iter_op_with_key_prefix(&mut op, &[&[111], &[12], &[13], &[14]]));
    // cnt += 0
    pnk!(map.iter_op_with_key_prefix(&mut op, &[&[111], &[12], &[13], &[15]]));

    assert_eq!(cnt, 10);
}
