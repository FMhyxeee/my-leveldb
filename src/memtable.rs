#![allow(dead_code)]

use std::cmp::Ordering;

use integer_encoding::{FixedInt, VarInt};

use crate::{
    skipmap::{Comparator, SkipMap, SkipMapIter, StandardComparator},
    types::{LdbIterator, SequenceNumber, Status, ValueType},
};

#[derive(Debug)]
pub struct LookupKey {
    key: Vec<u8>,
    key_offset: usize,
}

impl LookupKey {
    #[allow(unused_assignments)]
    fn new(k: &[u8], s: SequenceNumber) -> LookupKey {
        let mut key = Vec::with_capacity(
            k.len() + k.len().required_space() + <u64 as FixedInt>::required_space(),
        );
        let mut i = 0;

        key.resize(k.len().required_space(), 0);
        i += k.len().encode_var(&mut key[i..]);

        key.extend(k.iter());
        i += k.len();

        key.resize(i + <u64 as FixedInt>::required_space(), 0);
        (s << 8 | ValueType::TypeValue as u64).encode_fixed(&mut key[i..]);
        i += <u64 as FixedInt>::required_space();

        LookupKey {
            key,
            key_offset: k.len().required_space(),
        }
    }

    fn memtable_key(&self) -> &[u8] {
        &self.key
    }

    fn user_key(&self) -> Vec<u8> {
        self.key[self.key_offset..].to_vec()
    }
}

// Provides Insert/Get/Iterate, based on the SkipMap implementation.
// MemTable used MemtableKeys internally, that is, it stores key and value in the [Skipmap] key.
pub struct MemTable<C: Comparator> {
    map: SkipMap<C>,
}

impl MemTable<StandardComparator> {
    pub fn new() -> MemTable<StandardComparator> {
        MemTable::new_custom_cmp(StandardComparator {})
    }
}

impl<C: Comparator> MemTable<C> {
    pub fn new_custom_cmp(comparator: C) -> MemTable<C> {
        MemTable {
            map: SkipMap::new_with_cmp(comparator),
        }
    }

    pub fn approx_mem_usage(&self) -> usize {
        self.map.approx_memory()
    }

    pub fn add(&mut self, seq: SequenceNumber, t: ValueType, key: &[u8], value: &[u8]) {
        self.map
            .insert(Self::build_memtable_key(key, value, t, seq), Vec::new())
    }

    fn build_memtable_key(key: &[u8], value: &[u8], t: ValueType, seq: SequenceNumber) -> Vec<u8> {
        // We are using the original LevelDB approach here -- encoding key and value into the
        // key that is used for insertion into the SkipMap.
        // The format is : [key_size: varint32, key_data: [u8], flags: u64, value_size: varint32, value_data: [u8]]
        let keysize = key.len();
        let valsize = value.len();

        let mut i = 0;
        let mut buf = Vec::with_capacity(
            keysize
                + keysize.required_space()
                + valsize
                + valsize.required_space()
                + <u64 as FixedInt>::required_space(),
        );
        buf.resize(keysize.required_space(), 0);
        i += keysize.encode_var(&mut buf[i..]);

        buf.extend(key.iter());
        i += key.len();

        let flag = (t as u64) | (seq << 8);
        buf.resize(i + <u64 as FixedInt>::required_space(), 0);

        flag.encode_fixed(&mut buf[i..]);
        i += <u64 as FixedInt>::required_space();

        buf.resize(i + valsize.required_space(), 0);
        i += valsize.encode_var(&mut buf[i..]);

        buf.extend(value.iter());
        i += key.len();

        assert_eq!(i, buf.len());

        buf
    }

    #[allow(unused_variables)]
    // returns (keylen, key, tag, vallen, val)
    fn parse_memtable_key(mkey: &[u8]) -> (usize, Vec<u8>, u64, usize, Vec<u8>) {
        println!("mkey: {:?}", mkey);
        let (keylen, mut i): (usize, usize) = VarInt::decode_var(mkey).unwrap();
        println!("keylen:{:?}, mut i: {:?}", keylen, i);

        let key = mkey[i..i + keylen].to_vec();
        i += keylen;

        println!("mkey.len(): {:?}, i: {:?}", mkey.len(), i);

        if mkey.len() > i + 8 {
            let tag = FixedInt::decode_fixed(&mkey[i..i + 8]);
            i += 8;
            println!("tag: {:?}, i: {:?}", tag, i);

            let (vallen, j): (usize, usize) = VarInt::decode_var(&mkey[i..]).unwrap();
            i += j;

            let val = mkey[i..].to_vec();

            (keylen, key, tag, vallen, val)
        } else {
            (keylen, key, 0, 0, Vec::new())
        }
    }

    #[allow(unused_variables)]
    pub fn get(&self, key: &LookupKey) -> Result<Vec<u8>, Status> {
        println!("key: {key:?}, key.memtable_key is {:?}", key.memtable_key());
        let mut iter = self.map.iter();
        iter.seek(key.memtable_key());

        if iter.valid() {
            let foundkey = iter.current().0;
            println!("foundkey: {:?}", foundkey);
            println!("key: {:?}", key);
            let (lkeylen, lkey, _, _, _) = Self::parse_memtable_key(key.memtable_key());
            println!("lkeylen: {:?}, lkey: {:?}", lkeylen, lkey);
            let (fkeylen, fkey, tag, vallen, val) = Self::parse_memtable_key(foundkey);
            println!(
                "lkeylen: {:?}, lkey: {:?}, fkeylen: {:?}, fkey: {:?}",
                lkeylen, lkey, fkeylen, fkey
            );

            if C::cmp(&lkey, &fkey) == Ordering::Equal {
                if tag & 0xff == ValueType::TypeValue as u64 {
                    return Result::Ok(val);
                } else {
                    return Result::Err(Status::NotFound(String::new()));
                }
            }
        }
        Result::Err(Status::NotFound("not found".to_string()))
    }

    pub fn iter(&self) -> MemtableIterator<C> {
        MemtableIterator {
            _tbl: self,
            skipmapiter: self.map.iter(),
        }
    }
}

pub struct MemtableIterator<'a, C: 'a + Comparator> {
    _tbl: &'a MemTable<C>,
    skipmapiter: SkipMapIter<'a, C>,
}

impl<'a, C: 'a + Comparator> Iterator for MemtableIterator<'a, C> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((foundkey, _)) = self.skipmapiter.next() {
                let (_, key, tag, _, val) = MemTable::<C>::parse_memtable_key(foundkey);

                if tag & 0xff == ValueType::TypeValue as u64 {
                    return Some((key, val));
                } else {
                    continue;
                }
            } else {
                return None;
            }
        }
    }
}

impl<'a, C: 'a + Comparator> LdbIterator<'a> for MemtableIterator<'a, C> {
    fn valid(&self) -> bool {
        self.skipmapiter.valid()
    }
    fn current(&self) -> Self::Item {
        assert!(self.valid());

        let (foundkey, _) = self.skipmapiter.current();
        let (_, key, tag, _, val) = MemTable::<C>::parse_memtable_key(foundkey);

        if tag & 0xff == ValueType::TypeValue as u64 {
            (key, val)
        } else {
            panic!("should not happen");
        }
    }
    fn seek(&mut self, to: &[u8]) {
        self.skipmapiter.seek(LookupKey::new(to, 0).memtable_key());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_memtable() -> MemTable<StandardComparator> {
        let mut mt = MemTable::new();
        let entries = vec![
            (120, "abc", "123"),
            (121, "abd", "124"),
            (122, "abe", "125"),
            (123, "abf", "126"),
        ];

        for e in entries.iter() {
            mt.add(e.0, ValueType::TypeValue, e.1.as_bytes(), e.2.as_bytes());
        }
        mt
    }

    #[test]
    fn test_add() {
        let mut mt = MemTable::new();
        mt.add(
            123,
            ValueType::TypeValue,
            "abc".as_bytes(),
            "123".as_bytes(),
        );

        assert_eq!(
            mt.map.iter().next().unwrap().0,
            &vec![3, 97, 98, 99, 1, 123, 0, 0, 0, 0, 0, 0, 3, 49, 50, 51]
        );
    }

    #[test]
    fn test_add_get() {
        let mt = get_memtable();

        if let Result::Ok(v) = mt.get(&LookupKey::new("abc".as_bytes(), 120)) {
            println!("{v:?}");
            assert_eq!(v, "123".as_bytes().to_vec());
        } else {
            // panic!("not found");
            println!("not found")
        }

        if let Result::Ok(v) = mt.get(&LookupKey::new("abe".as_bytes(), 122)) {
            assert_eq!(v, "125".as_bytes().to_vec());
        } else {
            panic!("not found");
        }

        if let Result::Ok(_v) = mt.get(&LookupKey::new("abc".as_bytes(), 124)) {
            panic!("found");
        }
    }

    #[test]
    fn test_memtable_iterator() {
        let mt = get_memtable();
        let mut iter = mt.iter();

        assert!(!iter.valid());

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.current().0, vec![97, 98, 99]);
        assert_eq!(iter.current().1, vec![49, 50, 51]);

        iter.seek("abf".as_bytes());
        assert_eq!(iter.current().0, vec![97, 98, 102]);
        assert_eq!(iter.current().1, vec![49, 50, 54]);
    }

    #[test]
    fn test_parse_memtable_key() {
        let key = vec![3, 1, 2, 3, 1, 123, 0, 0, 0, 0, 0, 0, 3, 4, 5, 6];
        let (keylen, key, tag, vallen, val) =
            MemTable::<StandardComparator>::parse_memtable_key(&key);
        assert_eq!(keylen, 3);
        assert_eq!(key, vec![1, 2, 3]);
        assert_eq!(tag, 123 << 8 | 1);
        assert_eq!(vallen, 3);
        assert_eq!(val, vec![4, 5, 6]);
    }
}
