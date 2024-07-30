use std::cmp::Ordering;

use integer_encoding::{FixedInt, VarInt};

use crate::{
    skipmap::{SkipMap, SkipMapIter},
    types::{LdbIterator, SequenceNumber, StandardComparator, Status, ValueType},
    Comparator,
};

pub struct LookupKey {
    key: Vec<u8>,
    key_offset: usize,
}

impl LookupKey {
    pub fn new(k: &[u8], s: SequenceNumber) -> Self {
        let mut key = Vec::with_capacity(
            k.len() + k.len().required_space() + <u64 as FixedInt>::ENCODED_SIZE,
        );

        let mut i = 0;
        key.reserve(8 + k.len().required_space() + k.len());

        key.resize(k.len().required_space(), 0);
        i = k.len().encode_var(&mut key[i..]);

        key.extend_from_slice(k);
        i += k.len();

        key.resize(i + <u64 as FixedInt>::ENCODED_SIZE, 0);
        (s << 8 | ValueType::TypeValue as u64).encode_fixed(&mut key[i..]);

        Self {
            key,
            key_offset: k.len().required_space(),
        }
    }

    // return full key
    fn memtable_key(&self) -> &[u8] {
        &self.key
    }

    /// Returns only key
    fn user_key(&self) -> &[u8] {
        &self.key[self.key_offset..self.key.len() - <u64 as FixedInt>::ENCODED_SIZE]
    }

    /// Returns key+tag
    fn internal_key(&self) -> &[u8] {
        &self.key[self.key_offset..]
    }
}

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
        self.map.approx_mem()
    }

    pub fn add(&mut self, seq: SequenceNumber, t: ValueType, key: &[u8], value: &[u8]) {
        self.map
            .insert(&Self::build_memtable_key(key, value, t, seq), value)
    }

    fn build_memtable_key(key: &[u8], value: &[u8], t: ValueType, seq: SequenceNumber) -> Vec<u8> {
        // We are using the original levelDB approach here -- encoding key and value into the
        // key that is used for insertion into the SkipMap.
        // The format is : [key_size: varint32, key_data: [u8], flags: u64, value_size: varint32, value_data: [u8]]
        let keysize = key.len();
        let valsize = value.len();
        let flagsize = <u64 as FixedInt>::ENCODED_SIZE;

        let mut i = 0;
        let mut buf = Vec::with_capacity(
            keysize.required_space()
                + keysize
                + flagsize
                + valsize.required_space()
                + valsize
                + <u64 as FixedInt>::ENCODED_SIZE,
        );

        buf.resize(keysize.required_space(), 0);
        i += keysize.encode_var(&mut buf[i..]);
        buf.extend(key.iter());
        i += keysize;

        let flag: u64 = (t as u64) | (seq << 8);
        buf.resize(i + flagsize, 0);
        flag.encode_fixed(&mut buf[i..]);
        i += flagsize;

        buf.resize(i + valsize.required_space(), 0);
        i += valsize.encode_var(&mut buf[i..]);

        buf.extend(value.iter());
        assert_eq!(i + valsize, buf.len());

        buf
    }

    /// Parses a memtable key and returns  (keylen, key offset, tag, vallen, val offset).
    /// If the key only contains (keylen, key, tag), the vallen and val offset return values will be
    fn parse_memtable_key(mkey: &[u8]) -> (usize, usize, u64, usize, usize) {
        let (keylen, mut i): (usize, usize) = VarInt::decode_var(mkey).unwrap();
        let keyoff = i;
        i += keylen;

        let flag = u64::decode_fixed(&mkey[i..i + <u64 as FixedInt>::ENCODED_SIZE]).unwrap();
        i += <u64 as FixedInt>::ENCODED_SIZE;

        if mkey.len() > i {
            let (vallen, j): (usize, usize) = VarInt::decode_var(&mkey[i..]).unwrap();
            i += j;
            let valoff = i;

            (keylen, keyoff, flag, vallen, valoff)
        } else {
            (keylen, keyoff, 0, 0, 0)
        }
    }

    pub fn get(&self, key: &LookupKey) -> Result<Vec<u8>, Status> {
        let mut iter = self.map.iter();
        iter.seek(key.memtable_key());

        if let Some(e) = iter.current() {
            let foundkey = e.0;
            let (lkeylen, lkeyoff, _, _, _) = Self::parse_memtable_key(key.memtable_key());
            let (fkeylen, fkeyoff, tag, vallen, valoff) = Self::parse_memtable_key(foundkey);

            if C::cmp(
                &key.memtable_key()[lkeyoff..lkeyoff + lkeylen],
                &foundkey[fkeyoff..fkeyoff + fkeylen],
            ) == Ordering::Equal
            {
                if tag & 0xff == ValueType::TypeValue as u64 {
                    return Result::Ok(foundkey[valoff..valoff + vallen].to_vec());
                } else {
                    return Result::Err(Status::NotFound("Not found".to_string()));
                }
            }
        }
        Err(Status::NotFound("Not found".to_string()))
    }

    pub fn iter(&self) -> MemtableIterator<C> {
        MemtableIterator {
            _tbl: self,
            skipmapiter: self.map.iter(),
        }
    }
}

pub struct MemtableIterator<'a, C: Comparator> {
    _tbl: &'a MemTable<C>,
    skipmapiter: SkipMapIter<'a, C>,
}

impl<'a, C: 'a + Comparator> Iterator for MemtableIterator<'a, C> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((foundkey, _)) = self.skipmapiter.next() {
                let (keylen, keyoff, tag, vallen, valoff) =
                    MemTable::<C>::parse_memtable_key(foundkey);

                if tag & 0xff == ValueType::TypeValue as u64 {
                    return Some((
                        &foundkey[keyoff..keyoff + keylen],
                        &foundkey[valoff..valoff + vallen],
                    ));
                } else {
                    continue;
                }
            } else {
                return None;
            }
        }
    }
}

impl<'a, C: 'a + Comparator> LdbIterator for MemtableIterator<'a, C> {
    fn seek(&mut self, to: &[u8]) {
        self.skipmapiter.seek(LookupKey::new(to, 0).memtable_key());
    }

    fn reset(&mut self) {
        self.skipmapiter.reset();
    }

    fn valid(&self) -> bool {
        self.skipmapiter.valid()
    }

    fn current(&self) -> Option<Self::Item> {
        if !self.valid() {
            return None;
        }

        if let Some((foundkey, _)) = self.skipmapiter.current() {
            let (keylen, keyoff, tag, vallen, valoff) = MemTable::<C>::parse_memtable_key(foundkey);

            if tag & 0xff == ValueType::TypeValue as u64 {
                Some((
                    &foundkey[keyoff..keyoff + keylen],
                    &foundkey[valoff..valoff + vallen],
                ))
            } else {
                panic!("should not happen");
            }
        } else {
            panic!("should not happen");
        }
    }

    fn prev(&mut self) -> Option<Self::Item> {
        self.skipmapiter.prev().and_then(|(k, _)| {
            let (keylen, keyoff, tag, vallen, valoff) = MemTable::<C>::parse_memtable_key(k);

            if tag & 0xff == ValueType::TypeValue as u64 {
                Some((&k[keyoff..keyoff + keylen], &k[valoff..valoff + vallen]))
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    fn get_memtable() -> MemTable<StandardComparator> {
        let mut mt = MemTable::new();
        let entries = vec![
            (120, "abc", "123"),
            (121, "abd", "124"),
            (122, "abe", "125"),
            (123, "abf", "126"),
        ];

        for (seq, k, v) in entries {
            mt.add(seq, ValueType::TypeValue, k.as_bytes(), v.as_bytes());
        }
        mt
    }

    #[test]
    fn test_memtable_lookupkey() {
        use integer_encoding::VarInt;

        let lk1 = LookupKey::new(b"abcde", 123);
        let lk2 = LookupKey::new(b"xyabxy", 97);

        // Assert correct allocation strategy
        assert_eq!(lk1.key.len(), 14);
        assert_eq!(lk1.key.capacity(), 14);

        assert_eq!(lk1.user_key(), "abcde".as_bytes());
        assert_eq!(u32::decode_var(lk1.memtable_key()).unwrap(), (5, 1));
        assert_eq!(
            lk2.internal_key(),
            vec![120, 121, 97, 98, 120, 121, 1, 97, 0, 0, 0, 0, 0, 0].as_slice()
        );
    }

    #[test]
    fn test_memtable_add() {
        let mut mt = MemTable::new();
        mt.add(123, ValueType::TypeValue, b"abc", b"123");

        assert_eq!(
            mt.map.iter().next().unwrap().0,
            &vec![3, 97, 98, 99, 1, 123, 0, 0, 0, 0, 0, 0, 3, 49, 50, 51]
        );
    }

    #[test]
    fn test_memtable_add_get() {
        let mt = get_memtable();

        if let Result::Ok(v) = mt.get(&LookupKey::new(b"abc", 120)) {
            assert_eq!(v, "123".as_bytes().to_vec());
        } else {
            panic!("not found");
        }

        if let Result::Ok(v) = mt.get(&LookupKey::new(b"abe", 122)) {
            assert_eq!(v, "125".as_bytes().to_vec());
        } else {
            panic!("not found");
        }

        if let Result::Ok(v) = mt.get(&LookupKey::new(b"abc", 124)) {
            println!("{:?}", v);
            panic!("found");
        }
    }

    #[test]
    fn test_memtable_iterator_init() {
        let mt = get_memtable();
        let mut iter = mt.iter();

        assert!(!iter.valid());
        iter.next();
        assert!(iter.valid());
    }

    #[test]
    fn test_memtable_iterator() {
        let mt = get_memtable();
        let mut iter = mt.iter();

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0, vec![97, 98, 99]);
        assert_eq!(iter.current().unwrap().1, vec![49, 50, 51]);

        iter.seek("abf".as_bytes());
        assert_eq!(iter.current().unwrap().0, vec![97, 98, 102]);
        assert_eq!(iter.current().unwrap().1, vec![49, 50, 54]);
    }

    #[test]
    fn test_memtable_iterator_reverse() {
        let mt = get_memtable();
        let mut iter = mt.iter();

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0, vec![97, 98, 99].as_slice());

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0, vec![97, 98, 100].as_slice());

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0, vec![97, 98, 99].as_slice());

        iter.prev();
        assert!(!iter.valid());
    }

    #[test]
    fn test_memtable_parse_key() {
        let key = vec![3, 1, 2, 3, 1, 123, 0, 0, 0, 0, 0, 0, 3, 4, 5, 6];
        let (keylen, keyoff, tag, vallen, valoff) =
            MemTable::<StandardComparator>::parse_memtable_key(&key);
        assert_eq!(keylen, 3);
        assert_eq!(&key[keyoff..keyoff + keylen], vec![1, 2, 3]);
        assert_eq!(tag, 123 << 8 | 1);
        assert_eq!(vallen, 3);
        assert_eq!(&key[valoff..valoff + vallen], vec![4, 5, 6]);
    }
}
