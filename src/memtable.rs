use std::cmp::Ordering;

use integer_encoding::{FixedInt, VarInt};

use crate::{
    skipmap::{SkipMap, SkipMapIter},
    types::{LdbIterator, SequenceNumber, StandardComparator, Status, ValueType},
    Comparator,
};

/// An iternal comparator wrapping a user-supplied comparator. This comparator is used to compare
/// memtable keys, which contain length prefixes and a sequence number number.
/// The ordering is determined by asking the wrapped comparator; ties are broken by *reverse*
/// ordering the sequence numbers. (This means that when having an entry abx/4 and searching for
/// abx/5. then abx/4 is counted as "greater-or-equal", making snaphost functionality work at all)
#[derive(Clone, Copy)]
struct MemtableKeyComparator<C: Comparator> {
    internal: C,
}

impl<C: Comparator> Comparator for MemtableKeyComparator<C> {
    fn cmp(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        let (akeylen, akeyoff, atag, _, _) = parse_memtable_key(a);
        let (bkeylen, bkeyoff, btag, _, _) = parse_memtable_key(b);

        let userkey_a = &a[akeyoff..akeyoff + akeylen];
        let userkey_b = &b[bkeyoff..bkeyoff + bkeylen];

        let userkey_order = self.internal.cmp(userkey_a, userkey_b);

        if userkey_order != Ordering::Equal {
            userkey_order
        } else {
            // look at sequence number, in reverse order
            let (_, aseq) = parse_tag(atag);
            let (_, bseq) = parse_tag(btag);

            // reverse!
            bseq.cmp(&aseq)
        }
    }
}

pub struct LookupKey {
    key: Vec<u8>,
    key_offset: usize,
}

/// Encapsulate a user key + sequence number, which is used for lookups in the internal map
/// implementation of a MemTable
/// Format: [keylen: varint32, key: *u8, tag: u64]
/// keylen is the length of key plus 8 (for the tag; this for LevelDB compatibility)
impl LookupKey {
    pub fn new(k: &[u8], s: SequenceNumber) -> Self {
        let mut key = Vec::with_capacity(
            k.len() + k.len().required_space() + <u64 as FixedInt>::ENCODED_SIZE,
        );

        let internal_keylen = k.len() + <u64 as FixedInt>::ENCODED_SIZE;

        let mut i = 0;
        key.reserve(internal_keylen.required_space() + internal_keylen);

        key.resize(k.len().required_space(), 0);
        i += internal_keylen.encode_var(&mut key[i..]);

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

/// Parses a tag into (type, sequence number)
fn parse_tag(tag: u64) -> (u8, u64) {
    let seq = tag >> 8;
    let typ = tag & 0xff;
    (typ as u8, seq)
}

/// A memtable key is a bytestring containing (keylen, key, tag, vallen, val). This function
/// builds such a key. It's called key because the underlying Map implementation will only be
/// concerned with keys; the value field is not used (instead, the value is encoded in the key,
/// and for lookups we just search for the next bigger entry).
/// keylen is the length of key + 8 (to account for the tag)
fn build_memtable_key(key: &[u8], value: &[u8], t: ValueType, seq: SequenceNumber) -> Vec<u8> {
    // We are using the original levelDB approach here -- encoding key and value into the
    // key that is used for insertion into the SkipMap.
    // The format is : [key_size: varint32, key_data: [u8], flags: u64, value_size: varint32, value_data: [u8]]
    let mut i = 0;
    let keysize = key.len() + 8;
    let valsize = value.len();

    let mut buf = Vec::with_capacity(
        keysize.required_space()
            + keysize
            + valsize.required_space()
            + valsize
            + <u64 as FixedInt>::ENCODED_SIZE,
    );

    buf.resize(keysize.required_space(), 0);
    i += keysize.encode_var(&mut buf[i..]);

    buf.extend(key.iter());
    i += key.len();

    let flag: u64 = (t as u64) | (seq << 8);
    buf.resize(i + <u64 as FixedInt>::ENCODED_SIZE, 0);
    flag.encode_fixed(&mut buf[i..]);
    i += <u64 as FixedInt>::ENCODED_SIZE;

    buf.resize(i + valsize.required_space(), 0);
    i += valsize.encode_var(&mut buf[i..]);

    buf.extend(value.iter());
    i += value.len();

    assert_eq!(i, buf.len());

    buf
}

/// Parses a memtable key and returns  (keylen, key offset, tag, vallen, val offset).
/// If the key only contains (keylen, key, tag), the vallen and val offset return values will be
/// meaningless.
fn parse_memtable_key(mkey: &[u8]) -> (usize, usize, u64, usize, usize) {
    let (keylen, mut i): (usize, usize) = VarInt::decode_var(mkey).unwrap();
    let keyoff = i;
    i += keylen - 8;

    if mkey.len() > i + 8 {
        let tag = FixedInt::decode_fixed(&mkey[i..i + 8]).unwrap();
        i += 8;

        let (vallen, j): (usize, usize) = VarInt::decode_var(&mkey[i..]).unwrap();
        i += j;
        let valoff = i;

        (keylen - 8, keyoff, tag, vallen, valoff)
    } else {
        (keylen - 8, keyoff, 0, 0, 0)
    }
}

/// Provides Insert/Iterata, based on the SkipMap implementation.
pub struct MemTable<C: Comparator> {
    map: SkipMap<MemtableKeyComparator<C>>,
    cmp: C,
}

impl MemTable<StandardComparator> {
    pub fn new() -> MemTable<StandardComparator> {
        MemTable::new_custom_cmp(StandardComparator {})
    }
}

impl<C: Comparator> MemTable<C> {
    pub fn new_custom_cmp(comparator: C) -> MemTable<C> {
        MemTable {
            map: SkipMap::new_with_cmp(MemtableKeyComparator {
                internal: comparator,
            }),
            cmp: comparator,
        }
    }

    pub fn approx_mem_usage(&self) -> usize {
        self.map.approx_mem()
    }

    pub fn add(&mut self, seq: SequenceNumber, t: ValueType, key: &[u8], value: &[u8]) {
        self.map
            .insert(&build_memtable_key(key, value, t, seq), &Vec::new())
    }

    pub fn get(&self, key: &LookupKey) -> Result<Vec<u8>, Status> {
        let mut iter = self.map.iter();
        iter.seek(key.memtable_key());
        println!("key.memtable_key() {:?}", key.memtable_key());

        if let Some(e) = iter.current() {
            let foundkey = e.0;
            println!("{:?}", foundkey);

            let (lkeylen, lkeyoff, _, _, _) = parse_memtable_key(key.memtable_key());
            let (fkeylen, fkeyoff, tag, vallen, valoff) = parse_memtable_key(foundkey);

            // Compare user key -- if equal, process

            if self.cmp.cmp(
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
    skipmapiter: SkipMapIter<'a, MemtableKeyComparator<C>>,
}

impl<'a, C: 'a + Comparator> Iterator for MemtableIterator<'a, C> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((foundkey, _)) = self.skipmapiter.next() {
                let (keylen, keyoff, tag, vallen, valoff) = parse_memtable_key(foundkey);

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
            let (keylen, keyoff, tag, vallen, valoff) = parse_memtable_key(foundkey);

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
        loop {
            if let Some((foundkey, _)) = self.skipmapiter.prev() {
                let (keylen, keyoff, tag, vallen, valoff) = parse_memtable_key(foundkey);

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

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    fn get_memtable() -> MemTable<StandardComparator> {
        let mut mt = MemTable::new();
        let entries = vec![
            (115, "abc", "122"),
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
    fn test_memtable_parse_tag() {
        let tag = (12345 << 8) | 67;
        assert_eq!(parse_tag(tag), (67, 12345));
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
        assert_eq!(u32::decode_var(lk1.memtable_key()).unwrap(), (13, 1));
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
            &vec![11, 97, 98, 99, 1, 123, 0, 0, 0, 0, 0, 0, 3, 49, 50, 51]
        );
    }

    #[test]
    #[ignore]
    fn test_memtable_add_get() {
        let mt = get_memtable();

        // // Smaller sequence number dosn't find entry
        // if let Result::Ok(v) = mt.get(&LookupKey::new("abc".as_bytes(), 110)) {
        //     println!("{:?}", v);
        //     panic!("found");
        // }

        // // Bigger sequence number falls back to next smaller
        // if let Result::Ok(v) = mt.get(&LookupKey::new("abc".as_bytes(), 116)) {
        //     assert_eq!(v, "122".as_bytes());
        // } else {
        //     panic!("not found");
        // }

        // // Bigger sequence number doesn't
        // if let Result::Ok(v) = mt.get(&LookupKey::new(b"abc", 124)) {
        //     println!("{:?}", v);
        //     panic!("found");
        // }

        // // Exact match works
        // if let Result::Ok(v) = mt.get(&LookupKey::new("abc".as_bytes(), 120)) {
        //     assert_eq!(v, "123".as_bytes());
        // } else {
        //     panic!("not found");
        // }

        if let Result::Ok(v) = mt.get(&LookupKey::new(b"abe", 122)) {
            assert_eq!(v, "125".as_bytes().to_vec());
        } else {
            panic!("not found");
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
    fn test_memtable_iterator_fwd_seek() {
        let mt = get_memtable();
        let iter = mt.iter();

        let expected = [
            "123".as_bytes(), /* i.e., the abc entry with
                               * higher sequence number comes first */
            "122".as_bytes(),
            "124".as_bytes(),
            "125".as_bytes(),
            "126".as_bytes(),
        ];

        for (i, (_k, v)) in iter.enumerate() {
            assert_eq!(v, expected[i]);
        }
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
        assert_eq!(iter.current().unwrap().0, vec![97, 98, 99].as_slice());

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0, vec![97, 98, 100].as_slice());

        iter.prev();
        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().0, vec![97, 98, 99].as_slice());

        iter.prev();
        assert!(!iter.valid());
    }

    #[test]
    fn test_memtable_parse_key() {
        let key = vec![11, 1, 2, 3, 1, 123, 0, 0, 0, 0, 0, 0, 3, 4, 5, 6];
        let (keylen, keyoff, tag, vallen, valoff) = parse_memtable_key(&key);
        assert_eq!(keylen, 3);
        assert_eq!(&key[keyoff..keyoff + keylen], vec![1, 2, 3]);
        assert_eq!(tag, 123 << 8 | 1);
        assert_eq!(vallen, 3);
        assert_eq!(&key[valoff..valoff + vallen], vec![4, 5, 6]);
    }
}
