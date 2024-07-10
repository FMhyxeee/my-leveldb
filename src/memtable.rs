use std::mem::size_of;

use integer_encoding::{FixedInt, VarInt};

use crate::{
    skipmap::{SkipMap, StandardComparator},
    types::{SequenceNumber, ValueType},
    Comparator,
};

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
        // We are using the original levelDB approach here -- encoding key and value into the
        // key that is used for insertion into the SkipMap.
        // The format is : [key_size: varint32, key_data: [u8], flags: u64, value_size: varint32, value_data: [u8]]
        let keysize = key.len();
        let valsize = value.len();
        let flagsize = size_of::<u64>();

        let mut i = 0;
        let mut buf = Vec::with_capacity(
            keysize.required_space() + keysize + flagsize + valsize.required_space() + valsize + 8,
        );

        buf.resize(keysize.required_space(), 0);
        i += keysize.encode_var(&mut buf[i..]);
        buf.extend(key.iter());
        i += keysize;

        let flag = (t as u64) | (seq << 8);
        buf.resize(i + flagsize, 0);
        flag.encode_fixed(&mut buf[i..]);
        i += flagsize;

        buf.resize(i + valsize.required_space(), 0);
        i += valsize.encode_var(&mut buf[i..]);

        buf.extend(value.iter());
        assert_eq!(i + valsize, buf.len());

        self.map.insert(&buf, &[]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        let mut mt = MemTable::new();
        mt.add(123, ValueType::TypeValue, b"abc", b"123");

        assert_eq!(
            mt.map.iter().next().unwrap().0,
            &[3, 97, 98, 99, 1, 123, 0, 0, 0, 0, 0, 0, 3, 49, 50, 51]
        )
    }
}
