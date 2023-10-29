use integer_encoding::VarInt;

use crate::{
    memtable::MemTable,
    types::{SequenceNumber, ValueType},
    Comparator,
};

struct BatchEntry<'a> {
    key: &'a [u8],
    val: Option<&'a [u8]>,
}

pub struct WriteBatch<'a> {
    entries: Vec<BatchEntry<'a>>,
    seq: SequenceNumber,
}

impl<'a> WriteBatch<'a> {
    fn new(seq: SequenceNumber) -> WriteBatch<'a> {
        Self {
            entries: Vec::new(),
            seq,
        }
    }

    fn with_capacity(seq: SequenceNumber, c: usize) -> WriteBatch<'a> {
        WriteBatch {
            entries: Vec::with_capacity(c),
            seq,
        }
    }

    fn put(&mut self, k: &'a [u8], v: &'a [u8]) {
        self.entries.push(BatchEntry {
            key: k,
            val: Some(v),
        })
    }

    fn delete(&mut self, k: &'a [u8]) {
        self.entries.push(BatchEntry { key: k, val: None })
    }

    fn clear(&mut self) {
        self.entries.clear()
    }

    fn byte_size(&self) -> usize {
        let mut size = 0;

        for e in self.entries.iter() {
            size += e.key.len() + e.key.len().required_space();

            if let Some(v) = e.val {
                size += v.len() + v.len().required_space();
            } else {
                size += 1;
            }

            size += 1; // account for tag
        }
        size
    }

    fn iter<'b>(&'b self) -> WriteBatchIter<'b, 'a> {
        WriteBatchIter { batch: self, ix: 0 }
    }

    fn insert_into_memtable<C: Comparator>(&self, mt: &mut MemTable<C>) {
        let mut sequence_num = self.seq;

        for (k, v) in self.iter() {
            match v {
                Some(v_) => mt.add(sequence_num, ValueType::TypeValue, k, v_),
                None => mt.add(sequence_num, ValueType::TypeDeletion, k, "".as_bytes()),
            }
            sequence_num += 1;
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.byte_size());
        let mut ix = 0;

        for (k, v) in self.iter() {
            if v.is_some() {
                buf.push(ValueType::TypeValue as u8);
            } else {
                buf.push(ValueType::TypeDeletion as u8);
            }

            ix += 1;

            let req = k.len().required_space();
            buf.resize(ix + req, 0);
            ix += k.len().encode_var(&mut buf[ix..ix + req]);

            buf.extend_from_slice(k);
            ix += k.len();

            let req2;
            let v_;

            if let Some(v__) = v {
                v_ = v__;
                req2 = v_.len().required_space();
            } else {
                v_ = "".as_bytes();
                req2 = 0.required_space();
            }

            buf.resize(ix + req2, 0);
            ix += v_.len().encode_var(&mut buf[ix..ix + req2]);

            buf.extend_from_slice(v_);
            ix += v_.len();
        }
        buf
    }
}

pub struct WriteBatchIter<'b, 'a: 'b> {
    batch: &'b WriteBatch<'a>,
    ix: usize,
}

/// `b` is the ilfttime of the WriteBatch; `'a` is the lifttime of the slice contained in the
/// batch
impl<'b, 'a: 'b> Iterator for WriteBatchIter<'b, 'a> {
    type Item = (&'a [u8], Option<&'a [u8]>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.ix < self.batch.entries.len() {
            self.ix += 1;
            Some((
                self.batch.entries[self.ix - 1].key,
                self.batch.entries[self.ix - 1].val,
            ))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WriteBatch;

    #[test]
    fn test_write_betch() {
        let mut b = WriteBatch::with_capacity(1, 16);

        let entries = vec![
            ("abc".as_bytes(), "def".as_bytes()),
            ("123".as_bytes(), "456".as_bytes()),
            ("xxx".as_bytes(), "yyy".as_bytes()),
            ("zzz".as_bytes(), "".as_bytes()),
            ("010".as_bytes(), "".as_bytes()),
        ];

        for &(k, v) in entries.iter() {
            if !v.is_empty() {
                b.put(k, v);
            } else {
                b.delete(k)
            }
        }

        assert_eq!(b.byte_size(), 39);
        assert_eq!(b.encode().len(), 39);
        assert_eq!(b.iter().count(), 5);

        for (i, (k, v)) in b.iter().enumerate() {
            assert_eq!(k, entries[i].0);

            match v {
                None => assert!(entries[i].1.is_empty()),
                Some(v_) => assert_eq!(v_, entries[i].1),
            }
        }
    }
}
