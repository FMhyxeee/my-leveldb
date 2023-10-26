#![allow(dead_code)]

use std::cmp::Ordering;

use integer_encoding::VarInt;

pub enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
}

/// Represents a sequence number of a single entry.
pub type SequenceNumber = u64;

pub enum Status {
    OK,
    NotFound(String),
    Corruption(String),
    NotSupported(String),
    InvalidArgument(String),
    IOError(String),
}

/// Trait used to influnce how SkipMap determines the order of elements, Use StandardComparator
/// for the normal implementation using numerical comparison.
pub trait Comparator {
    fn cmp(a: &[u8], b: &[u8]) -> Ordering;
}

#[derive(Debug)]
pub struct StandardComparator;

impl Comparator for StandardComparator {
    fn cmp(a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
}

/// [not all member types implemented yet]
///
#[derive(Debug)]
pub struct Options<C: Comparator> {
    pub cmp: C,
    pub create_if_missing: bool,
    pub error_if_exists: bool,
    pub paranoid_checks: bool,
    // pub logger: Logger
    pub write_buffer_size: usize,
    pub max_open_file: usize,
    // pub block_cache: Cache,
    pub block_size: usize,
    pub block_restart_interval: usize,
    // pub compression_type: CompressionType
    pub reuse_logs: bool, // pub filter_poilcy: FilterPoilcy,
}

impl Default for Options<StandardComparator> {
    fn default() -> Self {
        Self {
            cmp: StandardComparator,
            create_if_missing: true,
            error_if_exists: false,
            paranoid_checks: false,
            write_buffer_size: 4 << 20,
            max_open_file: 1 << 10,
            block_size: 4 << 10,
            block_restart_interval: 16,
            reuse_logs: false,
        }
    }
}

/// An extension of the standard `Iterator` trait that supports some methods necessary for LevelDB.
/// This works because the iterators used are stateful and keep the last returned element.
pub trait LdbIterator<'a>: Iterator {
    // We're emulating LeveDB's Slice tyoe here using actual slices with the lifetime of the iterator.
    // The lifetime of the iterator is usually the one of the backing storage (Block, MemTable, SkipMap...)
    // type Item = (&'a [u8], &'a [u8]);
    fn seek(&mut self, key: &[u8]);
    fn valid(&self) -> bool;
    fn current(&'a self) -> Self::Item;
    fn prev(&mut self) -> Option<Self::Item>;
}

/// Supplied to DB read operations
pub struct ReadOptions {
    pub verify_checksums: bool,
    pub fill_cache: bool,
    pub snapshot: Option<SequenceNumber>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            verify_checksums: false,
            fill_cache: true,
            snapshot: None,
        }
    }
}

pub struct BlockHandle {
    offset: usize,
    size: usize,
}

impl BlockHandle {
    /// Decodes a block handle from `from` and returns a block handle
    /// together with how many bytes we read from the slice.
    pub fn decode(from: &[u8]) -> (BlockHandle, usize) {
        let (off, offsize) = usize::decode_var(from).unwrap();
        let (sz, szsize) = usize::decode_var(&from[offsize..]).unwrap();

        (
            BlockHandle {
                offset: off,
                size: sz,
            },
            offsize + szsize,
        )
    }

    pub fn new(offset: usize, size: usize) -> BlockHandle {
        BlockHandle { offset, size }
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns how many bytes were written, or 0 if the write failed because `dst` is too small.
    pub fn encode_to(&self, dst: &mut [u8]) -> usize {
        if dst.len() < self.offset.required_space() + self.size.required_space() {
            0
        } else {
            let off = self.offset.encode_var(dst);
            off + self.size.encode_var(&mut dst[off..])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blockhandle() {
        let hb = BlockHandle::new(890, 777);
        let mut dst = [0u8; 128];

        let enc_sz = hb.encode_to(&mut dst[..]);

        let (hb2, dec_sz) = BlockHandle::decode(&dst);

        assert_eq!(enc_sz, dec_sz);
        assert_eq!(hb.size(), hb2.size());
        assert_eq!(hb.offset(), hb2.offset());
    }
}
