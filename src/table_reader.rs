use crc::{crc32, Hasher32};
use integer_encoding::FixedInt;
use std::{
    cmp::Ordering,
    io::{Read, Seek, SeekFrom},
    sync::Arc,
};

use crate::{
    block::{Block, BlockIter},
    blockhandle::BlockHandle,
    cache::CacheID,
    cmp::InternalKeyCmp,
    error::{Result, Status, StatusCode},
    filter::{BoxedFilterPolicy, InternalFilterPolicy},
    filter_block::FilterBlockReader,
    key_types::InternalKey,
    options::{self, CompressionType, Options},
    table_builder::{self, Footer},
    types::LdbIterator,
};

/// Reads the table footer.
fn read_footer<R: Read + Seek>(f: &mut R, size: usize) -> Result<Footer> {
    f.seek(SeekFrom::Start(
        (size - table_builder::FULL_FOOTER_LENGTH) as u64,
    ))?;
    let mut buf = [0; table_builder::FULL_FOOTER_LENGTH];
    f.read_exact(&mut buf)?;
    Ok(Footer::decode(&buf))
}

fn read_bytes<R: Read + Seek>(f: &mut R, location: &BlockHandle) -> Result<Vec<u8>> {
    f.seek(SeekFrom::Start(location.offset() as u64))?;

    let mut buf = Vec::new();
    buf.resize(location.size(), 0);

    f.read_exact(&mut buf[0..location.size()])?;

    Ok(buf)
}

struct TableBlock {
    block: Block,
    checksum: u32,
    compression: CompressionType,
}

impl TableBlock {
    /// Reads a block at location.
    fn read_block<R: Read + Seek>(
        opt: Options,
        f: &mut R,
        location: &BlockHandle,
    ) -> Result<TableBlock> {
        // The block is denoted by offset and length in BlockHandle. A block in an encoded
        // table is followed by 1B compression type and 4B checksum.
        let buf = read_bytes(f, location)?;
        let compress = read_bytes(
            f,
            &BlockHandle::new(
                location.offset() + location.size(),
                table_builder::TABLE_BLOCK_COMPRESS_LEN,
            ),
        )
        .unwrap();
        let cksum = read_bytes(
            f,
            &BlockHandle::new(
                location.offset(),
                location.size()
                    + table_builder::TABLE_BLOCK_COMPRESS_LEN
                    + table_builder::TBALE_BLOCK_CKSUM_LEN,
            ),
        )
        .unwrap();

        Ok(TableBlock {
            block: Block::new(opt, buf),
            checksum: u32::decode_fixed(&cksum),
            compression: options::int_to_compressiontype(compress[0] as u32)
                .unwrap_or(CompressionType::CompressionNone),
        })
    }

    fn verify(&self) -> bool {
        let mut digest = crc32::Digest::new(crc32::CASTAGNOLI);
        digest.write(&self.block.contents());
        digest.write(&[self.compression as u8; 1]);

        digest.sum32() == self.checksum
    }
}

#[derive(Clone)]
pub struct Table<R: Read + Seek> {
    file: R,
    file_size: usize,
    cache_id: CacheID,

    opt: Options,

    footer: Footer,
    indexblock: Block,
    filters: Option<FilterBlockReader>,
}

impl<R: Read + Seek> Table<R> {
    /// Creates a new table reader operating on unformatted keys(i.e., UserKeys).
    pub fn new_raw(
        opt: Options,
        mut file: R,
        size: usize,
        fp: BoxedFilterPolicy,
    ) -> Result<Table<R>> {
        let footer = read_footer(&mut file, size).unwrap();

        let indexblock = TableBlock::read_block(opt.clone(), &mut file, &footer.index)?;
        let metaindexblock = TableBlock::read_block(opt.clone(), &mut file, &footer.meta_index)?;

        if !indexblock.verify() || !metaindexblock.verify() {
            return Err(Status::new(
                StatusCode::InvalidData,
                "Indexblock/Metaindexblock failed verification",
            ));
        }

        // Open filter block for reading
        let mut filter_block_reader = None;
        let filter_name = format!("filter.{}", fp.name()).as_bytes().to_vec();

        let mut metaindexiter = metaindexblock.block.iter();

        metaindexiter.seek(&filter_name);

        if let Some((_key, val)) = metaindexiter.current() {
            let filter_block_location = BlockHandle::decode(&val).0;

            if filter_block_location.size() > 0 {
                let buf = read_bytes(&mut file, &filter_block_location)?;
                filter_block_reader = Some(FilterBlockReader::new_owned(fp, buf));
            }
        }

        metaindexiter.reset();

        let cache_id = opt.block_cache.lock().unwrap().new_cache_id();

        Ok(Table {
            file,
            file_size: size,
            cache_id,
            opt,
            footer,
            filters: filter_block_reader,
            indexblock: indexblock.block,
        })
    }

    /// Creates a new table reader operating on internal keys (i.e., InternalKey). This means that
    /// a different comparator (internal_key_cmp) and a different filter policy
    /// (InternalFilterPolicy) are used.
    pub fn new(mut opt: Options, file: R, size: usize, fp: BoxedFilterPolicy) -> Result<Table<R>> {
        opt.cmp = Arc::new(Box::new(InternalKeyCmp(opt.cmp.clone())));
        let t = Table::new_raw(opt, file, size, InternalFilterPolicy::new_wrap(fp))?;
        Ok(t)
    }

    fn read_block(&mut self, location: &BlockHandle) -> Result<TableBlock> {
        let b = TableBlock::read_block(self.opt.clone(), &mut self.file, location)?;

        if !b.verify() {
            Err(Status::new(
                StatusCode::InvalidData,
                "Data block failed verification",
            ))
        } else {
            Ok(b)
        }
    }

    /// Returns the offset of the block that contains `key`
    pub fn approx_offset_of(&self, key: &[u8]) -> usize {
        let mut iter = self.indexblock.iter();

        iter.seek(key);

        if let Some((_, val)) = iter.current() {
            let location = BlockHandle::decode(&val).0;
            return location.offset();
        }

        self.footer.meta_index.offset()
    }

    // Iterators read from the file; thus only one iterator can be borrowed (mutably) per scope
    fn iter(&mut self) -> TableIterator<R> {
        TableIterator {
            current_block: self.indexblock.iter(), // just for filling in here
            current_block_off: 0,
            index_block: self.indexblock.iter(),
            opt: self.opt.clone(),
            table: self,
            init: false,
        }
    }

    /// Retrieve value from table. This function uses the attached filters, so is better suited if
    /// you frequently look for non-existing values (as it will detect the non-existence of an
    /// entry in a block without having to load the block).
    pub fn get(&mut self, to: InternalKey) -> Option<Vec<u8>> {
        let mut index_iter = self.indexblock.iter();
        index_iter.seek(to);

        let handle;
        if let Some((last_in_block, h)) = index_iter.current() {
            if self.opt.cmp.cmp(to, &last_in_block) == Ordering::Less {
                handle = BlockHandle::decode(&h).0;
            } else {
                return None;
            }
        } else {
            return None;
        }

        // found correct block.

        // Check bloom (or whatever) filter
        if let Some(ref filters) = self.filters {
            if !filters.key_may_match(handle.offset(), to) {
                return None;
            }
        }

        // Read block (potentially from cache)
        let mut iter;
        if let Ok(tb) = self.read_block(&handle) {
            iter = tb.block.iter();
        } else {
            return None;
        }

        // Go to entry and check if it's the wanted entry.
        iter.seek(to);
        if let Some((k, v)) = iter.current() {
            if self.opt.cmp.cmp(to, &k) == Ordering::Equal {
                Some(v)
            } else {
                None
            }
        } else {
            None
        }
    }
}

/// This iterator is a "TwoLevelIterator"; it uses an index block in order to get an offset hint
/// into the data blocks.
pub struct TableIterator<'a, R: 'a + Read + Seek> {
    table: &'a mut Table<R>,
    opt: Options,
    current_block: BlockIter,
    current_block_off: usize,
    index_block: BlockIter,

    init: bool,
}

impl<'a, R: Read + Seek> TableIterator<'a, R> {
    // Skips to the entry referenced by the next index block.
    // This is called once a block has run out of entries.
    fn skip_to_next_entry(&mut self) -> Result<bool> {
        if let Some((_key, val)) = self.index_block.next() {
            self.load_block(&val).map(|_| true)
        } else {
            Ok(false)
        }
    }

    // Load the block at `handle` into `self.current_block`
    fn load_block(&mut self, handle: &[u8]) -> Result<()> {
        let (new_block_handle, _) = BlockHandle::decode(handle);
        let block = self.table.read_block(&new_block_handle)?;
        self.current_block = block.block.iter();
        Ok(())
    }
}

impl<'a, R: Read + Seek> Iterator for TableIterator<'a, R> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        // init essentially means that `current_block` is a data block (it's initially filled with
        // an index block as filler).
        if self.init {
            if let Some((key, val)) = self.current_block.next() {
                Some((key, val))
            } else {
                match self.skip_to_next_entry() {
                    Ok(true) => self.next(),
                    Ok(false) => None,
                    // try next block, this might be corruption
                    Err(_) => self.next(),
                }
            }
        } else {
            match self.skip_to_next_entry() {
                Ok(true) => {
                    // Only initialize if we got an entry
                    self.init = true;
                    self.next()
                }
                Ok(false) => None,
                // try next block from index, this might be corruption
                Err(_) => self.next(),
            }
        }
    }
}

impl<'a, R: Read + Seek> LdbIterator for TableIterator<'a, R> {
    // A call to valid() after seeking is necessary to ensure that the seek worked (e.g., no error
    // while reading from disk)
    fn seek(&mut self, to: &[u8]) {
        // first seek in index block, rewind by one entry (so we get the next smaller index entry),
        // then set current_block and seek there

        self.index_block.seek(to);

        if let Some((past_block, handle)) = self.index_block.current() {
            if self.opt.cmp.cmp(to, &past_block) <= Ordering::Equal {
                // ok, found right block: continue below
                if let Ok(()) = self.load_block(&handle) {
                    self.current_block.seek(to);
                    self.init = true;
                } else {
                    self.reset();
                }
            } else {
                self.reset();
            }
        } else {
            panic!("Unexpected None from current() (bug)");
        }
    }

    fn prev(&mut self) -> Option<Self::Item> {
        // happy path: current block contains previous entry
        if let Some(result) = self.current_block.prev() {
            Some(result)
        } else {
            // Go back one block and look for the last entry in the previous block
            if let Some((_, handle)) = self.index_block.prev() {
                if self.load_block(&handle).is_ok() {
                    self.current_block.seek_to_last();
                    self.current_block.current()
                } else {
                    self.reset();
                    None
                }
            } else {
                None
            }
        }
    }

    fn reset(&mut self) {
        self.index_block.reset();
        self.init = false;
    }

    // This iterator is special in that it's valid even before the first call to next(). It behaves
    // correctly, though.
    fn valid(&self) -> bool {
        self.init && (self.current_block.valid() || self.index_block.valid())
    }

    fn current(&self) -> Option<Self::Item> {
        if self.init {
            self.current_block.current()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;

    use crate::{filter::BloomPolicy, key_types::LookupKey, table_builder::TableBuilder};

    use super::*;

    fn build_data() -> Vec<(&'static str, &'static str)> {
        vec![
            ("abc", "def"),
            ("abd", "dee"),
            ("bcd", "asa"),
            ("bsr", "a00"),
            ("xyz", "xxx"),
            ("xzz", "yyy"),
            ("zzz", "111"),
        ]
    }

    // Build a table containing raw keys (no format)
    fn build_table() -> (Vec<u8>, usize) {
        let mut d = Vec::with_capacity(512);
        let opt = Options {
            block_restart_interval: 2,
            block_size: 32,
            ..Default::default()
        };

        {
            // Uses the standard comparator in opt.
            let mut b = TableBuilder::new(opt, &mut d, BloomPolicy::new_wrap(4));
            let data = build_data();

            for &(k, v) in data.iter() {
                b.add(k.as_bytes(), v.as_bytes());
            }

            b.finish();
        }

        let size = d.len();

        (d, size)
    }

    // Build a table containing keys in InternalKey format.
    fn build_internal_table() -> (Vec<u8>, usize) {
        let mut d = Vec::with_capacity(512);

        let opt = Options {
            block_restart_interval: 2,
            block_size: 32,
            ..Default::default()
        };

        let mut i = 0u64;
        let data: Vec<(Vec<u8>, &'static str)> = build_data()
            .into_iter()
            .map(|(k, v)| {
                i += 1;
                (LookupKey::new(k.as_bytes(), i).internal_key().to_vec(), v)
            })
            .collect();

        {
            // Uses InternalKeyCmp
            let mut b = TableBuilder::new(
                opt,
                &mut d,
                InternalFilterPolicy::new_wrap(BloomPolicy::new_wrap(4)),
            );

            for (k, v) in data.iter() {
                b.add(k.as_slice(), v.as_bytes());
            }

            b.finish();
        }

        let size = d.len();

        (d, size)
    }

    #[test]
    #[ignore]
    fn test_table_reader_checksum() {
        let (mut src, size) = build_table();
        println!("{}", size);

        src[45] = 0;

        let mut table = Table::new_raw(
            Options::default(),
            Cursor::new(&src as &[u8]),
            size,
            BloomPolicy::new_wrap(4),
        )
        .unwrap();

        assert!(table.filters.is_some());
        assert_eq!(table.filters.as_ref().unwrap().num(), 1);

        {
            let iter = table.iter();
            // Last block is skipped
            assert_eq!(iter.count(), 3);
        }

        {
            let iter = table.iter();

            for (k, _) in iter {
                if k == build_data()[2].0.as_bytes() {
                    return;
                }
            }

            panic!("Should have hit 3rd record in table!");
        }
    }

    #[test]
    #[ignore]
    fn test_table_iterator_fwd_bwd() {
        let (src, size) = build_table();
        let data = build_data();

        let mut table = Table::new_raw(
            Options::default(),
            Cursor::new(&src as &[u8]),
            size,
            BloomPolicy::new_wrap(4),
        )
        .unwrap();
        let mut iter = table.iter();
        let mut i = 0;

        for (k, v) in iter.by_ref() {
            assert_eq!(
                (data[i].0.as_bytes(), data[i].1.as_bytes()),
                (k.as_ref(), v.as_ref())
            );
            i += 1;
        }

        assert_eq!(i, data.len());
        assert!(iter.next().is_none());

        // backwards count
        let mut j = 0;

        while let Some((k, v)) = iter.prev() {
            j += 1;
            assert_eq!(
                (
                    data[data.len() - 1 - j].0.as_bytes(),
                    data[data.len() - 1 - j].1.as_bytes()
                ),
                (k.as_ref(), v.as_ref())
            );
        }

        // expecting 7 - 1, because the last entry that the iterator stopped on is the last entry
        // in the table; that is, it needs to go back over 6 entries.
        assert_eq!(j, 6);
    }

    #[test]
    #[ignore]
    fn test_table_iterator_filter() {
        let (src, size) = build_table();

        let mut table = Table::new_raw(
            Options::default(),
            Cursor::new(&src as &[u8]),
            size,
            BloomPolicy::new_wrap(4),
        )
        .unwrap();

        assert!(table.filters.is_some());
        let filter_reader = table.filters.clone().unwrap();
        let mut iter = table.iter();

        while let Some((k, _)) = iter.next() {
            assert!(filter_reader.key_may_match(iter.current_block_off, &k));
            assert!(
                !filter_reader.key_may_match(iter.current_block_off, "somerandomkey".as_bytes())
            );
        }
    }

    #[test]
    #[ignore]
    fn test_table_iterator_state_behavior() {
        let (src, size) = build_table();

        let mut table = Table::new_raw(
            Options::default(),
            Cursor::new(&src as &[u8]),
            size,
            BloomPolicy::new_wrap(4),
        )
        .unwrap();
        let mut iter = table.iter();

        // behavior test

        // See comment on valid()
        assert!(!iter.valid());
        assert!(iter.current().is_none());

        let first = iter.current();
        assert!(iter.next().is_some());
        assert!(iter.valid());
        assert!(iter.current().is_some());

        assert!(iter.next().is_some());
        assert!(iter.prev().is_some());
        assert!(iter.current().is_some());

        iter.reset();
        assert!(!iter.valid());
        assert!(iter.current().is_none());
        assert_eq!(first, iter.next());
    }

    #[test]
    #[ignore]
    fn test_table_iterator_values() {
        let (src, size) = build_table();
        let data = build_data();

        let mut table = Table::new_raw(
            Options::default(),
            Cursor::new(&src as &[u8]),
            size,
            BloomPolicy::new_wrap(4),
        )
        .unwrap();
        let mut iter = table.iter();
        let mut i = 0;

        iter.next();
        iter.next();

        // Go back to previous entry, check, go forward two entries, repeat
        // Verifies that prev/next works well.
        while iter.valid() && i < data.len() {
            iter.prev();

            if let Some((k, v)) = iter.current() {
                assert_eq!(
                    (data[i].0.as_bytes(), data[i].1.as_bytes()),
                    (k.as_ref(), v.as_ref())
                );
            } else {
                break;
            }

            i += 1;
            iter.next();
            iter.next();
        }

        assert_eq!(i, 7);
    }

    #[test]
    #[ignore]
    fn test_table_iterator_seek() {
        let (src, size) = build_table();

        let mut table = Table::new_raw(
            Options::default(),
            Cursor::new(&src as &[u8]),
            size,
            BloomPolicy::new_wrap(4),
        )
        .unwrap();
        let mut iter = table.iter();

        iter.seek("bcd".as_bytes());
        assert!(iter.valid());
        assert_eq!(
            iter.current(),
            Some(("bcd".as_bytes().to_vec(), "asa".as_bytes().to_vec()))
        );
        iter.seek("abc".as_bytes());
        assert!(iter.valid());
        assert_eq!(
            iter.current(),
            Some(("abc".as_bytes().to_vec(), "def".as_bytes().to_vec()))
        );
    }

    #[test]
    #[ignore]
    fn test_table_get() {
        let (src, size) = build_table();

        let mut table = Table::new_raw(
            Options::default(),
            Cursor::new(&src as &[u8]),
            size,
            BloomPolicy::new_wrap(4),
        )
        .unwrap();
        let mut table2 = table.clone();

        // Test that all of the table's entries are reachable via get()
        for (k, v) in table.iter() {
            assert_eq!(table2.get(&k), Some(v));
        }

        assert_eq!(table.opt.block_cache.lock().unwrap().count(), 3);

        assert!(table.get("aaa".as_bytes()).is_none());
        assert!(table.get("aaaa".as_bytes()).is_none());
        assert!(table.get("aa".as_bytes()).is_none());
        assert!(table.get("abcd".as_bytes()).is_none());
        assert!(table.get("zzy".as_bytes()).is_none());
        assert!(table.get("zz1".as_bytes()).is_none());
        assert!(table.get("zz{".as_bytes()).is_none());
    }

    /// This test verifies that the tbale and filters work with internal keys. This means:
    /// The table contains key in InternalKey format and it use a filter wrapped by
    /// InternalFilterPolicy.
    /// All the other tests use raw keys, that don't have any internal structure; this is fine in
    /// general, but here we want to see that the other infrastructure works too.
    #[test]
    #[ignore]
    fn test_table_internal_keys() {
        let (src, size) = build_internal_table();

        let mut table = Table::new(
            Options::default(),
            Cursor::new(&src as &[u8]),
            size,
            BloomPolicy::new_wrap(4),
        )
        .unwrap();
        let filter_reader = table.filters.clone().unwrap();

        // Check that we're actually using internal keys
        for (ref k, _) in table.iter() {
            assert_eq!(k.len(), 3 + 8);
        }

        let mut iter = table.iter();

        while let Some((k, _)) = iter.next() {
            let lk = LookupKey::new(&k, 123);
            let userkey = lk.user_key();

            assert!(filter_reader.key_may_match(iter.current_block_off, userkey));
            assert!(
                !filter_reader.key_may_match(iter.current_block_off, "somerandomkey".as_bytes())
            );
        }
    }
}
