use crc::{crc32, Hasher32};
use integer_encoding::{FixedInt, FixedIntWriter};
use std::{cmp::Ordering, sync::Arc};

use crate::{
    block::{Block, BlockIter},
    blockhandle::BlockHandle,
    cache::{CacheID, CacheKey},
    cmp::InternalKeyCmp,
    env::RandomAccess,
    error::{Result, Status, StatusCode},
    filter::InternalFilterPolicy,
    filter_block::FilterBlockReader,
    key_types::InternalKey,
    options::{self, CompressionType, Options},
    table_builder::{self, Footer},
    types::{current_key_val, LdbIterator},
};

/// Reads the table footer.
fn read_footer(f: &dyn RandomAccess, size: usize) -> Result<Footer> {
    let mut buf = vec![0; table_builder::FULL_FOOTER_LENGTH];
    f.read_at(size - table_builder::FULL_FOOTER_LENGTH, &mut buf)?;
    Ok(Footer::decode(&buf))
}

fn read_bytes(f: &dyn RandomAccess, location: &BlockHandle) -> Result<Vec<u8>> {
    let mut buf = vec![0; location.size()];
    f.read_at(location.offset(), &mut buf).map(|_| buf)
}

#[derive(Clone)]
struct TableBlock {
    block: Block,
    checksum: u32,
    compression: CompressionType,
}

impl TableBlock {
    /// Reads a block at location.
    fn read_block(
        opt: Options,
        f: &dyn RandomAccess,
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
pub struct Table {
    file: Arc<Box<dyn RandomAccess>>,
    file_size: usize,
    cache_id: CacheID,

    opt: Options,

    footer: Footer,
    indexblock: Block,
    filters: Option<FilterBlockReader>,
}

impl Table {
    /// Creates a new table reader operating on unformatted keys(i.e., UserKeys).
    pub fn new_raw(opt: Options, file: Arc<Box<dyn RandomAccess>>, size: usize) -> Result<Table> {
        let footer = read_footer(file.as_ref().as_ref(), size).unwrap();

        let indexblock =
            TableBlock::read_block(opt.clone(), file.as_ref().as_ref(), &footer.index)?;
        let metaindexblock =
            TableBlock::read_block(opt.clone(), file.as_ref().as_ref(), &footer.meta_index)?;

        if !indexblock.verify() || !metaindexblock.verify() {
            return Err(Status::new(
                StatusCode::InvalidData,
                "Indexblock/Metaindexblock failed verification",
            ));
        }

        // Open filter block for reading
        let mut filter_block_reader = None;
        let filter_name = format!("filter.{}", opt.filter_policy.name())
            .as_bytes()
            .to_vec();

        let mut metaindexiter = metaindexblock.block.iter();

        metaindexiter.seek(&filter_name);

        if let Some((_key, val)) = current_key_val(&metaindexiter) {
            let filter_block_location = BlockHandle::decode(&val).0;

            if filter_block_location.size() > 0 {
                let buf = read_bytes(file.as_ref().as_ref(), &filter_block_location)?;
                filter_block_reader =
                    Some(FilterBlockReader::new_owned(opt.filter_policy.clone(), buf));
            }
        }

        metaindexiter.reset();
        let cache_id = opt.block_cache.lock().unwrap().new_cache_id();

        Ok(Table {
            // clone file here so that we can use a immutable reference rfile above.
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
    pub fn new(mut opt: Options, file: Arc<Box<dyn RandomAccess>>, size: usize) -> Result<Table> {
        opt.cmp = Arc::new(Box::new(InternalKeyCmp(opt.cmp.clone())));
        opt.filter_policy = InternalFilterPolicy::new_wrap(opt.filter_policy);
        let t = Table::new_raw(opt, file, size)?;
        Ok(t)
    }

    fn block_cache_handle(&self, block_off: usize) -> CacheKey {
        let mut dst = [0; 2 * 8];
        (&mut dst[..8])
            .write_fixedint(self.cache_id)
            .expect("error writing to vec");
        (&mut dst[8..])
            .write_fixedint(block_off as u64)
            .expect("error writing to vec");
        dst
    }

    /// Read a block from the current table at `location`, and cache it in the options' block
    /// cache
    fn read_block(&self, location: &BlockHandle) -> Result<TableBlock> {
        let cachekey = self.block_cache_handle(location.offset());
        if let Ok(ref mut block_cache) = self.opt.block_cache.lock() {
            if let Some(_block) = block_cache.get(&cachekey) {
                // Ok(block.clone())
                todo!()
            }
        }

        // Two times as_ref(): First time to get a ref from Arc<>, then on from Box<>.
        let b = TableBlock::read_block(self.opt.clone(), self.file.as_ref().as_ref(), location)?;

        if !b.verify() {
            return Err(Status::new(
                StatusCode::InvalidData,
                "Data block failed verification",
            ));
        }
        if let Ok(ref mut _block_cache) = self.opt.block_cache.lock() {
            // inserting a cheap copy (Rc)
            // block_cache.insert(&cachekey, b.clone());
            todo!()
        }

        Ok(b)
    }

    /// Returns the offset of the block that contains `key`
    pub fn approx_offset_of(&self, key: &[u8]) -> usize {
        let mut iter = self.indexblock.iter();

        iter.seek(key);

        if let Some((_, val)) = current_key_val(&iter) {
            let location = BlockHandle::decode(&val).0;
            return location.offset();
        }

        self.footer.meta_index.offset()
    }

    /// Iterators read from the file; thus only one iterator can be borrowed (mutably) per scope
    pub fn iter(&self) -> TableIterator {
        TableIterator {
            current_block: self.indexblock.iter(),
            init: false,
            current_block_off: 0,
            index_block: self.indexblock.iter(),
            table: self.clone(),
        }
    }

    /// Retrieve value from table. This function uses the attached filters, so is better suited if
    /// you frequently look for non-existing values (as it will detect the non-existence of an
    /// entry in a block without having to load the block).
    pub fn get(&self, key: InternalKey) -> Option<Vec<u8>> {
        let mut index_iter = self.indexblock.iter();
        index_iter.seek(key);

        let handle;
        if let Some((last_in_block, h)) = current_key_val(&index_iter) {
            if self.opt.cmp.cmp(key, &last_in_block) == Ordering::Less {
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
            if !filters.key_may_match(handle.offset(), key) {
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
        iter.seek(key);
        if let Some((k, v)) = current_key_val(&iter) {
            if self.opt.cmp.cmp(key, &k) == Ordering::Equal {
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
pub struct TableIterator {
    // A tableIterator is independent of its table (on the syntax level -- it does not know its
    // Table's lifetime). This is mainly required by the dynamic iterators used everywhere, where a
    // lifetime makes things like returning an iterator from a function neigh-impossible.
    //
    // Instead, reference-counted pointers and locks inside the Table ensure that all
    // TableIterators still share a table.
    table: Table,
    current_block: BlockIter,
    current_block_off: usize,
    index_block: BlockIter,

    init: bool,
}

impl TableIterator {
    // Skips to the entry referenced by the next index block.
    // This is called once a block has run out of entries.
    // Err means corruption or I/O error; Ok(true) means a new block was loaded; Ok(false) means
    // that there's no more entries.
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

impl LdbIterator for TableIterator {
    fn advance(&mut self) -> bool {
        // init essentially means that `current_block` is a data block (it's initially filled with
        // an index block as filler).
        if self.init {
            if self.current_block.advance() {
                true
            } else {
                match self.skip_to_next_entry() {
                    Ok(true) => self.advance(),
                    Ok(false) => {
                        self.reset();
                        false
                    }
                    // try next block, this might be corruption
                    Err(_) => self.advance(),
                }
            }
        } else {
            match self.skip_to_next_entry() {
                Ok(true) => {
                    // Only initialize if we got an entry
                    self.init = true;
                    self.advance()
                }
                Ok(false) => {
                    self.reset();
                    false
                }
                // try next block from index, this might be corruption
                Err(_) => self.advance(),
            }
        }
    }
    // A call to valid() after seeking is necessary to ensure that the seek worked (e.g., no error
    // while reading from disk)
    fn seek(&mut self, to: &[u8]) {
        // first seek in index block, rewind by one entry (so we get the next smaller index entry),
        // then set current_block and seek there

        self.index_block.seek(to);

        if let Some((past_block, handle)) = current_key_val(&self.index_block) {
            if self.table.opt.cmp.cmp(to, &past_block) <= Ordering::Equal {
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

    fn prev(&mut self) -> bool {
        // happy path: current block contains previous entry
        if self.current_block.prev() {
            true
        } else {
            // Go back one block and look for the last entry in the previous block
            if self.index_block.prev() {
                if let Some((_, handle)) = current_key_val(&self.index_block) {
                    if self.load_block(&handle).is_ok() {
                        self.current_block.seek_to_last();
                        self.current_block.valid()
                    } else {
                        self.reset();
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        }
    }

    fn reset(&mut self) {
        self.index_block.reset();
        self.init = false;
    }

    // This iterator is special in that it's valid even before the first call to advance().
    // It behaves correctly, though.
    fn valid(&self) -> bool {
        self.init && (self.current_block.valid() || self.index_block.valid())
    }

    fn current(&self, key: &mut Vec<u8>, val: &mut Vec<u8>) -> bool {
        if self.init {
            self.current_block.current(key, val)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        filter::BloomPolicy,
        key_types::LookupKey,
        table_builder::TableBuilder,
        test_util::{test_iterator_properties, LdbIteratorIter},
    };

    use super::*;

    fn build_data() -> Vec<(&'static str, &'static str)> {
        vec![
            // block 1
            ("abc", "def"),
            ("abd", "dee"),
            ("bcd", "asa"),
            // block 2
            ("bsr", "a00"),
            ("xyz", "xxx"),
            ("xzz", "yyy"),
            // block 3
            ("zzz", "111"),
        ]
    }

    // Build a table containing raw keys (no format). It returns (vector, length) for convenience
    // reason, a call f(v, v.len()) doesn't work for borrowing reasons.
    fn build_table(data: Vec<(&'static str, &'static str)>) -> (Vec<u8>, usize) {
        let mut d = Vec::with_capacity(512);
        let opt = Options {
            block_restart_interval: 2,
            block_size: 32,
            ..Default::default()
        };

        {
            // Uses the standard comparator in opt.
            let mut b = TableBuilder::new(opt, &mut d);

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
            block_restart_interval: 1,
            block_size: 32,
            filter_policy: BloomPolicy::new_wrap(4),
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
            let mut b = TableBuilder::new(opt, &mut d);

            for (k, v) in data.iter() {
                b.add(k.as_slice(), v.as_bytes());
            }

            b.finish();
        }

        let size = d.len();

        (d, size)
    }

    fn wrap_buffer(src: Vec<u8>) -> Arc<Box<dyn RandomAccess>> {
        Arc::new(Box::new(src))
    }

    #[test]
    #[ignore]
    fn test_table_cache_use() {
        let (src, size) = build_table(build_data());
        let opt = Options {
            block_size: 32,
            ..Default::default()
        };

        let table = Table::new_raw(opt.clone(), wrap_buffer(src), size).unwrap();
        let mut iter = table.iter();

        // index/metaindex blocks are not cached. That'd be a waste of memory.
        assert_eq!(opt.block_cache.lock().unwrap().count(), 0);
        iter.next();
        assert_eq!(opt.block_cache.lock().unwrap().count(), 1);
        // This may fail if block parameters or data change. In that case, adapt it.
        iter.next();
        iter.next();
        iter.next();
        iter.next();
        assert_eq!(opt.block_cache.lock().unwrap().count(), 2);
    }

    #[test]
    #[ignore]
    fn test_table_iterator_behavior() {
        let mut data = build_data();
        data.truncate(4);
        let (src, size) = build_table(data);
        let table = Table::new_raw(Options::default(), wrap_buffer(src), size).unwrap();
        test_iterator_properties(table.iter());
    }

    #[test]
    #[ignore]
    fn test_table_iterator_fwd_bwd() {
        let (src, size) = build_table(build_data());
        let data = build_data();

        let table = Table::new_raw(Options::default(), wrap_buffer(src), size).unwrap();

        let mut iter = table.iter();
        let mut i = 0;

        while let Some((k, v)) = iter.next() {
            assert_eq!(
                (data[i].0.as_bytes(), data[i].1.as_bytes()),
                (k.as_ref(), v.as_ref())
            );
            i += 1;
        }

        assert_eq!(i, data.len());
        assert!(!iter.valid());

        // Go forward again, to last entry.
        while let Some((key, _)) = iter.next() {
            println!("{:?}", key);
            if key.as_slice() == "zzz".as_bytes() {
                break;
            }
        }
        assert!(iter.valid());
        println!("{:?}", current_key_val(&iter));

        // backwards count
        let mut j = 0;

        while iter.prev() {
            println!("{:?}", current_key_val(&iter));
            if let Some((k, v)) = current_key_val(&iter) {
                j += 1;
                assert_eq!(
                    (
                        data[data.len() - 1 - j].0.as_bytes(),
                        data[data.len() - 1 - j].1.as_bytes()
                    ),
                    (k.as_ref(), v.as_ref())
                );
            } else {
                break;
            }
        }

        // expecting 7 - 1, because the last entry that the iterator stopped on is the last entry
        // in the table; that is, it needs to go back over 6 entries.
        assert_eq!(j, 6);
    }

    #[test]
    #[ignore]
    fn test_table_iterator_filter() {
        let (src, size) = build_table(build_data());

        let table = Table::new_raw(Options::default(), wrap_buffer(src), size).unwrap();

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
        let (src, size) = build_table(build_data());

        let table = Table::new_raw(Options::default(), wrap_buffer(src), size).unwrap();

        let mut iter = table.iter();

        // behavior test

        // See comment on valid()
        assert!(!iter.valid());
        assert!(current_key_val(&iter).is_none());
        assert!(!iter.prev());

        assert!(iter.advance());
        let first = current_key_val(&iter);
        assert!(iter.valid());
        assert!(current_key_val(&iter).is_some());

        assert!(iter.advance());
        assert!(iter.prev());
        assert!(iter.valid());

        iter.reset();
        assert!(!iter.valid());
        assert!(current_key_val(&iter).is_none());
        assert_eq!(first, iter.next());
    }

    #[test]
    #[ignore]
    fn test_table_iterator_values() {
        let (src, size) = build_table(build_data());
        let data = build_data();

        let table = Table::new_raw(Options::default(), wrap_buffer(src), size).unwrap();

        let mut iter = table.iter();
        let mut i = 0;

        iter.next();
        iter.next();

        // Go back to previous entry, check, go forward two entries, repeat
        // Verifies that prev/next works well.
        loop {
            iter.prev();

            if let Some((k, v)) = current_key_val(&iter) {
                assert_eq!(
                    (data[i].0.as_bytes(), data[i].1.as_bytes()),
                    (k.as_ref(), v.as_ref())
                );
            } else {
                break;
            }

            i += 1;
            if iter.next().is_none() || iter.next().is_none() {
                break;
            }
        }

        // Skipping the last value because the second next() above will break the loop
        assert_eq!(i, 6);
    }

    #[test]
    #[ignore]
    fn test_table_iterator_seek() {
        let (src, size) = build_table(build_data());

        let table = Table::new_raw(Options::default(), wrap_buffer(src), size).unwrap();
        let mut iter = table.iter();

        iter.seek("bcd".as_bytes());
        assert!(iter.valid());
        assert_eq!(
            current_key_val(&iter),
            Some(("bcd".as_bytes().to_vec(), "asa".as_bytes().to_vec()))
        );
        iter.seek("abc".as_bytes());
        assert!(iter.valid());
        assert_eq!(
            current_key_val(&iter),
            Some(("abc".as_bytes().to_vec(), "def".as_bytes().to_vec()))
        );
    }

    #[test]
    #[ignore]
    fn test_table_get() {
        let (src, size) = build_table(build_data());

        let table = Table::new_raw(Options::default(), wrap_buffer(src), size).unwrap();
        let table2 = table.clone();

        let mut _iter = table.iter();
        // Test that all of the table's entries are reachable via get()
        for (k, v) in LdbIteratorIter::wrap(&mut _iter) {
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

    // This test verifies that the table and filters work with internal keys. This means:
    // The table contains keys in InternalKey format and it uses a filter wrapped by
    // InternalFilterPolicy.
    // All the other tests use raw keys that don't have any internal structure; this is fine in
    // general, but here we want to see that the other infrastructure works too.
    #[test]
    #[ignore]
    fn test_table_internal_keys() {
        let (src, size) = build_internal_table();

        let table = Table::new(Options::default(), wrap_buffer(src), size).unwrap();

        let filter_reader = table.filters.clone().unwrap();

        // Check that we're actually using internal keys
        let mut _iter = table.iter();
        for (ref k, _) in LdbIteratorIter::wrap(&mut _iter) {
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

    #[test]
    #[ignore]
    fn test_table_reader_checksum() {
        let (mut src, size) = build_table(build_data());
        println!("{}", size);

        src[10] += 1;

        let table = Table::new_raw(Options::default(), wrap_buffer(src), size).unwrap();

        assert!(table.filters.is_some());
        assert_eq!(table.filters.as_ref().unwrap().num(), 1);

        {
            let mut _iter = table.iter();
            let iter = LdbIteratorIter::wrap(&mut _iter);
            // first block is skipped
            assert_eq!(iter.count(), 4);
        }

        {
            let mut _iter = table.iter();
            let iter = LdbIteratorIter::wrap(&mut _iter);

            for (k, _) in iter {
                if k == build_data()[5].0.as_bytes() {
                    return;
                }
            }

            panic!("Should have hit 5th record in table!");
        }
    }
}
