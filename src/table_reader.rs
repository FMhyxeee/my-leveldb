use std::io::{Read, Result, Seek, SeekFrom};

use crate::{
    block::BlockIter,
    blockhandle::BlockHandle,
    filter::FilterPolicy,
    filter_block::FilterBlockReader,
    options::Options,
    table_builder::{self, Footer},
    types::LdbIterator,
    Comparator,
};

/// Reads the table footer.
fn read_footer<R: Read + Seek>(f: &mut R, size: usize) -> Result<Footer> {
    f.seek(SeekFrom::Start(
        (size - table_builder::FULL_FOOTER_LENGTH) as u64,
    ))?;
    let mut buf = [0; table_builder::FULL_FOOTER_LENGTH];
    f.read_exact(&mut buf)?;
    let footer = Footer::decode(&buf);
    println!("Footer: {:?}", footer);
    Ok(footer)
}

fn read_bytes<R: Read + Seek>(f: &mut R, location: &BlockHandle) -> Result<Vec<u8>> {
    f.seek(SeekFrom::Start(0))?;
    f.seek(SeekFrom::Start(location.offset() as u64))?;

    let mut buf = vec![0; location.size()];

    f.read_exact(&mut buf[0..location.size()])?;

    Ok(buf)
}

/// Reads a block at location.
fn read_block<R: Read + Seek, C: Comparator>(
    cmp: &C,
    f: &mut R,
    location: &BlockHandle,
) -> Result<BlockIter<C>> {
    println!("Reading block at {:?}", location);
    let buf = read_bytes(f, location)?;
    Ok(BlockIter::new(buf, *cmp))
}

pub struct Table<R: Read + Seek, C: Comparator, FP: FilterPolicy> {
    file: R,
    file_size: usize,

    opt: Options,
    cmp: C,

    footer: Footer,
    indexblock: BlockIter<C>,
    filters: Option<FilterBlockReader<FP>>,
}

impl<R: Read + Seek, C: Comparator, FP: FilterPolicy> Table<R, C, FP> {
    pub fn new(mut file: R, size: usize, cmp: C, fp: FP, opt: Options) -> Result<Table<R, C, FP>> {
        let footer = read_footer(&mut file, size)?;

        println!("start reading index block");
        let indexblock = read_block(&cmp, &mut file, &footer.index)?;
        println!("Index block: {:?}", indexblock.block);

        let mut metaindexblock = read_block(&cmp, &mut file, &footer.meta_index)?;

        let mut filter_block_reader = None;
        let mut filter_name = "filter.".as_bytes().to_vec();
        filter_name.extend_from_slice(fp.name().as_bytes());

        metaindexblock.seek(&filter_name);
        if let Some((_key, val)) = metaindexblock.current() {
            let filter_block_location = BlockHandle::decode(&val).0;

            if filter_block_location.size() > 0 {
                let buf = read_bytes(&mut file, &filter_block_location)?;
                filter_block_reader = Some(FilterBlockReader::new_owned(fp, buf));
            }
        }

        metaindexblock.reset();

        Ok(Table {
            file,
            file_size: size,
            cmp,
            opt,
            footer,
            filters: filter_block_reader,
            indexblock,
        })
    }

    fn read_block_(&mut self, location: &BlockHandle) -> Result<BlockIter<C>> {
        read_block(&self.cmp, &mut self.file, location)
    }

    /// Returns the offset of the block that contains `key`.
    pub fn approx_offset_of(&self, key: &[u8]) -> usize {
        // cheap clone!
        let mut iter = self.indexblock.clone();

        iter.seek(key);

        if let Some((_, val)) = iter.current() {
            let location = BlockHandle::decode(&val).0;
            return location.offset();
        }

        self.footer.meta_index.offset()
    }

    // Iterators read from the file; thus only one iteratorcan be borrowed (mutably) per scope
    fn iter(&mut self) -> TableIterator<R, C, FP> {
        let mut iter = TableIterator {
            current_block: self.indexblock.clone(),
            index_block: self.indexblock.clone(),
            table: self,
        };
        iter.skip_to_next_entry();
        iter
    }
}

/// This iterator is a "TwoLevelIterator"; it uses an index block in order to get an offset hint
/// into data blocks.
pub struct TableIterator<'a, R: 'a + Read + Seek, C: 'a + Comparator, FP: 'a + FilterPolicy> {
    table: &'a mut Table<R, C, FP>,
    current_block: BlockIter<C>,
    index_block: BlockIter<C>,
}

impl<'a, C: Comparator, R: Read + Seek, FP: FilterPolicy> TableIterator<'a, R, C, FP> {
    // Skips to the entry referenced by the next index block.
    fn skip_to_next_entry(&mut self) -> bool {
        if let Some((_key, val)) = self.index_block.next() {
            let (new_block_h, _) = BlockHandle::decode(&val);
            if let Ok(block) = self.table.read_block_(&new_block_h) {
                self.current_block = block;
                true
            } else {
                false
            }
        } else {
            false
        }
    }
}

impl<'a, C: Comparator, R: Read + Seek, FP: FilterPolicy> Iterator for TableIterator<'a, R, C, FP> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key, val)) = self.current_block.next() {
            Some((key, val))
        } else if self.skip_to_next_entry() {
            self.next()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use table_builder::TableBuilder;

    use crate::{filter::BloomPolicy, types::StandardComparator};

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

    fn build_table() -> (Vec<u8>, usize) {
        let mut d = Vec::with_capacity(512);
        let opt = Options {
            block_restart_interval: 2,
            ..Default::default()
        };

        {
            let mut b = TableBuilder::new(opt, StandardComparator, &mut d, BloomPolicy::new(4));
            let data = build_data();

            for &(k, v) in data.iter() {
                b.add(k.as_bytes(), v.as_bytes());
            }

            b.finish();
        }

        let size = d.len();

        println!("Data: {:?}", d);

        (d, size)
    }

    #[test]
    #[ignore]
    fn test_table_iterator() {
        let (src, size) = build_table();
        let data = build_data();

        let mut table = Table::new(
            Cursor::new(&src as &[u8]),
            size,
            StandardComparator,
            BloomPolicy::new(4),
            Options::default(),
        )
        .unwrap();
        let iter = table.iter();

        for (i, (k, v)) in iter.enumerate() {
            assert_eq!(
                (data[i].0.as_bytes(), data[i].1.as_bytes()),
                (k.as_ref(), v.as_ref())
            );
        }
    }
}
