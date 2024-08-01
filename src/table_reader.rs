use std::io::{Read, Result, Seek, SeekFrom};

use crate::{
    block::Block, blockhandle::BlockHandle, filter::FilterPolicy, filter_block::FilterBlockReader,
    options::Options, table_builder, Comparator,
};

struct TableFooter {
    pub mataindex: BlockHandle,
    pub index: BlockHandle,
}

impl TableFooter {
    fn parse(footer: &[u8]) -> TableFooter {
        assert_eq!(footer.len(), table_builder::FULL_FOOTER_LENGTH);
        assert_eq!(
            &footer[footer.len() - 8..],
            table_builder::MAGIC_FOOTER_ENCODED
        );

        let (mi, n1) = BlockHandle::decode(footer);
        let (ix, _) = BlockHandle::decode(&footer[n1..]);

        TableFooter {
            mataindex: mi,
            index: ix,
        }
    }
}

pub struct Table<R: Read + Seek, C: Comparator, FP: FilterPolicy> {
    file: R,
    file_size: usize,
    opt: Options,
    footer: TableFooter,
    filters: Option<FilterBlockReader<FP>>,
    indexblock: Block<C>,
    c: C,
}

impl<R: Read + Seek, C: Comparator, FP: FilterPolicy> Table<R, C, FP> {
    pub fn new(mut file: R, size: usize, cmp: C, fp: FP, opt: Options) -> Result<Table<R, C, FP>> {
        let footer = Table::<R, C, FP>::read_footer(&mut file, size)?;

        let indexblock = Table::<R, C, FP>::read_block(&mut file, &footer.index)?;
        let metaindexblock = Table::<R, C, FP>::read_block(&mut file, &footer.mataindex)?;

        let mut filter_block_reader = None;
        let mut filter_block_location = BlockHandle::new(0, 0);
        let mut filter_name = "filter.".as_bytes().to_vec();
        filter_name.extend_from_slice(fp.name().as_bytes());

        for (key, val) in metaindexblock.iter() {
            if key == filter_name {
                filter_block_location = BlockHandle::decode(val).0;
                break;
            }
        }

        if filter_block_location.size() > 0 {
            let filter_block = Table::<R, C, FP>::read_block(&mut file, &filter_block_location)?;
            filter_block_reader = Some(FilterBlockReader::new(fp, filter_block.obtain()));
        }

        Ok(Table {
            file,
            file_size: size,
            c: cmp,
            opt,
            footer,
            filters: filter_block_reader,
            indexblock,
        })
    }

    fn read_footer(f: &mut R, size: usize) -> Result<TableFooter> {
        f.seek(SeekFrom::Start(
            (size - table_builder::FULL_FOOTER_LENGTH) as u64,
        ))?;
        let mut buf = [0; table_builder::FULL_FOOTER_LENGTH];
        f.read_exact(&mut buf)?;
        Ok(TableFooter::parse(&buf))
    }

    fn read_block(f: &mut R, location: &BlockHandle) -> Result<Block<C>> {
        f.seek(SeekFrom::Start(location.offset() as u64))?;
        let mut buf = vec![0; location.size()];
        f.read_exact(&mut buf[0..location.size()])?;
        Ok(Block::new_with_cmp(buf, C::default()))
    }

    pub fn approx_offset_of(&self, _key: &[u8]) -> usize {
        unimplemented!()
    }
}
