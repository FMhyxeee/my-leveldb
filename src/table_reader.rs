use std::io::{Read, Result, Seek, SeekFrom};

use crate::{
    blockhandle::BlockHandle, filter::FilterPolicy, filter_block::FilterBlockReader,
    options::Options, table_builder, Comparator,
};

struct TableFooter {
    mataindex: BlockHandle,
    index: BlockHandle,
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
    c: C,
    filters: FilterBlockReader<FP>,
}

impl<R: Read + Seek, C: Comparator, FP: FilterPolicy> Table<R, C, FP> {
    pub fn new(mut file: R, size: usize, cmp: C, fp: FP, opt: Options) -> Table<R, C, FP> {
        let indexblock = Table::<R, C, FP>::read_index_block(&mut file, size);
        let fblockreader = FilterBlockReader::new(fp, indexblock);
        Table {
            file,
            file_size: size,
            c: cmp,
            opt,
            filters: fblockreader,
        }
    }

    fn read_footer(f: &mut R, size: usize) -> Result<TableFooter> {
        f.seek(SeekFrom::Start(
            (size - table_builder::FULL_FOOTER_LENGTH) as u64,
        ))?;
        let mut buf = [0; table_builder::FULL_FOOTER_LENGTH];
        f.read_exact(&mut buf)?;
        Ok(TableFooter::parse(&buf))
    }

    fn read_index_block(_f: &mut R, _size: usize) -> Vec<u8> {
        unimplemented!()
    }

    pub fn approx_offset_of(&self, _key: &[u8]) -> usize {
        unimplemented!()
    }
}
