use std::{cmp::Ordering, io::Write};

use integer_encoding::FixedInt;

use crate::{
    block::{BlockBuilder, BlockContents},
    blockhandle::BlockHandle,
    filter::{FilterPolicy, NoFilterPolicy},
    filter_block::FilterBlockBuilder,
    options::{CompressionType, Options},
    Comparator,
};

pub const FOOTER_LENGTH: usize = 40;
pub const FULL_FOOTER_LENGTH: usize = FOOTER_LENGTH + 8;
pub const MAGIC_FOOTER_NUMBER: u64 = 0xdb4775248b80fb57;
pub const MAGIC_FOOTER_ENCODED: [u8; 8] = [0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb];

fn find_shortest_sep<C: Comparator>(c: &C, lo: &[u8], hi: &[u8]) -> Vec<u8> {
    let min = if lo.len() < hi.len() {
        lo.len()
    } else {
        hi.len()
    };

    let mut diff_at = 0;
    while diff_at < min && lo[diff_at] == hi[diff_at] {
        diff_at += 1;
    }

    if diff_at == min {
        return lo.to_vec();
    } else if lo[diff_at] < 0xff && lo[diff_at] + 1 < hi[diff_at] {
        let mut result = Vec::from(&lo[0..diff_at + 1]);
        result[diff_at] += 1;
        assert_eq!(c.cmp(&result, hi), Ordering::Less);
        return result;
    }

    lo.to_vec()
}

// Footer is a helper for encoding/decoding a table footer.
pub struct Footer {
    pub meta_index: BlockHandle,
    pub index: BlockHandle,
}

impl Footer {
    pub fn new(metaix: BlockHandle, index: BlockHandle) -> Footer {
        Footer {
            meta_index: metaix,
            index,
        }
    }

    pub fn decode(from: &[u8]) -> Footer {
        assert!(from.len() >= FULL_FOOTER_LENGTH);
        assert_eq!(&from[FOOTER_LENGTH..], &MAGIC_FOOTER_ENCODED);
        let (meta, metalen) = BlockHandle::decode(&from[0..]);
        let (ix, _) = BlockHandle::decode(&from[metalen..]);

        Footer {
            meta_index: meta,
            index: ix,
        }
    }

    pub fn encode(&self, to: &mut [u8]) {
        assert!(to.len() >= FULL_FOOTER_LENGTH);
        let s1 = self.meta_index.encode_to(to);
        let s2 = self.index.encode_to(&mut to[s1..]);

        for item in to.iter_mut().take(FOOTER_LENGTH).skip(s1 + s2) {
            *item = 0;
        }

        to[FOOTER_LENGTH..FULL_FOOTER_LENGTH]
            .copy_from_slice(&MAGIC_FOOTER_ENCODED[..(FULL_FOOTER_LENGTH - FOOTER_LENGTH)]);
    }
}

/// A table consists of DATA BLOCKS, METABLOCKS, a METAINDEXBLOCK and a FOOTER.
///
/// DATABLOCKS, METABLOCKS, INDEX BLOCK and METAINDEX BLOCK are built using the code in
/// the `block` module.
///
/// The FOOTER consists of a BlockHandle that points to the metaindex block, another pointing to
/// the index block, padding to fill up to 40 B and at the end the 8B magic number
/// 0xdb4775248b80fb57.

pub struct TableBuilder<'a, C: Comparator, Dst: Write, FilterPol: FilterPolicy> {
    o: Options,
    cmp: C,
    dst: Dst,

    offset: usize,
    num_entries: usize,
    prev_block_last_key: Vec<u8>,

    data_block: Option<BlockBuilder<C>>,
    index_block: Option<BlockBuilder<C>>,
    filter_block: Option<FilterBlockBuilder<'a, FilterPol>>,
}

impl<'a, C: Comparator, Dst: Write> TableBuilder<'a, C, Dst, NoFilterPolicy> {
    pub fn new_no_filter(
        opt: Options,
        cmp: C,
        dst: Dst,
    ) -> TableBuilder<'a, C, Dst, NoFilterPolicy> {
        TableBuilder {
            o: opt,
            cmp,
            dst,
            offset: 0,
            prev_block_last_key: vec![],
            num_entries: 0,
            data_block: Some(BlockBuilder::new(opt, cmp)),
            index_block: Some(BlockBuilder::new(opt, cmp)),
            filter_block: None,
        }
    }
}

impl<'a, C: Comparator, Dst: Write, FilterPol: FilterPolicy> TableBuilder<'a, C, Dst, FilterPol> {
    pub fn new(
        opt: Options,
        cmp: C,
        dst: Dst,
        fpol: FilterPol,
    ) -> TableBuilder<'a, C, Dst, FilterPol> {
        TableBuilder {
            o: opt,
            cmp,
            dst,
            offset: 0,
            prev_block_last_key: vec![],
            num_entries: 0,
            data_block: Some(BlockBuilder::new(opt, cmp)),
            index_block: Some(BlockBuilder::new(opt, cmp)),
            filter_block: Some(FilterBlockBuilder::new(fpol)),
        }
    }

    pub fn entries(&self) -> usize {
        self.num_entries
    }

    pub fn add(&mut self, key: &'a [u8], val: &'a [u8]) {
        assert!(self.data_block.is_some());
        assert!(
            self.num_entries == 0
                || self.cmp.cmp(key, &self.prev_block_last_key) == Ordering::Greater
        );

        if self.data_block.as_ref().unwrap().size_estimate() > self.o.block_size {
            self.write_data_block(key);
        }

        let dblock = &mut self.data_block.as_mut().unwrap();

        if let Some(ref mut fblock) = self.filter_block {
            fblock.add_key(key);
        }

        self.num_entries += 1;
        dblock.add(key, val);
    }

    /// Writes an index entry for the current data_block where `next_key` is the first key of the
    /// next block.
    fn write_data_block(&mut self, next_key: &[u8]) {
        assert!(self.data_block.is_some());

        let block = self.data_block.take().unwrap();
        let sep = find_shortest_sep::<C>(&self.cmp, block.last_key(), next_key);
        self.prev_block_last_key = block.last_key().to_vec();
        let contents = block.finish();

        let handle = BlockHandle::new(self.offset, contents.len());
        let mut handle_enc = [0u8; 16];
        let enc_len = handle.encode_to(&mut handle_enc);

        self.index_block
            .as_mut()
            .unwrap()
            .add(&sep, &handle_enc[0..enc_len]);
        self.data_block = Some(BlockBuilder::new(self.o, self.cmp));
        let ctype = self.o.compression_type;
        self.write_block(contents, ctype);

        if let Some(ref mut fblock) = self.filter_block {
            fblock.start_block(self.offset);
        }
    }

    fn write_block(&mut self, c: BlockContents, t: CompressionType) -> BlockHandle {
        // compression is still unimplemented
        assert_eq!(t, CompressionType::CompressionNone);

        let mut buf = [0u8; 4];
        let crc_alg = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);
        let mut digest = crc_alg.digest();
        digest.update(&c);
        digest.update(&[self.o.compression_type as u8]);

        digest.finalize().encode_fixed(&mut buf);

        // TODO: Handle errors here.
        self.dst.write_all(&buf).unwrap(); //crc32 checksum
        self.dst.write_all(&[t as u8; 1]).unwrap(); //compression type
        self.dst.write_all(&c).unwrap(); //block contents

        self.offset += c.len() + 1 + buf.len();

        BlockHandle::new(self.offset, c.len())
    }

    fn finish(mut self) {
        assert!(self.data_block.is_some());
        let ctype = self.o.compression_type;

        // If there's a pending data block, write that one
        if self.data_block.as_ref().unwrap().entries() > 0 {
            self.write_data_block(&[0xff_u8; 1]);
        }

        // Create metaindex block
        let mut meta_ix_block = BlockBuilder::new(self.o, self.cmp);
        if let Some(fblock) = self.filter_block.take() {
            let filter_key = format!("filter.{}", fblock.filter_name());
            let fblock_data = fblock.finish();
            let fblock_handle = self.write_block(fblock_data, CompressionType::CompressionNone);

            let mut handle_enc = [0u8; 16];
            let enc_len = fblock_handle.encode_to(&mut handle_enc);
            meta_ix_block.add(filter_key.as_bytes(), &handle_enc[0..enc_len]);
        }

        // write metaindex block
        let meta_ix_handle = self.write_block(meta_ix_block.finish(), ctype);

        // write index block
        let index_cont = self.index_block.take().unwrap().finish();
        let ix_handle = self.write_block(index_cont, ctype);

        // write footer
        let footer = Footer::new(meta_ix_handle, ix_handle);
        let mut buf = [0; FULL_FOOTER_LENGTH];
        footer.encode(&mut buf);
        self.offset += self.dst.write(buf.as_ref()).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::{filter::BloomPolicy, types::StandardComparator};

    use super::*;

    #[test]
    fn test_shortest_sep() {
        assert_eq!(
            find_shortest_sep::<StandardComparator>(
                &StandardComparator,
                "abcd".as_bytes(),
                "abcf".as_bytes()
            ),
            "abce".as_bytes()
        );
        assert_eq!(
            find_shortest_sep::<StandardComparator>(
                &StandardComparator,
                "abcdefghi".as_bytes(),
                "abcffghi".as_bytes()
            ),
            "abce".as_bytes()
        );
        assert_eq!(
            find_shortest_sep::<StandardComparator>(
                &StandardComparator,
                "a".as_bytes(),
                "a".as_bytes()
            ),
            "a".as_bytes()
        );
        assert_eq!(
            find_shortest_sep::<StandardComparator>(
                &StandardComparator,
                "a".as_bytes(),
                "b".as_bytes()
            ),
            "a".as_bytes()
        );
        assert_eq!(
            find_shortest_sep::<StandardComparator>(
                &StandardComparator,
                "abc".as_bytes(),
                "zzz".as_bytes()
            ),
            "b".as_bytes()
        );
        assert_eq!(
            find_shortest_sep::<StandardComparator>(
                &StandardComparator,
                "".as_bytes(),
                "".as_bytes()
            ),
            "".as_bytes()
        );
    }

    #[test]
    fn test_footer() {
        let f = Footer::new(BlockHandle::new(44, 4), BlockHandle::new(55, 5));

        let mut buf = [0; 48];
        f.encode(buf.as_mut());

        let f2 = Footer::decode(buf.as_ref());
        assert_eq!(f2.meta_index.offset(), 44);
        assert_eq!(f2.meta_index.size(), 4);
        assert_eq!(f2.index.offset(), 55);
        assert_eq!(f2.index.size(), 5);
    }

    #[test]
    fn test_table_builder() {
        let mut d = Vec::with_capacity(512);
        {
            let opt = Options {
                block_restart_interval: 3,
                ..Default::default()
            };
            let mut b = TableBuilder::new(opt, StandardComparator, &mut d, BloomPolicy::new(4));

            let data = [
                ("abc", "def"),
                ("abd", "dee"),
                ("bcd", "asa"),
                ("bsr", "a00"),
            ];

            for &(k, v) in data.iter() {
                b.add(k.as_bytes(), v.as_bytes());
            }

            b.finish();
        }
    }

    #[test]
    #[should_panic]
    fn test_bad_input() {
        let mut d = Vec::with_capacity(512);
        let opt = Options {
            block_restart_interval: 3,
            ..Default::default()
        };

        let mut b = TableBuilder::new(opt, StandardComparator, &mut d, BloomPolicy::new(4));

        // Test two equal consecutive keys
        let data = [
            ("abc", "def"),
            ("abc", "dee"),
            ("bcd", "asa"),
            ("bsr", "a00"),
        ];

        for &(k, v) in data.iter() {
            b.add(k.as_bytes(), v.as_bytes());
        }
    }
}
