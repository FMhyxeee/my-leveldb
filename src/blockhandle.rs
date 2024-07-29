use integer_encoding::VarInt;

pub struct BlockHandle {
    offset: usize,
    size: usize,
}

impl BlockHandle {
    /// Decodes a block handle from `from` and returns a block handle.
    /// together with how many bytes were read from the slice.

    pub fn decode(from: &[u8]) -> (BlockHandle, usize) {
        let (off, offset) = usize::decode_var(from).unwrap();
        let (sz, size) = usize::decode_var(&from[offset..]).unwrap();
        (
            BlockHandle {
                offset: off,
                size: sz,
            },
            offset + size,
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

    /// Returns how many bytes were written, or 0 if the write failed because `dst` was too small.
    pub fn encode_to(&self, dst: &mut [u8]) -> usize {
        assert!(dst.len() >= self.offset.required_space() + self.size.required_space());

        let off = self.offset.encode_var(dst);
        off + self.size.encode_var(&mut dst[off..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blockhandle() {
        let bh = BlockHandle::new(890, 777);
        let mut dst = [0u8; 128];
        let enc_sz = bh.encode_to(&mut dst[..]);

        let (bh2, dec_sz) = BlockHandle::decode(&dst);
        // println!("{:?}", dst);

        assert_eq!(enc_sz, dec_sz);
        assert_eq!(bh.size(), bh2.size());
        assert_eq!(bh.offset(), bh2.offset());
    }
}
