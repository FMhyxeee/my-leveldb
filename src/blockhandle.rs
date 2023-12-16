use integer_encoding::VarInt;

#[derive(Debug, Clone)]
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
        let hb = BlockHandle::new(890, 777);
        let mut dst = [0u8; 128];

        let enc_sz = hb.encode_to(&mut dst[..]);

        let (hb2, dec_sz) = BlockHandle::decode(&dst);

        assert_eq!(enc_sz, dec_sz);
        assert_eq!(hb.size(), hb2.size());
        assert_eq!(hb.offset(), hb2.offset());
    }
}
