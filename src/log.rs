use std::{
    io::{self, Write},
    vec,
};

use integer_encoding::FixedInt;

const BLOCK_SIZE: usize = 32 * 1024;
const HEADER_SIZE: usize = 4 + 2 + 1;

#[derive(Clone, Copy)]
pub enum RecordType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

pub struct LogWriter<W: Write> {
    dst: W,
    current_block_offset: usize,
    block_size: usize,
    crc_alg: crc::Crc<u32>,
}

impl<W: Write> LogWriter<W> {
    pub fn new(writer: W) -> LogWriter<W> {
        let crc_alg = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);
        LogWriter {
            dst: writer,
            current_block_offset: 0,
            block_size: BLOCK_SIZE,
            crc_alg,
        }
    }

    pub fn add_record(&mut self, r: &[u8]) -> io::Result<usize> {
        let mut record = r;
        let mut first_frag = true;
        let mut result = Ok(0);
        while result.is_ok() && !record.is_empty() {
            assert!(self.block_size > HEADER_SIZE);
            let space_left = self.block_size - self.current_block_offset;

            // Fill up block; go to next block.
            if space_left < HEADER_SIZE {
                let _ = self.dst.write(&vec![0; space_left]);
                self.current_block_offset = 0;
            }

            let avail_for_data = self.block_size - self.current_block_offset - HEADER_SIZE;

            let data_frag_len = if record.len() < avail_for_data {
                record.len()
            } else {
                avail_for_data
            };

            let recordtype;

            if first_frag && data_frag_len == record.len() {
                recordtype = RecordType::Full;
            } else if first_frag {
                recordtype = RecordType::First;
            } else if data_frag_len == record.len() {
                recordtype = RecordType::Last;
            } else {
                recordtype = RecordType::Middle;
            }

            result = self.emit_record(recordtype, record, data_frag_len);
            record = &record[data_frag_len..];
            first_frag = false;
        }
        result
    }

    fn emit_record(&mut self, t: RecordType, data: &[u8], len: usize) -> io::Result<usize> {
        assert!(len < 256 * 256);

        let mut digest = self.crc_alg.digest();
        let mut combined_data = vec![t as u8];
        combined_data.extend_from_slice(data);
        digest.update(&combined_data);

        let chksum = digest.finalize();

        let mut s = 0;
        s += self.dst.write(&chksum.encode_fixed_vec())?;
        s += self.dst.write(&(len as u16).to_le_bytes())?;
        s += self.dst.write(&[t as u8])?;
        s += self.dst.write(&data[0..len])?;

        self.current_block_offset += s;
        Ok(s)
    }
}

pub struct LogReader<R: io::Read> {
    src: R,
    blk_off: usize,
    blocksize: usize,
    checksums: bool,

    crc_alg: crc::Crc<u32>,
    head_scratch: [u8; HEADER_SIZE],
}

impl<R: io::Read> LogReader<R> {
    pub fn new(src: R, checksums: bool, offset: usize) -> LogReader<R> {
        let crc_alg = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);
        LogReader {
            src,
            blk_off: offset,
            blocksize: BLOCK_SIZE,
            checksums,
            crc_alg,
            head_scratch: [0; HEADER_SIZE],
        }
    }

    /// EOF is signalled by Ok(0)
    pub fn read(&mut self, dst: &mut Vec<u8>) -> io::Result<usize> {
        let mut checksum: u32;
        let mut length: u16;
        let mut typ: u8;

        let mut dst_offset: usize = 0;

        dst.clear();

        loop {
            if self.blocksize - self.blk_off < HEADER_SIZE {
                // skip to next block
                self.src
                    .read_exact(&mut self.head_scratch[0..self.blocksize - self.blk_off])?;
                self.blk_off = 0;
            }

            let mut bytes_read = self.src.read(&mut self.head_scratch)?;

            // EOF
            if bytes_read == 0 {
                return Ok(0);
            }

            self.blk_off += bytes_read;

            checksum = u32::decode_fixed(&self.head_scratch[0..4]).unwrap();
            length = u16::decode_fixed(&self.head_scratch[4..6]).unwrap();
            typ = self.head_scratch[6];

            dst.resize(dst_offset + length as usize, 0);

            bytes_read = self
                .src
                .read(&mut dst[dst_offset..dst_offset + length as usize])?;
            dst_offset += bytes_read;

            if self.checksums
                && !self.check_integrity(typ, &dst[dst_offset..dst_offset + bytes_read], checksum)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid Checksum".to_string(),
                ));
            }

            dst_offset += length as usize;

            if typ == RecordType::Full as u8 {
                return Ok(dst_offset);
            } else if typ == RecordType::First as u8 || typ == RecordType::Middle as u8 {
                continue;
            } else if typ == RecordType::Last as u8 {
                return Ok(dst_offset);
            }
        }
    }

    fn check_integrity(&mut self, typ: u8, data: &[u8], checksum: u32) -> bool {
        let mut digest = self.crc_alg.digest();
        let mut combined_data = vec![typ];
        combined_data.extend_from_slice(data);
        digest.update(&combined_data);

        let chksum = digest.finalize();

        checksum == chksum
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writer() {
        let data = b"First Log";
        let mut lw = LogWriter::new(Vec::new());

        let cksum = lw.add_record(&data[..]);
        assert!(cksum.is_ok());

        assert_eq!(lw.current_block_offset, data.len() + super::HEADER_SIZE);
        assert_eq!(&lw.dst[super::HEADER_SIZE..], data.as_slice());
        println!("{:?}", lw.dst);
    }

    #[test]
    fn test_reader() {
        let data = [
            "abcdefghi".as_bytes().to_vec(),    // fits one block of 17
            "123456789012".as_bytes().to_vec(), // spans two blocks of 17
            "0101010101010101010101".as_bytes().to_vec(),
        ]; // spans three blocks of 17
        let mut lw = LogWriter::new(Vec::new());
        lw.block_size = super::HEADER_SIZE + 10;

        for e in data.iter() {
            assert!(lw.add_record(e).is_ok());
        }

        assert_eq!(lw.dst.len(), 93);

        println!("{:?}", lw.dst);

        // let mut lr = LogReader::new(lw.dst.as_slice(), true, 0);
        // lr.blocksize = super::HEADER_SIZE + 10;
        // let mut dst = Vec::with_capacity(128);
        // let mut i = 0;

        // loop {
        //     let r = lr.read(&mut dst);

        //     if !r.is_ok() {
        //         panic!("{}", r.unwrap_err());
        //     } else if r.unwrap() == 0 {
        //         break;
        //     }

        //     assert_eq!(dst, data[i]);
        //     i += 1;
        // }
        // assert_eq!(i, data.len());
    }
}
