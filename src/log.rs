//! A log consists of a number of blocks.
//! A block consists of a number of records, and an optional trailer (filler).
//! A record is a bytestring: [checksum: uint32, length: uint16, type: uint8, data: [u8]]
//! checksum is the crc32 sum of type and data; type is one of RecordType::{Full/First/Middle/Last}

use std::io::{Error, ErrorKind, Read, Result, Write};

use crc::{crc32, Hasher32};
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
    digest: crc32::Digest,
    current_block_offset: usize,
    block_size: usize,
}

impl<W: Write> LogWriter<W> {
    pub fn new(writer: W) -> LogWriter<W> {
        LogWriter {
            dst: writer,
            current_block_offset: 0,
            block_size: BLOCK_SIZE,
            digest: crc32::Digest::new(crc32::CASTAGNOLI),
        }
    }

    pub fn add_record(&mut self, r: &[u8]) -> Result<usize> {
        let mut record = r;
        let mut first_frag = true;
        let mut result = Ok(0);
        while result.is_ok() && !record.is_empty() {
            assert!(self.block_size > HEADER_SIZE);
            let space_left = self.block_size - self.current_block_offset;

            // Fill up block; go to the next block;
            if space_left < HEADER_SIZE {
                let _ = self.dst.write(&vec![0, 0, 0, 0, 0, 0][0..space_left]);
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

    fn emit_record(&mut self, t: RecordType, data: &[u8], len: usize) -> Result<usize> {
        assert!(len < 256 * 256);

        self.digest.reset();
        self.digest.write(&[t as u8]);
        self.digest.write(&data[0..len]);

        let chksum = self.digest.sum32();

        let mut s = 0;

        s += self.dst.write(&chksum.encode_fixed_vec()).unwrap();
        s += self.dst.write(&(len as u16).encode_fixed_vec()).unwrap();
        s += self.dst.write(&[t as u8]).unwrap();
        s += self.dst.write(&data[0..len]).unwrap();

        self.current_block_offset += s;
        Ok(s)
    }
}

pub struct LogReader<R: Read> {
    src: R,
    blk_off: usize,
    blocksize: usize,
    checksums: bool,
    digest: crc32::Digest,
    head_scratch: [u8; 7],
}

impl<R: Read> LogReader<R> {
    pub fn new(src: R, chksum: bool, offset: usize) -> LogReader<R> {
        LogReader {
            src,
            blk_off: offset,
            blocksize: BLOCK_SIZE,
            checksums: chksum,
            digest: crc32::Digest::new(crc32::CASTAGNOLI),
            head_scratch: [0; 7],
        }
    }

    // EOF is signalled by Ok(0)
    pub fn read(&mut self, dst: &mut Vec<u8>) -> Result<usize> {
        let mut checksum: u32;
        let mut length: u16;
        let mut typ: u8;
        let mut dst_offset: usize = 0;

        dst.clear();

        loop {
            if self.blocksize - self.blk_off < HEADER_SIZE {
                //skip to next block
                let _ = self
                    .src
                    .read(&mut self.head_scratch[0..self.blocksize - self.blk_off]);

                self.blk_off = 0;
            }

            let mut bytes_read = self.src.read(&mut self.head_scratch).unwrap();

            // EOF
            if bytes_read == 0 {
                return Ok(0);
            }

            self.blk_off += bytes_read;

            checksum = u32::decode_fixed(&self.head_scratch[0..4]);
            length = u16::decode_fixed(&self.head_scratch[4..6]);
            typ = self.head_scratch[6];

            dst.resize(dst_offset + length as usize, 0);
            bytes_read = self
                .src
                .read(&mut dst[dst_offset..dst_offset + length as usize])
                .unwrap();

            self.blk_off += bytes_read;

            if self.checksums
                && !self.check_integerity(typ, &dst[dst_offset..dst_offset + bytes_read], checksum)
            {
                return Err(Error::new(
                    ErrorKind::InvalidData,
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

    fn check_integerity(&mut self, typ: u8, data: &[u8], expected: u32) -> bool {
        self.digest.reset();
        self.digest.write(&[typ]);
        self.digest.write(data);
        expected == self.digest.sum32()
    }
}

#[cfg(test)]
mod tests {
    use crate::log::{LogReader, HEADER_SIZE};

    use super::LogWriter;

    #[test]
    fn test_writer() {
        let data = "hello world, My first Log entry".as_bytes();
        let mut lw = LogWriter::new(Vec::new());

        let _ = lw.add_record(data);

        assert_eq!(lw.current_block_offset, data.len() + HEADER_SIZE);
        assert_eq!(&lw.dst[HEADER_SIZE..], data)
    }

    #[test]
    fn test_reader() {
        let data = vec![
            "abcdefghi".as_bytes().to_vec(),              // fits one block of 17
            "123456789012".as_bytes().to_vec(),           // spans two blocks of 17
            "0101010101010101010101".as_bytes().to_vec(), // spans three blocks of 17
        ];
        let mut lw = LogWriter::new(Vec::new());
        lw.block_size = HEADER_SIZE + 10;

        for e in data.iter() {
            assert!(lw.add_record(e).is_ok());
        }

        assert_eq!(lw.dst.len(), 93);

        let mut lr = LogReader::new(lw.dst.as_slice(), true, 0);
        lr.blocksize = super::HEADER_SIZE + 10;
        let mut dst = Vec::with_capacity(128);
        let mut i = 0;

        loop {
            let r = lr.read(&mut dst);

            if r.is_err() {
                panic!("{}", r.unwrap_err());
            } else if r.unwrap() == 0 {
                break;
            }

            assert_eq!(dst, data[i]);
            i += 1;
        }

        assert_eq!(i, data.len());
    }
}
