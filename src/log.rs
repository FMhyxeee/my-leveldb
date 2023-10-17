#![allow(dead_code)]

//! A log consists of a number of blocks.
//! A block consists of a number of records, and an optional trailer (filler).
//! A record is a bytestring: [checksum: uint32, length: uint16, type: uint8, data: [u8]]
//! checksum is the crc32 sum of type and data; type is one of RecordType::{Full/First/Middle/Last}

use std::io::{Result, Write};

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
    current_block_offset: usize,
}

impl<W: Write> LogWriter<W> {
    pub fn new(writer: W) -> LogWriter<W> {
        LogWriter {
            dst: writer,
            current_block_offset: 0,
        }
    }

    pub fn add_record(&mut self, r: &[u8]) -> Result<usize> {
        let mut record = r;
        let mut first_frag = true;
        let mut result = Ok(0);
        while result.is_ok() && !record.is_empty() {
            let space_left = BLOCK_SIZE - self.current_block_offset;
            // Fill up block; go to the next block;
            if space_left < HEADER_SIZE {
                let _ = self.dst.write(&vec![0, 0, 0, 0, 0, 0][0..space_left]);
                self.current_block_offset = 0;
            }

            let avail_for_data = BLOCK_SIZE - self.current_block_offset - HEADER_SIZE;

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

        let mut digest = crc32::Digest::new(crc32::CASTAGNOLI);
        digest.write(&[t as u8]);
        digest.write(data);

        let chksum = digest.sum32();

        let mut s = 0;

        s += self.dst.write(&chksum.encode_fixed_vec()).unwrap();
        s += self.dst.write(&(len as u16).encode_fixed_vec()).unwrap();
        s += self.dst.write(&[t as u8]).unwrap();
        s += self.dst.write(data).unwrap();

        self.current_block_offset += s;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use crate::log::HEADER_SIZE;

    use super::LogWriter;

    #[test]
    fn test_writer() {
        let data = "hello world, My first Log entry".as_bytes();
        let mut lw = LogWriter::new(Vec::new());

        let _ = lw.add_record(data);

        assert_eq!(lw.current_block_offset, data.len() + HEADER_SIZE);

        assert_eq!(&lw.dst[HEADER_SIZE..], data)
    }
}
