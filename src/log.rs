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
}

impl<W: Write> LogWriter<W> {
    pub fn new(writer: W) -> LogWriter<W> {
        LogWriter {
            dst: writer,
            current_block_offset: 0,
        }
    }

    pub fn add_record(&mut self, r: &[u8]) -> io::Result<usize> {
        let mut record = r;
        let mut first_frag = true;
        let mut result = Ok(0);
        while result.is_ok() && !record.is_empty() {
            let space_left = BLOCK_SIZE - self.current_block_offset;

            // Fill up block; go to next block.
            if space_left < HEADER_SIZE {
                let _ = self.dst.write(&vec![0; space_left]);
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

    fn emit_record(&mut self, t: RecordType, data: &[u8], len: usize) -> io::Result<usize> {
        assert!(len < 256 * 256);

        const X25: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);
        let mut digest = X25.digest();
        let mut combined_data = vec![t as u8];
        combined_data.extend_from_slice(data);
        digest.update(&combined_data);

        let chksum = digest.finalize();

        let mut s = 0;
        s += self.dst.write(&chksum.encode_fixed_vec())?;
        s += self.dst.write(&(len as u16).to_le_bytes())?;
        s += self.dst.write(&[t as u8])?;
        s += self.dst.write(data)?;

        self.current_block_offset += s;
        Ok(s)
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
}
