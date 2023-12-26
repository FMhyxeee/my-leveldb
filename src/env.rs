//! An `env` is an abstraction layer that allows the database to run both on different platforms as
//! well as persisting data on disk or in memory.

use crate::error::{from_io_result, Result};

use std::fs::File;
use std::io::{Read, Write};
use std::os::windows::fs::FileExt;
use std::path::Path;

pub trait RandomAccess {
    fn read_at(&self, off: usize, dst: &mut [u8]) -> Result<usize>;
}

impl RandomAccess for File {
    fn read_at(&self, off: usize, dst: &mut [u8]) -> Result<usize> {
        from_io_result((self as &dyn FileExt).seek_read(dst, off as u64))
    }
}

/// BufferBackedFile is a simple type implementing RandomAccess on a Vec<u8>.
pub type BufferBackedFile = Vec<u8>;

impl RandomAccess for BufferBackedFile {
    fn read_at(&self, off: usize, dst: &mut [u8]) -> Result<usize> {
        if off > self.len() {
            return Ok(0);
        }
        let remaining = self.len() - off;
        let to_read = if dst.len() > remaining {
            remaining
        } else {
            dst.len()
        };
        (&mut dst[0..to_read]).copy_from_slice(&self[off..off + to_read]);
        Ok(to_read)
    }
}

pub struct FileLock {
    pub id: String,
}

pub trait Env {
    fn open_sequential_file(&self, _: &Path) -> Result<Box<dyn Read>>;
    fn open_random_access_file(&self, _: &Path) -> Result<Box<dyn RandomAccess>>;
    fn open_writable_file(&self, _: &Path) -> Result<Box<dyn Write>>;
    fn open_appendable_file(&self, _: &Path) -> Result<Box<dyn Write>>;

    fn exists(&self, _: &Path) -> Result<bool>;
    fn children(&self, _: &Path) -> Result<Vec<String>>;
    fn size_of(&self, _: &Path) -> Result<usize>;

    fn delete(&self, _: &Path) -> Result<()>;
    fn mkdir(&self, _: &Path) -> Result<()>;
    fn rmdir(&self, _: &Path) -> Result<()>;
    fn rename(&self, _: &Path, _: &Path) -> Result<()>;

    fn lock(&self, _: &Path) -> Result<FileLock>;
    fn unlock(&self, l: FileLock) -> Result<()>;

    fn new_logger(&self, _: &Path) -> Result<Logger>;

    fn micros(&self) -> u64;
    fn sleep_for(&self, micros: u32);
}

pub struct Logger {
    dst: Box<dyn Write>,
}

impl Logger {
    pub fn new(w: Box<dyn Write>) -> Logger {
        Logger { dst: w }
    }

    pub fn log(&mut self, message: &str) {
        let _ = self.dst.write(message.as_bytes());
        let _ = self.dst.write("\n".as_bytes());
    }
}

pub fn path_to_string(p: &Path) -> String {
    p.to_str().map(String::from).unwrap()
}

pub fn path_to_str(p: &Path) -> &str {
    p.to_str().unwrap()
}
