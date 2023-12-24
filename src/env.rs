//! An `env` is an abstraction layer that allows the database to run both on different platforms as
//! well as persisting data on disk or in memory.

use crate::error::{from_io_result, from_lock_result, Result};

use std::io::{self, prelude::*, Cursor};
use std::path::Path;
use std::sync::Mutex;
use std::{fs::File, sync::Arc};

pub trait RandomAccess: Read + Seek {}
impl RandomAccess for File {}
impl<T: AsRef<[u8]>> RandomAccess for Cursor<T> {}

/// RandomAccessFile dynamically wraps a type implementing read and seek to enable atomic random
/// reads.
#[derive(Clone)]
pub struct RandomAccessFile {
    f: Arc<Mutex<Box<dyn RandomAccess>>>,
}

impl RandomAccessFile {
    pub fn new(f: Box<dyn RandomAccess>) -> RandomAccessFile {
        RandomAccessFile {
            f: Arc::new(Mutex::new(f)),
        }
    }

    pub fn read_at(&self, off: usize, len: usize) -> Result<Vec<u8>> {
        let mut f = from_lock_result(self.f.lock()).unwrap();
        from_io_result(f.seek(io::SeekFrom::Start(off as u64))).unwrap();

        let mut buf = Vec::new();
        buf.resize(len, 0);
        from_io_result(f.read_exact(&mut buf)).map(|_| buf)
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
