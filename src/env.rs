//! An `env` is an abstraction layer that allows the database to run both on different platforms as
//! well as persisting data on disk or in memory.

use std::{
    io::{Read, Result, Seek, Write},
    path::Path,
};

pub trait Env {
    type SequentialReader: Read;
    type RandomReader: Read + Seek;
    type Writer: Write;
    type FileLock;

    fn open_sequential_file(&self, path: &Path) -> Result<Self::SequentialReader>;
    fn open_random_access_file(&self, path: &Path) -> Result<Self::RandomReader>;
    fn open_writable_file(&self, path: &Path) -> Result<Self::Writer>;
    fn open_appendable_file(&self, path: &Path) -> Result<Self::Writer>;

    fn exists(&self, path: &Path) -> Result<bool>;
    fn children(&self, dir: &Path) -> Result<Vec<String>>;
    fn size_of(&self, path: &Path) -> Result<usize>;

    fn delete(&self, path: &Path) -> Result<()>;
    fn mkdir(&self, dir: &Path) -> Result<()>;
    fn rmdir(&self, dir: &Path) -> Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    fn lock(&self, path: &Path) -> Result<Self::FileLock>;
    fn unlock(&self, l: Self::FileLock);

    fn new_logger(&self, path: &Path) -> Result<Logger>;

    fn micros(&self) -> u64;
    fn sleep_for(&self, micros: u32);
}

pub struct Logger {
    dst: Box<dyn Write>,
}

impl Logger {
    pub fn new(writer: Box<dyn Write>) -> Logger {
        Logger { dst: writer }
    }

    pub fn log(&mut self, message: &String) {
        let _ = self.dst.write_all(message.as_bytes());
        let _ = self.dst.write_all(b"\n");
    }
}
