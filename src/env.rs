//! An `env` is an abstraction layer that allows the database to run both on different platforms as
//! well as persisting data on disk or in memory.

use std::{
    collections::HashSet,
    fs,
    io::{Read, Result, Seek, Write},
    path::Path,
    sync::Mutex,
    thread, time,
};

pub trait Env {
    type SequentialReader: Read;
    type RandomReader: Read + Seek;
    type Writer: Write;

    fn open_sequential_file(&self, path: &Path) -> Result<Self::SequentialReader>;
    fn open_random_access_file(&self, path: &Path) -> Result<Self::RandomReader>;
    fn open_writable_file(&self, path: &Path) -> Result<Self::Writer>;
    fn open_appendable_file(&self, path: &Path) -> Result<Self::Writer>;

    fn exist(&self, path: &Path) -> Result<bool>;
    fn children(&self, dir: &Path) -> Result<Vec<String>>;
    fn size_of(&self, path: &Path) -> Result<usize>;

    fn delete_file(&self, path: &Path) -> Result<()>;
    fn mkdir(&self, dir: &Path) -> Result<()>;
    fn rmdir(&self, dir: &Path) -> Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    fn lock(&mut self, path: &Path) -> Result<FileLock>;
    fn unlock(&mut self, l: FileLock);

    fn new_logger(&self, path: &Path) -> Result<Logger>;

    fn micros(&self) -> u64;
    fn sleep_for(&self, micros: u32);
}

pub struct Logger {
    dst: fs::File,
}

impl Logger {
    fn log(&mut self, message: &String) {
        let _ = self.dst.write_all(message.as_bytes());
        let _ = self.dst.write_all(b"\n");
    }
}

pub struct FileLock {
    p: String,
    f: fs::File,
}

pub struct DiskPosixEnv {
    locks: Mutex<HashSet<String>>,
}

impl Env for DiskPosixEnv {
    type SequentialReader = fs::File;
    type RandomReader = fs::File;
    type Writer = fs::File;

    fn open_sequential_file(&self, path: &Path) -> Result<Self::SequentialReader> {
        fs::OpenOptions::new().read(true).open(path)
    }

    fn open_random_access_file(&self, path: &Path) -> Result<Self::RandomReader> {
        fs::OpenOptions::new().read(true).open(path)
    }

    fn open_writable_file(&self, path: &Path) -> Result<Self::Writer> {
        fs::OpenOptions::new().write(true).append(false).open(path)
    }

    fn open_appendable_file(&self, path: &Path) -> Result<Self::Writer> {
        fs::OpenOptions::new().append(true).open(path)
    }

    fn exist(&self, path: &Path) -> Result<bool> {
        Ok(path.exists())
    }

    fn children(&self, dir: &Path) -> Result<Vec<String>> {
        let dir_reader = fs::read_dir(dir)?;
        let filenames = dir_reader
            .map(|r| {
                if r.is_err() {
                    return String::new();
                }
                let direntry = r.unwrap();
                direntry.file_name().into_string().unwrap_or_default()
            })
            .filter(|s| !s.is_empty());

        Ok(filenames.collect())
    }

    fn size_of(&self, path: &Path) -> Result<usize> {
        let meta = fs::metadata(path)?;
        Ok(meta.len() as usize)
    }

    fn delete_file(&self, path: &Path) -> Result<()> {
        fs::remove_file(path)
    }

    fn mkdir(&self, dir: &Path) -> Result<()> {
        fs::create_dir(dir)
    }

    fn rmdir(&self, dir: &Path) -> Result<()> {
        fs::remove_dir(dir)
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        fs::rename(from, to)
    }

    fn lock(&mut self, _path: &Path) -> Result<FileLock> {
        todo!()
    }

    fn unlock(&mut self, _l: FileLock) {
        // let mut locks = self.locks.lock().unwrap();

        todo!()
    }

    fn new_logger(&self, p: &Path) -> Result<Logger> {
        self.open_appendable_file(p).map(|dst| Logger { dst })
    }

    fn micros(&self) -> u64 {
        loop {
            let now = time::SystemTime::now().duration_since(time::UNIX_EPOCH);

            match now {
                Err(_) => continue,
                Ok(dur) => return dur.as_secs() * 1000000 + dur.subsec_micros() as u64,
            }
        }
    }

    fn sleep_for(&self, micros: u32) {
        thread::sleep(time::Duration::new(0, micros * 1000));
    }
}
