use std::{collections::HashSet, fs, io::Result, path::Path, sync::Mutex, thread, time};

use crate::env::{Env, Logger};

pub struct DiskFileLock {
    p: String,
    f: fs::File,
}

pub struct PosixDiskEnv {
    locks: Mutex<HashSet<String>>,
}

impl PosixDiskEnv {
    pub fn new() -> PosixDiskEnv {
        PosixDiskEnv {
            locks: Mutex::new(HashSet::new()),
        }
    }
}

impl Env for PosixDiskEnv {
    type SequentialReader = fs::File;
    type RandomReader = fs::File;
    type Writer = fs::File;
    type FileLock = DiskFileLock;

    fn open_sequential_file(&self, path: &Path) -> Result<Self::SequentialReader> {
        fs::OpenOptions::new().read(true).open(path)
    }

    fn open_random_access_file(&self, path: &Path) -> Result<Self::RandomReader> {
        fs::OpenOptions::new().read(true).open(path)
    }

    fn open_writable_file(&self, path: &Path) -> Result<Self::Writer> {
        fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .append(false)
            .open(path)
    }

    fn open_appendable_file(&self, path: &Path) -> Result<Self::Writer> {
        fs::OpenOptions::new().create(true).append(true).open(path)
    }

    fn exists(&self, path: &Path) -> Result<bool> {
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

    fn delete(&self, path: &Path) -> Result<()> {
        fs::remove_file(path)
    }

    fn mkdir(&self, dir: &Path) -> Result<()> {
        fs::create_dir(dir)
    }

    fn rmdir(&self, dir: &Path) -> Result<()> {
        fs::remove_dir_all(dir)
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        fs::rename(from, to)
    }

    fn lock(&self, _path: &Path) -> Result<Self::FileLock> {
        todo!()
    }

    fn unlock(&self, _l: Self::FileLock) {
        // let mut locks = self.locks.lock().unwrap();

        todo!()
    }

    fn new_logger(&self, p: &Path) -> Result<Logger> {
        self.open_appendable_file(p)
            .map(|dst| Logger::new(Box::new(dst)))
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::convert::AsRef;
    use std::io::Write;
    use std::iter::FromIterator;

    #[test]
    fn test_files() {
        let n = "testfile.xyz".to_string();
        let name = n.as_ref();
        let env = PosixDiskEnv::new();

        assert!(env.open_appendable_file(name).is_ok());
        assert!(env.exists(name).unwrap_or(false));
        assert_eq!(env.size_of(name).unwrap_or(1), 0);
        assert!(env.delete(name).is_ok());

        assert!(env.open_writable_file(name).is_ok());
        assert!(env.exists(name).unwrap_or(false));
        assert_eq!(env.size_of(name).unwrap_or(1), 0);
        assert!(env.delete(name).is_ok());

        {
            let mut f = env.open_writable_file(name).unwrap();
            let _ = f.write("123xyz".as_bytes());
            assert_eq!(env.size_of(name).unwrap_or(0), 6);
        }

        assert!(env.open_sequential_file(name).is_ok());
        assert!(env.open_random_access_file(name).is_ok());

        assert!(env.delete(name).is_ok());
    }

    #[test]
    #[ignore]
    fn test_locking() {
        let env = PosixDiskEnv::new();
        let n = "testfile.123".to_string();
        let name = n.as_ref();

        {
            let mut f = env.open_writable_file(name).unwrap();
            let _ = f.write("123xyz".as_bytes());
            assert_eq!(env.size_of(name).unwrap_or(0), 6);
        }

        {
            let r = env.lock(name);
            assert!(r.is_ok());
            env.unlock(r.unwrap());
        }

        {
            let r = env.lock(name);
            assert!(r.is_ok());
            let s = env.lock(name);
            assert!(s.is_err());
            env.unlock(r.unwrap());
        }

        assert!(env.delete(name).is_ok());
    }

    #[test]
    fn test_dirs() {
        let d = "subdir/";
        let dirname = d.as_ref();
        let env = PosixDiskEnv::new();

        assert!(env.mkdir(dirname).is_ok());
        assert!(env
            .open_writable_file(
                String::from_iter(vec![d.to_string(), "f1.txt".to_string()].into_iter()).as_ref()
            )
            .is_ok());
        assert_eq!(env.children(dirname).unwrap().len(), 1);
        assert!(env.rmdir(dirname).is_ok());
    }
}
