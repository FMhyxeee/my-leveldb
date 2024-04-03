use std::{io, rc::Rc};

use crate::{
    block::Block,
    cache::Cache,
    cmp::{Cmp, DefaultCmp},
    compressor::{self, Compressor, CompressorId},
    disk_env::PosixDiskEnv,
    env::Env,
    error::StatusCode,
    filter::{self, BoxedFilterPolicy},
    infolog::{self, Logger},
    mem_env::MemEnv,
    types::{share, Shared},
    Result, Status,
};

const KB: usize = 1 << 10;
const MB: usize = 1 << 20;

const BLOCK_MAX_SIZE: usize = 4 * KB;
const BLOCK_CACHE_CAPACITY: usize = 8 * MB;
const WRITE_BUFFER_SIZE: usize = 4 * MB;
const DEFAULT_BITS_PER_KEY: u32 = 10; // NOTE: This may need to be optimized.

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum CompressionType {
    CompressionNone = 0,
    CompressionSnappy = 1,
}

pub fn int_to_compressiontype(i: u32) -> Option<CompressionType> {
    match i {
        0 => Some(CompressionType::CompressionNone),
        1 => Some(CompressionType::CompressionSnappy),
        _ => None,
    }
}

/// Options contains general parameters for a LevelDB instance. Most of the names are
/// self-explanatory; the defaults are defined in the `Default` implementation.
///
/// Note: Compression is not yet implemented.
#[derive(Clone)]
pub struct Options {
    pub cmp: Rc<Box<dyn Cmp>>,
    pub env: Rc<Box<dyn Env>>,
    pub log: Shared<Logger>,
    pub create_if_missing: bool,
    pub error_if_exists: bool,
    pub paranoid_checks: bool,
    pub write_buffer_size: usize,
    pub max_open_file: usize,
    pub max_file_size: usize,
    pub block_cache: Shared<Cache<Block>>,
    pub block_size: usize,
    pub block_restart_interval: usize,
    /// Compressor id in compressor list
    ///
    /// Note: you have to open a database with the same compression type as it was written to, in otder
    /// to not lose data! (this is a bug and will be fixed)
    pub compressor: u8,

    pub compressor_list: Rc<CompressorList>,
    pub reuse_logs: bool,
    pub reuse_manifest: bool,
    pub filter_policy: BoxedFilterPolicy,
}

#[cfg(feature = "fs")]
type DefaultEnv = crate::disk_env::PosixDiskEnv;

#[cfg(not(feature = "fs"))]
type DefaultEnv = crate::mem_env::MemEnv;

impl Default for Options {
    fn default() -> Self {
        Self {
            cmp: Rc::new(Box::new(DefaultCmp)),
            env: Rc::new(Box::new(PosixDiskEnv::new())),
            log: share(Logger(Box::new(io::sink()))),
            create_if_missing: true,
            error_if_exists: false,
            paranoid_checks: false,
            write_buffer_size: WRITE_BUFFER_SIZE,
            max_open_file: 1 << 10,
            max_file_size: 2 << 20,
            // 2000 elements by default
            block_cache: share(Cache::new(BLOCK_CACHE_CAPACITY / BLOCK_MAX_SIZE)),
            block_size: BLOCK_MAX_SIZE,
            block_restart_interval: 16,
            reuse_logs: true,
            reuse_manifest: true,
            compressor: 0,
            compressor_list: Rc::new(CompressorList::default()),
            filter_policy: Rc::new(Box::new(filter::BloomPolicy::new(DEFAULT_BITS_PER_KEY))),
        }
    }
}

/// Customize compressor method for leveldb
///
/// `Default` value is like the code below
/// ```
/// # use my_leveldb::{compressor, CompressorList};
/// let mut list = CompressorList::new();
/// list.set(compressor::NoneCompressor);
/// list.set(compressor::SnappyCompressor);
/// ```
pub struct CompressorList([Option<Box<dyn Compressor>>; 256]);

impl CompressorList {
    /// Create a **Empty** compressor list
    pub fn new() -> Self {
        const INIT: Option<Box<dyn Compressor>> = None;
        Self([INIT; 256])
    }

    /// Set compressor with the id in `CompressorId` trait
    pub fn set<T>(&mut self, compressor: T)
    where
        T: Compressor + CompressorId + 'static,
    {
        self.set_with_id(T::ID, compressor)
    }

    /// Set compressor with id
    pub fn set_with_id(&mut self, id: u8, compressor: impl Compressor + 'static) {
        self.0[id as usize] = Some(Box::new(compressor));
    }

    pub fn is_set(&self, id: u8) -> bool {
        self.0[id as usize].is_some()
    }

    #[allow(clippy::borrowed_box)]
    pub fn get(&self, id: u8) -> Result<&Box<dyn Compressor + 'static>> {
        self.0[id as usize].as_ref().ok_or_else(|| Status {
            code: StatusCode::NotSupported,
            err: format!("invalid compression id `{}`", id),
        })
    }
}

impl Default for CompressorList {
    fn default() -> Self {
        let mut list = Self::new();
        list.set(compressor::NoneCompressor);
        list.set(compressor::SnappyCompressor);
        list
    }
}

/// Returns Options that will cause a database to exist purely in-memory instead of being stored on
/// disk. This is useful for testing or ephemeral databases.
pub fn in_memory() -> Options {
    Options {
        env: Rc::new(Box::new(MemEnv::new())),
        ..Default::default()
    }
}

pub fn for_test() -> Options {
    Options {
        env: Rc::new(Box::new(MemEnv::new())),
        log: share(infolog::stderr()),

        ..Default::default()
    }
}
