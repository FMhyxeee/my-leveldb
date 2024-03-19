use std::{io, rc::Rc};

use crate::{
    block::Block,
    cache::Cache,
    cmp::{Cmp, DefaultCmp},
    disk_env::PosixDiskEnv,
    env::Env,
    filter::{self, BoxedFilterPolicy},
    infolog::{self, Logger},
    mem_env::MemEnv,
    types::{share, Shared},
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
    pub compression_type: CompressionType,
    pub reuse_logs: bool,
    pub reuse_manifest: bool,
    pub filter_policy: BoxedFilterPolicy,
}

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
            compression_type: CompressionType::CompressionNone,
            filter_policy: Rc::new(Box::new(filter::BloomPolicy::new(DEFAULT_BITS_PER_KEY))),
        }
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
