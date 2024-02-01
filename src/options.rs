use std::{io, rc::Rc};

use crate::{
    block::Block,
    cache::Cache,
    cmp::{Cmp, DefaultCmp},
    disk_env::PosixDiskEnv,
    env::Env,
    filter::{self, BoxedFilterPolicy},
    infolog::Logger,
    types::{share, SequenceNumber, Shared},
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

/// [not all member types implemented yet]
///
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
            reuse_logs: false,
            compression_type: CompressionType::CompressionNone,
            filter_policy: Rc::new(Box::new(filter::BloomPolicy::new(DEFAULT_BITS_PER_KEY))),
        }
    }
}

impl Options {
    /// Set the comparator to use in all operations and structures that need to compare keys.
    ///
    /// DO NOT set the comparator after having written any record with a different comparator.
    /// If the comparator used differs from the one used when writing a database that is being
    /// opened, the library is free to panic.
    pub fn set_comparator(&mut self, c: Box<dyn Cmp>) {
        self.cmp = Rc::new(c);
    }

    /// Supplied to DB read operations
    /// Deprecated: Will soon be removed to reduce complexity.
    pub fn set_env(&mut self, e: Box<dyn Env>) {
        self.env = Rc::new(e);
    }
}

/// Supplied to DB read operations
/// Deprecated: Will soon be removed to reduce complexity.
pub struct ReadOptions {
    pub verify_checksums: bool,
    pub fill_cache: bool,
    pub snapshot: Option<SequenceNumber>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            verify_checksums: true,
            fill_cache: true,
            snapshot: None,
        }
    }
}

/// Supplied to write operations
#[derive(Default)]
pub struct WriteOptions {
    pub sync: bool,
}
