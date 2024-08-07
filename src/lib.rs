//! my-leveldb is a reimplementation of LevelDB in pure rust. It depends only on a few crates,
//! and is very close to the original, implementation-wise. The external API is relatively small
//! and should be easy to use.
//!
//! ```
//! use my_leveldb::{DB, DBIterator, LdbIterator, Options};
//!
//! let opt = my_leveldb::in_memory();
//! let mut db = DB::open("mydatabase", opt).unwrap();
//!
//! db.put(b"Hello", b"World").unwrap();
//! assert_eq!(b"World", db.get(b"Hello").unwrap().as_slice());
//!
//! let mut iter = db.new_iter().unwrap();
//! // Note: For efficiency reasons, it's recommended to use advance() and current() instead of
//! // next() when iterating over many elements.
//! assert_eq!((b"Hello".to_vec(), b"World".to_vec()), iter.next().unwrap());
//!
//! db.delete(b"Hello").unwrap();
//! db.flush().unwrap();
//! ```
//!

#![allow(dead_code)]

#[macro_use]
mod infolog;

// #[cfg(feature = "async")]
mod asyncdb;

mod block;
mod block_builder;
mod blockhandle;
mod cache;
mod cmp;
mod disk_env;
mod env_common;
mod error;
mod filter;
mod filter_block;
mod key_types;
mod log;
mod mem_env;
mod memtable;
mod merging_iter;
mod options;
mod skipmap;
mod snapshot;
mod table_block;
mod table_builder;
mod table_cache;
mod table_reader;
mod test_util;
mod types;
mod version;
mod version_edit;
mod version_set;
mod write_batch;

mod db_impl;
mod db_iter;

pub mod compressor;
pub mod env;

pub use cmp::{Cmp, DefaultCmp};
pub use compressor::{Compressor, CompressorId};
pub use db_impl::DB;
pub use db_iter::DBIterator;

pub use disk_env::PosixDiskEnv;
pub use env::Env;
pub use error::{Result, Status};
pub use filter::{BloomPolicy, FilterPolicy};
pub use mem_env::MemEnv;
pub use options::{in_memory, CompressorList, Options};
pub use skipmap::SkipMap;
pub use types::LdbIterator;
pub use write_batch::WriteBatch;
