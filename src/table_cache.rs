//! table_cache implements a cache providing access to the immutable SSTables on disk. It's a
//! read-through cache, meaning that non-present tables are read from disk and cached before being
//! returned.

// use std::{sync::Arc, path::Path};

// use integer_encoding::FixedIntWriter;

// use crate::{cache::{Cache, CacheKey}, options::Options, table_reader::Table, env::RandomAccess, error::Result};

// const DEFAULT_SUFFIX: &str = "ldb";

// fn table_name(name: &str, num: u64, suff: &str) -> String {
//     assert!(num > 0);
//     format!("{}/{:06}.{}", name, num, suff)
// }

// fn filenum_to_key(num: u64) -> CacheKey {
//     let mut buf = [0; 16];
//     (&mut buf[..]).write_fixedint(num).unwrap();
//     buf
// }

// pub struct TableCache {
//     dbname: String,
//     cache: Cache<Table>,
//     opts: Options,
// }

// impl TableCache {
//     /// Create a new TableCache for the database name `db`, caching up to `entries` tables.
//     pub fn new(db: &str, opt: Options, entries: usize) -> TableCache {
//         TableCache {
//             dbname: String::from(db),
//             cache: Cache::new(entries),
//             opts: opt,
//         }
//     }

//     /// Return a table from cache, or open the backing file, then cache and return it.
//     pub fn get_table(&mut self, file_num: u64) -> Result<Table> {
//         let key = filenum_to_key(file_num);
//         if let Some(t) = self.cache.get(&key) {
//             return Ok(t.clone());
//         }
//         self.open_table(file_num)
//     }

//     fn open_table(&mut self, file_num: u64) -> Result<Table> {
//         let name = table_name(&self.dbname, file_num, DEFAULT_SUFFIX);
//         let path = Path::new(&name);
//         let file = Arc::new(self.opts.env.open_random_access_file(&path)?);
//         let file_size = self.opts.env.size_of(&path)?;
//         // No SSTable file name compatibility.
//         let table = Table::new(self.opts.clone(), file, file_size)?;
//         self.cache.insert(&filenum_to_key(file_num), table.clone());
//         Ok(table)
//     }

// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_table_name() {
//         assert_eq!("abc/000122.ldb", table_name("abc", 122, "ldb"));
//     }

//     // TODO: Write tests after memenv has been implemented.
// }
