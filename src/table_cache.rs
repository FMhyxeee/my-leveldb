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

// struct TableAndFile {
//     file: Arc<Box<dyn RandomAccess>>,
//     table: Table,
// }

// pub struct TableCache {
//     dbname: String,
//     cache: Cache<TableAndFile>,
//     opts: Options,
// }

// impl TableCache {
//     pub fn new(db: &str, opt: Options, entries: usize) -> TableCache {
//         TableCache {
//             dbname: String::from(db),
//             cache: Cache::new(entries),
//             opts: opt,
//         }
//     }

//     pub fn evict(&mut self, id: u64) {
//         self.cache.remove(&filenum_to_key(id));
//     }

//     /// Return a table from cache, or open the backing file, then cache and return it.
//     pub fn get_table(&mut self, file_num: u64, file_size: usize) -> Result<Table> {
//         let key = filenum_to_key(file_num);
//         match self.cache.get(&key) {
//             Some(t) => return Ok(t.table.clone()),
//             _ => {}
//         }
//         self.open_table(file_num, file_size)
//     }

// Open a table on the file system and read it.
//     fn open_table(&mut self, file_num: u64, file_size: usize) -> Result<Table> {
//         let name = table_name(&self.dbname, file_num, DEFAULT_SUFFIX);
//         let path = Path::new(&name);
//         let file = Arc::new(self.opts.env.open_random_access_file(&path)?);
//         // No SSTable file name compatibility.
//         let table = Table::new(self.opts.clone(), file.clone(), file_size)?;
//         self.cache.insert(&filenum_to_key(file_num),
//                           TableAndFile {
//                               file: file.clone(),
//                               table: table.clone(),
//                           });
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
