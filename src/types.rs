//! A collection of fundamentail and/or simple types used by other modules
use crate::error::{err, Result, StatusCode};
use std::{cell::RefCell, rc::Rc};

pub const NUM_LEVELS: usize = 7;

// Represents a sequence number of a single entry.
pub type SequenceNumber = u64;

pub const MAX_SEQUENCE_NUMBER: SequenceNumber = (1 << 56) - 1;

/// A shared thingy with interior mutability.
pub type Shared<T> = Rc<RefCell<T>>;

pub fn share<T>(t: T) -> Rc<RefCell<T>> {
    Rc::new(RefCell::new(t))
}

/// Denotes a key range
pub struct Range<'a> {
    pub start: &'a [u8],
    pub limit: &'a [u8],
}

/// An extension of the standard `Iterator` trait that supports some methods necessary for LevelDB.
/// This works because the iterators used are stateful and keep the last returned element.
///
/// Note: Implementing types are expected to hold `!valid()` before the first call to `advance()`
///
/// test_util::test_iterator_properties() verifies that all properties hold.
pub trait LdbIterator {
    /// advance advances the position of the iterator by one element (which can be retrieved using
    /// current(). If no more elements are available, advance() return false, and the iterator
    /// becomes invalid. (i.e like reset() has been called).
    fn advance(&mut self) -> bool;
    /// Return the currentite4m(i.e the item most recently returned by get_next())
    fn current(&self, key: &mut Vec<u8>, val: &mut Vec<u8>) -> bool;
    /// Seek the iterator to `key` or the next bigger key. If the seek is invalid (past last
    /// element), the iterator is reset() and not valid.
    /// After a seek to an existing key, current() returns that entry.
    fn seek(&mut self, key: &[u8]);
    /// Resets the iterator to the beginning.
    fn reset(&mut self);
    /// Returns true if the iterator is not positioned before the first or after the last element,
    /// i.e if current() would return an entry.
    fn valid(&self) -> bool;
    /// Go to the previous item; if the iterator has reached the "before-first" item, prev()
    /// returns false, and the iterator is invalid. This is inefficient for most iterators.
    fn prev(&mut self) -> bool;

    // default implementations.

    /// next is like Iterator::next(). It's implemented here because "only traits defined in the
    /// current crate can be implemented for a type parameter" (says rustc).
    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if !self.advance() {
            return None;
        }
        let (mut key, mut val) = (vec![], vec![]);
        if self.current(&mut key, &mut val) {
            Some((key, val))
        } else {
            None
        }
    }

    /// seek_to_first seeks to the first element.
    fn seek_to_first(&mut self) {
        self.reset();
        self.advance();
    }
}

/// current_key_val is a helper allocating two vectors and filling them with the current key/value
/// of the specified iterator.
pub fn current_key_val<It: LdbIterator + ?Sized>(it: &It) -> Option<(Vec<u8>, Vec<u8>)> {
    let (mut k, mut v) = (vec![], vec![]);
    if it.current(&mut k, &mut v) {
        Some((k, v))
    } else {
        None
    }
}

impl LdbIterator for Box<dyn LdbIterator> {
    fn advance(&mut self) -> bool {
        self.as_mut().advance()
    }
    fn current(&self, key: &mut Vec<u8>, val: &mut Vec<u8>) -> bool {
        self.as_ref().current(key, val)
    }
    fn seek(&mut self, key: &[u8]) {
        self.as_mut().seek(key)
    }
    fn reset(&mut self) {
        self.as_mut().reset()
    }
    fn valid(&self) -> bool {
        self.as_ref().valid()
    }
    fn prev(&mut self) -> bool {
        self.as_mut().prev()
    }
}

/// The unique (sequential) number of a file
pub type FileNum = u64;

/// Descsribes a file on disk
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FileMetaData {
    // default: size/ 16384.
    pub allowed_seeks: usize,
    pub num: u64,
    pub size: usize,
    // these are in InternalKey format:
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FileType {
    Log,
    BDLock,
    Table,
    Descriptor,
    Current,
    Temp,
    InfoLog,
}

pub fn parse_file_name(f: &str) -> Result<(FileNum, FileType)> {
    if f == "CURRENT" {
        return Ok((0, FileType::Current));
    } else if f == "LOCK" {
        return Ok((0, FileType::BDLock));
    } else if f == "LOG" || f == "LOG.old" {
        return Ok((0, FileType::InfoLog));
    } else if f.starts_with("MANIFEST-") {
        if let Some(ix) = f.find('-') {
            if let Ok(num) = FileNum::from_str_radix(&f[ix + 1..], 10) {
                return Ok((num, FileType::Descriptor));
            }
            return err(
                StatusCode::InvalidArgument,
                "manifest file number is invalid",
            );
        }
        return err(StatusCode::InvalidArgument, "manifest file has no dash");
    } else if let Some(ix) = f.find('.') {
        // 00012345.log 00123.sst ...
        if let Ok(num) = FileNum::from_str_radix(&f[0..ix], 10) {
            let typ = match &f[ix + 1..] {
                "sst" | "ldb" => FileType::Table,
                "dbtmp" => FileType::Temp,
                _ => {
                    return err(
                        StatusCode::InvalidArgument,
                        "unknown numbered file extension",
                    )
                }
            };
            return Ok((num, typ));
        }
        return err(
            StatusCode::InvalidArgument,
            "invalid file number for table or temp file",
        );
    }
    err(StatusCode::InvalidArgument, "unknown file type")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_types_parse_file_name() {
        for c in &[
            ("CURRENT", (0, FileType::Current)),
            ("LOCK", (0, FileType::BDLock)),
            ("LOG", (0, FileType::InfoLog)),
            ("LOG.old", (0, FileType::InfoLog)),
            ("MANIFEST-01234", (1234, FileType::Descriptor)),
            ("001122.sst", (1122, FileType::Table)),
            ("001122.ldb", (1122, FileType::Table)),
            ("001122.dbtmp", (1122, FileType::Temp)),
        ] {
            assert_eq!(parse_file_name(c.0).unwrap(), c.1);
        }
        assert!(parse_file_name("xyz.LOCK").is_err());
        assert!(parse_file_name("01a.sst").is_err());
        assert!(parse_file_name("0011.abc").is_err());
        assert!(parse_file_name("MANIFEST-trolol").is_err());
    }
}
