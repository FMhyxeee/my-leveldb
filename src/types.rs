#![allow(dead_code)]
pub enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
}

pub type SequenceNumber = u64;

pub enum Status {
    OK,
    NotFound(String),
    Corruption(String),
    NotSupported(String),
    InvalidArgument(String),
    IOError(String),
}

pub trait LdbIterator<'a>: Iterator {
    fn seek(&mut self, key: &[u8]);
    fn valid(&self) -> bool;
    fn current(&'a self) -> Self::Item;
}
