#![allow(dead_code)]
pub enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
}

pub type SequenceNumber = u64;

pub enum Status {
    OK,
    NotFOUND(String),
    Corruption(String),
    NotSupported(String),
    InvalidArgument(String),
    IOError(String),
}
