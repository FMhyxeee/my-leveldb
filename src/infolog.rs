use std::io::{self, Write};

pub struct Logger(pub Box<dyn Write>);

pub fn stderr() -> Logger {
    Logger(Box::new(io::stderr()))
}

#[macro_export]
#[allow(clippy::unused_io_amount)]
macro_rules! log {
    ($l:expr) => ($l.borrow_mut().0.write("\n".as_bytes()).unwrap());
    ($l:expr, $fmt:expr) => ($l.borrow_mut().0.write(concat!($fmt, "\n").as_bytes()).unwrap());
    ($l:expr, $fmt:expr, $($arg:tt)*) => (
        $l.borrow_mut().0.write_fmt(format_args!(concat!($fmt, "\n"), $($arg)*)).unwrap());
}
