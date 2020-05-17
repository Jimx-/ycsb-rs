use std::{
    error::Error as StdError,
    fmt::{self, Display},
    io,
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    InvalidArgument(String),
    UnknownSpecFormat,
    TransactionAborted,
    Io(io::Error),
}

impl From<io::Error> for Error {
    fn from(ioe: io::Error) -> Self {
        Error::Io(ioe)
    }
}

impl StdError for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        use self::Error::*;

        match *self {
            InvalidArgument(ref e) => write!(f, "Invalid argument: {}", e),
            UnknownSpecFormat => write!(f, "Unknown workload spec format"),
            TransactionAborted => write!(f, "Transaction aborted"),
            Io(ref e) => write!(f, "IO error: {}", e),
        }
    }
}
