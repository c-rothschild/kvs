use std::{fmt, io};

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug)]

pub enum StoreError {
    Io(io::Error),

    // The on-disk log isn't valid (unknown opcode, impossible lengths, etc.)
    CorruptLog { msg: String },

    // Invalid CLI keys/values (empty key, too large, etc.)
    InvalidInput { msg: String },

    // Actor/channel Errors
    StoreClosed { msg: String },
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Io(e) => write!(f, "{e}"),
            StoreError::CorruptLog { msg } => write!(f, "corrupt log: {msg}"),
            StoreError::InvalidInput { msg } => write!(f, "invalid input: {msg}"),
            StoreError::StoreClosed { msg} => write!(f, "store closed: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StoreError::Io(e) => Some(e),
            _ => None,
        }
    }
}

// lets you write '?' on io::Result and it becomes StoreError::Io
impl From<io::Error> for StoreError {
    fn from(e: io::Error) -> Self {
        StoreError::Io(e)
    }
}

impl StoreError {
    pub fn is_corrupt_log(&self) -> bool {
        matches!(self, StoreError::CorruptLog { .. })
    }
}


