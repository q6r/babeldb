//! Error type shared by every module.

use std::fmt;

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    Io(std::io::Error),
    /// Error reported by the storage backend (redb, LMDB, ...).
    Backend(String),
    /// Persisted bytes do not follow the documented format.
    Format(String),
    /// Decoded bytes do not match the stored length/digest, or a referenced object is missing.
    Integrity { object_id: Option<u64>, detail: String },
    RevisionConflict { key: Vec<u8>, expected: String, actual: Option<u64> },
    UnknownCodec { id: u8, version: u8 },
    UnknownGenerator { id: u16, version: u16 },
    MissingDependency { param_id: u64 },
    LimitExceeded(String),
    IdExhausted(&'static str),
    InvalidArgument(String),
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Wrap any backend error.
    pub fn backend(e: impl fmt::Display) -> Error {
        Error::Backend(e.to_string())
    }

    pub fn format(msg: impl Into<String>) -> Error {
        Error::Format(msg.into())
    }

    pub fn integrity(object_id: Option<u64>, detail: impl Into<String>) -> Error {
        Error::Integrity { object_id, detail: detail.into() }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Backend(e) => write!(f, "backend error: {e}"),
            Error::Format(e) => write!(f, "format error: {e}"),
            Error::Integrity { object_id: Some(id), detail } => {
                write!(f, "integrity error in object {id}: {detail}")
            }
            Error::Integrity { object_id: None, detail } => write!(f, "integrity error: {detail}"),
            Error::RevisionConflict { key, expected, actual } => write!(
                f,
                "revision conflict on key {}: expected {expected}, actual {actual:?}",
                String::from_utf8_lossy(key)
            ),
            Error::UnknownCodec { id, version } => {
                write!(f, "unknown codec id 0x{id:02x} version {version}")
            }
            Error::UnknownGenerator { id, version } => {
                write!(f, "unknown generator id {id} version {version}")
            }
            Error::MissingDependency { param_id } => write!(f, "missing dependency param {param_id}"),
            Error::LimitExceeded(e) => write!(f, "limit exceeded: {e}"),
            Error::IdExhausted(what) => write!(f, "identifier space exhausted: {what}"),
            Error::InvalidArgument(e) => write!(f, "invalid argument: {e}"),
            Error::Unsupported(e) => write!(f, "unsupported: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
