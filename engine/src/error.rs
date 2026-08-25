//! Error types for the storage engine.
//!
//! The engine distinguishes three kinds of failure, because callers respond to
//! them very differently. `Io` is the environment misbehaving and is usually
//! retryable or fatal at the process level. `Corrupt` means the bytes on disk
//! do not say what they should, which is never retryable and must never be
//! silently repaired. `InvalidArgument` is the caller's fault and the engine's
//! state is untouched.

use std::io;
use std::path::PathBuf;

/// The engine's result type.
pub type Result<T> = std::result::Result<T, Error>;

/// A storage engine failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An operating system level failure.
    #[error("io error on {path}: {source}")]
    Io {
        /// The file involved.
        path: PathBuf,
        /// The underlying failure.
        #[source]
        source: io::Error,
    },

    /// An I/O failure with no specific file.
    #[error("io error: {0}")]
    RawIo(#[from] io::Error),

    /// On-disk data failed structural or checksum validation.
    ///
    /// This is deliberately not merged with `Io`. An I/O error means the read
    /// did not happen; corruption means it happened and returned something
    /// untrue, which is the more dangerous of the two and must stop the
    /// operation rather than being retried into a loop.
    #[error("corruption in {path}: {detail}")]
    Corrupt {
        /// The file involved.
        path: PathBuf,
        /// What was wrong.
        detail: String,
    },

    /// The caller asked for something impossible.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The engine has been shut down.
    #[error("engine is closed")]
    Closed,

    /// A background thread panicked and the engine can no longer be trusted.
    #[error("engine has failed and is refusing further writes: {0}")]
    Poisoned(String),
}

impl Error {
    /// Builds an [`Error::Io`] carrying the file it happened on.
    pub fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Error::Io {
            path: path.into(),
            source,
        }
    }

    /// Builds an [`Error::Corrupt`].
    pub fn corrupt(path: impl Into<PathBuf>, detail: impl Into<String>) -> Self {
        Error::Corrupt {
            path: path.into(),
            detail: detail.into(),
        }
    }

    /// Reports whether this error means on-disk data is untrustworthy.
    pub fn is_corruption(&self) -> bool {
        matches!(self, Error::Corrupt { .. })
    }
}

/// Adds file context to an [`io::Result`].
///
/// Rust's [`io::Error`] famously does not carry the path, so `No such file or
/// directory` on its own is nearly useless in a log. This trait makes adding
/// the path a one-call habit rather than a per-site decision.
pub(crate) trait IoContext<T> {
    /// Attaches `path` to any error.
    fn ctx(self, path: impl Into<PathBuf>) -> Result<T>;
}

impl<T> IoContext<T> for io::Result<T> {
    fn ctx(self, path: impl Into<PathBuf>) -> Result<T> {
        self.map_err(|e| Error::io(path, e))
    }
}
