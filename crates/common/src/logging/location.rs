use std::fmt::{self, Display, Formatter};
use std::hash::BuildHasher;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::{collections::hash_map::RandomState, ffi::OsStr};

static LOCATION_HASHER: OnceLock<RandomState> = OnceLock::new();

/// Process-local pseudonymous identity for a path in diagnostic events.
///
/// It allows logs from one process to be correlated without printing object
/// paths. The value is intentionally not a persistent or cryptographic ID.
#[derive(Clone, Copy)]
pub struct LogLocation<'a>(&'a OsStr);

impl<'a> LogLocation<'a> {
    /// Creates a pseudonymous log identity for a borrowed path.
    pub fn from_path(path: &'a Path) -> Self {
        Self(path.as_os_str())
    }
}

impl<'a> From<&'a str> for LogLocation<'a> {
    fn from(path: &'a str) -> Self {
        Self(Path::new(path).as_os_str())
    }
}

impl<'a> From<&'a Path> for LogLocation<'a> {
    fn from(path: &'a Path) -> Self {
        Self::from_path(path)
    }
}

impl<'a> From<&'a PathBuf> for LogLocation<'a> {
    fn from(path: &'a PathBuf) -> Self {
        Self::from_path(path.as_path())
    }
}

impl Display for LogLocation<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let hash = LOCATION_HASHER
            .get_or_init(RandomState::new)
            .hash_one(self.0);
        write!(f, "loc:{hash:016x}")
    }
}
