use thiserror::Error;

pub type Result<T> = std::result::Result<T, VfsError>;

#[derive(Error, Debug)]
pub enum VfsError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Metadata engine error: {0}")]
    Metadata(String),

    #[error("Storage backend error: {0}")]
    Storage(#[from] opendal::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("Block full, must wrap")]
    BlockFull,

    #[error("File or partition not found: {0}")]
    NotFound(String),

    #[error("Exo format error: {0}")]
    Exo(#[from] exo::ExoError),

    #[error("Internal VFS error: {0}")]
    Internal(String),
}
