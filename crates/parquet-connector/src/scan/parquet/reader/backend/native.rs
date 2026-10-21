use std::fs::File;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use parquet::errors::{ParquetError, Result};

use super::ParquetRangeSource;

#[cfg(unix)]
mod mapped;

pub(super) struct NativeRangeSource {
    path: PathBuf,
    file: Arc<File>,
    mapped: Option<Bytes>,
}

impl NativeRangeSource {
    pub(super) async fn open(path: &Path, expected_size: u64) -> Result<Self> {
        let path = path.to_path_buf();
        let open_path = path.clone();
        let (file, mapped) = tokio::task::spawn_blocking(move || {
            let file = File::open(&open_path).map_err(|error| {
                ParquetError::General(format!("open local Parquet file {}: {error}", open_path.display()))
            })?;
            let metadata = file.metadata().map_err(|error| {
                ParquetError::General(format!("stat local Parquet file {}: {error}", open_path.display()))
            })?;
            if !metadata.is_file() || metadata.len() != expected_size {
                return Err(ParquetError::General(format!(
                    "local Parquet file {} changed or is not a regular file",
                    open_path.display()
                )));
            }
            let mapped = map_read_only(&file, metadata.len(), &open_path);
            Ok::<_, ParquetError>((Arc::new(file), mapped))
        })
            .await
            .map_err(|error| ParquetError::General(format!("local Parquet open task failed: {error}")))??;
        Ok(Self { path, file, mapped })
    }

    fn read_at(&self, offset: u64, length: usize) -> Result<Bytes> {
        let mut bytes = vec![0; length];
        read_exact_at(&self.file, &mut bytes, offset).map_err(|error| {
            ParquetError::General(format!("read local Parquet file {}: {error}", self.path.display()))
        })?;
        Ok(Bytes::from(bytes))
    }
}

impl ParquetRangeSource for NativeRangeSource {
    fn read_range(&self, range: std::ops::Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        Box::pin(async move {
            let end = usize::try_from(range.end).map_err(|_| {
                ParquetError::General("Parquet byte range exceeds addressable memory".into())
            })?;
            let start = usize::try_from(range.start).map_err(|_| {
                ParquetError::General("Parquet byte range exceeds addressable memory".into())
            })?;
            if start > end {
                return Err(ParquetError::General(
                    "invalid Parquet byte range: end precedes start".into(),
                ));
            }
            if let Some(mapped) = &self.mapped {
                if end > mapped.len() {
                    return Err(ParquetError::General(format!(
                        "Parquet byte range ends at {end}, beyond mapped file size {}",
                        mapped.len()
                    )));
                }
                return Ok(mapped.slice(start..end));
            }
            self.read_at(range.start, end - start)
        })
    }

    fn representation(&self) -> &'static str {
        if self.mapped.is_some() {
            "read_only_mmap"
        } else {
            "native_range_buffer"
        }
    }
}

#[cfg(unix)]
fn map_read_only(file: &File, size: u64, path: &Path) -> Option<Bytes> {
    if size == 0 {
        return None;
    }
    let length = usize::try_from(size).ok()?;
    match mapped::MappedRegion::open(file, length) {
        Ok(region) => Some(Bytes::from_owner(region)),
        Err(error) => {
            tracing::warn!(
                target: "vein.audit",
                event = "parquet_mmap_unavailable",
                path = %path.display(),
                error = %error,
                fallback = "native_range_buffer",
                "read-only Parquet mapping unavailable; using bounded range reads"
            );
            None
        }
    }
}

#[cfg(not(unix))]
fn map_read_only(_file: &File, _size: u64, _path: &Path) -> Option<Bytes> {
    None
}

fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buffer.is_empty() {
        let bytes_read = positional_read(file, buffer, offset)?;
        if bytes_read == 0 {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "range extends past end of file"));
        }
        offset = offset
            .checked_add(u64::try_from(bytes_read).map_err(|_| {
                io::Error::new(ErrorKind::InvalidInput, "read length exceeds offset range")
            })?)
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "file offset overflow"))?;
        buffer = &mut buffer[bytes_read..];
    }
    Ok(())
}

#[cfg(unix)]
fn positional_read(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buffer, offset)
}

#[cfg(windows)]
fn positional_read(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_read(buffer, offset)
}

#[cfg(not(any(unix, windows)))]
fn positional_read(_file: &File, _buffer: &mut [u8], _offset: u64) -> io::Result<usize> {
    Err(io::Error::new(ErrorKind::Unsupported, "positional reads are unavailable on this platform"))
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
