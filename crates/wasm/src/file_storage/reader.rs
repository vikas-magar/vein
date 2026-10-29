use std::sync::Arc;

use opendal::{Buffer, Result, raw::oio};

use super::FileRangeReader;

pub(super) struct FileReader<R: FileRangeReader> {
    source: Arc<R>,
    path: Arc<str>,
    size: u64,
}

impl<R: FileRangeReader> FileReader<R> {
    pub(super) fn new(source: Arc<R>, path: &str, size: u64) -> Self {
        Self { source, path: Arc::from(path), size }
    }
}

impl<R: FileRangeReader> oio::PositionRead for FileReader<R> {
    type Handle = Self;
    async fn open(&self) -> Result<Self::Handle> {
        Ok(Self { source: Arc::clone(&self.source), path: Arc::clone(&self.path), size: self.size })
    }

    async fn read_at(handle: &Self::Handle, offset: u64, size: usize) -> Result<Buffer> {
        if offset >= handle.size { return Ok(Buffer::new()); }
        let length = usize::try_from((handle.size - offset).min(size as u64))
            .map_err(|_| opendal::Error::new(opendal::ErrorKind::RangeNotSatisfied, "file range is too large"))?;
        let mut output = vec![0; length];
        handle.source.read_into(&handle.path, offset, &mut output)?;
        Ok(Buffer::from(output))
    }
}
