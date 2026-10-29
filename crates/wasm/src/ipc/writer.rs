use std::io::{Error, ErrorKind, Result, Write};

/// Rejects growth before allocation. Oversized row slices are retried by the encoder.
pub(super) struct LimitedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl LimitedBuffer {
    pub(super) fn new(limit: usize) -> Self {
        Self { bytes: Vec::new(), limit, exceeded: false }
    }
    pub(super) fn exceeded(&self) -> bool { self.exceeded }
    pub(super) fn into_bytes(self) -> Vec<u8> { self.bytes }
}

impl Write for LimitedBuffer {
    fn write(&mut self, buffer: &[u8]) -> Result<usize> {
        if buffer.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(Error::new(ErrorKind::OutOfMemory, "IPC packet limit exceeded"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }
    fn flush(&mut self) -> Result<()> { Ok(()) }
}
