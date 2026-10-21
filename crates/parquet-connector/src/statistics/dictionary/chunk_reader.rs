//! Borrowed-range adapter for a serialized Parquet dictionary page.

use std::io::Cursor;

use bytes::Bytes;
use parquet::file::reader::{ChunkReader, Length};

pub(super) struct DictionaryPageChunkReader {
    bytes: Bytes,
    base_offset: u64,
    file_length: u64,
}

impl DictionaryPageChunkReader {
    pub(super) fn new(bytes: Bytes, base_offset: u64, file_length: u64) -> Self {
        Self {
            bytes,
            base_offset,
            file_length,
        }
    }
}

impl Length for DictionaryPageChunkReader {
    fn len(&self) -> u64 {
        self.file_length
    }
}

impl ChunkReader for DictionaryPageChunkReader {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let position = self
            .relative_position(start)
            .filter(|position| *position <= self.bytes.len() as u64)
            .ok_or_else(outside_dictionary_range)?;
        let mut cursor = Cursor::new(self.bytes.clone());
        cursor.set_position(position);
        Ok(cursor)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let start = self
            .relative_position(start)
            .and_then(|offset| usize::try_from(offset).ok())
            .ok_or_else(outside_dictionary_range)?;
        let end = start
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(outside_dictionary_range)?;
        Ok(self.bytes.slice(start..end))
    }
}

impl DictionaryPageChunkReader {
    fn relative_position(&self, start: u64) -> Option<u64> {
        start.checked_sub(self.base_offset)
    }
}

fn outside_dictionary_range() -> parquet::errors::ParquetError {
    parquet::errors::ParquetError::General("read outside dictionary page range".into())
}
