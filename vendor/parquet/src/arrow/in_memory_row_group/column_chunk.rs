// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::errors::ParquetError;
use crate::file::reader::{ChunkReader, Length};
use bytes::{Buf, Bytes};
use std::ops::Range;

/// In-memory column bytes retained either as a full chunk or selected ranges.
#[derive(Clone, Debug)]
pub(crate) enum ColumnChunkData {
    Sparse {
        length: usize,
        data: Vec<(usize, Bytes)>,
    },
    Dense {
        offset: usize,
        data: Bytes,
    },
}

impl ColumnChunkData {
    pub(crate) fn sparse(length: usize, data: Vec<(usize, Bytes)>) -> Self {
        Self::Sparse {
            length,
            data: Self::coalesce_pages(data),
        }
    }

    pub(crate) fn dense(offset: usize, data: Bytes) -> Self {
        Self::Dense { offset, data }
    }

    pub(crate) fn can_fetch_more(&self) -> bool {
        matches!(self, Self::Sparse { .. })
    }

    pub(crate) fn coalesce_ranges(mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
        ranges.retain(|range| range.start < range.end);
        ranges.sort_unstable_by_key(|range| range.start);

        let mut merged: Vec<Range<u64>> = Vec::with_capacity(ranges.len());
        for range in ranges {
            if let Some(last) = merged.last_mut() {
                if range.start <= last.end {
                    last.end = last.end.max(range.end);
                    continue;
                }
            }
            merged.push(range);
        }
        merged
    }

    /// Return portions of requested ranges not already retained in this sparse chunk.
    pub(crate) fn missing_ranges(&self, ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
        let Self::Sparse { data, .. } = self else {
            return Vec::new();
        };
        let ranges = Self::coalesce_ranges(ranges);
        let mut missing = Vec::new();

        for range in ranges {
            let mut cursor = range.start;
            for (offset, bytes) in data {
                let cached_start = u64::try_from(*offset).unwrap_or(u64::MAX);
                let cached_end = cached_start.saturating_add(
                    u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                );
                if cached_end <= cursor {
                    continue;
                }
                if cached_start >= range.end {
                    break;
                }
                if cached_start > cursor {
                    missing.push(cursor..cached_start.min(range.end));
                }
                cursor = cursor.max(cached_end.min(range.end));
                if cursor == range.end {
                    break;
                }
            }
            if cursor < range.end {
                missing.push(cursor..range.end);
            }
        }
        missing
    }

    /// Extend a sparse chunk with non-overlapping fetched ranges. `Bytes` clones
    /// share their backing allocation, so this only copies range metadata.
    pub(crate) fn merge_sparse_pages(
        &self,
        additions: Vec<(usize, Bytes)>,
    ) -> Result<Self, ParquetError> {
        let Self::Sparse { length, data } = self else {
            return Err(ParquetError::General(
                "cannot add sparse ranges to a dense column chunk".into(),
            ));
        };

        let mut merged = data.clone();
        for (offset, bytes) in additions {
            if bytes.is_empty() {
                continue;
            }
            let end = offset.checked_add(bytes.len()).ok_or_else(|| {
                ParquetError::General("sparse column range overflows platform size".into())
            })?;
            let overlap = merged.iter().find(|(existing_offset, existing)| {
                let existing_end = existing_offset.saturating_add(existing.len());
                *existing_offset < end && offset < existing_end
            });
            if let Some((existing_offset, existing)) = overlap {
                if *existing_offset == offset || (offset >= *existing_offset && end <= existing_offset.saturating_add(existing.len())) {
                    continue;
                }
                return Err(ParquetError::General(
                    "overlapping sparse column ranges cannot be merged".into(),
                ));
            }
            merged.push((offset, bytes));
        }

        Ok(Self::sparse(*length, merged))
    }

    fn coalesce_pages(mut data: Vec<(usize, Bytes)>) -> Vec<(usize, Bytes)> {
        data.sort_unstable_by_key(|(offset, _)| *offset);
        let mut coalesced: Vec<(usize, Bytes)> = Vec::with_capacity(data.len());
        for (offset, bytes) in data {
            if let Some((last_offset, last_bytes)) = coalesced.last() {
                if *last_offset == offset {
                    continue;
                }
                debug_assert!(last_offset.saturating_add(last_bytes.len()) <= offset);
            }
            coalesced.push((offset, bytes));
        }
        coalesced
    }

    fn get(&self, start: u64) -> Result<Bytes, ParquetError> {
        match self {
            Self::Sparse { data, .. } => {
                let idx = data
                    .partition_point(|(offset, _)| u64::try_from(*offset).unwrap_or(u64::MAX) <= start)
                    .checked_sub(1)
                    .ok_or_else(|| Self::invalid_offset(start))?;
                let (offset, bytes) = &data[idx];
                let relative = usize::try_from(start - u64::try_from(*offset).unwrap_or(u64::MAX))
                    .map_err(|_| Self::invalid_offset(start))?;
                if relative >= bytes.len() {
                    return Err(Self::invalid_offset(start));
                }
                Ok(bytes.slice(relative..))
            }
            Self::Dense { offset, data } => {
                let relative = usize::try_from(start)
                    .ok()
                    .and_then(|start| start.checked_sub(*offset))
                    .filter(|relative| *relative < data.len())
                    .ok_or_else(|| Self::invalid_offset(start))?;
                Ok(data.slice(relative..))
            }
        }
    }

    fn invalid_offset(start: u64) -> ParquetError {
        ParquetError::General(format!(
            "Invalid offset in sparse column chunk data: {start}; the required range was not fetched"
        ))
    }
}

impl Length for ColumnChunkData {
    fn len(&self) -> u64 {
        match self {
            Self::Sparse { length, .. } => u64::try_from(*length).unwrap_or(u64::MAX),
            Self::Dense { data, .. } => u64::try_from(data.len()).unwrap_or(u64::MAX),
        }
    }
}

impl ChunkReader for ColumnChunkData {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        Ok(self.get(start)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        let bytes = self.get(start)?;
        if length > bytes.len() {
            return Err(Self::invalid_offset(start));
        }
        Ok(bytes.slice(..length))
    }
}

#[cfg(test)]
#[path = "column_chunk_tests.rs"]
mod tests;
