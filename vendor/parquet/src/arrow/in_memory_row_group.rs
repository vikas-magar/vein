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

use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::RowGroups;
use crate::arrow::arrow_reader::RowSelection;
use crate::column::page::{PageIterator, PageReader};
use crate::errors::ParquetError;
use crate::file::metadata::page_index::RowGroupPageIndex;
use crate::file::metadata::{ParquetMetaData, RowGroupMetaData};
use crate::file::page_index::offset_index::PageLocation;
use crate::file::reader::SerializedPageReader;
use bytes::Bytes;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

mod column_chunk;
pub(crate) use column_chunk::ColumnChunkData;

/// An in-memory collection of column chunks
#[derive(Debug)]
pub(crate) struct InMemoryRowGroup<'a> {
    pub(crate) page_index: Option<RowGroupPageIndex>,
    /// Column chunks for this row group
    pub(crate) column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
    pub(crate) row_count: usize,
    pub(crate) row_group_idx: usize,
    pub(crate) metadata: &'a ParquetMetaData,
    /// Data-page locations supplied from page headers when the footer has no
    /// usable OffsetIndex for a column.
    pub(crate) page_locations: Arc<HashMap<usize, Vec<PageLocation>>>,
}

/// What ranges to fetch for the columns in this row group
#[derive(Debug)]
pub(crate) struct FetchRanges {
    /// The byte ranges to fetch
    pub(crate) ranges: Vec<Range<u64>>,
    /// If `Some`, the start offsets of each page for each column chunk
    /// One entry per requested column. `Some(offsets)` describes a sparse
    /// chunk made from those page starts; `None` describes a dense full chunk.
    pub(crate) page_start_offsets: Option<Vec<Option<Vec<u64>>>>,
}

impl InMemoryRowGroup<'_> {
    /// Returns the byte ranges to fetch for the columns specified in
    /// `projection` and `selection`.
    ///
    /// `cache_mask` indicates which columns, if any, are being cached by
    /// [`RowGroupCache`](crate::arrow::array_reader::RowGroupCache).
    /// The `selection` for Cached columns is expanded to batch boundaries to simplify
    /// accounting for what data is cached.
    pub(crate) fn fetch_ranges(
        &self,
        projection: &ProjectionMask,
        selection: Option<&RowSelection>,
        batch_size: usize,
        cache_mask: Option<&ProjectionMask>,
    ) -> FetchRanges {
        let metadata = self.metadata.row_group(self.row_group_idx);
        if let Some(selection) = selection {
            let expanded_selection =
                selection.expand_to_batch_boundaries(batch_size, self.row_count);

            // If we have a `RowSelection` and an `OffsetIndex` then only fetch
            // pages required for the `RowSelection`
            // Consider preallocating outer vec: https://github.com/apache/arrow-rs/issues/8667
            let mut page_start_offsets: Vec<Option<Vec<u64>>> = vec![];

            let ranges = self
                .column_chunks
                .iter()
                .zip(metadata.columns())
                .enumerate()
                .filter(|&(idx, (chunk, _chunk_meta))| {
                    projection.leaf_included(idx)
                        && chunk
                        .as_deref()
                        .is_none_or(ColumnChunkData::can_fetch_more)
                })
                .flat_map(|(idx, (chunk, chunk_meta))| {
                    // If the first page does not start at the beginning of the column,
                    // then we need to also fetch a dictionary page.
                    let mut ranges: Vec<Range<u64>> = vec![];
                    let (start, len) = chunk_meta.byte_range();
                    let indexed_locations = self
                        .page_index
                        .as_ref()
                        .and_then(|page_index| page_index.offset_index(idx))
                        .map(|offset_index| offset_index.page_locations().as_slice());
                    let locations = self
                        .page_locations
                        .get(&idx)
                        .map(Vec::as_slice)
                        .or(indexed_locations);
                    let Some(locations) = locations.filter(|locations| !locations.is_empty()) else {
                        // Without a complete page map, retain the whole column.
                        // A row selection still applies during decoding.
                        ranges.push(start..start + len);
                        page_start_offsets.push(None);
                        return ranges;
                    };

                    // A dictionary page precedes the data pages and is required
                    // to decode any selected dictionary-encoded data page.
                    let first_page_offset = u64::try_from(locations[0].offset).ok();
                    let Some(first_page_offset) = first_page_offset
                        .filter(|offset| *offset >= start && *offset < start + len)
                    else {
                        // Invalid locations must never cause data loss. Fall
                        // back to the full chunk for this column.
                        ranges.push(start..start + len);
                        page_start_offsets.push(None);
                        return ranges;
                    };

                    if first_page_offset > start {
                        ranges.push(start..first_page_offset);
                    }

                    // Expand selection to batch boundaries if needed for caching
                    // (see doc comment for this function for details on `cache_mask`)
                    let use_expanded = cache_mask.map(|m| m.leaf_included(idx)).unwrap_or(false);
                    if use_expanded {
                        ranges.extend(expanded_selection.scan_ranges(locations));
                    } else {
                        ranges.extend(selection.scan_ranges(locations));
                    }

                    let ranges = match chunk.as_deref() {
                        Some(chunk) => chunk.missing_ranges(ranges),
                        None => ColumnChunkData::coalesce_ranges(ranges),
                    };
                    page_start_offsets.push(Some(ranges.iter().map(|range| range.start).collect()));

                    ranges
                })
                .collect();
            FetchRanges {
                ranges,
                page_start_offsets: Some(page_start_offsets),
            }
        } else {
            let ranges = self
                .column_chunks
                .iter()
                .enumerate()
                .filter(|&(idx, chunk)| {
                    projection.leaf_included(idx)
                        && chunk
                        .as_deref()
                        .is_none_or(ColumnChunkData::can_fetch_more)
                })
                .map(|(idx, _chunk)| {
                    let column = metadata.column(idx);
                    let (start, length) = column.byte_range();
                    start..(start + length)
                })
                .collect();
            FetchRanges {
                ranges,
                page_start_offsets: None,
            }
        }
    }

    /// Fills in `self.column_chunks` with the data fetched from `chunk_data`.
    ///
    /// This function **must** be called with the data from the ranges returned by
    /// `fetch_ranges` and the corresponding page_start_offsets, with the exact same and `selection`.
    pub(crate) fn fill_column_chunks<I>(
        &mut self,
        projection: &ProjectionMask,
        page_start_offsets: Option<Vec<Option<Vec<u64>>>>,
        chunk_data: I,
    ) -> Result<(), ParquetError>
    where
        I: IntoIterator<Item=Bytes>,
    {
        let mut chunk_data = chunk_data.into_iter();
        let metadata = self.metadata.row_group(self.row_group_idx);
        if let Some(page_start_offsets) = page_start_offsets {
            // Rebuild sparse chunks only for columns whose page ranges were
            // planned. Columns without a usable page map remain dense.
            let mut page_start_offsets = page_start_offsets.into_iter();

            for (idx, chunk) in self.column_chunks.iter_mut().enumerate() {
                if !projection.leaf_included(idx)
                    || chunk
                    .as_deref()
                    .is_some_and(|chunk| !chunk.can_fetch_more())
                {
                    continue;
                }

                let offsets = page_start_offsets.next().ok_or_else(|| {
                    ParquetError::General("missing page offsets for requested column".into())
                })?;
                match offsets {
                    Some(offsets) => {
                        let has_additions = !offsets.is_empty();
                        let mut chunks = Vec::with_capacity(offsets.len());
                        for _ in 0..offsets.len() {
                            chunks.push(chunk_data.next().ok_or_else(|| {
                                ParquetError::General("missing fetched page range".into())
                            })?);
                        }

                        let additions = offsets
                            .iter()
                            .copied()
                            .map(|offset| {
                                usize::try_from(offset)
                                    .map_err(|_| ParquetError::General("page offset exceeds platform size".into()))
                            })
                            .collect::<Result<Vec<_>, _>>()?
                            .into_iter()
                            .zip(chunks)
                            .collect();
                        let length = usize::try_from(metadata.column(idx).byte_range().1)
                            .map_err(|_| ParquetError::General("column chunk exceeds platform size".into()))?;
                        let updated = match chunk.as_deref() {
                            Some(existing) => existing.merge_sparse_pages(additions)?,
                            None => ColumnChunkData::sparse(length, additions),
                        };
                        if has_additions || chunk.is_none() {
                            *chunk = Some(Arc::new(updated));
                        }
                    }
                    None => {
                        let data = chunk_data.next().ok_or_else(|| {
                            ParquetError::General("missing fetched column chunk".into())
                        })?;
                        let offset = usize::try_from(metadata.column(idx).byte_range().0)
                            .map_err(|_| ParquetError::General("column offset exceeds platform size".into()))?;
                        *chunk = Some(Arc::new(ColumnChunkData::dense(offset, data)));
                    }
                }
            }
        } else {
            for (idx, chunk) in self.column_chunks.iter_mut().enumerate() {
                if !projection.leaf_included(idx)
                    || chunk
                    .as_deref()
                    .is_some_and(|chunk| !chunk.can_fetch_more())
                {
                    continue;
                }

                let data = chunk_data.next().ok_or_else(|| {
                    ParquetError::General("missing fetched column chunk".into())
                })?;
                let offset = usize::try_from(metadata.column(idx).byte_range().0)
                    .map_err(|_| ParquetError::General("column offset exceeds platform size".into()))?;
                *chunk = Some(Arc::new(ColumnChunkData::dense(offset, data)));
            }
        }
        if chunk_data.next().is_some() {
            return Err(ParquetError::General(
                "fetched data contains ranges that were not assigned to columns".into(),
            ));
        }
        Ok(())
    }
}

impl RowGroups for InMemoryRowGroup<'_> {
    fn num_rows(&self) -> usize {
        self.row_count
    }

    /// Return chunks for column i
    fn column_chunks(&self, i: usize) -> crate::errors::Result<Box<dyn PageIterator>> {
        match &self.column_chunks[i] {
            None => Err(ParquetError::General(format!(
                "Invalid column index {i}, column was not fetched"
            ))),
            Some(data) => {
                let page_locations = self.page_locations.get(&i).cloned().or_else(|| {
                    self.page_index
                        .as_ref()
                        .and_then(|page_index| page_index.page_locations(i).cloned())
                });
                let column_chunk_metadata = self.metadata.row_group(self.row_group_idx).column(i);
                let page_reader = SerializedPageReader::new(
                    data.clone(),
                    column_chunk_metadata,
                    self.row_count,
                    page_locations,
                )?;
                let page_reader = page_reader.add_crypto_context(
                    self.row_group_idx,
                    i,
                    self.metadata,
                    column_chunk_metadata,
                )?;

                let page_reader: Box<dyn PageReader> = Box::new(page_reader);

                Ok(Box::new(ColumnChunkIterator {
                    reader: Some(Ok(page_reader)),
                }))
            }
        }
    }

    fn row_groups(&self) -> Box<dyn Iterator<Item=&RowGroupMetaData> + '_> {
        Box::new(std::iter::once(self.metadata.row_group(self.row_group_idx)))
    }

    fn metadata(&self) -> &ParquetMetaData {
        self.metadata
    }
}

/// Implements [`PageIterator`] for a single column chunk, yielding a single [`PageReader`]
struct ColumnChunkIterator {
    reader: Option<crate::errors::Result<Box<dyn PageReader>>>,
}

impl Iterator for ColumnChunkIterator {
    type Item = crate::errors::Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for ColumnChunkIterator {}
