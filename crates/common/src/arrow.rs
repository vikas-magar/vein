//! Arrow-compatible row selection metadata shared by columnar execution views.
use std::mem::size_of;

/// A mask indicating which physical row indices in a source array are active.
#[derive(Debug, Clone)]
pub struct SelectionVector {
    /// Sparse physical indices. `None` represents a dense range.
    indices: Option<Vec<u32>>,
    /// Physical row at which a dense selection starts.
    offset: usize,
    /// Number of logical rows currently selected.
    size: usize,
}

impl SelectionVector {
    /// Creates a dense selection containing every row in the range `0..size`.
    pub fn new_dense(size: usize) -> Self {
        Self {
            indices: None,
            offset: 0,
            size,
        }
    }

    /// Creates a sparse selection from physical row indices.
    pub fn new_sparse(indices: Vec<u32>) -> Self {
        Self {
            size: indices.len(),
            indices: Some(indices),
            offset: 0,
        }
    }

    /// Returns the physical row for a logical row index.
    #[inline(always)]
    pub fn get_index(&self, logical_idx: usize) -> usize {
        match &self.indices {
            Some(indices) => indices[logical_idx] as usize,
            None => self.offset + logical_idx,
        }
    }

    /// Number of currently selected rows.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Heap bytes retained by sparse selection indices, including spare capacity.
    pub fn retained_bytes(&self) -> usize {
        self.indices.as_ref().map_or(0, |indices| indices.capacity() * size_of::<u32>())
    }

    /// Returns the active physical rows as a contiguous range, when possible.
    pub fn contiguous_range(&self) -> Option<std::ops::Range<usize>> {
        if self.indices.is_none() {
            return Some(self.offset..self.offset.checked_add(self.size)?);
        }
        let first = *self.indices.as_ref()?.first()? as usize;
        let end = first.checked_add(self.size)?;
        self.indices
            .as_ref()?
            .iter()
            .enumerate()
            .all(|(offset, &index)| index as usize == first + offset)
            .then_some(first..end)
    }

    /// Whether no rows are selected.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Keeps only rows for which `predicate` returns true.
    ///
    /// A dense selection that fully survives remains dense and allocation-free.
    pub fn filter(&self, mut predicate: impl FnMut(usize) -> bool) -> Self {
        let mut surviving: Option<Vec<u32>> = None;
        for i in 0..self.size {
            let row_idx = self.get_index(i);
            if predicate(row_idx) {
                if let Some(surviving) = &mut surviving {
                    surviving.push(row_idx as u32);
                }
            } else if surviving.is_none() {
                let mut kept = Vec::with_capacity(i + 1);
                kept.extend((0..i).map(|j| self.get_index(j) as u32));
                surviving = Some(kept);
            }
        }
        match surviving {
            None => self.clone(),
            Some(surviving) => Self::new_sparse(surviving),
        }
    }

    /// Returns a selection sliced by `offset` and limited to `limit` rows.
    pub fn slice(&self, offset: usize, limit: usize) -> Self {
        if offset >= self.size || limit == 0 {
            return Self::new_dense(0);
        }
        let count = limit.min(self.size - offset);
        match &self.indices {
            None => Self {
                indices: None,
                offset: self.offset + offset,
                size: count,
            },
            Some(_) => {
                let mut sliced = Vec::with_capacity(count);
                for i in offset..(offset + count) {
                    sliced.push(self.get_index(i) as u32);
                }
                Self::new_sparse(sliced)
            }
        }
    }
}

#[cfg(test)]
#[path = "arrow_tests.rs"]
mod tests;
