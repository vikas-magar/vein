use crate::pipeline::DataChunkView;
use common::{Result, VeinError};

/// Shared row-window accounting for recursive and resumable LIMIT adapters.
pub(super) struct LimitWindow {
    offset: u64,
    count: u64,
    rows_seen: u64,
    rows_emitted: u64,
}

impl LimitWindow {
    pub(super) fn new(offset: u64, count: u64) -> Self {
        Self { offset, count, rows_seen: 0, rows_emitted: 0 }
    }

    pub(super) fn is_complete(&self) -> bool { self.rows_emitted >= self.count }

    pub(super) fn apply(&mut self, input: DataChunkView) -> Result<Option<DataChunkView>> {
        if self.is_complete() || input.num_rows() == 0 { return Ok(None); }
        let rows = u64::try_from(input.num_rows())
            .map_err(|_| VeinError::internal("LIMIT input row count exceeds u64"))?;
        let start = self.offset.saturating_sub(self.rows_seen).min(rows) as usize;
        let available = input.num_rows().saturating_sub(start);
        let remaining = self.count.saturating_sub(self.rows_emitted);
        let take = available.min(usize::try_from(remaining).unwrap_or(usize::MAX));
        self.rows_seen = self.rows_seen.saturating_add(rows);
        self.rows_emitted = self.rows_emitted.saturating_add(u64::try_from(take)
            .map_err(|_| VeinError::internal("LIMIT output row count exceeds u64"))?);
        if take == 0 { return Ok(None); }
        let selection = input.selection().slice(start, take);
        input.into_selection(selection).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::LimitWindow;
    use crate::pipeline::test_support::dense_view;
    use arrow::array::Int64Array;
    use std::sync::Arc;

    #[test]
    fn saturating_window_counts_do_not_overflow() {
        let mut window = LimitWindow::new(u64::MAX, u64::MAX);
        let array: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        assert!(window.apply(dense_view(vec![array])).unwrap().is_none());
        assert!(!window.is_complete());
    }
}
