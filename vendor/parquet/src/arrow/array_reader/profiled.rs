//! Optional measurement decorator for every primitive leaf reader.

use super::ArrayReader;
use crate::arrow::arrow_reader::metrics::{ColumnMetrics, ReadPhase};
use crate::errors::Result;
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use std::{any::Any, sync::{Arc, atomic::Ordering}};

pub(super) struct ProfiledArrayReader {
    inner: Box<dyn ArrayReader>,
    metrics: Arc<ColumnMetrics>,
}

impl ProfiledArrayReader {
    pub(super) fn new(inner: Box<dyn ArrayReader>, metrics: Arc<ColumnMetrics>) -> Self {
        Self { inner, metrics }
    }

    fn measure<T>(&mut self, phase: ReadPhase, operation: impl FnOnce(&mut dyn ArrayReader) -> Result<T>) -> Result<T> {
        let inner = self.inner.as_mut();
        self.metrics.measure(phase, || operation(inner))
    }
}

impl ArrayReader for ProfiledArrayReader {
    fn as_any(&self) -> &dyn Any { self }
    fn get_data_type(&self) -> &DataType { self.inner.get_data_type() }
    fn read_records(&mut self, count: usize) -> Result<usize> {
        let result = self.measure(ReadPhase::Read, |reader| reader.read_records(count));
        if let Ok(rows) = &result { self.metrics.rows_read.fetch_add(*rows as u64, Ordering::Relaxed); }
        result
    }
    fn skip_records(&mut self, count: usize) -> Result<usize> {
        let result = self.measure(ReadPhase::Skip, |reader| reader.skip_records(count));
        if let Ok(rows) = &result { self.metrics.rows_skipped.fetch_add(*rows as u64, Ordering::Relaxed); }
        result
    }
    fn consume_batch(&mut self) -> Result<ArrayRef> {
        self.measure(ReadPhase::Materialize, |reader| reader.consume_batch())
    }
    fn get_def_levels(&self) -> Option<&[i16]> { self.inner.get_def_levels() }
    fn get_rep_levels(&self) -> Option<&[i16]> { self.inner.get_rep_levels() }
    fn max_def_level(&self) -> i16 { self.inner.max_def_level() }
}
