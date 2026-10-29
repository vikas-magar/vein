//! Reference-backed aggregate output. Group keys keep their original Arrow arrays.

use super::*;
use crate::pipeline::view::{DerivedColumnBuilder, ViewColumn, ViewMemoryLease};

impl AggregateSink {
    pub(super) async fn finish_in_memory_view_with(
        &mut self,
        batch_rows: NonZeroUsize,
        emit: &mut impl crate::pipeline::delivery::BatchConsumer,
    ) -> Result<()> {
        self.ensure_empty_groups()?;
        for start in (0..self.groups.len()).step_by(batch_rows.get()) {
            let end = start.saturating_add(batch_rows.get()).min(self.groups.len());
            emit.consume(self.output_range_view(start..end)?).await?;
        }
        Ok(())
    }

    pub(super) fn output_range_view(&mut self, range: Range<usize>) -> Result<DataChunkView> {
        self.tracker.check_cancelled()?;
        let started = Instant::now();
        let width = self.plan.types.len();
        let mut columns = Vec::with_capacity(width);
        for column in 0..self.plan.groups as usize {
            columns.push(self.group_key_output(column, range.clone())?);
        }
        for column in self.plan.groups as usize..width {
            let array = self.column_range(column, &self.plan.types[column], range.clone())?;
            columns.push(owned_array_view(array, range.len())?);
        }
        let output = DataChunkView::new(range.len(), columns, SelectionVector::new_dense(range.len()))?;
        self.metrics.output_materialize_ns = self.metrics.output_materialize_ns
            .saturating_add(started.elapsed().as_nanos() as u64);
        Ok(output)
    }

    fn group_key_output(&self, column: usize, range: Range<usize>) -> Result<ViewColumn> {
        let expected = self.plan.types[column].to_arrow();
        if !self.group_key_range_matches(column, range.clone(), &expected)? {
            let array = self.column_range(column, &self.plan.types[column], range.clone())?;
            return owned_array_view(array, range.len());
        }

        let mut builder = DerivedColumnBuilder::new(expected, range.len());
        for group in &self.groups[range] {
            if !self.grouping_sets_by_column[column].contains(&group.grouping_set_id) {
                builder.push(None)?;
                continue;
            }
            let Some(location) = group.key_location else {
                builder.push(None)?;
                continue;
            };
            let Some((array, row)) = self.group_key_source(column, location)? else {
                builder.push(None)?;
                continue;
            };
            let identity = Arc::as_ptr(array) as *const () as usize;
            let lease = self.group_key_batch_leases[location.batch]
                .get(&identity)
                .map(|reservation| {
                    let lease: Arc<dyn ViewMemoryLease> = reservation.clone();
                    lease
                });
            builder.push(Some((array, row, lease.as_ref())))?;
        }
        builder.finish()?.column_view(0)
    }

    fn group_key_range_matches(
        &self,
        column: usize,
        range: Range<usize>,
        expected: &arrow::datatypes::DataType,
    ) -> Result<bool> {
        for group in &self.groups[range] {
            if !self.grouping_sets_by_column[column].contains(&group.grouping_set_id) {
                continue;
            }
            let Some(location) = group.key_location else {
                continue;
            };
            if let Some((array, _)) = self.group_key_source(column, location)?
                && array.data_type() != expected
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn group_key_source(
        &self,
        column: usize,
        location: GroupKeyLocation,
    ) -> Result<Option<(&ArrayRef, usize)>> {
        let key = self.group_key_batches[location.batch][column]
            .as_ref()
            .ok_or_else(|| VeinError::internal("aggregate output group key is missing"))?;
        match key.as_ref() {
            GroupKeyColumn::View(view) => view.resolve(0, location.row),
            _ => self.group_key_batch_arrays[location.batch][column]
                .first()
                .map(|array| (array, location.row))
                .map(Some)
                .ok_or_else(|| VeinError::internal("aggregate key source array is missing")),
        }
    }
}

fn owned_array_view(array: ArrayRef, rows: usize) -> Result<ViewColumn> {
    DataChunkView::from_arrays(
        vec![array],
        rows,
        SelectionVector::new_dense(rows),
    )?.column_view(0).and_then(|column| {
        if column.row_mapping().len() == rows {
            Ok(column)
        } else {
            Err(VeinError::internal("aggregate output array has an unexpected row count"))
        }
    })
}
