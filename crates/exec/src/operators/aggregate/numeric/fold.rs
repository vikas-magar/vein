//! Selected-row group mapping and shared accumulator update helpers.

use super::*;

/// Shared selected-row fold, monomorphized for a kernel's accumulator type.
pub(in crate::operators::aggregate) fn update_numeric_states<T: AggregateValue>(
    states: &mut [NumericAccumulator<T>],
    group_rows: GroupRows<'_>,
    value_at: impl Fn(usize) -> Result<Option<T>>,
) -> Result<()> {
    for (slot, row) in group_rows {
        states[slot].update(value_at(row)?)?;
    }
    Ok(())
}

/// Zero-allocation view pairing per-row group IDs with the input selection.
#[derive(Clone, Copy)]
pub(in crate::operators::aggregate) struct GroupRows<'a> {
    group_ids: &'a [usize],
    selection: &'a SelectionVector,
    position: usize,
}

impl<'a> GroupRows<'a> {
    pub(in crate::operators::aggregate) fn new(group_ids: &'a [usize], selection: &'a SelectionVector) -> Self {
        debug_assert_eq!(group_ids.len(), selection.size());
        Self {
            group_ids,
            selection,
            position: 0,
        }
    }
}

impl Iterator for GroupRows<'_> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let position = self.position;
        let group_id = *self.group_ids.get(position)?;
        self.position += 1;
        Some((group_id, self.selection.get_index(position)))
    }
}

pub(in crate::operators::aggregate) fn numeric_values<T: Copy>(states: &[NumericAccumulator<T>]) -> Vec<Option<T>> {
    states.iter().map(|state| state.value).collect()
}

pub(in crate::operators::aggregate) fn average_values<T: AggregateValue>(states: &[NumericAccumulator<T>]) -> Vec<Option<f64>> {
    states
        .iter()
        .map(|state| {
            (state.count > 0).then(|| {
                state
                    .value
                    .expect("a non-empty average has a sum")
                    .to_f64(state.scale)
                    / state.count as f64
            })
        })
        .collect()
}
