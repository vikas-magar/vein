use super::*;
use super::input_batch::AggregateFunctionInputSource;
use functions::FunctionInput;

mod flow;
mod prepare;
mod resolve;
mod retain;
mod update;

#[derive(Clone, Copy)]
struct PreparedAggregateBatch {
    candidate_count: usize,
    dictionary_fast_path: bool,
}

impl AggregateSink {
    pub(super) fn push_group(
        groups: &mut Vec<Group>,
        aggregate_states: &mut [Box<dyn AggregateAccumulator>],
        hash: u64,
        grouping_set_id: usize,
        key_location: Option<GroupKeyLocation>,
    ) -> Result<usize> {
        let slot = groups.len();
        for state in aggregate_states.iter_mut() {
            state.ensure_groups(slot + 1)?;
        }
        groups.push(Group {
            hash,
            grouping_set_id,
            key_location,
        });
        Ok(slot)
    }
}
