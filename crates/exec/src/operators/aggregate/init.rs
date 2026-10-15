use super::*;

impl AggregateSink {
    /// An accumulator for `plan`, refusing a shape it cannot emit.
    pub fn new(plan: AggregatePlan) -> Result<Self> {
        Self::with_memory_tracker(
            plan,
            QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new()),
        )
    }

    /// An accumulator that charges owned group/index state to `tracker`.
    pub fn with_memory_tracker(plan: AggregatePlan, tracker: QueryMemoryTracker) -> Result<Self> {
        let expected = plan.groups as usize + plan.aggs.len();
        if plan.aggregate_input_types.len() != plan.aggs.len() {
            return Err(VeinError::internal(format!(
                "aggregate plan describes {} inputs but {} aggregate operations",
                plan.aggregate_input_types.len(),
                plan.aggs.len()
            )));
        }
        if plan.types.len() != expected {
            return Err(VeinError::internal(format!(
                "aggregate plan describes {} output columns but {} types",
                expected,
                plan.types.len()
            )));
        }
        let groups = plan.groups as usize;
        if plan.aggs.contains(&AggOpCode::SingleValue) && groups != 0 {
            return Err(VeinError::internal(
                "SINGLE_VALUE currently requires a global aggregate",
            ));
        }
        let grouping_sets = plan
            .grouping_sets
            .clone()
            .unwrap_or_else(|| vec![(0..plan.groups).collect()]);
        if grouping_sets.is_empty() {
            return Err(VeinError::internal(
                "aggregate plan must contain at least one grouping set",
            ));
        }
        for grouping_set in &grouping_sets {
            let mut seen = Vec::with_capacity(grouping_set.len());
            for &column in grouping_set {
                if column as usize >= groups {
                    return Err(VeinError::internal(format!(
                        "aggregate grouping set references key column {column}, but only {groups} key columns exist"
                    )));
                }
                if seen.contains(&column) {
                    return Err(VeinError::internal(format!(
                        "aggregate grouping set repeats key column {column}"
                    )));
                }
                seen.push(column);
            }
        }
        let mut grouping_sets_by_column = vec![Vec::new(); groups];
        for (set_id, grouping_set) in grouping_sets.iter().enumerate() {
            for &column in grouping_set {
                grouping_sets_by_column[column as usize].push(set_id);
            }
        }
        let aggregate_states = plan
            .aggs
            .iter()
            .zip(plan.aggregate_input_types.iter())
            .zip(plan.types.iter().skip(groups))
            .map(|((op, input_type), result_type)| {
                AggregateStateColumn::new(*op, input_type, result_type)
            })
            .collect::<Result<Vec<_>>>()?;
        let spill_schema = AggregateSpillSchema::try_new(
            plan.types[..groups].to_vec(),
            grouping_sets
                .iter()
                .map(|set| set.iter().map(|&column| column as usize).collect())
                .collect(),
        )?;
        let group_ids_by_set = (0..grouping_sets.len()).map(|_| Vec::new()).collect();
        let mut memory = tracker.reserve(0, "aggregate state")?;
        let mut index = GroupIndex::default();
        let group_index_reserve_ns = plan
            .estimated_groups
            .and_then(|groups| usize::try_from(groups).ok())
            .map(|groups| {
                let bytes = groups
                    .checked_mul(size_of::<GroupIndexEntry>())
                    .ok_or_else(|| VeinError::internal("aggregate index size overflow"))?;
                memory.grow(bytes, "aggregate group index")?;
                index.reserve(groups)
            })
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            plan,
            metrics: AggregateMetrics {
                grouping_sets: grouping_sets.len() as u64,
                group_index_reserve_ns,
                ..AggregateMetrics::default()
            },
            grouping_sets,
            grouping_sets_by_column,
            index,
            hash_builder: DefaultHashBuilder::default(),
            group_hashers: Vec::new(),
            group_hashes: Vec::new(),
            batch_group_ids: Vec::new(),
            dictionary_ids: Vec::new(),
            dictionary_group_slots: Vec::new(),
            batch_new_groups: Vec::new(),
            group_key_batches: Vec::new(),
            group_key_batch_leases: Vec::new(),
            dictionary_first_rows: Vec::new(),
            group_ids_by_set,
            groups: Vec::new(),
            aggregate_states,
            memory,
            tracker,
            spill_schema,
            spill_runs: Vec::new(),
        })
    }
}
