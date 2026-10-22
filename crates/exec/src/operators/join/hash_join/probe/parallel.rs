//! Worker-local probing and barrier-time state combination.
use super::*;

impl HashJoin {
    pub(super) fn fork_worker(&self) -> Result<Option<Box<dyn PipelineNode>>> {
        let residual = match &self.residual_transform {
            Some(transform) => {
                let output = JoinFilterCollector::default();
                let Some(transform) = transform.fork_with_output(Box::new(output.clone()))? else {
                    return Ok(None);
                };
                Some((transform, output))
            }
            None => None,
        };
        let Some(next) = self.next.fork_local()? else {
            return Ok(None);
        };
        let columns = self.output_columns.iter().map(output_column).collect();
        let mut local = Self::new(
            Arc::clone(&self.table), self.join_type, columns,
            self.probe_types.clone(), self.build_types.clone(), next,
        );
        local.emit_unmatched_build = false;
        if let Some((transform, output)) = residual {
            let columns = self.residual_view_columns.iter().map(output_column).collect();
            local = local.with_residual_filter(columns, transform, output);
        }
        Ok(Some(Box::new(local)))
    }

    pub(super) fn combine_worker(
        mut self: Box<Self>,
        canonical: &mut dyn PipelineNode,
    ) -> Result<()> {
        let target = canonical.as_any_mut().downcast_mut::<HashJoin>()
            .ok_or_else(|| VeinError::internal("hash join local state has a mismatched target"))?;
        if !Arc::ptr_eq(&self.table, &target.table)
            || self.join_type != target.join_type
            || !same_columns(&self.output_columns, &target.output_columns)
            || !same_columns(&self.residual_view_columns, &target.residual_view_columns)
            || self.residual_transform.is_some() != target.residual_transform.is_some()
            || self.probe_types != target.probe_types
            || self.build_types != target.build_types
        {
            return Err(VeinError::internal("cannot combine hash join states from different plans"));
        }
        if let Some(local) = self.residual_transform.take() {
            let canonical = target.residual_transform.as_mut()
                .ok_or_else(|| VeinError::internal("canonical join residual is missing"))?;
            local.combine_transform(canonical.as_mut())?;
        }
        target.combine_build_matches(&mut self)?;
        self.next.combine_local(target.next.as_mut())?;
        target.probed_rows = target.probed_rows.saturating_add(self.probed_rows);
        target.matched_rows = target.matched_rows.saturating_add(self.matched_rows);
        target.candidate_rows_examined = target.candidate_rows_examined.saturating_add(self.candidate_rows_examined);
        target.max_candidate_batch_rows = target.max_candidate_batch_rows.max(self.max_candidate_batch_rows);
        target.output_batches = target.output_batches.saturating_add(self.output_batches);
        target.output_rows = target.output_rows.saturating_add(self.output_rows);
        target.max_output_batch_rows = target.max_output_batch_rows.max(self.max_output_batch_rows);
        target.probe_us = target.probe_us.saturating_add(self.probe_us);
        target.view_compose_us = target.view_compose_us.saturating_add(self.view_compose_us);
        target.downstream_push_us = target.downstream_push_us.saturating_add(self.downstream_push_us);
        target.residual_us = target.residual_us.saturating_add(self.residual_us);
        target.probe_key_materialization.accumulate(self.probe_key_materialization);
        target.probe_key_materialize_us =
            target.probe_key_materialize_us.saturating_add(self.probe_key_materialize_us);
        Ok(())
    }
}

fn output_column(column: &ViewColumnRef) -> OutputColumn {
    match column.input {
        0 => OutputColumn::Probe(column.column),
        1 => OutputColumn::Build(column.column),
        _ => unreachable!("join output references only probe or build input"),
    }
}

fn same_columns(left: &[ViewColumnRef], right: &[ViewColumnRef]) -> bool {
    left.len() == right.len() && left.iter().zip(right)
        .all(|(left, right)| left.input == right.input && left.column == right.column)
}
