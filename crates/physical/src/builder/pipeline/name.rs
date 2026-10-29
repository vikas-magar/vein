use plan::LogicalPlan;

pub(super) fn logical_node_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan { .. } => "scan",
        LogicalPlan::ExternalScan { .. } => "external scan",
        LogicalPlan::Intermediate { .. } => "intermediate relation",
        LogicalPlan::Filter { .. } => "filter",
        LogicalPlan::Project { .. } => "projection",
        LogicalPlan::Aggregate { .. } => "aggregate",
        LogicalPlan::Window { .. } => "window",
        LogicalPlan::Join { .. } => "join",
        LogicalPlan::Sort { .. } => "sort",
        LogicalPlan::Limit { .. } => "limit",
        LogicalPlan::Distinct { .. } => "distinct",
        LogicalPlan::SubqueryAlias { .. } => "subquery alias",
        LogicalPlan::Explain { .. } => "explain",
        LogicalPlan::SetOp { .. } => "set operation",
        LogicalPlan::Sample { .. } => "sample",
        LogicalPlan::Assert { .. } => "assert",
        LogicalPlan::Pivot { .. } => "pivot",
        LogicalPlan::Unpivot { .. } => "unpivot",
        LogicalPlan::Fork { .. } => "fork",
        LogicalPlan::Tee { .. } => "tee",
        LogicalPlan::SemanticFilter { .. } => "semantic filter",
        LogicalPlan::SemanticJoin { .. } => "semantic join",
        LogicalPlan::SemanticCalibrate { .. } => "semantic calibration",
        LogicalPlan::Write { .. } => "write",
        LogicalPlan::Call { .. } => "call",
    }
}
