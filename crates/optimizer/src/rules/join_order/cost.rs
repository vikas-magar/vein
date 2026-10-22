/// Cost information for one input to a join-cost model.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JoinCostInput {
    /// Estimated rows produced by this input.
    pub rows: f64,
    /// Cumulative cost of joins below this input.
    pub cumulative_cost: f64,
}

/// Extension point for comparing legal join trees.
///
/// Return a finite, non-negative cumulative cost. Returning None declines
/// to make a choice and causes the optimizer to preserve the original order.
pub trait JoinCostModel: Send + Sync {
    /// Combines child costs with the estimated output cardinality.
    fn combine(
        &self,
        left: JoinCostInput,
        right: JoinCostInput,
        output_rows: f64,
    ) -> Option<f64>;

    /// Exposes the local cost term when the model can decompose its result.
    ///
    /// Custom models may leave this unknown while still participating in
    /// planning; diagnostic inputs and the cumulative result remain available.
    fn local_cost(
        &self,
        _left: JoinCostInput,
        _right: JoinCostInput,
        _output_rows: f64,
        _cumulative_cost: f64,
    ) -> Option<f64> {
        None
    }
}

/// DuckDB-style cumulative cardinality cost: sum each intermediate output.
#[derive(Debug, Default, Clone, Copy)]
pub struct CumulativeCardinalityCost;

impl JoinCostModel for CumulativeCardinalityCost {
    fn combine(
        &self,
        left: JoinCostInput,
        right: JoinCostInput,
        output_rows: f64,
    ) -> Option<f64> {
        let cost = left.cumulative_cost + right.cumulative_cost + output_rows;
        (left.rows.is_finite()
            && right.rows.is_finite()
            && output_rows.is_finite()
            && cost.is_finite()
            && left.rows >= 0.0
            && right.rows >= 0.0
            && output_rows >= 0.0
            && cost >= 0.0)
            .then_some(cost)
    }

    fn local_cost(
        &self,
        _left: JoinCostInput,
        _right: JoinCostInput,
        output_rows: f64,
        _cumulative_cost: f64,
    ) -> Option<f64> {
        Some(output_rows)
    }
}
