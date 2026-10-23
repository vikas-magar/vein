//! Relative hash-join work units, not predictions of elapsed time.
use super::{JoinCostInput, JoinCostModel};

/// Counts probe hashing, build hashing plus insertion, and output processing.
/// Coefficients are injectable through the existing cost-model extension point.
#[derive(Clone, Copy, Debug)]
pub struct HashJoinWorkCost {
    /// Relative work for hashing and inserting one build row.
    pub build_row: f64,
    /// Relative work for hashing and probing one probe row.
    pub probe_row: f64,
    /// Relative work for processing one matched output row.
    pub output_row: f64,
}

impl Default for HashJoinWorkCost {
    fn default() -> Self {
        Self { build_row: 2.0, probe_row: 1.0, output_row: 1.0 }
    }
}

impl HashJoinWorkCost {
    fn work(&self, left: JoinCostInput, right: JoinCostInput, output: f64) -> Option<f64> {
        if [self.build_row, self.probe_row, self.output_row, left.rows, right.rows, output,
            left.cumulative_cost, right.cumulative_cost]
            .iter().any(|value| !value.is_finite() || *value < 0.0)
        { return None; }
        // Physical inner joins choose their smaller input as the build side.
        let work = left.rows.min(right.rows) * self.build_row
            + left.rows.max(right.rows) * self.probe_row + output * self.output_row;
        work.is_finite().then_some(work)
    }
}

impl JoinCostModel for HashJoinWorkCost {
    fn combine(&self, left: JoinCostInput, right: JoinCostInput, output: f64) -> Option<f64> {
        let cost = self.work(left, right, output)? + left.cumulative_cost + right.cumulative_cost;
        cost.is_finite().then_some(cost)
    }

    fn local_cost(&self, left: JoinCostInput, right: JoinCostInput, output: f64, _: f64) -> Option<f64> {
        self.work(left, right, output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(rows: f64) -> JoinCostInput { JoinCostInput { rows, cumulative_cost: 0.0 } }

    #[test]
    fn accounts_for_inputs_even_when_output_is_empty() {
        let model = HashJoinWorkCost::default();
        assert_eq!(model.combine(input(100.0), input(10.0), 0.0), Some(120.0));
        assert_eq!(model.combine(input(10.0), input(100.0), 0.0), Some(120.0));
        assert!(model.combine(input(1.0), input(1.0), 0.0)
            < model.combine(input(100.0), input(100.0), 0.0));
    }

    #[test]
    fn validates_coefficients_and_overflow() {
        let model = HashJoinWorkCost::default();
        for invalid in [f64::NAN, f64::INFINITY, -1.0] {
            assert_eq!(model.combine(input(invalid), input(1.0), 0.0), None);
            assert_eq!(HashJoinWorkCost { build_row: invalid, ..model }
                           .combine(input(1.0), input(1.0), 0.0), None);
        }
        assert_eq!(model.combine(input(f64::MAX), input(f64::MAX), 0.0), None);
    }
}
