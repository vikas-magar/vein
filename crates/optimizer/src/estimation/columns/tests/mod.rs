use plan::ColumnId;

use super::*;
use crate::estimation::EstimateConfidence;

fn relation(distinct: f64) -> RelationEstimate<'static> {
    let mut relation = RelationEstimate::new(Estimate::known(
        50.0,
        EstimateSource::Join,
        EstimateConfidence::Model,
    ));
    relation.columns.insert(
        ColumnId(1),
        ColumnEstimate {
            distinct: Some(distinct),
            distinct_is_prior: false,
            null_fraction: Some(0.0),
            min: None,
            max: None,
            synopsis: None,
            synopsis_population: None,
        },
    );
    relation
}

#[test]
fn join_summary_comparison_includes_column_distributions() {
    let first = relation(20.0);
    let equivalent = relation(20.0);
    let different_distribution = relation(10.0);

    assert!(first.has_same_join_summary(&equivalent));
    assert!(!first.has_same_join_summary(&different_distribution));
}
