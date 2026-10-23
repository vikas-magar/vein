//! Domain requests remain actionable even when cardinality estimates are known.
use super::super::{CheckpointCostModel, CheckpointWork};
use super::*;
use parser::ast::{expr::BinaryOp, pipe_op::JoinType};
use plan::{LogicalExpr as E, LogicalPlan as P};

fn column(id: usize) -> Arc<ResolvedColumn> {
    Arc::new(ResolvedColumn::new(
        ColumnId(id),
        format!("key_{id}"),
        None,
        VeinDataType::Int64,
    ))
}

fn relation(name: &str, rows: u64, columns: &[usize]) -> P {
    let schema = VeinSchema::new(columns.iter().map(|id| column(*id)).collect());
    let table = TableMetadata::new(name, Arc::new(schema.clone())).with_statistics(
        catalog::TableStatistics {
            num_rows: Some(rows),
            ..Default::default()
        },
    );
    P::Scan {
        table: Arc::new(table),
        projection: None,
        filters: vec![],
        schema: Box::new(schema),
    }
}

fn equality(a: usize, b: usize) -> E {
    E::BinaryOp {
        left: Box::new(E::Column(column(a))),
        op: BinaryOp::Eq,
        right: Box::new(E::Column(column(b))),
        return_type: VeinDataType::Boolean,
    }
}

fn domain_join() -> P {
    let left = relation("probe", 100, &[1]);
    let source = relation("measurements", 1000, &[10, 20]);
    let right = P::Aggregate {
        input: Box::new(source),
        grouping_sets: vec![vec![E::Column(column(10))]],
        aggs: vec![E::FunctionCall {
            name: "sum".into(),
            args: vec![E::Column(column(20))],
            is_aggregate: true,
            distinct: false,
            return_type: VeinDataType::Int64,
        }],
        schema: Box::new(VeinSchema::new(vec![column(10), column(99)])),
    };
    let schema = VeinSchema::new(
        left.schema()
            .fields
            .iter()
            .chain(&right.schema().fields)
            .cloned()
            .collect(),
    );
    P::Join {
        join_type: JoinType::Left,
        left: Box::new(left),
        right: Box::new(right),
        on: Some(equality(1, 10)),
        using: vec![],
        schema: Box::new(schema),
    }
}

#[test]
fn unfiltered_known_inputs_can_be_retained_for_domain_reduction() {
    let plan = domain_join();
    let boundary = StatisticsCheckpointPolicy
        .choose_for_test(&plan)
        .expect("domain producer");
    assert_eq!(boundary.producer.table_name(), Some("probe"));
}

#[test]
fn cost_approved_domain_producer_precedes_an_unrelated_cheap_scan() {
    let left = domain_join();
    let right = relation("unrelated", 25, &[30]);
    let schema = VeinSchema::new(
        left.schema()
            .fields
            .iter()
            .chain(&right.schema().fields)
            .cloned()
            .collect(),
    );
    let plan = P::Join {
        join_type: JoinType::Inner,
        left: Box::new(left),
        right: Box::new(right),
        on: Some(equality(1, 30)),
        using: vec![],
        schema: Box::new(schema),
    };
    let boundary = StatisticsCheckpointPolicy
        .choose_for_test(&plan)
        .expect("domain producer");
    assert_eq!(boundary.producer.table_name(), Some("probe"));
}

#[test]
fn domain_requests_do_not_bypass_the_injected_cost_gate() {
    struct Decline;
    impl CheckpointCostModel for Decline {
        fn allows(&self, _: CheckpointWork, _: CheckpointWork) -> bool {
            false
        }
    }
    let policy = StatisticsCheckpointPolicy::with_cost_model(Decline);
    assert!(
        policy
            .choose_boundary(&domain_join(), Arc::new(QueryStatisticsContext::new([])))
            .is_none()
    );
}

#[test]
fn domain_requests_keep_continuation_alive_after_a_cbo_no_improvement() {
    let plan = domain_join();
    assert_eq!(
        optimizer::rules::CheckpointAggregateDomainRule::pending_checkpoints(&plan),
        1
    );
    assert!(super::super::execution::followup_needed(0, &plan).0);
}
