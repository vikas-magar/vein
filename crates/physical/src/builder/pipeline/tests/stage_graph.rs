use super::support::{equality, test_column};
use super::super::{DefaultPipelineStageBuilder, PipelineStageBuilder};
use common::protocol::{DataSink, DataSource, PhysicalPlanOperator};
use parser::ast::pipe_op::JoinType;
use plan::schema::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};
use plan::LogicalPlan;
use std::sync::Arc;

#[test]
fn multi_file_scan_keeps_one_canonical_task_and_graph_references() {
    let column = test_column(0, "id");
    let schema = VeinSchema::new(vec![column]);
    let scan = LogicalPlan::Scan {
        table: Arc::new(
            plan::TableMetadata::new("items", Arc::new(schema.clone()))
                .with_format(plan::TableFormat::Parquet)
                .with_files(vec!["first.parquet".into(), "second.parquet".into()]),
        ),
        projection: None,
        filters: Vec::new(),
        schema: Box::new(schema),
    };

    let stage = DefaultPipelineStageBuilder::new()
        .build(&scan)
        .expect("multi-file scan should lower to one stage");
    assert_eq!(stage.tasks().len(), 1);
    assert_eq!(stage.pipeline_graph().pipelines().len(), 1);
    assert!(stage.pipeline_graph().dependencies().is_empty());

    let PhysicalPlanOperator::Sink { input, .. } = &stage.tasks()[0].plan.root.operator else {
        panic!("stage task should retain a canonical sink root");
    };
    let PhysicalPlanOperator::Pipeline { source, .. } = &input.operator else {
        panic!("scan should remain one canonical source pipeline");
    };
    let DataSource::File(scan) = source.as_ref() else {
        panic!("scan source should retain its file set");
    };
    assert_eq!(scan.files, vec!["first.parquet", "second.parquet"]);
    assert!(Arc::ptr_eq(
        &stage.pipeline_graph().pipelines()[0].source.node,
        input
    ));
}

#[test]
fn write_over_recursive_join_preserves_storage_sink_metadata() {
    let left_key = test_column(0, "left_key");
    let left_value = Arc::new(ResolvedColumn::new(
        ColumnId(1),
        "left_value",
        None,
        VeinDataType::Utf8,
    ));
    let left_schema = VeinSchema::new(vec![Arc::clone(&left_key), left_value]);
    let left_scan = LogicalPlan::Scan {
        table: Arc::new(
            plan::TableMetadata::new("left_table", Arc::new(left_schema.clone()))
                .with_format(plan::TableFormat::Parquet)
                .with_files(vec!["left.parquet".to_string()]),
        ),
        projection: None,
        filters: Vec::new(),
        schema: Box::new(left_schema.clone()),
    };

    let right_key = test_column(2, "right_key");
    let right_value = Arc::new(ResolvedColumn::new(
        ColumnId(3),
        "right_value",
        None,
        VeinDataType::Utf8,
    ));
    let right_schema = VeinSchema::new(vec![Arc::clone(&right_key), right_value]);
    let right_scan = LogicalPlan::Scan {
        table: Arc::new(
            plan::TableMetadata::new("right_table", Arc::new(right_schema.clone()))
                .with_format(plan::TableFormat::Parquet)
                .with_files(vec!["right.parquet".to_string()]),
        ),
        projection: None,
        filters: Vec::new(),
        schema: Box::new(right_schema.clone()),
    };

    let output_schema = VeinSchema::new(
        left_schema
            .fields
            .iter()
            .chain(&right_schema.fields)
            .cloned()
            .collect(),
    );
    let join = LogicalPlan::Join {
        join_type: JoinType::Inner,
        left: Box::new(left_scan),
        right: Box::new(right_scan),
        on: Some(equality(left_key, right_key)),
        using: Vec::new(),
        schema: Box::new(output_schema.clone()),
    };
    let write = LogicalPlan::Write {
        input: Box::new(join),
        target: "file:///output/result".to_string(),
        options: vec![
            ("format".to_string(), "parquet".to_string()),
            ("compression".to_string(), "zstd".to_string()),
        ],
        schema: Box::new(output_schema.clone()),
    };

    let stage = DefaultPipelineStageBuilder::new()
        .build(&write)
        .expect("WRITE over a recursively lowered join should build");
    assert_eq!(stage.tasks().len(), 1);
    assert_eq!(stage.pipeline_graph().pipelines().len(), 2);
    assert_eq!(stage.pipeline_graph().dependencies().len(), 1);
    assert_eq!(
        stage.pipeline_graph().execution_order(),
        Some(vec![crate::pipeline_graph::PipelineId(0), crate::pipeline_graph::PipelineId(1)])
    );

    let root = &stage.tasks()[0].plan.root;
    let PhysicalPlanOperator::Sink { input, sink } = &root.operator else {
        panic!("WRITE must lower to an explicit sink root");
    };
    let DataSink::Storage(target) = sink.as_ref() else {
        panic!("WRITE must retain a storage sink instead of a client sink");
    };
    assert_eq!(target.destination_uri, "file:///output/result");
    assert_eq!(target.get_option("format"), Some("parquet"));
    assert_eq!(target.get_option("compression"), Some("zstd"));
    assert_eq!(target.schema.fields, output_schema.fields);
    assert!(!target.schema_ipc.is_empty());
    assert_eq!(root.output_schema.fields, output_schema.fields);
    assert!(matches!(input.operator, PhysicalPlanOperator::Join { .. }));
}
