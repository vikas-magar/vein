use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::DataType;
use common::protocol::{MicroOp, MicroProgram};

use super::*;

#[test]
fn not_like_residual_reads_declared_utf8_slots_independent_of_join_output() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table
        .insert(DataChunk::from_vectors(vec![
            ints(&[1, 2]),
            Arc::new(StringArray::from(vec![Some("%blocked%"), Some("%blocked%")])),
        ]))
        .expect("insert build keys and residual patterns");

    let output = JoinFilterCollector::default();
    let program = Arc::new(MicroProgram::new(
        3,
        vec![
            MicroOp::Like {
                dst: 2,
                src: 0,
                pattern: 1,
                negated: true,
            },
            MicroOp::FilterMask { bool_reg: 2 },
        ],
    ));
    let vm = VectorVmExecutor::<DataChunkView>::new_view(
        program,
        Arc::new(FunctionRegistry::new()),
        Box::new(output.clone()),
    );
    let sink = Collect::default();
    let mut join = HashJoin::new(
        Arc::new(table),
        JoinType::Left,
        vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
        vec![DataType::Int64, DataType::Utf8],
        vec![DataType::Int64, DataType::Utf8],
        Box::new(sink.clone()),
    )
        .with_residual_filter(
            vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
            Box::new(vm),
            output,
        );

    join.push(DataChunk::from_vectors(vec![
        ints(&[1, 2]),
        Arc::new(StringArray::from(vec![Some("visible"), Some("blocked item")])),
    ]))
        .expect("probe rows through the residual predicate");
    join.flush().expect("flush left join");

    let chunks = sink.0.lock().expect("collect lock");
    let mut result = chunks
        .iter()
        .flat_map(|chunk| selected_cells(chunk, 0).into_iter().zip(selected_cells(chunk, 1)))
        .collect::<Vec<_>>();
    result.sort_unstable();
    assert_eq!(result, vec![(Some(1), Some(1)), (Some(2), None)]);
}
