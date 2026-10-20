use super::*;

#[test]
fn dense_only_udf_rejects_mapped_input_without_gathering_it() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let input = DataChunkView::from_chunk(DataChunk::new(
        vec![source],
        SelectionVector::new_sparse(vec![3, 1]),
    )).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = FunctionRegistry::new();
    registry.register_scalar(Arc::new(IncrementDense(Arc::clone(&calls))));
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut vm = VectorVmExecutor::new_view(
        program("increment_dense"),
        Arc::new(registry),
        Box::new(Capture(Arc::clone(&output))),
    );

    let error = vm.push(input).expect_err("mapped input must not be gathered for a dense UDF");

    assert!(error.to_string().contains("invoke_mapped"));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    assert_eq!(vm.stats.dense_udf_fallback_batches, 0);
    assert!(output.lock().unwrap().is_empty());
}
