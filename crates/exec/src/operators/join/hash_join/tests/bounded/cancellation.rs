use super::super::*;

struct StopAfterBatch {
    rows: Arc<Mutex<usize>>,
}
impl PipelineNode for StopAfterBatch {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        *self.rows.lock().unwrap() += view.num_rows();
        Ok(())
    }
    fn flush(&mut self) -> Result<()> { Ok(()) }
    fn finished(&self) -> bool { *self.rows.lock().unwrap() > 0 }
}

#[test]
fn bounded_probe_stops_without_materializing_duplicate_join_product() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    for _ in 0..10 {
        table.insert(crate::pipeline::test_support::dense_view(vec![ints(&[7; 100])])).unwrap();
    }
    let rows = Arc::new(Mutex::new(0));
    let mut join = HashJoin::new(Arc::new(table), JoinType::Inner,
                                 vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
                                 vec![DataType::Int64], vec![DataType::Int64], Box::new(StopAfterBatch { rows: Arc::clone(&rows) }));
    join.push(crate::pipeline::test_support::dense_view(vec![ints(&[7])])).unwrap();
    assert_eq!(*rows.lock().unwrap(), 100, "reuse existing batch geometry for a small probe");
    assert_eq!(join.candidate_rows_examined, 100, "do not construct the other 900 matches");
    assert_eq!(join.max_candidate_batch_rows, 100);
}

#[test]
fn join_checks_single_cardinality_across_candidate_boundaries() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    for _ in 0..2 {
        table.insert(crate::pipeline::test_support::dense_view(vec![ints(&[7])])).unwrap();
    }
    let mut join = HashJoin::new(Arc::new(table), JoinType::Single,
                                 vec![OutputColumn::Build(0)], vec![DataType::Int64], vec![DataType::Int64], Box::new(Collect::default()));
    let error = join.push(crate::pipeline::test_support::dense_view(vec![ints(&[7])])).unwrap_err();
    assert!(error.to_string().contains("scalar subquery returned more than one row"));
    assert_eq!(join.max_candidate_batch_rows, 1);
}
