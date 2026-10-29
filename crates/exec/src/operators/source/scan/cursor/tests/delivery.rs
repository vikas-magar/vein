use super::*;
use crate::pipeline::PipelineNode;

#[derive(Default)]
struct OneBatch {
    received: Vec<DataChunkView>,
}
#[async_trait::async_trait]
impl PipelineNode for OneBatch {
    async fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.received.push(view);
        Ok(())
    }
    async fn flush(&mut self) -> Result<()> { Ok(()) }
    fn finished(&self) -> bool { !self.received.is_empty() }
}

#[test]
fn early_stop_does_not_poll_the_next_batch() {
    let polls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&polls);
    let cursor = Box::pin(futures::stream::repeat_with(move || {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(view())
    }));
    let mut sink = OneBatch::default();
    futures::executor::block_on(drain(cursor, &mut sink)).unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(sink.received.len(), 1);
}

#[test]
fn empty_batch_is_not_eof_and_errors_are_not_replayed() {
    let cursor = Box::pin(futures::stream::iter([
        Ok(dense_view(vec![Arc::new(Int64Array::from(Vec::<i64>::new()))])),
        Ok(view()), Err(common::VeinError::invalid_input("scan failed")),
    ]));
    let mut stage = ScanStage::new(cursor, CancellationToken::new());
    let OperatorStep::Output(empty) = stage.step().unwrap() else { panic!("empty output") };
    assert_eq!(empty.num_rows(), 0);
    assert!(matches!(stage.step().unwrap(), OperatorStep::Output(_)));
    assert!(stage.step().err().unwrap().to_string().contains("scan failed"));
    assert!(stage.step().is_err());
}
