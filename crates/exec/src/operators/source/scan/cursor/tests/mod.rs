use super::*;
use crate::pipeline::resumable::BatchProcessor;
use crate::pipeline::resumable::{OperatorStep, ResumableOperator};
use crate::pipeline::test_support::dense_view;
use arrow::array::{ArrayRef, Int64Array};
use common::Result;
use common::cancellation::CancellationToken;
use futures::{channel::oneshot, task::{ArcWake, waker}};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

mod delivery;

#[derive(Default)]
struct Counter(AtomicUsize);
impl ArcWake for Counter {
    fn wake_by_ref(this: &Arc<Self>) { this.0.fetch_add(1, Ordering::SeqCst); }
}

fn view() -> DataChunkView {
    dense_view(vec![Arc::new(Int64Array::from(vec![7, 11])) as ArrayRef])
}

struct IdentityProcessor;

impl BatchProcessor for IdentityProcessor {
    fn process(&mut self, input: DataChunkView) -> Result<Option<DataChunkView>> {
        Ok(Some(input))
    }
    fn fork(&self) -> Result<Box<dyn BatchProcessor>> { Ok(Box::new(Self)) }
}

#[test]
fn pending_io_wakes_the_scheduler_and_preserves_the_output_buffer() {
    let (send, receive) = oneshot::channel();
    let cursor = Box::pin(futures::stream::once(async move { Ok(receive.await.unwrap()) }));
    let mut stage = ScanStage::new(cursor, CancellationToken::new());
    let OperatorStep::Blocked(ready) = stage.step().unwrap() else { panic!("expected I/O wait") };
    let value = view();
    let pointer = value.resolve(0, 0).unwrap().unwrap().0.to_data().buffers()[0].as_ptr();
    send.send(value).unwrap();
    let count = Arc::new(Counter::default());
    let _registration = ready.register(waker(Arc::clone(&count)));
    assert_eq!(count.0.load(Ordering::SeqCst), 1, "wake before subscription must survive");
    let OperatorStep::Output(output) = stage.step().unwrap() else { panic!("expected output") };
    assert_eq!(output.resolve(0, 0).unwrap().unwrap().0.to_data().buffers()[0].as_ptr(), pointer);
    assert!(matches!(stage.step().unwrap(), OperatorStep::Finished));
    assert!(stage.step().is_err());
}

#[test]
fn fused_scan_processor_keeps_the_source_array_owner() {
    let input = view();
    let original = input.column_view(0).unwrap().source_arrays()[0].clone();
    let cursor = Box::pin(futures::stream::once(async move { Ok(input) }));
    let mut stage = ScanStage::new(cursor, CancellationToken::new())
        .with_processor(Box::new(IdentityProcessor));
    let OperatorStep::Output(output) = stage.step().unwrap() else { panic!("expected processed output") };
    assert!(Arc::ptr_eq(output.column_view(0).unwrap().source_arrays().first().unwrap(), &original));
}

#[test]
fn an_old_registration_cannot_remove_the_new_subscriber() {
    let ready = ScanReady::default();
    let first = Arc::new(Counter::default());
    let second = Arc::new(Counter::default());
    let old = ready.register(waker(Arc::clone(&first)));
    let current = ready.register(waker(Arc::clone(&second)));
    drop(old);
    ready.before_poll().wake();
    assert_eq!(first.0.load(Ordering::SeqCst), 0);
    assert_eq!(second.0.load(Ordering::SeqCst), 1);
    drop(current);
    ready.before_poll().wake();
    assert_eq!(second.0.load(Ordering::SeqCst), 1);
}

#[test]
fn cancellation_drops_pending_io_without_polling_it_again() {
    let (send, receive) = oneshot::channel::<DataChunkView>();
    let cancel = CancellationToken::new();
    let cursor = Box::pin(futures::stream::once(async move { Ok(receive.await.unwrap()) }));
    let mut stage = ScanStage::new(cursor, cancel.clone());
    assert!(matches!(stage.step().unwrap(), OperatorStep::Blocked(_)));
    cancel.cancel();
    assert!(matches!(stage.step(), Err(common::VeinError::Cancelled)));
    assert!(send.send(view()).is_err());
}
