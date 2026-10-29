use super::*;
use functions::FunctionInput;
use std::any::Any;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Observes both codec directions without changing accumulator semantics.
struct ObservedState {
    inner: Box<dyn AggregateAccumulator>,
    codec_calls: Arc<AtomicUsize>,
}

impl AggregateAccumulator for ObservedState {
    fn ensure_groups(&mut self, groups: usize) -> Result<()> {
        self.inner.ensure_groups(groups)
    }
    fn update_batch(&mut self, args: &[FunctionInput<'_>], groups: &[usize]) -> Result<()> {
        self.inner.update_batch(args, groups)
    }
    fn combine_group(
        &mut self,
        target: usize,
        source: &dyn AggregateAccumulator,
        group: usize,
    ) -> Result<()> {
        let source = source.as_any().downcast_ref::<Self>().unwrap();
        self.inner
            .combine_group(target, source.inner.as_ref(), group)
    }
    fn finalize(&self, groups: Range<usize>) -> Result<ArrayRef> {
        self.inner.finalize(groups)
    }
    fn estimated_bytes_per_group(&self) -> usize {
        self.inner.estimated_bytes_per_group()
    }
    fn encode_group(&self, group: usize, output: &mut Vec<u8>) -> Result<()> {
        self.codec_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.encode_group(group, output)
    }
    fn merge_encoded_group(&mut self, group: usize, input: &mut &[u8]) -> Result<()> {
        self.codec_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.merge_encoded_group(group, input)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn observe(sink: &mut AggregateSink, calls: &Arc<AtomicUsize>) {
    let inner = sink.aggregate_states.remove(0);
    sink.aggregate_states.push(Box::new(ObservedState {
        inner,
        codec_calls: Arc::clone(calls),
    }));
}

#[tokio::test]
async fn in_memory_combination_never_invokes_the_spill_codec() {
    let (shared, mut canonical) = shared(plan(None)).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    observe(&mut shared.lock().unwrap(), &calls);
    let mut worker = canonical.fork_local().unwrap().unwrap();
    let local = worker
        .as_any_mut()
        .downcast_mut::<LocalAggregateHandle>()
        .unwrap();
    observe(&mut local.0, &calls);
    worker
        .push(
            DataChunkView::from_dense_arrays(vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), None])),
                Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
            ])
            .unwrap(),
        )
        .await.unwrap();
    worker.combine_local(canonical.as_mut()).unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    let mut target = shared.lock().unwrap();
    assert_eq!(target.group_count(), 3);
    assert_eq!(
        target.group_key_batches.len(),
        1,
        "retain source batches, not one copy of their metadata per group"
    );
    let output = target.finish().await.unwrap();
    let total: i64 = output
        .iter()
        .map(|batch| {
            (0..batch.num_rows())
                .map(|row| {
                    let (values, index) = batch.resolve(1, row).unwrap().unwrap();
                    values
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(index)
                })
                .sum::<i64>()
        })
        .sum();
    assert_eq!(total, 4);
}
