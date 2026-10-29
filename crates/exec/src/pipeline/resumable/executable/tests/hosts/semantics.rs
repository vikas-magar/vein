use super::*;
use super::super::fixtures::*;
use super::super::super::ExecutablePipelinePlan;
use crate::pipeline::resumable::OperatorFactoryRegistry;
use crate::pipeline::test_support::dense_view;
use arrow::array::{Array, ArrayRef, Int64Array};

#[test]
fn cooperative_and_native_hosts_preserve_values_eof_and_payload_identity() {
    for mode in 0..3 {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(3), None, Some(3)]));
        let pointer = array.to_data().buffers()[0].as_ptr();
        let (plan, records) = prepared(vec![
            vec![Action::Work, Action::Output(dense_view(vec![array])), Action::Finish],
            vec![Action::Need, Action::RetryBatch, Action::Work, Action::Need,
                 Action::AcceptBatch, Action::Need, Action::AcceptEof, Action::Work, Action::Finish],
        ]);
        let driver = HostDriver::new(&plan, n(1), n(1), Io::default());
        let terminal = if mode == 0 {
            let mut host = EventHost::default();
            let outcome = futures::executor::block_on(driver.run_cooperative(&mut host)).unwrap();
            assert!(host.yields > 1);
            outcome
        } else {
            #[cfg(not(target_arch = "wasm32"))]
            { driver.run_native(n(mode)).unwrap() }
            #[cfg(target_arch = "wasm32")]
            { continue; }
        };
        assert_eq!(terminal, SchedulerStep::Finished);
        let sink = records[1].lock().unwrap();
        assert_eq!((sink.batches, sink.eof, sink.combined), (1, 1, 1));
        let output = sink.received[0].materialize_column(0).unwrap();
        assert_eq!(output.to_data().buffers()[0].as_ptr(), pointer);
        assert_eq!(output.as_any().downcast_ref::<Int64Array>().unwrap().iter().collect::<Vec<_>>(),
                   vec![Some(3), None, Some(3)]);
        assert_eq!(records[0].lock().unwrap().steps, 3);
    }
}

#[test]
fn both_hosts_finish_every_parallel_local_state_and_dependent_pipeline() {
    for cooperative in [true, false] {
        let provider = Provider { combine: true, ..Default::default() };
        let probe = source(empty());
        let build = source(empty());
        let plan = ExecutablePipelinePlan::prepare(
            &client(join(Arc::clone(&probe), Arc::clone(&build))),
            &OperatorFactoryRegistry::new(), &provider,
        ).unwrap();
        let driver = HostDriver::new(&plan, n(4), n(1), Io::default());
        let terminal = if cooperative {
            futures::executor::block_on(driver.run_cooperative(&mut EventHost::default())).unwrap()
        } else {
            #[cfg(not(target_arch = "wasm32"))]
            { driver.run_native(n(4)).unwrap() }
            #[cfg(target_arch = "wasm32")]
            { continue; }
        };
        assert_eq!(terminal, SchedulerStep::Finished);
        let records = provider.records.lock().unwrap();
        for node in [&probe, &build] {
            let record = &records[&pointer(node)];
            assert_eq!(record.created.lock().unwrap().len(), 4);
            assert_eq!(record.combined.lock().unwrap().len(), 4);
        }
    }
}
