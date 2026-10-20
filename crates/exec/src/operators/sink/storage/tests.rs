use super::*;
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use common::arrow::SelectionVector;

#[derive(Clone)]
struct RecordingTransaction {
    views: Arc<Mutex<Vec<DataChunkView>>>,
    report: MaterializationReport,
}

impl WriteTransaction for RecordingTransaction {
    fn write_view(&mut self, view: &DataChunkView) -> Result<MaterializationReport> {
        self.views
            .lock()
            .map_err(|_| VeinError::internal("recording transaction mutex poisoned"))?
            .push(view.clone());
        Ok(self.report)
    }

    fn commit(self: Box<Self>) -> Result<WriteReceipt> {
        Ok(WriteReceipt {
            destination: "test".to_string(),
            row_count: 0,
        })
    }
}

fn write_sink(
    nullable: bool,
    transaction: Option<Box<dyn WriteTransaction>>,
) -> WriteSink {
    WriteSink {
        expected_schema: Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            nullable,
        )])),
        transaction: transaction.map(Mutex::new),
        rows_written: 0,
        batches_written: 0,
        write_us: 0,
        materialization: MaterializationReport::default(),
        receipt: None,
    }
}

fn view(source: &ArrayRef, selection: SelectionVector) -> DataChunkView {
    DataChunkView::from_arrays(vec![Arc::clone(source)], source.len(), selection)
        .expect("valid view over source arrays")
}

#[test]
fn write_sink_forwards_sparse_view_without_copying_payloads() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let views = Arc::new(Mutex::new(Vec::new()));
    let report = MaterializationReport {
        gathered_columns: 1,
        gathered_output_buffer_bytes: 16,
        ..MaterializationReport::default()
    };
    let transaction = RecordingTransaction {
        views: Arc::clone(&views),
        report,
    };
    let mut sink = write_sink(false, Some(Box::new(transaction)));
    let selected = view(&source, SelectionVector::new_sparse(vec![3, 1]));

    sink.push(selected).expect("forward selected view");

    assert_eq!(sink.row_count(), 2);
    assert_eq!(sink.batches_written, 1);
    assert_eq!(sink.materialization, report);
    let recorded = views.lock().expect("recorded views lock");
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].num_rows(), 2);
    for (logical_row, expected_row, expected_value) in [(0, 3, 40), (1, 1, 20)] {
        let (array, row) = recorded[0]
            .resolve(0, logical_row)
            .expect("resolve forwarded row")
            .expect("row is not null-extended");
        assert!(Arc::ptr_eq(array, &source));
        assert_eq!(row, expected_row);
        assert_eq!(
            array.as_any().downcast_ref::<Int64Array>().unwrap().value(row),
            expected_value,
        );
    }
}

#[test]
fn write_sink_validates_nullability_on_selected_rows() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None]));
    let selected_null = view(&source, SelectionVector::new_sparse(vec![1]));
    assert!(write_sink(false, None).validate_view(&selected_null).is_err());

    let selected_value = view(&source, SelectionVector::new_sparse(vec![0]));
    assert!(write_sink(false, None).validate_view(&selected_value).is_ok());
}

#[test]
fn write_sink_skips_empty_views_without_opening_a_transaction() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
    let empty = view(&source, SelectionVector::new_sparse(Vec::new()));
    let mut sink = write_sink(false, None);

    sink.push(empty).expect("empty view is a no-op");

    assert_eq!(sink.row_count(), 0);
    assert_eq!(sink.batches_written, 0);
    assert_eq!(sink.materialization, MaterializationReport::default());
}
