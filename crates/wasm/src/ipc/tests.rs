use std::sync::Arc;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use super::*;

#[test]
fn packets_preserve_rows_nulls_order_and_the_byte_bound() {
    let values = (0..10_000).map(|i| if i % 7 == 0 { None } else { Some(i as i64) }).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
                                     vec![Arc::new(Int64Array::from(values.clone()))]).unwrap();
    let mut offset = 0;
    let mut actual = Vec::new();
    let mut packets = 0;
    let mut encoder = PacketEncoder::new(2048);
    while offset < batch.num_rows() {
        let (bytes, rows) = encoder.encode(&batch, offset).unwrap();
        assert!(bytes.len() <= 2048);
        assert!(rows > 0);
        for packet in StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap() {
            let packet = packet.unwrap();
            let column = packet.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            actual.extend(column.iter());
        }
        offset += rows;
        packets += 1;
    }
    assert!(packets > 2);
    assert_eq!(actual, values);
}

#[test]
fn empty_results_retain_schema_and_oversized_values_fail_explicitly() {
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let empty = RecordBatch::new_empty(Arc::clone(&schema));
    let (bytes, rows) = PacketEncoder::new(1024).encode(&empty, 0).unwrap();
    assert_eq!(rows, 0);
    assert_eq!(StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap().schema(), schema);
    let large = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["x".repeat(4096)]))]).unwrap();
    assert!(PacketEncoder::new(1024).encode(&large, 0).unwrap_err().to_string().contains("exceeds"));
    assert!(PacketEncoder::new(1).encode(&empty, 0).is_err());
    assert!(PacketEncoder::new(1024).encode(&empty, 1).is_err());
}
