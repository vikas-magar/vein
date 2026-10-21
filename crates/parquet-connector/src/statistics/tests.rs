use super::*;
use super::bloom::predicate_may_match_bloom;
use arrow::array::{
    BooleanArray, Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use catalog::{ColumnStatistics, StatisticsComparison, StatisticsPredicate};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowGroupSelection};
use parquet::arrow::ArrowWriter;
use parquet::basic::Type as ParquetType;
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::{ColumnChunkMetaData, PageIndexPolicy, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use types::LogicalLiteral;

mod diagnostics_rollups;
mod unsigned_bounds;
mod row_group_pages;
mod dictionary_predicates;
mod dictionary_pruning;
mod bloom_filters;

fn file_unit(rows: u64, bytes: u64) -> DataUnitStatistics {
    let mut unit = DataUnitStatistics::new(StatisticsUnitKind::File, 0);
    unit.num_rows = Some(rows);
    unit.physical_bytes = Some(bytes);
    unit
}

fn row_group(
    ordinal: usize,
    rows: u64,
    column: ColumnStatistics,
) -> DataUnitStatistics {
    let mut unit = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, ordinal);
    unit.parent = Some(0);
    unit.num_rows = Some(rows);
    unit.compressed_bytes = Some(10);
    unit.uncompressed_bytes = Some(20);
    unit.column_statistics.insert("value".into(), column);
    unit
}

fn int64_bloom_filter(name: &str, values: impl IntoIterator<Item=i64>) -> ParquetBloomFilter {
    let mut filter = Sbbf::new_with_num_of_bytes(4096);
    for value in values {
        filter.insert(&value);
    }
    ParquetBloomFilter::new(
        BloomFilterColumn {
            name: name.to_string(),
            parquet_column_index: 0,
            arrow_type: DataType::Int64,
            parquet_type: ParquetType::INT64,
            type_length: -1,
        },
        filter,
    )
}

fn dictionary_equality(column: &str, literal: LogicalLiteral) -> StatisticsPredicate {
    StatisticsPredicate::Comparison {
        column: column.to_string(),
        comparison: StatisticsComparison::Equal,
        literal,
    }
}

fn write_dictionary_row_groups(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("shipmode", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_max_row_group_row_count(Some(2))
        .build();
    let file = std::fs::File::create(path).expect("create dictionary Parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
        .expect("create dictionary writer");
    for (modes, ids) in [
        (vec!["MAIL", "RAIL"], vec![10, 20]),
        (vec!["SHIP", "AIR"], vec![30, 40]),
    ] {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(modes)),
                Arc::new(Int64Array::from(ids)),
            ],
        )
            .expect("valid dictionary test batch");
        writer.write(&batch).expect("write dictionary test rows");
        writer.flush().expect("finish dictionary row group");
    }
    writer.close().expect("finish dictionary Parquet file");
}

fn dictionary_page_bytes(
    file_bytes: &Bytes,
    file_length: u64,
    column: &ColumnChunkMetaData,
) -> Bytes {
    let range = dictionary_page_range(column, file_length)
        .expect("dictionary page range must be available");
    let start = usize::try_from(range.start).expect("range start fits usize");
    let end = usize::try_from(range.end).expect("range end fits usize");
    file_bytes.slice(start..end)
}

fn equality(column: &str, value: i64) -> StatisticsPredicate {
    StatisticsPredicate::Comparison {
        column: column.to_string(),
        comparison: StatisticsComparison::Equal,
        literal: LogicalLiteral::Int64(value),
    }
}
