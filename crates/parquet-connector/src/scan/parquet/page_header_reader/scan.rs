use std::sync::Arc;
use std::time::Instant;

use super::super::reader::ParquetFileReader;
use super::*;

pub(in crate::scan::parquet) async fn read_page_header_statistics(
    request: PageHeaderReadRequest<'_>,
) -> PageHeaderReadResult {
    let PageHeaderReadRequest {
        storage,
        path,
        absolute_path,
        backend,
        file_size_bytes,
        bytes_read,
        io,
        arrow_schema,
        metadata,
        plan,
    } = request;
    let started = Instant::now();
    let mut result = PageHeaderReadResult::default();
    let parquet_schema = metadata.file_metadata().schema_descr();
    let needed_columns = plan.column_names();
    let columns = needed_columns
        .iter()
        .filter_map(|name| {
            let (_, field) = arrow_schema.column_with_name(name)?;
            let converter = parquet::arrow::arrow_reader::statistics::StatisticsConverter::try_new(
                name,
                arrow_schema,
                parquet_schema,
            )
                .ok()?;
            let ordinal = converter.parquet_column_index()?;
            let descriptor = parquet_schema.column(ordinal);
            Some((
                (*name).to_string(),
                ordinal,
                field.data_type().clone(),
                descriptor.physical_type(),
                descriptor.max_rep_level(),
            ))
        })
        .collect::<Vec<_>>();
    result.metrics.columns_requested = u64::try_from(needed_columns.len()).unwrap_or(u64::MAX);
    result.metrics.column_row_group_pairs_requested =
        u64::try_from(plan.pair_count()).unwrap_or(u64::MAX);
    let mut reader = match ParquetFileReader::new(
        storage,
        path,
        absolute_path,
        backend,
        file_size_bytes,
        Arc::clone(bytes_read),
        Arc::clone(io),
    )
        .await
    {
        Ok(reader) => reader,
        Err(error) => {
            result.metrics.failed_chunks = u64::try_from(plan.pair_count()).unwrap_or(u64::MAX);
            tracing::debug!(
                target: "vein.audit",
                event = "parquet_page_header_reader_unavailable",
                source_id = %common::logging::LogLocation::from(path),
                backend = backend.name(),
                error = %error,
                "page-header pruning unavailable; retaining all candidate rows"
            );
            result.metrics.elapsed_us = started.elapsed().as_micros() as u64;
            return result;
        }
    };

    for (row_group_index, requested_columns) in plan.requests() {
        let Some(row_group) = metadata.row_groups().get(*row_group_index) else {
            continue;
        };
        chunk::read_requested_row_group(
            &mut reader,
            path,
            file_size_bytes,
            *row_group_index,
            row_group,
            requested_columns,
            &columns,
            &mut result,
        )
            .await;
    }

    result.metrics.elapsed_us = started.elapsed().as_micros() as u64;
    result
}
