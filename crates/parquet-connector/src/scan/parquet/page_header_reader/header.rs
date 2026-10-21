use std::io::Cursor;

use parquet::arrow::async_reader::AsyncFileReader;

use super::super::reader::ParquetFileReader;

const INITIAL_PAGE_HEADER_PROBE_BYTES: u64 = 32;
const MAX_PAGE_HEADER_PROBE_BYTES: u64 = 64 * 1024;

pub(super) async fn read_page_header_at(
    reader: &mut ParquetFileReader,
    offset: u64,
    end: u64,
    physical_type: parquet::basic::Type,
) -> std::result::Result<(parquet::file::serialized_reader::PageHeaderInfo, u64), PageHeaderReadError>
{
    let available = end
        .checked_sub(offset)
        .filter(|available| *available > 0)
        .ok_or_else(|| PageHeaderReadError {
            message: "page header offset is outside the column chunk".to_string(),
            probe_bytes: 0,
        })?;
    let max_probe = available.min(MAX_PAGE_HEADER_PROBE_BYTES);
    let mut probe_size = INITIAL_PAGE_HEADER_PROBE_BYTES.min(max_probe);
    let mut probe_bytes_read = 0_u64;
    loop {
        let probe_end = offset
            .checked_add(probe_size)
            .ok_or_else(|| PageHeaderReadError {
                message: "page header probe range overflowed".to_string(),
                probe_bytes: probe_bytes_read,
            })?;
        let bytes =
            reader
                .get_bytes(offset..probe_end)
                .await
                .map_err(|error| PageHeaderReadError {
                    message: error.to_string(),
                    probe_bytes: probe_bytes_read,
                })?;
        probe_bytes_read =
            probe_bytes_read.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let parse_error = match parquet::file::serialized_reader::read_page_header(
            &mut Cursor::new(bytes.as_ref()),
            physical_type,
        ) {
            Ok(header) if header.header_size > 0 && header.header_size <= bytes.len() => {
                return Ok((header, probe_bytes_read));
            }
            Ok(_) => "page header parser returned an invalid header length".to_string(),
            Err(error) => error.to_string(),
        };
        if probe_size >= max_probe {
            return Err(PageHeaderReadError {
                message: parse_error,
                probe_bytes: probe_bytes_read,
            });
        }
        probe_size = probe_size.saturating_mul(2).min(max_probe);
    }
}

#[derive(Debug)]
pub(super) struct PageHeaderReadError {
    pub(super) message: String,
    pub(super) probe_bytes: u64,
}
