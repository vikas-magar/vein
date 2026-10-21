use arrow::datatypes::SchemaRef;
use common::{Result, VeinError};

pub(super) fn decode_schema(bytes: &[u8]) -> Result<SchemaRef> {
    if bytes.is_empty() {
        return Err(VeinError::planning("WRITE target has no planned Arrow schema", None));
    }
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|error| VeinError::planning(format!("invalid WRITE Arrow schema: {error}"), None))?;
    Ok(reader.schema())
}
