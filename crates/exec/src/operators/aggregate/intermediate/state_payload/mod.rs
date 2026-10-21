//! Versioned framing for opaque aggregate-owned spill state payloads.

use std::sync::Arc;

use common::Result;
use error::VeinError;

const MAGIC: &[u8; 4] = b"AGG\x02";

pub(super) fn encode(states: &[Arc<Vec<u8>>]) -> Result<Vec<u8>> {
    let count = u32::try_from(states.len())
        .map_err(|_| VeinError::internal("too many aggregate states in spill record"))?;
    let capacity = states.iter().try_fold(8_usize, |total, state| {
        total.checked_add(4)?.checked_add(state.len())
    }).ok_or_else(|| VeinError::internal("aggregate spill payload size overflow"))?;
    let mut output = Vec::new();
    output.try_reserve_exact(capacity)
        .map_err(|error| VeinError::internal(format!("cannot reserve aggregate spill payload: {error}")))?;
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&count.to_le_bytes());
    for state in states {
        let len = u32::try_from(state.len())
            .map_err(|_| VeinError::internal("aggregate spill state exceeds format limit"))?;
        output.extend_from_slice(&len.to_le_bytes());
        output.extend_from_slice(state);
    }
    Ok(output)
}

pub(super) fn decode(bytes: &[u8]) -> Result<Vec<Arc<Vec<u8>>>> {
    if bytes.get(..MAGIC.len()) != Some(MAGIC.as_slice()) {
        return Err(VeinError::internal("invalid aggregate spill state header"));
    }
    let mut cursor = MAGIC.len();
    let count = read_u32(bytes, &mut cursor)? as usize;
    let mut states = Vec::new();
    states.try_reserve_exact(count)
        .map_err(|error| VeinError::internal(format!("cannot reserve aggregate spill states: {error}")))?;
    for _ in 0..count {
        let len = read_u32(bytes, &mut cursor)? as usize;
        let end = cursor.checked_add(len)
            .ok_or_else(|| VeinError::internal("aggregate spill state length overflow"))?;
        let state = bytes.get(cursor..end)
            .ok_or_else(|| VeinError::internal("truncated aggregate spill state payload"))?;
        states.push(Arc::new(state.to_vec()));
        cursor = end;
    }
    if cursor != bytes.len() {
        return Err(VeinError::internal("trailing bytes in aggregate spill state payload"));
    }
    Ok(states)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let end = cursor.checked_add(4)
        .ok_or_else(|| VeinError::internal("aggregate spill cursor overflow"))?;
    let value = bytes.get(*cursor..end)
        .ok_or_else(|| VeinError::internal("truncated aggregate spill state header"))?;
    *cursor = end;
    Ok(u32::from_le_bytes(value.try_into().expect("four-byte slice")))
}
