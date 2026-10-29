use crate::pipeline::resumable::OperatorConfigurationCodec;
use common::{Result, VeinError};
use std::sync::Arc;

pub(in crate::pipeline::resumable::executable::tests) struct Codec;
impl OperatorConfigurationCodec for Codec {
    type Configuration = usize;
    fn version(&self) -> u32 { 1 }
    fn encode(&self, ports: &usize) -> Result<Arc<[u8]>> { Ok((*ports as u64).to_le_bytes().into()) }
    fn decode(&self, version: u32, payload: &[u8]) -> Result<usize> {
        if version != 1 { return Err(VeinError::invalid_input("unsupported test sink version")); }
        let bytes = payload.try_into().map_err(|_| VeinError::invalid_input("invalid test sink configuration"))?;
        usize::try_from(u64::from_le_bytes(bytes)).map_err(|_| VeinError::invalid_input("test sink ports overflow"))
    }
}
