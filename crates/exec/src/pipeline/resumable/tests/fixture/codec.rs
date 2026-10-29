use super::*;
use crate::pipeline::resumable::OperatorConfigurationCodec;

pub(in crate::pipeline::resumable::tests) struct SourceCodec;

impl OperatorConfigurationCodec for SourceCodec {
    type Configuration = Config;
    fn version(&self) -> u32 { 1 }
    fn encode(&self, configuration: &Config) -> Result<Arc<[u8]>> {
        let array = configuration.array.as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| VeinError::invalid_input("test codec expects Int64"))?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(configuration.combined_value as u64).to_le_bytes());
        for value in array.values() { bytes.extend_from_slice(&value.to_le_bytes()); }
        Ok(bytes.into())
    }
    fn decode(&self, version: u32, payload: &[u8]) -> Result<Config> {
        if version != self.version() || payload.len() < 8 || payload.len() % 8 != 0 {
            return Err(VeinError::invalid_input("invalid test source configuration"));
        }
        let mut chunks = payload.chunks_exact(8);
        let combined_value = u64::from_le_bytes(chunks.next().unwrap().try_into().unwrap());
        let combined_value = usize::try_from(combined_value)
            .map_err(|_| VeinError::invalid_input("test local state exceeds addressable range"))?;
        let values = chunks.map(|bytes| i64::from_le_bytes(bytes.try_into().unwrap())).collect::<Vec<_>>();
        Ok(Config { array: Arc::new(Int64Array::from(values)), combined_value })
    }
}
