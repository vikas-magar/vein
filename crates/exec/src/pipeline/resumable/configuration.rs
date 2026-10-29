use common::Result;
use std::any::Any;
use std::sync::Arc;

/// Typed codec for immutable extension configuration metadata.
///
/// Decoding occurs during preparation, before any pipeline starts. Implementors
/// reject unsupported versions and malformed payloads without allocating query data.
pub trait OperatorConfigurationCodec: Send + Sync {
    /// Concrete configuration consumed by a registered typed factory.
    type Configuration: Any + Send + Sync;
    /// Version written into newly lowered canonical descriptors.
    fn version(&self) -> u32;
    /// Encodes configuration metadata; plan clones share the returned owner.
    fn encode(&self, configuration: &Self::Configuration) -> Result<Arc<[u8]>>;
    /// Decodes and validates one portable configuration payload.
    fn decode(&self, version: u32, payload: &[u8]) -> Result<Self::Configuration>;
}
