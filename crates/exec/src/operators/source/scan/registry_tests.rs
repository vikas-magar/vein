use async_trait::async_trait;
use catalog::DataFileFormatId;
use common::Result;
use error::VeinError;

use crate::pipeline::PipelineNode;

use super::{ScanProvider, ScanProviderRegistry, ScanRequest};

struct NoopProvider(DataFileFormatId);

#[async_trait]
impl ScanProvider for NoopProvider {
    fn format_id(&self) -> &DataFileFormatId {
        &self.0
    }

    async fn execute(&self, _request: ScanRequest<'_>, _next: &mut dyn PipelineNode) -> Result<()> {
        Ok(())
    }
}

#[test]
fn default_registry_is_empty_until_the_composition_root_registers_providers() {
    let registry = ScanProviderRegistry::default();

    assert!(registry.is_empty());
    assert_eq!(registry.len(), 0);
}

#[test]
fn registry_rejects_duplicate_format_providers() {
    let id = DataFileFormatId::new("mock");
    let mut registry = ScanProviderRegistry::empty();
    registry
        .register(NoopProvider(id.clone()))
        .expect("first provider registration");

    let error = registry
        .register(NoopProvider(id))
        .expect_err("duplicate registration must fail");

    assert!(matches!(error, VeinError::InvalidInput(_)));
}

#[test]
fn registry_reports_unregistered_formats_as_unsupported() {
    let Err(error) = ScanProviderRegistry::empty().resolve(&DataFileFormatId::new("unknown"))
    else {
        panic!("unknown format must not select a provider");
    };

    assert!(matches!(error, VeinError::Unsupported { .. }));
}
