//! Registration and lookup for format-specific scan providers.

use std::collections::HashMap;
use std::sync::Arc;

use catalog::DataFileFormatId;
use common::Result;
use error::VeinError;

use super::ScanProvider;

/// Startup-time registry mapping open data-format identifiers to providers.
#[derive(Clone)]
pub struct ScanProviderRegistry {
    providers: HashMap<DataFileFormatId, Arc<dyn ScanProvider>>,
}

impl ScanProviderRegistry {
    /// Creates an empty registry for custom provider compositions.
    pub fn empty() -> Self {
        Self {
            providers: HashMap::new(),
        }
    }

    /// Registers a provider, rejecting duplicate format identifiers.
    pub fn register<P>(&mut self, provider: P) -> Result<()>
    where
        P: ScanProvider + 'static,
    {
        self.register_shared(Arc::new(provider))
    }

    /// Registers an already type-erased provider implementation.
    pub fn register_shared(&mut self, provider: Arc<dyn ScanProvider>) -> Result<()> {
        let format_id = provider.format_id().clone();
        if self.providers.contains_key(&format_id) {
            return Err(VeinError::invalid_input(format!(
                "a scan provider is already registered for format '{format_id}'"
            )));
        }
        self.providers.insert(format_id, provider);
        Ok(())
    }

    /// Resolves a provider for a scan format.
    pub fn resolve(&self, format_id: &DataFileFormatId) -> Result<Arc<dyn ScanProvider>> {
        self.providers.get(format_id).cloned().ok_or_else(|| {
            VeinError::unsupported(
                format!("no scan provider is registered for format '{format_id}'"),
                None,
            )
        })
    }

    /// Returns the number of registered formats.
    pub fn len(&self) -> usize {
        self.providers.len()
    }

    /// Returns whether this registry has no registered providers.
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}

impl Default for ScanProviderRegistry {
    fn default() -> Self {
        Self::empty()
    }
}
