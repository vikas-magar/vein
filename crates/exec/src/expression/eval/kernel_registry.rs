use std::collections::HashMap;
use std::sync::Arc;

use common::Result;
use error::VeinError;

use super::builtin::builtin_kernels;
use super::kernel::{ExpressionKernel, ExpressionKernelProvider, ExpressionNodeKind};

/// Mutable registry that composes built-in and plugin expression kernels.
pub struct ExpressionKernelRegistry {
    kernels: HashMap<ExpressionNodeKind, Arc<dyn ExpressionKernel>>,
}

impl ExpressionKernelRegistry {
    /// Creates a registry populated with Vein's standard typed-expression kernels.
    pub fn with_builtins() -> Self {
        let mut registry = Self { kernels: HashMap::new() };
        for kernel in builtin_kernels() {
            registry.kernels.insert(kernel.kind(), kernel);
        }
        registry
    }

    /// Registers a new kernel, returning an error if the category is occupied.
    pub fn register(&mut self, kernel: Arc<dyn ExpressionKernel>) -> Result<()> {
        let kind = kernel.kind();
        if self.kernels.contains_key(&kind) {
            return Err(VeinError::internal(format!(
                "an expression kernel is already registered for {kind:?}"
            )));
        }
        self.kernels.insert(kind, kernel);
        Ok(())
    }

    /// Replaces the kernel for a category and returns the previous implementation.
    pub fn replace(
        &mut self,
        kernel: Arc<dyn ExpressionKernel>,
    ) -> Option<Arc<dyn ExpressionKernel>> {
        self.kernels.insert(kernel.kind(), kernel)
    }
}

impl ExpressionKernelProvider for ExpressionKernelRegistry {
    fn kernel(&self, kind: ExpressionNodeKind) -> Option<&dyn ExpressionKernel> {
        self.kernels.get(&kind).map(Arc::as_ref)
    }
}
