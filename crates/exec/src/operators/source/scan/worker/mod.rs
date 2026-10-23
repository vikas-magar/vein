//! Format-neutral reader state retained for the lifetime of one scan worker.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::Mutex;

use common::Result;
use error::VeinError;

/// Provider extensions shared by a worker's successive work units.
///
/// Each worker receives a fresh instance. Providers keep format-specific policy
/// here without placing reader types in the execution coordinator. The callback
/// must only access local state; it must not perform I/O or retain the lock.
pub struct ScanWorkerState {
    diagnostic_id: u64,
    extensions: Mutex<HashMap<TypeId, Box<dyn Any + Send>>>,
}

impl Default for ScanWorkerState {
    fn default() -> Self {
        Self {
            diagnostic_id: RandomState::new().build_hasher().finish(),
            extensions: Mutex::default(),
        }
    }
}

impl ScanWorkerState {
    /// Opaque trace tag; never used to validate or share provider state.
    pub fn diagnostic_id(&self) -> u64 {
        self.diagnostic_id
    }

    /// Accesses one provider's typed state, initializing it on first use.
    pub fn with<T: Default + Send + 'static, R>(
        &self,
        update: impl FnOnce(&mut T) -> R,
    ) -> Result<R> {
        let mut extensions = self.extensions.lock()
            .map_err(|_| VeinError::internal("scan worker state mutex poisoned"))?;
        let state = extensions.entry(TypeId::of::<T>())
            .or_insert_with(|| Box::<T>::default())
            .downcast_mut::<T>()
            .ok_or_else(|| VeinError::internal("scan worker state type mismatch"))?;
        Ok(update(state))
    }
}

#[cfg(test)]
mod tests;
