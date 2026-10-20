//! Type-safe state shared by tasks belonging to one prepared query.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Hash, PartialEq, Eq)]
struct StateKey {
    scan_id: u128,
    signature: String,
    state_type: TypeId,
}

/// Query-lifetime cache for execution policies shared by independently scheduled tasks.
#[derive(Clone, Default)]
pub struct QueryStateRegistry {
    states: Arc<Mutex<HashMap<StateKey, Arc<dyn Any + Send + Sync>>>>,
}

impl QueryStateRegistry {
    /// Returns the state for one logical scan and policy signature, creating it once.
    pub fn get_or_insert_with<T, F>(
        &self,
        scan_id: u128,
        signature: impl Into<String>,
        create: F,
    ) -> Arc<T>
    where
        T: Any + Send + Sync,
        F: FnOnce() -> T,
    {
        let key = StateKey {
            scan_id,
            signature: signature.into(),
            state_type: TypeId::of::<T>(),
        };
        let mut states = self.states.lock().unwrap_or_else(|poison| poison.into_inner());
        let state = Arc::clone(states.entry(key).or_insert_with(|| Arc::new(create())));
        Arc::downcast(state).unwrap_or_else(|_| unreachable!("state type is part of its key"))
    }
}

#[cfg(test)]
mod tests {
    use super::QueryStateRegistry;
    use std::sync::Arc;

    #[test]
    fn shares_only_matching_scan_signature_and_context() {
        let registry = QueryStateRegistry::default();
        let clone = registry.clone();
        let first = registry.get_or_insert_with(7, "filters-a", || vec![1, 2]);
        let shared = clone.get_or_insert_with(7, "filters-a", || vec![3]);
        let other_signature = clone.get_or_insert_with(7, "filters-b", Vec::<usize>::new);
        let other_scan = clone.get_or_insert_with(8, "filters-a", Vec::<usize>::new);

        assert!(Arc::ptr_eq(&first, &shared));
        assert_eq!(&*shared, &[1, 2]);
        assert!(!Arc::ptr_eq(&first, &other_signature));
        assert!(!Arc::ptr_eq(&first, &other_scan));
    }
}
