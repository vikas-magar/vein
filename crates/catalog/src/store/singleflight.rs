use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::MetadataKey;

#[derive(Default)]
pub(super) struct SingleFlight {
    active: Mutex<HashMap<MetadataKey, Arc<Flight>>>,
}

#[derive(Default)]
struct Flight {
    gate: Mutex<()>,
    users: AtomicUsize,
}

struct Lease<'a> {
    owner: &'a SingleFlight,
    key: MetadataKey,
    flight: Arc<Flight>,
}

impl SingleFlight {
    pub(super) fn with_key<T>(&self, key: &MetadataKey, operation: impl FnOnce() -> T) -> T {
        let flight = {
            let mut active = self.active.lock().unwrap_or_else(|error| error.into_inner());
            let flight = Arc::clone(active.entry(key.clone()).or_default());
            flight.users.fetch_add(1, Ordering::Relaxed);
            flight
        };
        let lease = Lease {
            owner: self,
            key: key.clone(),
            flight: Arc::clone(&flight),
        };
        let _guard = flight.gate.lock().unwrap_or_else(|error| error.into_inner());
        let result = operation();
        drop(_guard);
        drop(lease);
        result
    }

    fn release(&self, key: &MetadataKey, flight: &Arc<Flight>) {
        flight.users.fetch_sub(1, Ordering::AcqRel);
        let mut active = self.active.lock().unwrap_or_else(|error| error.into_inner());
        let is_current = active
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, flight));
        if is_current && flight.users.load(Ordering::Acquire) == 0 {
            active.remove(key);
        }
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        self.owner.release(&self.key, &self.flight);
    }
}
