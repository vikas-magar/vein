use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use super::{DEFAULT_CAPACITY, MetadataKey};

type Payload = Arc<dyn Any + Send + Sync>;

pub(super) struct Entries {
    parsed: HashMap<MetadataKey, Payload>,
    order: VecDeque<MetadataKey>,
    capacity: usize,
}

impl Default for Entries {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }
}

impl Entries {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            parsed: HashMap::new(),
            order: VecDeque::new(),
            capacity: if capacity == 0 {
                DEFAULT_CAPACITY
            } else {
                capacity
            },
        }
    }

    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(super) fn len(&self) -> usize {
        self.parsed.len()
    }

    pub(super) fn get<T: Any + Send + Sync>(&self, key: &MetadataKey) -> Option<Arc<T>> {
        self.parsed.get(key)?.clone().downcast::<T>().ok()
    }

    pub(super) fn insert<T: Any + Send + Sync>(
        &mut self,
        key: MetadataKey,
        metadata: Arc<T>,
    ) -> Vec<MetadataKey> {
        self.parsed.retain(|existing, _| !existing.same_object_and_kind(&key));
        self.order.retain(|existing| !existing.same_object_and_kind(&key));
        self.parsed.insert(key.clone(), metadata);
        self.order.push_back(key);

        let mut evicted = Vec::new();
        while self.parsed.len() > self.capacity {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if self.parsed.remove(&oldest).is_some() {
                evicted.push(oldest);
            }
        }
        evicted
    }

    pub(super) fn clear(&mut self) {
        self.parsed.clear();
        self.order.clear();
    }
}
