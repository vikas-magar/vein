use std::sync::Arc;
use std::task::Wake;
use tokio::sync::watch;

/// Versioned broadcast readiness for native workers and cooperative hosts.
pub(super) struct Signal {
    generation: watch::Sender<u64>,
}
impl Default for Signal {
    fn default() -> Self { Self { generation: watch::channel(0).0 } }
}
impl Signal {
    pub fn generation(&self) -> u64 { *self.generation.borrow() }

    pub async fn changed(&self, generation: u64) {
        let mut receiver = self.generation.subscribe();
        loop {
            if *receiver.borrow_and_update() != generation { return; }
            if receiver.changed().await.is_err() { return; }
        }
    }

    fn notify(&self) {
        self.generation.send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}
impl Wake for Signal {
    fn wake(self: Arc<Self>) { self.notify(); }
    fn wake_by_ref(self: &Arc<Self>) { self.notify(); }
}

#[cfg(test)]
mod tests;
