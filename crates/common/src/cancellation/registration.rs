use super::Inner;
use std::collections::BTreeMap;
use std::sync::Weak;

pub(super) type Callback = Box<dyn FnOnce() + Send>;
#[derive(Default)]
pub(super) struct Callbacks {
    pub next: u64,
    pub entries: BTreeMap<u64, Callback>,
}

/// Removes a pending cancellation callback on drop.
pub struct CancellationRegistration {
    pub(super) owner: Weak<Inner>,
    pub(super) id: Option<u64>,
}

impl Drop for CancellationRegistration {
    fn drop(&mut self) {
        let retired = self.owner.upgrade().and_then(|owner| {
            let mut callbacks = owner.callbacks.lock().unwrap_or_else(|p| p.into_inner());
            self.id.and_then(|id| callbacks.entries.remove(&id))
        });
        // Captured resource owners may call back into the token from Drop.
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(retired))).is_err() {
            tracing::error!(target: "vein.audit", event = "cancellation_subscription_drop_panicked");
        }
    }
}

pub(super) fn invoke(callback: Callback) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)).is_err() {
        tracing::error!(target: "vein.audit", event = "cancellation_callback_panicked");
    }
}
