//! One cancellation signal shared by tasks, resources and host readiness.
mod registration;

pub use registration::CancellationRegistration;
use registration::{Callbacks, invoke};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

struct Inner {
    signal: watch::Sender<bool>,
    callbacks: Mutex<Callbacks>,
}

/// Shared, idempotent cancellation with asynchronous and callback subscriptions.
#[derive(Clone)]
pub struct CancellationToken {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl CancellationToken {
    /// Creates one root signal.
    pub fn new() -> Self {
        let (signal, _) = watch::channel(false);
        Self {
            inner: Arc::new(Inner {
                signal,
                callbacks: Mutex::new(Callbacks::default()),
            }),
        }
    }

    /// Shares this root; cancellation propagates in both directions.
    pub fn child(&self) -> Self {
        self.clone()
    }

    /// Tests whether the root has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        *self.inner.signal.borrow()
    }

    /// Cancels the root and invokes subscriptions outside all token locks.
    ///
    /// Callbacks run at most once. On unwind-enabled hosts, callback panics are
    /// logged and remaining callbacks still run. Already-running computation remains
    /// cooperative and releases state at its next execution boundary.
    pub fn cancel(&self) {
        // Publishing can wake external async code: hold no callback lock here.
        self.inner.signal.send_replace(true);
        let retired = {
            let mut callbacks = self
                .inner
                .callbacks
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            std::mem::take(&mut callbacks.entries)
        };
        for (_, callback) in retired {
            invoke(callback);
        }
    }

    /// Waits without polling until this root is cancelled.
    pub async fn cancelled(&self) {
        let mut receiver = self.inner.signal.subscribe();
        if *receiver.borrow() {
            return;
        }
        let _ = receiver.changed().await;
    }

    /// Subscribes cleanup/readiness work, observing cancellation atomically.
    ///
    /// Already-cancelled roots invoke immediately. Dropping the registration
    /// unregisters pending work; a callback already taken by cancel may finish.
    /// Capture weak resource owners to avoid retaining a subscription cycle.
    pub fn on_cancel(
        &self,
        callback: impl FnOnce() + Send + 'static,
    ) -> crate::Result<CancellationRegistration> {
        let mut callback = Some(Box::new(callback) as registration::Callback);
        let id = {
            let mut callbacks = self
                .inner
                .callbacks
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if self.is_cancelled() {
                None
            } else {
                let id = callbacks.next.checked_add(1).ok_or_else(|| {
                    crate::VeinError::internal("cancellation subscription ids exhausted")
                })?;
                callbacks.next = id;
                callbacks.entries.insert(id, callback.take().unwrap());
                Some(id)
            }
        };
        if let Some(callback) = callback {
            invoke(callback);
        }
        Ok(CancellationRegistration {
            owner: Arc::downgrade(&self.inner),
            id,
        })
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
