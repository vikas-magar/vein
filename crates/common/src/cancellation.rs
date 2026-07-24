use std::sync::Arc;
use tokio::sync::watch;

/// A cancellation token that can be shared across async tasks.
///
/// When `cancel()` is called, all tokens derived from the same root
/// (via `child()` or `Clone`) also report `is_cancelled()`.
#[derive(Clone)]
pub struct CancellationToken {
    inner: Arc<watch::Sender<bool>>,
}

impl std::fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl CancellationToken {
    /// Create a new, non-cancelled token.
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Self {
            inner: Arc::new(tx),
        }
    }

    /// Create a child token sharing cancellation with this token.
    pub fn child(&self) -> Self {
        self.clone()
    }

    /// Returns `true` if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        *self.inner.borrow()
    }

    /// Request cancellation. Idempotent and thread-safe.
    pub fn cancel(&self) {
        self.inner.send_replace(true);
    }

    /// Wait until cancellation is requested.
    pub async fn cancelled(&self) {
        let mut rx = self.inner.subscribe();
        if *rx.borrow() {
            return;
        }
        let _ = rx.changed().await;
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cancellation_basic() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn test_cancellation_idempotent() {
        let token = CancellationToken::new();
        token.cancel();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn test_cancellation_child() {
        let parent = CancellationToken::new();
        let child = parent.child();
        assert!(!child.is_cancelled());
        parent.cancel();
        assert!(child.is_cancelled());
    }

    #[tokio::test]
    async fn test_cancellation_await() {
        let token = CancellationToken::new();
        let token_clone = token.clone();
        let handle = tokio::spawn(async move {
            tokio::time::timeout(std::time::Duration::from_secs(1), token_clone.cancelled())
                .await
                .ok()
        });
        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_child_already_cancelled() {
        let parent = CancellationToken::new();
        parent.cancel();
        let child = parent.child();
        assert!(child.is_cancelled());
    }
}
