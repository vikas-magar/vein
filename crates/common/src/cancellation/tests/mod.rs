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
mod subscriptions;
