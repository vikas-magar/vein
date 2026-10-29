use super::spawn;
use super::run_blocking;

#[tokio::test]
async fn spawn_runs_task_on_the_host_executor() {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    spawn(async move {
        let _ = sender.send(42);
    });

    assert_eq!(receiver.await.expect("spawned task should run"), 42);
}

#[tokio::test]
async fn blocking_work_runs_off_the_native_runtime_thread() {
    let caller = std::thread::current().id();
    let worker = run_blocking(|| Ok(std::thread::current().id())).await.unwrap();
    assert_ne!(caller, worker);
}

#[tokio::test]
async fn blocking_work_preserves_typed_failures() {
    let error = run_blocking(|| Err::<(), _>(crate::VeinError::invalid_input("invalid synopsis")))
        .await.unwrap_err();
    assert!(error.to_string().contains("invalid synopsis"));
}
