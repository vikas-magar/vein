use super::{ScanReady, ScanRegistration};
use crate::pipeline::resumable::{ErasedWaitReason, scheduler::WaitRegistrar};
use common::Result;
use std::task::Waker;

/// Adds scan I/O readiness to any existing scheduler resource registry.
pub struct ScanWaitRegistrar<R>(pub R);

/// Retains the subscription of the resource that owns the wait.
pub enum ScanWaitRegistration<R> {
    /// Cursor I/O subscription.
    Scan(ScanRegistration),
    /// Subscription owned by the wrapped resource registry.
    Other(R),
}

impl<R: WaitRegistrar> WaitRegistrar for ScanWaitRegistrar<R> {
    type Registration = ScanWaitRegistration<R::Registration>;
    fn register(&self, reason: ErasedWaitReason, waker: Waker) -> Result<Self::Registration> {
        if reason.is::<ScanReady>() {
            Ok(ScanWaitRegistration::Scan(reason.downcast::<ScanReady>()?.register(waker)))
        } else {
            self.0.register(reason, waker).map(ScanWaitRegistration::Other)
        }
    }
}
