use super::*;

#[test]
fn only_the_current_task_message_completes_its_yield() {
    let mut pending = PendingYield::default();
    let (ticket, mut receiver) = pending.start().unwrap();
    assert!(pending.start().is_err());
    assert!(pending.complete(f64::NAN).is_none());
    assert!(pending.complete(f64::from(ticket) + 0.5).is_none());
    assert_eq!(receiver.try_recv().unwrap(), None);
    pending.complete(f64::from(ticket)).unwrap().send(()).unwrap();
    assert_eq!(receiver.try_recv().unwrap(), Some(()));
    assert!(pending.complete(f64::from(ticket)).is_none());
}

#[test]
fn abandoned_task_messages_and_old_cancellation_cannot_resume_new_yields() {
    let mut pending = PendingYield::default();
    let (old, mut abandoned) = pending.start().unwrap();
    drop(pending.cancel(old));
    assert!(abandoned.try_recv().is_err());
    let (current, mut receiver) = pending.start().unwrap();
    assert!(pending.complete(f64::from(old)).is_none());
    assert!(pending.cancel(old).is_none());
    assert_eq!(receiver.try_recv().unwrap(), None);
    pending.complete(f64::from(current)).unwrap().send(()).unwrap();
    assert_eq!(receiver.try_recv().unwrap(), Some(()));
}

#[test]
fn ticket_exhaustion_is_explicit_and_does_not_alias_an_old_message() {
    let mut pending = PendingYield { next: u32::MAX, active: None };
    assert!(pending.start().is_err());
    assert!(pending.clear().is_none());
}
