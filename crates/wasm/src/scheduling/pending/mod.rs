use common::{Result, VeinError};
use futures::channel::oneshot;

/// Tickets prevent an abandoned yield's queued message from resuming a new one.
#[derive(Default)]
pub(super) struct PendingYield {
    next: u32,
    active: Option<(u32, oneshot::Sender<()>)>,
}

impl PendingYield {
    pub fn start(&mut self) -> Result<(u32, oneshot::Receiver<()>)> {
        if self.active.is_some() {
            return Err(VeinError::internal("browser host already has a pending yield"));
        }
        self.next = self.next.checked_add(1)
            .ok_or_else(|| VeinError::internal("browser yield ticket space exhausted"))?;
        let (sender, receiver) = oneshot::channel();
        self.active = Some((self.next, sender));
        Ok((self.next, receiver))
    }

    pub fn complete(&mut self, ticket: f64) -> Option<oneshot::Sender<()>> {
        if self.active.as_ref().is_some_and(|(active, _)| f64::from(*active) == ticket) {
            self.clear()
        } else { None }
    }

    pub fn cancel(&mut self, ticket: u32) -> Option<oneshot::Sender<()>> {
        self.complete(f64::from(ticket))
    }

    // Return the sender: dropping or sending can wake external code, so the
    // caller releases its RefCell borrow before doing either.
    pub fn clear(&mut self) -> Option<oneshot::Sender<()>> {
        self.active.take().map(|(_, sender)| sender)
    }
}

#[cfg(test)]
mod tests;
