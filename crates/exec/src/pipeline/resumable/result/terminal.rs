use super::core::{Core, State, Terminal};
use common::{Result, VeinError};

impl Core {
    pub fn ensure_open(state: &State) -> Result<()> {
        match state.terminal {
            Some(Terminal::Closed) => Err(VeinError::invalid_input("result buffer is closed")),
            Some(Terminal::Cancelled) => Err(VeinError::cancelled()),
            Some(Terminal::Failed) => Err(VeinError::internal("result stream failed")),
            None => Ok(()),
        }
    }
    pub fn terminate(&self, terminal: Terminal, error: Option<VeinError>) {
        let error = error.map(|error| match self.query.get() {
            Some(query) => VeinError::from_shared(query.record_failure(error)),
            None => error,
        });
        let retired = {
            let mut state = self.lock();
            if state.terminal.is_some() { return; }
            state.terminal = Some(terminal);
            state.error = error;
            let entries = state.producers.iter_mut()
                .flat_map(|slot| std::mem::take(&mut slot.queue)).collect::<Vec<_>>();
            for entry in &entries { entry.footprint.release(&mut state); }
            state.queued = 0;
            state.arrivals.clear();
            entries
        };
        // Payload leases, user-owned arrays and callbacks are released outside locks.
        drop(retired);
        self.notify_ready();
        if let Some(query) = self.query.get() {
            match terminal {
                Terminal::Closed => query.close_query(),
                _ if !query.cancellation().is_cancelled() => query.cancel(),
                _ => {}
            }
        }
    }
}
