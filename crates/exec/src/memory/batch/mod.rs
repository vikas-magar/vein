//! Batch-scoped reservation credit with an exact-charge fallback.
use super::MemoryReservation;
use common::Result;
use error::VeinError;

/// Amortizes shared query-accounting updates without changing committed bytes.
/// Credit is acquired before allocation and unused credit is released on every
/// exit path. An upper bound that does not fit never prevents an exact charge.
pub(crate) struct BatchReservation<'a> {
    reservation: &'a mut MemoryReservation,
    credit: usize,
    bulk: bool,
    resource: &'static str,
}

impl<'a> BatchReservation<'a> {
    pub(crate) fn new(reservation: &'a mut MemoryReservation, resource: &'static str) -> Self {
        Self { reservation, credit: 0, bulk: true, resource }
    }

    pub(crate) fn charge(&mut self, bytes: usize, remaining: usize) -> Result<()> {
        if remaining == 0 {
            return Err(VeinError::internal("batch reservation has no remaining candidates"));
        }
        if self.credit < bytes {
            let exact = bytes - self.credit;
            let requested = if self.bulk {
                bytes.checked_mul(remaining).unwrap_or(bytes).saturating_sub(self.credit)
            } else { exact };
            match self.reservation.grow(requested, self.resource) {
                Ok(()) => self.credit += requested,
                Err(VeinError::ResourceExhausted { .. }) if requested > exact => {
                    self.bulk = false;
                    self.reservation.grow(exact, self.resource)?;
                    self.credit += exact;
                }
                Err(error) => return Err(error),
            }
        }
        self.credit -= bytes;
        Ok(())
    }
}

impl Drop for BatchReservation<'_> {
    fn drop(&mut self) { self.reservation.shrink(self.credit); }
}

#[cfg(test)]
mod tests;
