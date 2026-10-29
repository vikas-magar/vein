use super::{ResultConsumer, core::Terminal};
use crate::runtime::QueryExecutionResources;
use common::{Result, VeinError};
use std::sync::Arc;

impl ResultConsumer {
    pub(crate) fn bind_query(&mut self, scope: Arc<QueryExecutionResources>) -> Result<()> {
        self.core.query.set(Arc::clone(&scope))
            .map_err(|_| VeinError::invalid_input("result buffer already belongs to a query"))?;
        let target = Arc::downgrade(&self.core);
        let subscription = scope.cancellation().on_cancel(move || {
            if let Some(core) = target.upgrade() { core.terminate(Terminal::Cancelled, None); }
        })?;
        self.core.cancellation.set(subscription)
            .map_err(|_| VeinError::internal("result buffer cancellation already registered"))?;
        Ok(())
    }
}
