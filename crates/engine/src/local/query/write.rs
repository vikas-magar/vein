use super::*;

impl LocalQuery {
    /// Runs a transactional write and publishes success only after its commit receipt.
    pub async fn execute_write(self) -> Result<WriteReceipt> {
        let resources = Arc::clone(self.scope.resources());
        resources.begin_query();
        let result = async {
            if !self.is_write || self.tasks.len() != 1 {
                return Err(VeinError::planning("prepared query is not a single-task WRITE", None));
            }
            self.executor.execute_with_resources(&self.tasks[0], Arc::clone(&resources))
                .await?.write_receipt
                .ok_or_else(|| VeinError::internal("WRITE task completed without a commit receipt"))
        }.await;
        match result {
            Ok(receipt) => {
                resources.finish_query(Some(receipt.clone()));
                Ok(receipt)
            }
            Err(error) => Err(VeinError::from_shared(resources.fail_query(error))),
        }
    }
}
