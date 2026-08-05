use common::identifiers::TaskId;
use common::protocol::{TaskTelemetry, PreemptResponse, MorselTask};
use std::collections::HashMap;

pub struct TaskStatus {
    pub telemetry: TaskTelemetry,
    /// UNIX timestamp of the last telemetry update
    pub last_updated_at: u64,
}

pub struct ElasticityController {
    pub active_tasks: HashMap<TaskId, TaskStatus>,
}

impl Default for ElasticityController {
    fn default() -> Self {
        Self::new()
    }
}

impl ElasticityController {
    pub fn new() -> Self {
        Self { active_tasks: HashMap::new() }
    }

    /// Updates the internal state when telemetry is received
    pub fn update_telemetry(&mut self, t: TaskTelemetry, now: u64) {
        self.active_tasks.insert(t.task_id.clone(), TaskStatus { telemetry: t, last_updated_at: now });
    }

    /// Identifies tasks that are significantly slower than average.
    /// For simplicity, if a task's rows_processed_per_sec is < 1000.0, we flag it as a straggler.
    pub fn evaluate_stragglers(&self) -> Vec<TaskId> {
        self.active_tasks.iter()
            .filter(|(_, status)| status.telemetry.rows_processed_per_sec < 1000.0)
            .map(|(id, _)| id.clone())
            .collect()
    }
    
    /// Mock implementation for reallocating yielded work.
    /// In a full implementation, this splits the `morsels_yielded` into new tasks.
    pub fn reallocate_yielded_work(&mut self, _yielded: PreemptResponse) -> Vec<MorselTask> {
        // Return an empty list for now, we just want the struct interface to compile.
        Vec::new()
    }
}
