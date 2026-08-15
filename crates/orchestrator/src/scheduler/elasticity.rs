use common::identifiers::TaskId;
use common::protocol::{MorselTask, PreemptResponse, TaskTelemetry};
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
        Self {
            active_tasks: HashMap::new(),
        }
    }

    /// Updates the internal state when telemetry is received
    pub fn update_telemetry(&mut self, t: TaskTelemetry, now: u64) {
        self.active_tasks.insert(
            t.task_id.clone(),
            TaskStatus {
                telemetry: t,
                last_updated_at: now,
            },
        );
    }

    /// Identifies tasks that are significantly slower than average.
    /// For simplicity, if a task's rows_processed_per_sec is < 1000.0, we flag it as a straggler.
    pub fn evaluate_stragglers(&self) -> Vec<TaskId> {
        self.active_tasks
            .iter()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_elasticity_controller() {
        let mut controller = ElasticityController::new();
        let fast_task = TaskId::new();
        let slow_task = TaskId::new();

        let fast_telemetry = TaskTelemetry {
            task_id: fast_task.clone(),
            total_morsels_assigned: 100,
            morsels_completed: 100,
            rows_processed_per_sec: 2000.0,
        };
        let slow_telemetry = TaskTelemetry {
            task_id: slow_task.clone(),
            total_morsels_assigned: 100,
            morsels_completed: 10,
            rows_processed_per_sec: 500.0,
        };

        controller.update_telemetry(fast_telemetry, 100);
        controller.update_telemetry(slow_telemetry, 100);

        let stragglers = controller.evaluate_stragglers();
        assert_eq!(stragglers.len(), 1);
        assert_eq!(stragglers[0], slow_task);
    }
}
