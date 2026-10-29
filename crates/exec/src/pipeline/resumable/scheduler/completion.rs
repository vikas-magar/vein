use super::core::TaskState;
use super::lease::Lease;

impl<G> Lease<G> {
    pub fn complete(&mut self) -> bool {
        let mut state = self.core.lock();
        self.committed = true;
        if state.terminal.is_some() { return false; }
        state.tasks[self.id].state = TaskState::Finished;
        let pipeline = self.definition.id().0;
        state.remaining_workers[pipeline] -= 1;
        if state.remaining_workers[pipeline] != 0 { return false; }
        state.completed += 1;
        let workers = state.pipeline_tasks[pipeline].len();
        let dependents = state.dependents[pipeline].len();
        for index in 0..state.dependents[pipeline].len() {
            let dependent = state.dependents[pipeline][index];
            state.prerequisites[dependent] -= 1;
            if state.prerequisites[dependent] == 0 {
                for worker in 0..state.pipeline_tasks[dependent].len() {
                    let id = state.pipeline_tasks[dependent][worker];
                    debug_assert!(state.tasks[id].state == TaskState::Dependencies);
                    state.tasks[id].state = TaskState::Ready;
                    state.ready.push_back(id);
                }
            }
        }
        drop(state);
        tracing::debug!(target: "vein.audit", event = "resumable_pipeline_completed",
            pipeline_id = pipeline, workers, dependents, "resumable pipeline completed");
        true
    }
}
