use super::super::ParallelismCapability;
use super::super::executable::ExecutablePipelinePlan;
use super::core::{Core, State, Task, TaskState};
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::task::Waker;

impl<G> Core<G> {
    pub(super) fn new(plan: &ExecutablePipelinePlan, workers: NonZeroUsize, notify: Waker) -> Self {
        let count = plan.pipelines().len();
        let mut prerequisites = vec![0; count];
        let mut dependents = vec![Vec::new(); count];
        for edge in plan.graph().dependencies() {
            prerequisites[edge.dependent.0] += 1;
            dependents[edge.prerequisite.0].push(edge.dependent.0);
        }
        let mut tasks = Vec::new();
        let mut ready = VecDeque::new();
        let mut pipeline_tasks = vec![Vec::new(); count];
        let mut remaining_workers = vec![0; count];
        for pipeline in plan.pipelines() {
            let id = pipeline.id().0;
            let width = if pipeline.parallelism() == ParallelismCapability::Serial {
                1
            } else {
                workers.get()
            };
            remaining_workers[id] = width;
            for _ in 0..width {
                let index = tasks.len();
                pipeline_tasks[id].push(index);
                let state = if prerequisites[id] == 0 {
                    ready.push_back(index);
                    TaskState::Ready
                } else {
                    TaskState::Dependencies
                };
                tasks.push(Task {
                    definition: Arc::clone(pipeline),
                    state,
                    executor: None,
                    token: None,
                    registration: None,
                    generation: 0,
                    notified: false,
                });
            }
        }
        tracing::debug!(target: "vein.audit", event = "resumable_scheduler_created",
            pipelines = count, tasks = tasks.len(), requested_workers = workers.get(),
            "resumable scheduler created");
        Self {
            notify,
            failure_observer: std::sync::OnceLock::new(),
            state: Mutex::new(State {
                tasks,
                ready,
                pipeline_tasks,
                remaining_workers,
                prerequisites,
                dependents,
                completed: 0,
                terminal: None,
                failure: None,
            }),
        }
    }
}
