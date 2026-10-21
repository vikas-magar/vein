//! Executor-builder implementations and generic scan partition dispatch.

mod default_executor_builder;
mod scan_partition;
mod task_executor_builder;

pub use default_executor_builder::DefaultExecutorBuilder;
pub use task_executor_builder::TaskExecutorBuilder;

#[cfg(test)]
mod tests;
