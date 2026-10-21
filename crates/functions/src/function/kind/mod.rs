/// Execution environment associated with a runtime-backed user function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeKind {
    /// WebAssembly runtime.
    Wasm,
    /// eBPF runtime.
    Ebpf,
    /// Python runtime.
    Python,
}

/// Semantic category used by the binder and physical planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionKind {
    /// Row-wise or vectorized scalar function.
    Scalar,
    /// Temporal field extractor.
    TemporalExtract,
    /// Temporal shift rewritten to interval arithmetic during binding.
    TemporalShift,
    /// Temporal truncation rewritten to a typed scalar call during binding.
    TemporalTruncate,
    /// Group reduction executed by the aggregate operator.
    Aggregate,
    /// AI operator with planner-specific cost and execution handling.
    AiOperator,
    /// Function executed by an external runtime.
    UserDefined(RuntimeKind),
}
