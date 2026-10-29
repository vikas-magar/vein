use super::*;

/// Which view inputs an operator may convert to ordinary Arrow arrays.
///
/// Conversion can share contiguous buffers or gather mapped rows. This describes
/// conversion scope, not bytes copied, memory usage, or computed allocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMaterialization {
    /// The registered implementation determines its materialization requirements.
    ImplementationDefined,
    /// Reads row/column references without converting input arrays.
    None,
    /// Converts only columns required by kernels, keys, or residual expressions.
    KernelColumns,
    /// Requires every active input column as an ordinary Arrow array.
    WholeBatch,
    /// Client consumers choose between retaining views and requesting Arrow arrays.
    ConsumerBoundary,
}

/// How an operator represents output payloads in the current single-node runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputRepresentation {
    /// Representation is supplied by a registered implementation.
    ImplementationDefined,
    /// Output rows reference existing payload arrays, including NULL extension.
    References,
    /// Unchanged payloads remain references; computed columns may allocate arrays.
    ReferencesAndComputed,
    /// Produces result arrays which the runtime wraps in a view.
    ResultArrays,
    /// Consumes the batch at the client, storage, or discard boundary.
    Sink,
}

/// Derived data-flow contract of a canonical physical operator.
///
/// This is computed from operator semantics rather than serialized or stored as
/// an independent plan setting. It describes current runtime behavior; it does
/// not claim that hash keys, encoded sort keys, or aggregate state are zero-copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperatorDataFlow {
    /// Stable operator label used in runtime diagnostics.
    pub operator: &'static str,
    /// Scope of input view conversion required by the implementation.
    pub input_materialization: InputMaterialization,
    /// Output payload representation used by the implementation.
    pub output: OutputRepresentation,
}

impl PhysicalPlanOperator {
    /// Derives the view/materialization contract from this canonical operator.
    ///
    /// Kernel conversions are conditional: direct column projections do not need
    /// arrays, and Cross joins with no residual do not need hash-key conversion.
    /// Sort includes any computed-key program. Aggregate and DISTINCT kernels
    /// read only their required columns; grouping keys remain reference-backed.
    /// Storage and client boundaries decide when complete arrays are required.
    pub fn data_flow(&self) -> OperatorDataFlow {
        use InputMaterialization::{ConsumerBoundary, KernelColumns, None, WholeBatch};
        use OutputRepresentation::{References, ReferencesAndComputed, ResultArrays, Sink};

        let (operator, input_materialization, output) = match self {
            Self::Extension { .. } => ("extension", InputMaterialization::ImplementationDefined, OutputRepresentation::ImplementationDefined),
            Self::Pipeline { .. } => ("pipeline", KernelColumns, ReferencesAndComputed),
            Self::Transform { .. } => ("transform", KernelColumns, ReferencesAndComputed),
            Self::Join { .. } => ("join", KernelColumns, References),
            Self::Sort { .. } => ("sort", KernelColumns, References),
            Self::Limit { .. } => ("limit", None, References),
            Self::Aggregate { .. } => ("aggregate", KernelColumns, ResultArrays),
            Self::Distinct { .. } => ("distinct", KernelColumns, ResultArrays),
            Self::Sink { sink, .. } => (
                "sink",
                match sink.as_ref() {
                    DataSink::Client { .. } => ConsumerBoundary,
                    DataSink::Storage(_) => WholeBatch,
                    DataSink::Discard => None,
                },
                Sink,
            ),
        };
        OperatorDataFlow {
            operator,
            input_materialization,
            output,
        }
    }
}
