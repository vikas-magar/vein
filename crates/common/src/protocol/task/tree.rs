use super::*;

/// Immutable, schema-aware physical plan tree shared by planning and execution.
///
/// The root is normally a [`PhysicalPlanOperator::Sink`]. Each operator node
/// owns its output schema and references its input nodes through `Arc`, so a
/// plan can be inspected or shared without copying operator subtrees. Mutable
/// execution state such as hash tables and aggregate accumulators belongs to an
/// execution, not to these nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhysicalPlan {
    /// The root operator of the query plan.
    pub root: Arc<PhysicalPlanNode>,
}

impl PhysicalPlan {
    /// Whether preparing this plan requires registered physical implementations.
    pub fn has_extensions(&self) -> bool {
        fn contains(node: &PhysicalPlanNode) -> bool {
            matches!(node.operator, PhysicalPlanOperator::Extension { .. })
                || node.operator.inputs().any(|child| contains(child))
        }
        contains(&self.root)
    }
}

/// The two physical fragments produced by cutting a plan at a checkpoint.
pub struct CheckpointPlanFragments {
    /// Upstream subtree that materializes the checkpoint relation.
    pub producer: PhysicalPlan,
    /// Remaining downstream operators reading the registered relation.
    pub continuation: PhysicalPlan,
}

/// One immutable operator node in a [`PhysicalPlan`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhysicalPlanNode {
    /// Output columns produced by this operator. For a sink, this is the schema
    /// consumed by the sink.
    pub output_schema: VeinSchemaRef,
    /// Operator semantics and references to this node's inputs.
    pub operator: PhysicalPlanOperator,
}

/// Physical operator semantics stored in the canonical plan tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalPlanOperator {
    /// A registered operator with portable configuration and canonical children.
    Extension {
        /// Immutable implementation identity, codec payload and input requirements.
        descriptor: Arc<crate::protocol::extension::PhysicalExtension>,
        /// Canonical child nodes in explicit input-port order.
        inputs: Arc<[Arc<PhysicalPlanNode>]>,
    },
    /// A data source and its source-local expression pipeline.
    Pipeline {
        /// Source descriptor.
        source: Arc<DataSource>,
        /// Ordered columns emitted by the source before the program runs.
        input_schema: VeinSchemaRef,
        /// Expressions applied to each source batch.
        program: Arc<ExpressionProgram>,
    },
    /// Expressions applied to a recursively produced input.
    Transform {
        /// Child input.
        input: Arc<PhysicalPlanNode>,
        /// Expressions applied to the child batches.
        program: Arc<ExpressionProgram>,
    },
    /// A hash join over probe and build inputs.
    Join {
        /// Input streamed through the probe side.
        probe: Arc<PhysicalPlanNode>,
        /// Input fully consumed to build the hash table.
        build: Arc<PhysicalPlanNode>,
        /// Join keys, output layout, and join flavor.
        plan: JoinPlan,
    },
    /// A grouped or scalar aggregate over one input.
    Aggregate {
        /// Child input.
        input: Arc<PhysicalPlanNode>,
        /// Grouping and reduction instructions.
        plan: AggregatePlan,
    },
    /// Duplicate elimination by grouping every child output column.
    Distinct {
        /// Child input.
        input: Arc<PhysicalPlanNode>,
        /// Grouping layout and output types used by the shared aggregate engine.
        plan: AggregatePlan,
    },
    /// A global ordering boundary over one input.
    Sort {
        /// Child input.
        input: Arc<PhysicalPlanNode>,
        /// Sort keys and their ordering semantics.
        plan: SortPlan,
        /// Optional expression pipeline that appends computed sort keys.
        key_program: Option<Arc<ExpressionProgram>>,
        /// Number of child columns to retain after removing helper keys.
        output_columns: u32,
    },
    /// A row window over one input.
    Limit {
        /// Child input.
        input: Arc<PhysicalPlanNode>,
        /// Offset and maximum output row count.
        spec: LimitSpec,
    },
    /// The output boundary for client results or storage writes.
    Sink {
        /// Child input consumed by the sink.
        input: Arc<PhysicalPlanNode>,
        /// Client, storage, or discard destination.
        sink: Arc<DataSink>,
    },
}

impl PhysicalPlanNode {
    /// Lowers a registered extension into the authoritative physical tree.
    pub fn extension(
        descriptor: Arc<crate::protocol::extension::PhysicalExtension>,
        inputs: Vec<Arc<Self>>,
        output_schema: VeinSchemaRef,
    ) -> crate::Result<Arc<Self>> {
        descriptor.validate_inputs(&inputs)?;
        Ok(Arc::new(Self {
            output_schema,
            operator: PhysicalPlanOperator::Extension {
                descriptor,
                inputs: inputs.into(),
            },
        }))
    }
}
