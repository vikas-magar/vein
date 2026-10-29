use super::*;

impl ExecutionTrace {
    pub(super) fn visit(&mut self, node: &Arc<PhysicalPlanNode>) -> u32 {
        let identity = Arc::as_ptr(node) as usize;
        if let Some(id) = self.node_ids.get(&identity) {
            return *id;
        }

        let id = u32::try_from(self.nodes.len()).unwrap_or(u32::MAX);
        let statistics = self.statistics_context.register_operator(
            OperatorStatisticsIdentity {
                node_id: id,
                task_id: self.task_identity,
            },
            operator_name(&node.operator),
        );
        let metrics = Arc::new(NodeMetrics {
            node: Arc::clone(node),
            id,
            operator: operator_name(&node.operator),
            operator_config: operator_config(&node.operator),
            statistics: Arc::clone(&statistics),
            child_ids: OnceLock::new(),
            source_array_count_seen: statistics.register_counter("source_array_count_seen"),
            source_array_bytes_seen: statistics.register_counter("source_array_bytes_seen"),
            downstream_push_ns: statistics.register_counter("downstream_push_ns"),
            execution_completed: statistics.register_counter("execution_completed"),
            execution_cancelled: statistics.register_counter("execution_cancelled"),
            execution_failed: statistics.register_counter("execution_failed"),
            execution_dropped: statistics.register_counter("execution_dropped"),
            resources: Arc::new(OperatorResourceMetrics::default()),
        });
        self.node_ids.insert(identity, id);
        self.nodes.push(Arc::clone(&metrics));

        let child_ids = node.operator.inputs()
            .map(|child| self.visit(child))
            .collect();
        // This is initialized before recursive traversal only to make shared
        // nodes and a malformed cycle safe; no other observer can see it yet.
        let _ = metrics.child_ids.set(child_ids);
        id
    }
    pub(crate) fn log_plan(&self) {
        tracing::debug!(
            target: "vein.audit",
            event = "physical_plan_explain",
            query_id = %self.query_id,
            task_id = %self.task_id,
            node_count = self.nodes.len(),
            explanation = %self.format_explain(),
            "physical_plan_explain"
        );
        for metrics in &self.nodes {
            let schema = metrics
                .node
                .output_schema
                .fields
                .iter()
                .map(|field| {
                    format!(
                        "{}:{:?}{}",
                        field.name,
                        field.data_type,
                        if field.nullable { "?" } else { "" }
                    )
                })
                .collect::<Vec<_>>();
            tracing::debug!(
                target: "vein.audit",
                event = "physical_plan_node",
                query_id = %self.query_id,
                task_id = %self.task_id,
                node_id = metrics.id,
                operator = metrics.operator,
                operator_config = %metrics.operator_config,
                child_node_ids = ?metrics.child_ids.get().map(Vec::as_slice).unwrap_or_default(),
                output_schema = ?schema,
                input_materialization = ?metrics.node.operator.data_flow().input_materialization,
                output_representation = ?metrics.node.operator.data_flow().output,
                "physical_plan_node"
            );
        }
    }

    /// Deterministic compact explain text over the canonical plan-node graph.
    /// The stable preorder IDs and child references preserve DAG sharing.
    pub(super) fn format_explain(&self) -> String {
        self.nodes
            .iter()
            .map(|metrics| {
                let children = metrics
                    .child_ids
                    .get()
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                let schema = metrics
                    .node
                    .output_schema
                    .fields
                    .iter()
                    .map(|field| {
                        let qualifier = field
                            .qualifier
                            .as_deref()
                            .map(|name| format!("{name}."))
                            .unwrap_or_default();
                        format!(
                            "{qualifier}{}:{:?}{}",
                            field.name,
                            field.data_type,
                            if field.nullable { "?" } else { "" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let flow = metrics.node.operator.data_flow();
                let boundary = match flow.input_materialization {
                    common::protocol::InputMaterialization::ImplementationDefined => "implementation-defined",
                    common::protocol::InputMaterialization::WholeBatch => "materialize-input",
                    common::protocol::InputMaterialization::ConsumerBoundary => "consumer-boundary",
                    common::protocol::InputMaterialization::KernelColumns => "kernel-columns",
                    common::protocol::InputMaterialization::None => "reference-input",
                };
                format!(
                    "#{} {} children={:?} [{}] flow={}->{:?} schema=[{}]",
                    metrics.id,
                    metrics.operator,
                    children,
                    metrics.operator_config,
                    boundary,
                    flow.output,
                    schema,
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}
