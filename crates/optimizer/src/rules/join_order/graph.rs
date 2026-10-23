use super::*;

impl JoinGraph<'_> {
    pub(super) fn decision(
        &self,
        candidate_splits: usize,
        original_cost: Option<f64>,
        selected_cost: Option<f64>,
        outcome: JoinOrderOutcome,
    ) -> JoinOrderDecision {
        JoinOrderDecision {
            relation_count: self.leaves.len(),
            relation_labels: self.labels.clone(),
            original_order: (0..self.leaves.len()).collect(),
            selected_order: (0..self.leaves.len()).collect(),
            candidate_splits,
            original_cost,
            selected_cost,
            cardinality_assumption: JoinCardinalityAssumption::UniformDistinctValueOverlap,
            alternatives: Vec::new(),
            outcome,
        }
    }

    pub(super) fn build(root: &'_ LogicalPlan) -> std::result::Result<JoinGraph<'_>, GraphFailure> {
        let mut pending = vec![(root, false)];
        let mut leaves = Vec::new();
        let mut joins = Vec::new();
        let mut original_splits = HashMap::new();
        let mut subtree_masks = Vec::new();
        while let Some((node, visited)) = pending.pop() {
            if is_reorderable_join(node) {
                let LogicalPlan::Join { left, right, .. } = node else {
                    unreachable!("join eligibility checked the plan variant");
                };
                if !visited {
                    pending.push((node, true));
                    pending.push((right, false));
                    pending.push((left, false));
                } else {
                    let right_mask = subtree_masks
                        .pop()
                        .expect("right child mask is produced before its parent");
                    let left_mask = subtree_masks
                        .pop()
                        .expect("left child mask is produced before its parent");
                    let parent_mask = left_mask | right_mask;
                    original_splits.insert(parent_mask, (left_mask, right_mask));
                    joins.push((node, left_mask, right_mask));
                    subtree_masks.push(parent_mask);
                }
            } else {
                let relation_index = leaves.len();
                if relation_index >= usize::BITS as usize {
                    let mut relation_labels = leaves
                        .iter()
                        .map(|leaf: &&LogicalPlan| {
                            leaf.table_name()
                                .unwrap_or_else(|| leaf.operator_name())
                                .to_string()
                        })
                        .collect::<Vec<_>>();
                    relation_labels.push(
                        node.table_name()
                            .unwrap_or_else(|| node.operator_name())
                            .to_string(),
                    );
                    return Err(GraphFailure {
                        relation_count: relation_index + 1,
                        relation_labels,
                        outcome: JoinOrderOutcome::RelationLimitExceeded,
                    });
                }
                leaves.push(node);
                subtree_masks.push(1usize << relation_index);
            }
        }

        let labels = leaves
            .iter()
            .map(|leaf| {
                leaf.table_name()
                    .unwrap_or_else(|| leaf.operator_name())
                    .to_string()
            })
            .collect::<Vec<_>>();
        let relation_count = leaves.len();
        if relation_count >= usize::BITS as usize {
            return Err(GraphFailure {
                relation_count,
                relation_labels: labels,
                outcome: JoinOrderOutcome::RelationLimitExceeded,
            });
        }

        debug_assert_eq!(subtree_masks, vec![(1usize << relation_count) - 1]);

        let mut column_relations = HashMap::new();
        let mut all_fields = Vec::new();
        for (relation, leaf) in leaves.iter().enumerate() {
            for column in &leaf.schema().fields {
                if column_relations.insert(column.id, relation).is_some() {
                    return Err(GraphFailure {
                        relation_count,
                        relation_labels: labels,
                        outcome: JoinOrderOutcome::UnsupportedShape,
                    });
                }
                all_fields.push(std::sync::Arc::clone(column));
            }
        }

        let mut graph_predicates = Vec::new();
        let mut adjacency = vec![0usize; relation_count];
        for (node, left_mask, right_mask) in joins {
            let LogicalPlan::Join { on, .. } = node
            else {
                unreachable!("join eligibility checked the plan variant");
            };
            let Some(on) = on.as_ref() else {
                unreachable!("join eligibility requires a predicate");
            };
            let Some(predicates) = predicates::collect(
                on, &column_relations, left_mask | right_mask, &mut adjacency,
            ) else {
                return Err(GraphFailure {
                    relation_count,
                    relation_labels: labels,
                    outcome: JoinOrderOutcome::UnsupportedShape,
                });
            };
            graph_predicates.extend(predicates);
        }

        Ok(JoinGraph {
            leaves,
            predicates: graph_predicates,
            original_splits,
            adjacency,
            analysis_schema: VeinSchema::new(all_fields),
            full_mask: (1usize << relation_count) - 1,
            labels,
        })
    }
}
