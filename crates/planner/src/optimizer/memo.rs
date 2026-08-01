use std::collections::HashMap;
use crate::plan::LogicalPlan;
use error::Result;

/// An ID representing an equivalence group in the Memo.
pub type GroupId = usize;

/// A group of logically equivalent plan nodes (e.g. A Join B vs B Join A).
#[derive(Debug)]
pub struct Group {
    /// Unique identifier for this equivalence group.
    pub id: GroupId,
    /// The logically equivalent logical plans in this group.
    pub expressions: Vec<LogicalPlan>,
    /// The lowest cost found so far for this group.
    pub min_cost: Option<f64>,
    /// The index of the expression in `expressions` that yielded `min_cost`.
    pub best_expr_idx: Option<usize>,
}

/// The Memoization structure for the Cascades/Volcano Cost-Based Optimizer.
/// 
/// Instead of duplicating massive plan trees, the CBO stores logically equivalent
/// sub-plans in `Group`s.
#[derive(Debug, Default)]
pub struct Memo {
    groups: HashMap<GroupId, Group>,
    next_id: GroupId,
}

impl Memo {
    /// Creates a new empty Memo.
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
            next_id: 0,
        }
    }

    /// Inserts a plan into the Memo. If it matches an existing equivalence group,
    /// it is added to that group. Otherwise, a new group is created.
    /// Returns the GroupId.
    pub fn insert(&mut self, plan: LogicalPlan) -> Result<GroupId> {
        // In a full implementation, we'd hash the plan logically to find an existing group.
        // For scaffolding, we just create a new group.
        let id = self.next_id;
        self.next_id += 1;

        let group = Group {
            id,
            expressions: vec![plan],
            min_cost: None,
            best_expr_idx: None,
        };
        
        self.groups.insert(id, group);
        Ok(id)
    }

    /// Retrieves a group by ID.
    pub fn get_group(&self, id: GroupId) -> Option<&Group> {
        self.groups.get(&id)
    }
}
