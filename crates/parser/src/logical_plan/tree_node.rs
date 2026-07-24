use crate::logical_plan::LogicalPlan;

pub enum TreeNodeRecursion {
    Continue,
    Stop,
    Skip,
}

pub struct Transformed<T> {
    pub data: T,
    pub transformed: bool,
}

impl<T> Transformed<T> {
    pub fn yes(data: T) -> Self {
        Self { data, transformed: true }
    }

    pub fn no(data: T) -> Self {
        Self { data, transformed: false }
    }
}

pub trait TreeNode: Sized {
    fn apply_children<F>(&self, f: &mut F) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(&Self) -> Result<(), Box<dyn std::error::Error>>;

    fn map_children<F>(self, f: F) -> Result<Self, Box<dyn std::error::Error>>
    where
        F: FnMut(Self) -> Result<Self, Box<dyn std::error::Error>>;

    fn apply<F>(&self, f: &mut F) -> Result<TreeNodeRecursion, Box<dyn std::error::Error>>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion, Box<dyn std::error::Error>>;

    fn exists<F>(&self, f: &mut F) -> Result<bool, Box<dyn std::error::Error>>
    where
        F: FnMut(&Self) -> Result<bool, Box<dyn std::error::Error>>;

    fn transform_down<F>(self, f: &F) -> Result<Transformed<Self>, Box<dyn std::error::Error>>
    where
        F: Fn(Self) -> Result<Transformed<Self>, Box<dyn std::error::Error>>;

    fn transform_up<F>(self, f: &F) -> Result<Transformed<Self>, Box<dyn std::error::Error>>
    where
        F: Fn(Self) -> Result<Transformed<Self>, Box<dyn std::error::Error>>;
}

impl TreeNode for LogicalPlan {
    fn apply_children<F>(&self, f: &mut F) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(&Self) -> Result<(), Box<dyn std::error::Error>>,
    {
        match self {
            LogicalPlan::Scan { .. } | LogicalPlan::Empty | LogicalPlan::Call { .. } => {}
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Projection { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input, .. }
            | LogicalPlan::TableAlias { input, .. }
            | LogicalPlan::SubqueryAlias { input, .. }
            | LogicalPlan::Extend { input, .. }
            | LogicalPlan::Drop { input, .. }
            | LogicalPlan::Assert { input, .. }
            | LogicalPlan::Write { input, .. }
            | LogicalPlan::With { input, .. } => f(input)?,
            LogicalPlan::Aggregate { input, .. } => f(input)?,
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::SetOp { left, right, .. } => {
                f(left)?;
                f(right)?;
            }
        }
        Ok(())
    }

    fn map_children<F>(self, mut f: F) -> Result<Self, Box<dyn std::error::Error>>
    where
        F: FnMut(Self) -> Result<Self, Box<dyn std::error::Error>>,
    {
        match self {
            leaf @ (LogicalPlan::Scan { .. } | LogicalPlan::Empty | LogicalPlan::Call { .. }) => Ok(leaf),
            LogicalPlan::Filter { predicate, input } => {
                Ok(LogicalPlan::Filter { predicate, input: Box::new(f(*input)?) })
            }
            LogicalPlan::Projection { expr, input } => {
                Ok(LogicalPlan::Projection { expr, input: Box::new(f(*input)?) })
            }
            LogicalPlan::Aggregate { group_expr, aggregate_expr, input } => {
                Ok(LogicalPlan::Aggregate { group_expr, aggregate_expr, input: Box::new(f(*input)?) })
            }
            LogicalPlan::Sort { order_by, input } => {
                Ok(LogicalPlan::Sort { order_by, input: Box::new(f(*input)?) })
            }
            LogicalPlan::Limit { limit, offset, input } => {
                Ok(LogicalPlan::Limit { limit, offset, input: Box::new(f(*input)?) })
            }
            LogicalPlan::Distinct { input } => {
                Ok(LogicalPlan::Distinct { input: Box::new(f(*input)?) })
            }
            LogicalPlan::Join { join_type, left, right, predicate, using } => {
                Ok(LogicalPlan::Join {
                    join_type,
                    left: Box::new(f(*left)?),
                    right: Box::new(f(*right)?),
                    predicate,
                    using,
                })
            }
            LogicalPlan::TableAlias { alias, input } => {
                Ok(LogicalPlan::TableAlias { alias, input: Box::new(f(*input)?) })
            }
            LogicalPlan::SubqueryAlias { alias, input } => {
                Ok(LogicalPlan::SubqueryAlias { alias, input: Box::new(f(*input)?) })
            }
            LogicalPlan::SetOp { kind, all, left, right } => {
                Ok(LogicalPlan::SetOp {
                    kind,
                    all,
                    left: Box::new(f(*left)?),
                    right: Box::new(f(*right)?),
                })
            }
            LogicalPlan::Extend { items, input } => {
                Ok(LogicalPlan::Extend { items, input: Box::new(f(*input)?) })
            }
            LogicalPlan::Drop { columns, input } => {
                Ok(LogicalPlan::Drop { columns, input: Box::new(f(*input)?) })
            }
            LogicalPlan::Assert { condition, message, input } => {
                Ok(LogicalPlan::Assert { condition, message, input: Box::new(f(*input)?) })
            }
            LogicalPlan::Write { target, input } => {
                Ok(LogicalPlan::Write { target, input: Box::new(f(*input)?) })
            }
            LogicalPlan::With { ctes, input } => {
                Ok(LogicalPlan::With { ctes, input: Box::new(f(*input)?) })
            }
        }
    }

    fn apply<F>(&self, f: &mut F) -> Result<TreeNodeRecursion, Box<dyn std::error::Error>>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion, Box<dyn std::error::Error>>,
    {
        let recursion = f(self)?;
        if matches!(recursion, TreeNodeRecursion::Continue) {
            self.apply_children(&mut |child| {
                let r = child.apply(f)?;
                if matches!(r, TreeNodeRecursion::Stop) {
                    // Stop cascades — but apply_children doesn't support early exit
                }
                Ok(())
            })?;
        }
        Ok(recursion)
    }

    fn exists<F>(&self, f: &mut F) -> Result<bool, Box<dyn std::error::Error>>
    where
        F: FnMut(&Self) -> Result<bool, Box<dyn std::error::Error>>,
    {
        if f(self)? {
            return Ok(true);
        }
        let mut found = false;
        self.apply_children(&mut |child| {
            if child.exists(f)? {
                found = true;
            }
            Ok(())
        })?;
        Ok(found)
    }

    fn transform_down<F>(self, f: &F) -> Result<Transformed<Self>, Box<dyn std::error::Error>>
    where
        F: Fn(Self) -> Result<Transformed<Self>, Box<dyn std::error::Error>>,
    {
        let after_f = f(self)?;
        let mut children_changed = false;
        let after_children = after_f.data.map_children(|child| {
            let r = child.transform_down(f)?;
            children_changed = children_changed || r.transformed;
            Ok(r.data)
        })?;
        Ok(Transformed { data: after_children, transformed: after_f.transformed || children_changed })
    }

    fn transform_up<F>(self, f: &F) -> Result<Transformed<Self>, Box<dyn std::error::Error>>
    where
        F: Fn(Self) -> Result<Transformed<Self>, Box<dyn std::error::Error>>,
    {
        let (after_children, child_changed) = transform_up_children(self, f)?;
        let result = f(after_children)?;
        Ok(Transformed { data: result.data, transformed: result.transformed || child_changed })
    }
}

/// Internal: transform children bottom-up and return whether any child changed.
fn transform_up_children<F>(plan: LogicalPlan, f: &F) -> Result<(LogicalPlan, bool), Box<dyn std::error::Error>>
where
    F: Fn(LogicalPlan) -> Result<Transformed<LogicalPlan>, Box<dyn std::error::Error>>,
{
    match plan {
        leaf @ (LogicalPlan::Scan { .. } | LogicalPlan::Empty | LogicalPlan::Call { .. }) => Ok((leaf, false)),
        LogicalPlan::Filter { predicate, input } => {
            let r = input.transform_up(f)?;
            Ok((LogicalPlan::Filter { predicate, input: Box::new(r.data) }, r.transformed))
        }
        LogicalPlan::Projection { expr, input } => {
            let r = input.transform_up(f)?;
            Ok((LogicalPlan::Projection { expr, input: Box::new(r.data) }, r.transformed))
        }
        LogicalPlan::Aggregate { group_expr, aggregate_expr, input } => {
            let r = input.transform_up(f)?;
            Ok((LogicalPlan::Aggregate { group_expr, aggregate_expr, input: Box::new(r.data) }, r.transformed))
        }
        LogicalPlan::Sort { order_by, input } => {
            let r = input.transform_up(f)?;
            Ok((LogicalPlan::Sort { order_by, input: Box::new(r.data) }, r.transformed))
        }
        LogicalPlan::Limit { limit, offset, input } => {
            let r = input.transform_up(f)?;
            Ok((LogicalPlan::Limit { limit, offset, input: Box::new(r.data) }, r.transformed))
        }
        LogicalPlan::Distinct { input } => {
            let r = input.transform_up(f)?;
            Ok((LogicalPlan::Distinct { input: Box::new(r.data) }, r.transformed))
        }
        LogicalPlan::Join { join_type, left, right, predicate, using } => {
            let l = left.transform_up(f)?;
            let r = right.transform_up(f)?;
            Ok((LogicalPlan::Join {
                join_type,
                left: Box::new(l.data),
                right: Box::new(r.data),
                predicate,
                using,
            }, l.transformed || r.transformed))
        }
        LogicalPlan::TableAlias { alias, input } => {
            let r = input.transform_up(f)?;
            Ok((LogicalPlan::TableAlias { alias, input: Box::new(r.data) }, r.transformed))
        }
        LogicalPlan::SubqueryAlias { alias, input } => {
            let r = input.transform_up(f)?;
            Ok((LogicalPlan::SubqueryAlias { alias, input: Box::new(r.data) }, r.transformed))
        }
        LogicalPlan::SetOp { kind, all, left, right } => {
            let l = left.transform_up(f)?;
            let r = right.transform_up(f)?;
            Ok((LogicalPlan::SetOp {
                kind, all,
                left: Box::new(l.data),
                right: Box::new(r.data),
            }, l.transformed || r.transformed))
        }
        LogicalPlan::Extend { items, input } => {
            let r = input.transform_up(f)?;
            Ok((LogicalPlan::Extend { items, input: Box::new(r.data) }, r.transformed))
        }
        LogicalPlan::Drop { columns, input } => {
            let r = input.transform_up(f)?;
            Ok((LogicalPlan::Drop { columns, input: Box::new(r.data) }, r.transformed))
        }
        LogicalPlan::Assert { condition, message, input } => {
            let r = input.transform_up(f)?;
            Ok((LogicalPlan::Assert { condition, message, input: Box::new(r.data) }, r.transformed))
        }
        LogicalPlan::Write { target, input } => {
            let r = input.transform_up(f)?;
            Ok((LogicalPlan::Write { target, input: Box::new(r.data) }, r.transformed))
        }
        LogicalPlan::With { ctes, input } => {
            let r = input.transform_up(f)?;
            Ok((LogicalPlan::With { ctes, input: Box::new(r.data) }, r.transformed))
        }
    }
}
