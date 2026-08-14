use crate::expr::{LogicalExpr, LogicalLiteral};
use crate::plan::traversal::PlanTraversal;
use crate::schema::VeinDataType;

impl LogicalExpr {
    /// Returns the precise data type of the logical expression.
    /// Used by the binder and optimizer to validate schemas and enforce type safety.
    pub fn data_type(&self) -> VeinDataType {
        match self {
            LogicalExpr::Column(col) | LogicalExpr::OuterColumn(col) => col.data_type.clone(),
            LogicalExpr::Literal(lit) => match lit {
                LogicalLiteral::Null(t) => t.clone(),
                LogicalLiteral::Boolean(_) => VeinDataType::Boolean,
                LogicalLiteral::Int64(_) => VeinDataType::Int64,
                LogicalLiteral::Float64(_) => VeinDataType::Float64,
                LogicalLiteral::String(_) => VeinDataType::Utf8,
            },
            LogicalExpr::BinaryOp { return_type, .. } => return_type.clone(),
            LogicalExpr::Cast { target_type, .. } => target_type.clone(),
            
            // Logical and comparison operators always return Boolean
            LogicalExpr::Not { .. } 
            | LogicalExpr::IsNull { .. } 
            | LogicalExpr::IsNotNull { .. } 
            | LogicalExpr::InList { .. } 
            | LogicalExpr::Like { .. } 
            | LogicalExpr::Exists { .. } => VeinDataType::Boolean,
            
            // Mathematical negation preserves the type
            LogicalExpr::Negate { expr } => expr.data_type(),
            
            LogicalExpr::Case { return_type, .. } => return_type.clone(),
            
            LogicalExpr::FunctionCall { return_type, .. }
            | LogicalExpr::AiOperator { return_type, .. }
            | LogicalExpr::WasmUdf { return_type, .. }
            | LogicalExpr::EbpfUdf { return_type, .. }
            | LogicalExpr::PythonUdf { return_type, .. } => return_type.clone(),
            
            LogicalExpr::Lambda { body, .. } => body.data_type(), // Lambda returns its body's type
            
            LogicalExpr::ScalarSubquery(plan) => {
                // Scalar subqueries return the type of their first column
                plan.schema().fields.first().map(|f| f.data_type.clone()).unwrap_or(VeinDataType::Null)
            }
        }
    }
}
