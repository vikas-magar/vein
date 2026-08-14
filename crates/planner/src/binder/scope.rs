use std::collections::HashMap;
use crate::schema::ResolvedColumn;

/// A single lexical scope containing available columns
#[derive(Clone)]
pub struct Scope {
    /// Columns available in this scope
    pub columns: Vec<ResolvedColumn>,
    /// Optional qualifier (e.g., table alias) for this scope
    pub qualifier: Option<String>,
}

/// An index into the scope stack
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ScopeIndex(pub usize);

/// Stack of lexical scopes for resolving column references and correlated subqueries
#[derive(Clone)]
pub struct ScopeStack<'a> {
    /// Pointer to outer scope to enable Correlated Subquery resolution
    pub parent: Option<&'a ScopeStack<'a>>, 
    scopes: Vec<Scope>,                 
    aliases: Vec<HashMap<String, (usize, ScopeIndex)>>,
}

impl<'a> ScopeStack<'a> {
    /// Creates a new ScopeStack
    pub fn new(parent: Option<&'a ScopeStack<'a>>) -> Self {
        Self {
            parent,
            scopes: Vec::new(),
            aliases: Vec::new(),
        }
    }

    /// Pushes a new scope onto the stack
    pub fn push(&mut self, scope: Scope) {
        self.scopes.push(scope);
    }

    /// Pops the top scope from the stack
    pub fn pop(&mut self) -> Option<Scope> {
        self.scopes.pop()
    }

    /// Pushes a temporary layer (used for intra-pipe lateral aliases)
    pub fn push_temp_layer(&mut self) {
        // Used for intra-pipe lateral aliases
        self.aliases.push(HashMap::new());
    }

    /// Pops the temporary layer
    pub fn pop_temp_layer(&mut self) {
        self.aliases.pop();
    }

    /// Resolves a column reference by qualifier and name
    pub fn resolve(&self, qualifier: Option<&str>, name: &str) -> Result<ResolvedColumn, error::VeinError> {
        // 1. Search local scopes (from innermost to outermost)
        for scope in self.scopes.iter().rev() {
            for col in &scope.columns {
                let name_matches = col.name == name;
                let qual_matches = match (qualifier, &col.qualifier) {
                    (Some(q1), Some(q2)) => q1 == q2,
                    (None, _) => true, // If no qualifier provided, match purely on name
                    _ => false,
                };

                if name_matches && qual_matches {
                    return Ok(col.clone());
                }
            }
        }

        // 2. Search parent scope (for correlated subqueries)
        if let Some(parent) = self.parent
            && let Ok(col) = parent.resolve(qualifier, name) {
                // If it's found in the parent, we might want to flag it as an OuterColumn later
                return Ok(col);
            }

        Err(error::VeinError::internal(format!(
            "Column not found in current scope: {}.{}", 
            qualifier.unwrap_or(""), name
        )))
    }
}
