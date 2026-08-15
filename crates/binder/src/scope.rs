use plan::ResolvedColumn;
use error::VeinError;

/// A single lexical scope containing available columns plus the alias names
/// (table name, explicit alias, join side aliases, ...) it answers to.
#[derive(Clone)]
pub struct Scope {
    /// Columns available in this scope
    pub columns: Vec<ResolvedColumn>,
    /// Alias names this scope resolves under (`AS`-derived qualifiers).
    /// An empty list makes every qualified reference miss (SELECT output).
    pub aliases: Vec<String>,
}

impl Scope {
    /// Creates a scope that only answers to unqualified references
    /// (SELECT/EXTEND/AGGREGATE output columns lose their table qualifiers).
    pub fn unqualified(columns: Vec<ResolvedColumn>) -> Self {
        Self {
            columns,
            aliases: Vec::new(),
        }
    }
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
}

impl<'a> ScopeStack<'a> {
    /// Creates a new ScopeStack with an optional outer (correlation) scope.
    pub fn new(parent: Option<&'a ScopeStack<'a>>) -> Self {
        Self {
            parent,
            scopes: Vec::new(),
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

    /// Returns the innermost (top) scope for wildcard expansion.
    pub fn scopes_top(&self) -> Option<&Scope> {
        self.scopes.last()
    }

    /// Resolves a column reference, returning the column with a marker for
    /// whether it was found in a parent scope (a correlated reference).
    ///
    /// Resolution order is innermost-to-outermost. An unqualified reference
    /// matches any column name; a qualified one only inside scopes that carry
    /// the qualifier as an alias. Two different columns with the same name in
    /// the same scope make an unqualified reference ambiguous.
    pub fn resolve_correlated(
        &self,
        qualifier: Option<&str>,
        name: &str,
    ) -> Result<(ResolvedColumn, bool), VeinError> {
        for scope in self.scopes.iter().rev() {
            let scope_matches_qualifier = match qualifier {
                Some(q) => scope.aliases.iter().any(|a| a == q),
                None => true,
            };
            if !scope_matches_qualifier {
                continue;
            }

            let mut matches = scope.columns.iter().filter(|c| c.name == name).peekable();
            if let Some(first) = matches.next() {
                if let Some(second) = matches.next()
                    && first.id != second.id
                    && qualifier.is_none()
                {
                    return Err(VeinError::semantic(
                        format!(
                            "ambiguous column reference: '{name}' matches {} and {}",
                            first.qualifier.as_deref().unwrap_or("?"),
                            second.qualifier.as_deref().unwrap_or("?")
                        ),
                        None,
                    ));
                }
                return Ok((first.clone(), false));
            }
        }

        if let Some(parent) = self.parent
            && let Ok((col, _)) = parent.resolve_correlated(qualifier, name)
        {
            return Ok((col, true));
        }

        let display = match qualifier {
            Some(q) => format!("{q}.{name}"),
            None => name.to_string(),
        };
        Err(VeinError::semantic(
            format!("unknown column '{display}'"),
            None,
        ))
    }

    /// Resolves a column reference without correlation information
    /// (convenience for callers that reject correlated references anyway).
    pub fn resolve(
        &self,
        qualifier: Option<&str>,
        name: &str,
    ) -> Result<ResolvedColumn, VeinError> {
        self.resolve_correlated(qualifier, name).map(|(col, _)| col)
    }
}
