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

/// Stack of lexical scopes for resolving column references and correlated subqueries.
///
/// Scope data is fully owned. Correlated subqueries capture a *snapshot* of the
/// enclosing scopes in [`ScopeStack::correlation_scopes`] (see `bind_subquery`);
/// [`ScopeStack::resolve_correlated`] marks columns found there as correlated
/// (`OuterColumn`). Storing the enclosing scopes by value instead of as a
/// parent *reference* avoids a self-referential lifetime (`&'a ScopeStack<'a>`),
/// which Rust's borrow checker cannot express safely without `unsafe`.
#[derive(Clone)]
pub struct ScopeStack {
    scopes: Vec<Scope>,
    /// Snapshot of the enclosing query's scopes, used to resolve correlated
    /// references inside a subquery. For a nested subquery this includes the
    /// immediate parent's scopes plus any outer snapshot, so resolution walks
    /// the full enclosing chain outward.
    correlation_scopes: Vec<Scope>,
}

impl Default for ScopeStack {
    fn default() -> Self {
        Self::new()
    }
}

impl ScopeStack {
    /// Creates a new, empty ScopeStack.
    pub fn new() -> Self {
        Self {
            scopes: Vec::new(),
            correlation_scopes: Vec::new(),
        }
    }

    /// Captures `parent`'s scopes (and any outer correlation snapshot) so a
    /// child subquery can resolve correlated references against the enclosing
    /// query. Called on the child before binding it.
    pub fn capture_correlation(&mut self, parent: &ScopeStack) {
        self.correlation_scopes.clear();
        self.correlation_scopes.extend(parent.scopes.iter().cloned());
        self.correlation_scopes
            .extend(parent.correlation_scopes.iter().cloned());
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
    /// whether it was found in a correlated (enclosing) scope.
    ///
    /// Resolution order is innermost-to-outermost: a query's own scopes first,
    /// then the captured correlation scopes. An unqualified reference matches
    /// any column name; a qualified one only inside scopes that carry the
    /// qualifier as an alias. Two different columns with the same name in the
    /// same scope make an unqualified reference ambiguous.
    pub fn resolve_correlated(
        &self,
        qualifier: Option<&str>,
        name: &str,
    ) -> Result<(ResolvedColumn, bool), VeinError> {
        for (correlated, scopes) in
            [(false, &self.scopes[..]), (true, &self.correlation_scopes[..])]
        {
            for scope in scopes.iter().rev() {
                let scope_matches_qualifier = match qualifier {
                    Some(q) => scope.aliases.iter().any(|a| a == q),
                    // Strict Scoping: Unqualified identifiers can only match local scopes.
                    // Reaching into outer correlation scopes requires an explicit qualifier
                    // (e.g. `outer_table.col`) to prevent accidental identifier capture bugs.
                    None => !correlated,
                };
                if !scope_matches_qualifier {
                    continue;
                }

                let mut matches = scope
                    .columns
                    .iter()
                    .filter(|column| {
                        column.name == name
                            && qualifier
                                .map(|q| column.qualifier.as_deref() == Some(q))
                                .unwrap_or(true)
                    })
                    .peekable();
                if let Some(first) = matches.next() {
                    if let Some(second) = matches.next()
                        && first.id != second.id
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
                    return Ok((first.clone(), correlated));
                }
            }
        }

        // If an unqualified lookup missed locally, check if it exists in an outer scope
        // and provide an actionable hint to prevent silent scoping errors.
        if qualifier.is_none() {
            let outer_match = self.correlation_scopes.iter().rev().find_map(|s| {
                s.columns.iter().find(|c| c.name == name).map(|c| {
                    c.qualifier.as_deref().unwrap_or("outer query")
                })
            });

            if let Some(parent_alias) = outer_match {
                return Err(VeinError::semantic(
                    format!(
                        "column '{name}' was not found in the local subquery scope.\n\
                         Hint: '{name}' exists in outer scope '{parent_alias}'. \
                         In Vein, references to outer query columns must be explicitly qualified (e.g. '{parent_alias}.{name}') to prevent accidental identifier capture.",
                    ),
                    None,
                ));
            }
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
