/// Indicates the volatility of a function
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Volatility {
    /// Always returns the same result for the same arguments
    Immutable,
    /// Returns the same result within a single query execution
    Stable,
    /// Can return different results (e.g. random())
    Volatile,
}
