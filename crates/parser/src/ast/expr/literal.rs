use ordered_float::OrderedFloat;

/// A literal value.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Literal {
    /// `NULL`
    Null,
    /// `TRUE` / `FALSE`
    Boolean(bool),
    /// An integer literal.
    Int(i64),
    /// An exact decimal literal: unscaled value, precision, and scale.
    Decimal(i128, u8, i8),
    /// A floating-point literal.
    Float(OrderedFloat<f64>),
    /// A string literal, with escapes already resolved.
    String(String),
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Literal::Null, Literal::Null) => true,
            (Literal::Boolean(a), Literal::Boolean(b)) => a == b,
            (Literal::Int(a), Literal::Int(b)) => a == b,
            (Literal::Decimal(a, ap, as_), Literal::Decimal(b, bp, bs)) => {
                a == b && ap == bp && as_ == bs
            }
            (Literal::Float(a), Literal::Float(b)) => a.0 == b.0,
            (Literal::String(a), Literal::String(b)) => a == b,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_nan_is_not_equal_to_itself() {
        let nan = Literal::Float(OrderedFloat::from(f64::NAN));
        assert!(nan != nan.clone());
    }
}
