//! Generic numeric value semantics used by aggregate kernels.

use super::*;

/// Operations shared by every numeric accumulator type.
pub(in crate::operators::aggregate) trait AggregateValue: Copy + PartialOrd {
    fn checked_add(self, other: Self, decimal_precision_limit: bool) -> Option<Self>;
    fn minimum(self, other: Self) -> Self;
    fn maximum(self, other: Self) -> Self;
    fn to_f64(self, scale: i8) -> f64;
}

impl AggregateValue for i64 {
    fn checked_add(self, other: Self, _decimal_precision_limit: bool) -> Option<Self> {
        i64::checked_add(self, other)
    }

    fn minimum(self, other: Self) -> Self {
        self.min(other)
    }

    fn maximum(self, other: Self) -> Self {
        self.max(other)
    }

    fn to_f64(self, _scale: i8) -> f64 {
        self as f64
    }
}

impl AggregateValue for i128 {
    fn checked_add(self, other: Self, decimal_precision_limit: bool) -> Option<Self> {
        i128::checked_add(self, other).filter(|sum| {
            !decimal_precision_limit || {
                let max_decimal_38 = 10_i128.pow(38) - 1;
                (-max_decimal_38..=max_decimal_38).contains(sum)
            }
        })
    }

    fn minimum(self, other: Self) -> Self {
        self.min(other)
    }

    fn maximum(self, other: Self) -> Self {
        self.max(other)
    }

    fn to_f64(self, scale: i8) -> f64 {
        self as f64 / 10_f64.powi(scale as i32)
    }
}

impl AggregateValue for i256 {
    fn checked_add(self, other: Self, decimal_precision_limit: bool) -> Option<Self> {
        i256::checked_add(self, other).filter(|sum| {
            !decimal_precision_limit || {
                let max_decimal_76 = i256::from(10_i8)
                    .checked_pow(76)
                    .and_then(|limit| limit.checked_sub(i256::ONE))
                    .expect("decimal precision limit fits in i256");
                (-max_decimal_76..=max_decimal_76).contains(sum)
            }
        })
    }

    fn minimum(self, other: Self) -> Self {
        self.min(other)
    }

    fn maximum(self, other: Self) -> Self {
        self.max(other)
    }

    fn to_f64(self, scale: i8) -> f64 {
        self.to_string()
            .parse::<f64>()
            .expect("i256 decimal representation parses as f64")
            / 10_f64.powi(scale as i32)
    }
}

impl AggregateValue for u64 {
    fn checked_add(self, other: Self, _decimal_precision_limit: bool) -> Option<Self> {
        u64::checked_add(self, other)
    }

    fn minimum(self, other: Self) -> Self {
        self.min(other)
    }

    fn maximum(self, other: Self) -> Self {
        self.max(other)
    }

    fn to_f64(self, _scale: i8) -> f64 {
        self as f64
    }
}

impl AggregateValue for u128 {
    fn checked_add(self, other: Self, _decimal_precision_limit: bool) -> Option<Self> {
        u128::checked_add(self, other)
    }

    fn minimum(self, other: Self) -> Self {
        self.min(other)
    }

    fn maximum(self, other: Self) -> Self {
        self.max(other)
    }

    fn to_f64(self, _scale: i8) -> f64 {
        self as f64
    }
}

impl AggregateValue for f64 {
    fn checked_add(self, other: Self, _decimal_precision_limit: bool) -> Option<Self> {
        Some(self + other)
    }

    fn minimum(self, other: Self) -> Self {
        f64::min(self, other)
    }

    fn maximum(self, other: Self) -> Self {
        f64::max(self, other)
    }

    fn to_f64(self, _scale: i8) -> f64 {
        self
    }
}

impl AggregateValue for f32 {
    fn checked_add(self, other: Self, _decimal_precision_limit: bool) -> Option<Self> {
        Some(self + other)
    }

    fn minimum(self, other: Self) -> Self {
        f32::min(self, other)
    }

    fn maximum(self, other: Self) -> Self {
        f32::max(self, other)
    }

    fn to_f64(self, _scale: i8) -> f64 {
        self as f64
    }
}
