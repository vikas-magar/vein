use error::{Result, VeinError};
use arrow::datatypes::i256;

/// Operations required from primitive accumulator values.
pub(super) trait AggregateNumber: Copy + PartialOrd {
    fn checked_add(self, other: Self, decimal_limit: Option<Self>) -> Option<Self>;
    fn decimal_max() -> Option<Self>;
    fn minimum(self, other: Self) -> Self;
    fn maximum(self, other: Self) -> Self;
    fn to_f64(self, scale: i8) -> f64;
    fn encode(self, output: &mut Vec<u8>);
    fn decode(input: &mut &[u8]) -> Result<Self>;
}

macro_rules! integer_number {
    ($type:ty, $size:literal, $max:expr, $within:expr) => {
        impl AggregateNumber for $type {
            fn checked_add(self, other: Self, decimal_limit: Option<Self>) -> Option<Self> {
                let sum = self.checked_add(other)?;
                decimal_limit.is_none_or(|limit| ($within)(sum, limit)).then_some(sum)
            }
            fn decimal_max() -> Option<Self> { $max }
            fn minimum(self, other: Self) -> Self { self.min(other) }
            fn maximum(self, other: Self) -> Self { self.max(other) }
            fn to_f64(self, scale: i8) -> f64 { self as f64 / 10_f64.powi(scale as i32) }
            fn encode(self, output: &mut Vec<u8>) { output.extend_from_slice(&self.to_le_bytes()); }
            fn decode(input: &mut &[u8]) -> Result<Self> {
                let (bytes, remaining) = input.split_first_chunk::<$size>()
                    .ok_or_else(|| VeinError::internal("truncated aggregate state value"))?;
                *input = remaining;
                Ok(<$type>::from_le_bytes(*bytes))
            }
        }
    };
}

macro_rules! float_number {
    ($type:ty, $size:literal) => {
        impl AggregateNumber for $type {
            fn checked_add(self, other: Self, _: Option<Self>) -> Option<Self> { Some(self + other) }
            fn decimal_max() -> Option<Self> { None }
            fn minimum(self, other: Self) -> Self { self.min(other) }
            fn maximum(self, other: Self) -> Self { self.max(other) }
            fn to_f64(self, scale: i8) -> f64 { self as f64 / 10_f64.powi(scale as i32) }
            fn encode(self, output: &mut Vec<u8>) { output.extend_from_slice(&self.to_le_bytes()); }
            fn decode(input: &mut &[u8]) -> Result<Self> {
                let (bytes, remaining) = input.split_first_chunk::<$size>()
                    .ok_or_else(|| VeinError::internal("truncated aggregate state value"))?;
                *input = remaining;
                Ok(<$type>::from_le_bytes(*bytes))
            }
        }
    };
}

integer_number!(i64, 8, None, |_, _| true);
integer_number!(i128, 16, 10_i128.checked_pow(38).and_then(|value| value.checked_sub(1)), |sum: i128, limit: i128| (-limit..=limit).contains(&sum));
integer_number!(u64, 8, None, |sum: u64, limit: u64| sum <= limit);
integer_number!(u128, 16, None, |sum: u128, limit: u128| sum <= limit);
float_number!(f32, 4);
float_number!(f64, 8);

impl AggregateNumber for i256 {
    fn checked_add(self, other: Self, decimal_limit: Option<Self>) -> Option<Self> {
        let sum = i256::checked_add(self, other)?;
        decimal_limit.is_none_or(|limit| (-limit..=limit).contains(&sum)).then_some(sum)
    }
    fn decimal_max() -> Option<Self> {
        i256::from(10_i8).checked_pow(76)?.checked_sub(i256::ONE)
    }
    fn minimum(self, other: Self) -> Self { self.min(other) }
    fn maximum(self, other: Self) -> Self { self.max(other) }
    fn to_f64(self, scale: i8) -> f64 {
        self.to_string().parse::<f64>().unwrap_or(f64::NAN) / 10_f64.powi(scale as i32)
    }
    fn encode(self, output: &mut Vec<u8>) {
        let (low, high) = self.to_parts();
        output.extend_from_slice(&low.to_le_bytes());
        output.extend_from_slice(&high.to_le_bytes());
    }
    fn decode(input: &mut &[u8]) -> Result<Self> {
        let low = u128::decode(input)?;
        let high = i128::decode(input)?;
        Ok(i256::from_parts(low, high))
    }
}
