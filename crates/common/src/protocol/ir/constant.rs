//! Scalar and high-dimensional vector constants for Micro-IR.

use serde::{Deserialize, Serialize};

/// Literal scalar constant that can be loaded into a register.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarConstant {
    /// Null scalar value.
    Null,
    /// Boolean scalar value.
    Boolean(bool),
    /// 8-bit signed integer.
    Int8(i8),
    /// 16-bit signed integer.
    Int16(i16),
    /// 32-bit signed integer.
    Int32(i32),
    /// 64-bit signed integer.
    Int64(i64),
    /// 8-bit unsigned integer.
    UInt8(u8),
    /// 16-bit unsigned integer.
    UInt16(u16),
    /// 32-bit unsigned integer.
    UInt32(u32),
    /// 64-bit unsigned integer.
    UInt64(u64),
    /// 32-bit floating point number.
    Float32(f32),
    /// 64-bit floating point number.
    Float64(f64),
    /// UTF-8 string scalar.
    Utf8(String),
    /// Binary blob scalar.
    Binary(Vec<u8>),
    /// High-dimensional vector for AI embedding similarity.
    Vector(Vec<f32>),
}
