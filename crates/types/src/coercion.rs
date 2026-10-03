//! Type-level coercion rules shared by binding and planning.

use error::{Result, VeinError};

use crate::VeinDataType;

/// Resolves the common supertype of two data types, or errors if none exists.
///
/// Identical types pass through, `Null` unifies with any type, numeric and
/// string widening is symmetric, and `Date32` promotes to `Timestamp` while
/// preserving the timestamp's timezone.
pub fn unify_types(left: &VeinDataType, right: &VeinDataType) -> Result<VeinDataType> {
    if left == right {
        return Ok(left.clone());
    }

    if *left == VeinDataType::Null {
        return Ok(right.clone());
    }
    if *right == VeinDataType::Null {
        return Ok(left.clone());
    }

    if let Some(common) = common_numeric(left, right) {
        return Ok(common);
    }

    if is_string_type(left) && is_string_type(right) {
        return Ok(common_string(left, right));
    }

    if let VeinDataType::Timestamp(tz) = right
        && matches!(left, VeinDataType::Date32)
    {
        return Ok(VeinDataType::Timestamp(tz.clone()));
    }
    if let VeinDataType::Timestamp(tz) = left
        && matches!(right, VeinDataType::Date32)
    {
        return Ok(VeinDataType::Timestamp(tz.clone()));
    }

    Err(VeinError::semantic(
        format!("no implicit coercion between types {left:?} and {right:?}"),
        None,
    ))
}

fn is_string_type(data_type: &VeinDataType) -> bool {
    matches!(
        data_type,
        VeinDataType::Utf8 | VeinDataType::LargeUtf8 | VeinDataType::StringView
    )
}

fn common_string(left: &VeinDataType, right: &VeinDataType) -> VeinDataType {
    let rank = |data_type: &VeinDataType| -> u8 {
        match data_type {
            VeinDataType::Utf8 => 0,
            VeinDataType::StringView => 1,
            VeinDataType::LargeUtf8 => 2,
            _ => unreachable!("common_string called with non-string type {data_type:?}"),
        }
    };
    if rank(left) >= rank(right) {
        left.clone()
    } else {
        right.clone()
    }
}

fn common_numeric(left: &VeinDataType, right: &VeinDataType) -> Option<VeinDataType> {
    let rank = |data_type: &VeinDataType| -> Option<u8> {
        Some(match data_type {
            VeinDataType::Int8 => 0,
            VeinDataType::Int16 => 1,
            VeinDataType::Int32 => 2,
            VeinDataType::Int64 => 3,
            VeinDataType::Float32 => 4,
            VeinDataType::Float64 => 5,
            _ => return None,
        })
    };

    let (left_rank, right_rank) = (rank(left)?, rank(right)?);
    Some(if left_rank >= right_rank {
        left.clone()
    } else {
        right.clone()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unifies_numeric_types_symmetrically() {
        assert_eq!(
            unify_types(&VeinDataType::Int32, &VeinDataType::Int64).unwrap(),
            VeinDataType::Int64
        );
        assert_eq!(
            unify_types(&VeinDataType::Int64, &VeinDataType::Int32).unwrap(),
            VeinDataType::Int64
        );
    }

    #[test]
    fn null_unifies_with_other_types() {
        assert_eq!(
            unify_types(&VeinDataType::Utf8, &VeinDataType::Null).unwrap(),
            VeinDataType::Utf8
        );
    }

    #[test]
    fn date_promotes_to_timestamp_and_preserves_timezone() {
        assert_eq!(
            unify_types(
                &VeinDataType::Date32,
                &VeinDataType::Timestamp(Some("UTC".into()))
            )
                .unwrap(),
            VeinDataType::Timestamp(Some("UTC".into()))
        );
    }

    #[test]
    fn incompatible_types_return_semantic_error() {
        let error = unify_types(&VeinDataType::Boolean, &VeinDataType::Int64).unwrap_err();
        assert!(matches!(error, VeinError::Semantic { .. }));
    }
}
