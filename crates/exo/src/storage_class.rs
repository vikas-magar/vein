//! Central storage policy for EXO columns.
//!
//! The address plane stores either a reversible native value or a canonical
//! 64-bit fingerprint. Fingerprints are never treated as ordered values and
//! always require an exact payload for collision verification.

use arrow::datatypes::DataType;

/// How a logical column is represented in EXO.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExoStorageClass {
    /// The address plane contains the value itself in its native width.
    NativeAddress {
        /// On-disk width of the reversible native representation.
        slot_width: u8,
    },
    /// The address plane contains a fingerprint and the vault contains the
    /// exact value needed for reconstruction and collision verification.
    AddressWithVault {
        /// Whether address equality can be used as a candidate match only.
        requires_collision_verification: bool,
        /// Whether the address preserves logical ordering for pruning.
        preserves_order: bool,
    },
}

impl ExoStorageClass {
    #[inline]
    pub const fn slot_width(self) -> u8 {
        match self {
            Self::NativeAddress { slot_width } => slot_width,
            Self::AddressWithVault { .. } => 8,
        }
    }

    #[inline]
    pub const fn uses_vault(self) -> bool {
        matches!(self, Self::AddressWithVault { .. })
    }

    #[inline]
    pub const fn preserves_order(self) -> bool {
        match self {
            Self::NativeAddress { .. } => true,
            Self::AddressWithVault { preserves_order, .. } => preserves_order,
        }
    }

    #[inline]
    pub const fn requires_collision_verification(self) -> bool {
        match self {
            Self::NativeAddress { .. } => false,
            Self::AddressWithVault {
                requires_collision_verification,
                ..
            } => requires_collision_verification,
        }
    }

    /// Returns whether address-level range predicates are sound.
    #[inline]
    pub const fn supports_ordered_pruning(self) -> bool {
        self.preserves_order()
    }

    /// Returns whether the address can be used as an equality candidate key.
    #[inline]
    pub const fn supports_equality(self) -> bool {
        true
    }
}

/// Classifies an Arrow logical type into its EXO physical storage policy.
pub fn classify_storage(data_type: &DataType) -> Option<ExoStorageClass> {
    let native_width = match data_type {
        DataType::Boolean | DataType::Int8 | DataType::UInt8 => Some(1),
        DataType::Int16 | DataType::UInt16 => Some(2),
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_) => Some(4),
        _ => None,
    };

    native_width
        .map(|slot_width| ExoStorageClass::NativeAddress { slot_width })
        .or_else(|| match data_type {
            DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_) => Some(ExoStorageClass::AddressWithVault {
                requires_collision_verification: false,
                preserves_order: true,
            }),
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::FixedSizeBinary(_)
            | DataType::List(_)
            | DataType::ListView(_)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
            | DataType::Union(_, _)
            | DataType::Dictionary(_, _)
            | DataType::RunEndEncoded(_, _) => Some(ExoStorageClass::AddressWithVault {
                requires_collision_verification: true,
                preserves_order: false,
            }),
            _ => None,
        })
}

/// Returns the physical policy or a descriptive unsupported-type error.
pub fn require_storage_class(data_type: &DataType) -> Result<ExoStorageClass, String> {
    classify_storage(data_type).ok_or_else(|| {
        format!(
            "EXO does not support safe address storage for Arrow data type {data_type:?}"
        )
    })
}

/// Classifies the debug-form type names currently used by the execution protocol.
///
/// This adapter keeps string-based schema transport from becoming a second storage
/// policy. New typed callers should use [`classify_storage`] directly.
pub fn classify_type_name(type_name: &str) -> Option<ExoStorageClass> {
    let is_vault_backed = matches!(
        type_name,
        "Utf8" | "LargeUtf8" | "Utf8View" | "Binary" | "LargeBinary" | "BinaryView"
    ) || matches!(
        type_name,
        "Int64" | "UInt64" | "Float64" | "Date64" | "Duration(_)"
    ) || type_name.starts_with("Timestamp(")
        || type_name.starts_with("Time64(")
        || type_name.starts_with("Decimal128(")
        || type_name.starts_with("Decimal256(")
        || type_name.starts_with("FixedSizeBinary(")
        || type_name.starts_with("List(")
        || type_name.starts_with("LargeList(")
        || type_name.starts_with("FixedSizeList(")
        || type_name.starts_with("Struct(")
        || type_name.starts_with("Map(");

    if is_vault_backed {
        Some(ExoStorageClass::AddressWithVault {
            requires_collision_verification: true,
            preserves_order: false,
        })
    } else {
        None
    }
}

/// Returns whether the type requires exact payload storage.
#[inline]
pub fn data_type_uses_vault(data_type: &DataType) -> bool {
    classify_storage(data_type)
        .map(ExoStorageClass::uses_vault)
        .unwrap_or(false)
}

/// Returns whether address min/max values preserve logical ordering.
#[inline]
pub fn address_preserves_order(data_type: &DataType) -> bool {
    classify_storage(data_type)
        .map(ExoStorageClass::preserves_order)
        .unwrap_or(false)
}

/// Returns whether a type needs exact payload verification after address equality.
#[inline]
pub fn requires_collision_verification(data_type: &DataType) -> bool {
    classify_storage(data_type)
        .map(ExoStorageClass::requires_collision_verification)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;

    #[test]
    fn native_types_select_minimal_width() {
        assert_eq!(
            classify_storage(&DataType::Int8),
            Some(ExoStorageClass::NativeAddress { slot_width: 1 })
        );
        assert_eq!(
            classify_storage(&DataType::Int16),
            Some(ExoStorageClass::NativeAddress { slot_width: 2 })
        );
        assert_eq!(
            classify_storage(&DataType::Int32),
            Some(ExoStorageClass::NativeAddress { slot_width: 4 })
        );
        let class = classify_storage(&DataType::Int64).expect("i64 support");
        assert_eq!(class.slot_width(), 8);
        assert!(class.uses_vault());
        assert!(class.preserves_order());
        assert!(!class.requires_collision_verification());
    }

    #[test]
    fn decimal_and_variable_types_require_exact_payloads() {
        for data_type in [
            DataType::Decimal128(22, 1),
            DataType::Decimal256(38, 2),
            DataType::Utf8,
            DataType::Binary,
            DataType::FixedSizeBinary(16),
        ] {
            let class = classify_storage(&data_type).expect("supported EXO type");
            assert!(class.uses_vault());
            assert!(class.requires_collision_verification());
            assert!(!class.supports_ordered_pruning());
        }
    }

    #[test]
    fn sixty_four_bit_types_require_exact_payloads() {
        for data_type in [
            DataType::Int64,
            DataType::UInt64,
            DataType::Float64,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ] {
            let class = classify_storage(&data_type).expect("64-bit type support");
            assert_eq!(class.slot_width(), 8);
            assert!(class.uses_vault());
            assert!(!class.requires_collision_verification());
            assert!(class.supports_ordered_pruning());
        }
    }
}
