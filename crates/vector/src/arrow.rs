//! The Arrow interoperability boundary.
//!
//! ADR 0002 (Correction) settles what this module can and cannot do. Arrow's
//! `Buffer` has no public constructor that adopts an external allocation, so a
//! `BufferRef` cannot be handed to Arrow without copying, and the word-backed
//! `BufferRef` cannot adopt Arrow's byte buffer without copying either.
//!
//! So the boundary **converts, once, in each direction**. That satisfies
//! `02-execution-and-vir-lld.md` §15, whose requirement is to avoid *repeated*
//! `Arrow -> Vein -> Arrow` conversion per operator; internal execution never
//! converts, and sharing only becomes possible once the Phase 3 buffer manager
//! can allocate buffers both sides accept.
//!
//! This module is also the reference oracle the differential tests need
//! (`09-testing-and-correctness.md` §4, Rule 33).

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, DictionaryArray, Float64Array,
    Int64Array, PrimitiveArray, StringArray, UInt32Array,
};
use arrow::datatypes::{ArrowPrimitiveType, Date32Type, Float64Type, UInt32Type};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};

use types::{LogicalLiteral, VeinDataType};

use crate::buffer::{AlignedValue, BufferRef};
use crate::validity::Validity;
use crate::vector::{Encoding, Vector};

/// Converts a Vein vector into an Arrow array.
///
/// Returns `None` for a type the bridge does not yet cover rather than
/// inventing a conversion.
pub fn to_arrow(vector: &Vector) -> Option<ArrayRef> {
    if let Encoding::Dictionary { .. } = vector.encoding() {
        // Keep the encoding rather than expanding it: Rule 5 forbids flattening
        // a dictionary merely because a flat representation is easier, and
        // Arrow can express it directly.
        let keys: Vec<u32> = (0..vector.len())
            .map(|row| vector.index_at(row).unwrap_or_default())
            .collect();
        let nulls = nulls_from_validity(vector.validity(), vector.len() as usize);
        let keys = UInt32Array::new(ScalarBuffer::from(keys), nulls);
        let values = vector.dictionary_values()?;
        return Some(Arc::new(
            DictionaryArray::<UInt32Type>::try_new(keys, to_arrow(values)?).ok()?,
        ));
    }
    match vector.data_type() {
        VeinDataType::Int64 => to_arrow_i64(vector),
        VeinDataType::Float64 => to_arrow_primitive::<Float64Type>(vector),
        VeinDataType::Date32 => to_arrow_primitive::<Date32Type>(vector),
        VeinDataType::Utf8 => to_arrow_utf8(vector),
        VeinDataType::Boolean => to_arrow_boolean(vector),
        VeinDataType::Decimal128(precision, scale) => {
            to_arrow_decimal128(vector, *precision, *scale)
        }
        _ => None,
    }
}

/// Converts an Int64 vector, resolving its encoding.
///
/// Unlike the carried types, an Int64 vector is produced by the engine too and
/// may be constant-encoded, so this reads through `get_i64` rather than assuming
/// a flat buffer.
fn to_arrow_i64(vector: &Vector) -> Option<ArrayRef> {
    let len = vector.len() as usize;
    let nulls = nulls_from_validity(vector.validity(), len);
    // A flat vector copies its words straight out. Any other encoding is
    // resolved per row through the accessor, which is the only way to broadcast
    // a constant without expanding it here. Arrow needs a defined value beside
    // every null bit either way, and both branches supply one.
    let values: Vec<i64> = match vector.encoding() {
        Encoding::Flat { values, .. } => values.as_aligned::<i64>()?.to_vec(),
        _ => (0..vector.len())
            .map(|row| vector.get_i64(row).unwrap_or_default())
            .collect(),
    };
    Some(Arc::new(Int64Array::new(ScalarBuffer::from(values), nulls)))
}

/// Converts a flat *carried* fixed-width vector by copying its values through.
///
/// A type the engine carries but does not compute on arrives from `from_arrow`
/// flat and is never constant-encoded, so this reads the buffer directly. It is
/// shared by every carried type, so a new one costs a line in `to_arrow` (Rule
/// 38). Arrow requires a defined value beside every null bit, and the buffer
/// always holds one; `nulls` marks the bit.
fn to_arrow_primitive<T>(vector: &Vector) -> Option<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: AlignedValue,
{
    let Encoding::Flat { values, .. } = vector.encoding() else {
        // Expanding a constant or dictionary would be a materialisation, so
        // refuse rather than invent one.
        return None;
    };
    let values: Vec<T::Native> = values.as_aligned::<T::Native>()?.to_vec();
    let nulls = nulls_from_validity(vector.validity(), vector.len() as usize);
    Some(Arc::new(PrimitiveArray::<T>::new(
        ScalarBuffer::from(values),
        nulls,
    )))
}

fn to_arrow_utf8(vector: &Vector) -> Option<ArrayRef> {
    let len = vector.len() as usize;
    // Arrow wants `i32` offsets over a flat byte buffer, while Vein stores `u32`
    // offsets; only the offsets are rewritten, never the string bytes.
    let (offsets, values): (Vec<i32>, Vec<u8>) = match vector.encoding() {
        Encoding::Flat {
            values,
            offsets: Some(offsets),
        } => {
            let ends = (0..=vector.len()).map(|index| offsets.get_u32(index as usize));
            let offsets = ends
                .map(|end| end.map(|end| end as i32))
                .collect::<Option<Vec<i32>>>()?;
            (offsets, values.as_slice().to_vec())
        }
        Encoding::Constant(LogicalLiteral::String(value)) => {
            let mut bytes = Vec::new();
            let mut offsets = Vec::with_capacity(len + 1);
            offsets.push(0i32);
            for _ in 0..len {
                bytes.extend_from_slice(value.as_bytes());
                offsets.push(bytes.len() as i32);
            }
            (offsets, bytes)
        }
        _ => return None,
    };
    let nulls = nulls_from_validity(vector.validity(), len);
    Some(Arc::new(StringArray::new(
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Buffer::from_vec(values),
        nulls,
    )))
}

/// Converts a boolean vector into an Arrow boolean array.
///
/// The bits are already packed LSB-first, which is Arrow's layout, so this is a
/// byte copy rather than a per-row loop. A row whose bit is unknown is null in
/// Arrow, which is what keeps a projected condition nullable (ADR 0003).
fn to_arrow_boolean(vector: &Vector) -> Option<ArrayRef> {
    let Encoding::Flat { values, .. } = vector.encoding() else {
        // A constant boolean would have to be expanded to a buffer. That is a
        // materialisation, so refuse rather than invent one here.
        return None;
    };
    let len = vector.len() as usize;
    let bits = BooleanBuffer::new(Buffer::from_vec(values.as_slice().to_vec()), 0, len);
    let nulls = nulls_from_validity(vector.validity(), len);
    Some(Arc::new(BooleanArray::new(bits, nulls)))
}

/// Converts a `Decimal128` vector into an Arrow decimal array.
///
/// This does **not** go through `as_aligned::<i128>()`: a `BufferRef` guarantees
/// only `align_of::<u64>()`, so a 16-byte view would be address-dependent (ADR
/// 0002, Correction 2). Reconstructing each `i128` from its 16 bytes needs no
/// alignment at all, which keeps the conversion deterministic.
fn to_arrow_decimal128(vector: &Vector, precision: u8, scale: i8) -> Option<ArrayRef> {
    let Encoding::Flat { values, .. } = vector.encoding() else {
        return None;
    };
    let bytes = values.as_slice();
    if !bytes.len().is_multiple_of(16) {
        return None;
    }
    let values: Vec<i128> = bytes
        .chunks_exact(16)
        .map(|chunk| {
            // `chunks_exact(16)` yields exactly 16 bytes, so the conversion
            // cannot fail.
            i128::from_ne_bytes(chunk.try_into().expect("16 bytes"))
        })
        .collect();
    let nulls = nulls_from_validity(vector.validity(), vector.len() as usize);
    Decimal128Array::new(ScalarBuffer::from(values), nulls)
        .with_precision_and_scale(precision, scale)
        .ok()
        .map(|array| Arc::new(array) as ArrayRef)
}

/// Converts an Arrow array into a Vein vector.
///
/// Returns `None` for an Arrow type the bridge does not yet cover.
pub fn from_arrow(array: &ArrayRef) -> Option<Vector> {
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        let len = values.len();
        // Int64 is on the hot path, so it keeps the word-backed constructor: the
        // values are collected straight into the words the buffer stores, rather
        // than appended byte by byte and copied again.
        let words: Vec<u64> = values.values().iter().map(|value| *value as u64).collect();
        let validity = validity_from_nulls(values.nulls(), len);
        return Some(Vector::from_parts(
            VeinDataType::Int64,
            len as u32,
            BufferRef::from_words(words, len * 8),
            None,
            validity,
        ));
    }
    if let Some(bools) = array.as_any().downcast_ref::<BooleanArray>() {
        let len = bools.len();
        // Buffer 0 is the packed bit buffer; Arrow stores no second value buffer.
        let bits = BufferRef::from_vec(bools.to_data().buffers()[0].as_slice().to_vec());
        let validity = validity_from_nulls(bools.nulls(), len);
        return Some(Vector::from_parts(
            VeinDataType::Boolean,
            len as u32,
            bits,
            None,
            validity,
        ));
    }
    // Types the engine carries but does not compute on yet. `value(row)` reads
    // through the array's own offset, so a sliced batch copies the right rows.
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return Some(from_arrow_fixed(
            values.len(),
            VeinDataType::Float64,
            values.nulls(),
            |row, out| out.extend_from_slice(&values.value(row).to_ne_bytes()),
        ));
    }
    if let Some(values) = array.as_any().downcast_ref::<Date32Array>() {
        return Some(from_arrow_fixed(
            values.len(),
            VeinDataType::Date32,
            values.nulls(),
            |row, out| out.extend_from_slice(&values.value(row).to_ne_bytes()),
        ));
    }
    if let Some(values) = array.as_any().downcast_ref::<Decimal128Array>() {
        // The precision and scale travel with the type, so a decimal keeps its
        // meaning rather than becoming a bare 128-bit integer.
        let data_type = VeinDataType::Decimal128(values.precision(), values.scale());
        return Some(from_arrow_fixed(
            values.len(),
            data_type,
            values.nulls(),
            |row, out| out.extend_from_slice(&values.value(row).to_ne_bytes()),
        ));
    }
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        let len = strings.len();
        let mut offsets = Vec::with_capacity((len + 1) * 4);
        for offset in strings.value_offsets() {
            offsets.extend_from_slice(&(*offset as u32).to_ne_bytes());
        }
        let validity = validity_from_nulls(strings.nulls(), len);
        return Some(Vector::from_parts(
            VeinDataType::Utf8,
            len as u32,
            BufferRef::from_vec(strings.values().as_slice().to_vec()),
            Some(BufferRef::from_vec(offsets)),
            validity,
        ));
    }
    if let Some(dictionary) = array.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
        let values = from_arrow(dictionary.values())?;
        let len = dictionary.len();
        let indices: Vec<u32> = dictionary.keys().values().to_vec();
        let validity = validity_from_nulls(dictionary.keys().nulls(), len);
        return Some(Vector::dictionary(Arc::new(values), &indices).with_validity(validity));
    }
    None
}

/// Builds a fixed-width vector from an Arrow primitive array.
///
/// `write` appends one row's native-endian bytes. Arrow and Vein agree on
/// native-endian in-memory values — byte order belongs to file formats and is
/// handled at the storage boundary — so this is a copy, never a reinterpretation.
/// Shared by every fixed-width type (Rule 38).
fn from_arrow_fixed(
    len: usize,
    data_type: VeinDataType,
    nulls: Option<&NullBuffer>,
    mut write: impl FnMut(usize, &mut Vec<u8>),
) -> Vector {
    let mut values = Vec::new();
    for row in 0..len {
        write(row, &mut values);
    }
    Vector::from_parts(
        data_type,
        len as u32,
        BufferRef::from_vec(values),
        None,
        validity_from_nulls(nulls, len),
    )
}

/// Builds Arrow's null buffer from Vein validity, or `None` when there are no
/// nulls — an absent null buffer is what lets Arrow skip validity checks.
fn nulls_from_validity(validity: &Validity, len: usize) -> Option<NullBuffer> {
    if matches!(validity, Validity::AllValid) {
        return None;
    }
    let mut words = match validity {
        Validity::Bitmap(mask) => mask.to_vec(),
        _ => Vec::new(),
    };
    words.resize(len.div_ceil(64), 0);
    if matches!(validity, Validity::AllNull) {
        words.fill(0);
    }
    Some(NullBuffer::new(BooleanBuffer::new(
        Buffer::from_vec(mask_bytes(&words)),
        0,
        len,
    )))
}

/// Recovers Vein validity from Arrow's null buffer.
///
/// Bits are read one at a time rather than by reinterpreting the buffer, so the
/// bitmap's own bit offset cannot be mishandled.
fn validity_from_nulls(nulls: Option<&NullBuffer>, len: usize) -> Validity {
    let Some(nulls) = nulls else {
        return Validity::all_valid();
    };
    if nulls.null_count() == len {
        return Validity::all_null();
    }
    let mut words = vec![0u64; len.div_ceil(64)];
    for index in 0..len {
        if nulls.is_valid(index) {
            words[index / 64] |= 1 << (index % 64);
        }
    }
    Validity::Bitmap(words.into())
}

/// The byte view of bit-packed words, for Arrow's buffer constructors.
fn mask_bytes(words: &[u64]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(words.len() * 8);
    for word in words {
        bytes.extend_from_slice(&word.to_ne_bytes());
    }
    bytes
}

#[cfg(test)]
mod tests {
    use arrow::array::StringArray;

    use super::*;

    fn round_trip(vector: &Vector) -> Vector {
        let array = to_arrow(vector).expect("convertible to Arrow");
        from_arrow(&array).expect("convertible back to Vein")
    }

    #[test]
    fn values_survive_a_round_trip() {
        let vector = Vector::from_i64(&[1, -2, i64::MAX, i64::MIN]);
        let back = round_trip(&vector);
        assert_eq!(back.len(), vector.len());
        for row in 0..vector.len() {
            assert_eq!(back.get_i64(row), vector.get_i64(row));
        }
    }

    #[test]
    fn mixed_nulls_survive_a_round_trip() {
        // Rows 0 and 2 valid, row 1 null.
        let vector = Vector::from_i64(&[10, 20, 30]).with_validity(Validity::from_bitmap(vec![0b101]));
        let array = to_arrow(&vector).expect("convertible");
        assert_eq!(array.null_count(), 1, "Arrow must see the null");
        let back = from_arrow(&array).expect("convertible back");
        assert!(back.is_valid(0));
        assert!(back.is_null(1));
        assert!(back.is_valid(2));
        assert_eq!(back.get_i64(0), Some(10));
        assert_eq!(back.get_i64(1), None);
        assert_eq!(back.get_i64(2), Some(30));
    }

    #[test]
    fn all_null_survives_a_round_trip() {
        let vector = Vector::from_i64(&[1, 2, 3]).with_validity(Validity::all_null());
        let back = round_trip(&vector);
        for row in 0..3 {
            assert!(back.is_null(row));
        }
    }

    /// A vector with no nulls must not carry a null buffer: an absent buffer is
    /// what lets Arrow skip validity checks entirely.
    #[test]
    fn no_nulls_produces_no_null_buffer() {
        let array = to_arrow(&Vector::from_i64(&[1, 2, 3])).expect("convertible");
        assert_eq!(array.null_count(), 0);
        assert!(array.nulls().is_none());
    }

    #[test]
    fn constant_broadcasts_into_arrow() {
        let vector = Vector::constant(VeinDataType::Int64, types::LogicalLiteral::Int64(7), 4);
        let array = to_arrow(&vector).expect("convertible");
        let values = array.as_any().downcast_ref::<Int64Array>().expect("int64");
        assert_eq!(values.values(), &[7, 7, 7, 7]);
        assert!(values.nulls().is_none());
    }

    #[test]
    fn empty_vectors_round_trip() {
        let vector = Vector::from_i64(&[]);
        let back = round_trip(&vector);
        assert!(back.is_empty());
        assert_eq!(back.get_i64(0), None);
    }

    /// Unsupported types are refused rather than converted by guesswork.
    #[test]
    fn unsupported_types_return_none() {
        let floats = Vector::constant(
            VeinDataType::Float64,
            types::LogicalLiteral::Float64(1.0),
            2,
        );
        assert!(to_arrow(&floats).is_none());

        let arrows: ArrayRef = Arc::new(arrow::array::Int32Array::from(vec![1, 2]));
        assert!(from_arrow(&arrows).is_none());
    }

    /// A type the engine carries but does not compute on round-trips, nulls and
    /// all: this is what lets a scan project a column it never evaluates.
    #[test]
    fn carried_primitive_types_round_trip() {
        let floats = Vector::from_parts(
            VeinDataType::Float64,
            3,
            BufferRef::from_vec(
                [1.5f64, 0.0, -2.25]
                    .iter()
                    .flat_map(|value| value.to_ne_bytes())
                    .collect(),
            ),
            None,
            Validity::from_bitmap(vec![0b101]),
        );
        let array = to_arrow(&floats).expect("convertible");
        let values = array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64");
        assert_eq!(values.value(0), 1.5);
        assert_eq!(values.value(2), -2.25);
        assert!(values.is_null(1));
        assert_eq!(
            *from_arrow(&array).expect("back").data_type(),
            VeinDataType::Float64
        );

        let dates = Vector::from_parts(
            VeinDataType::Date32,
            2,
            BufferRef::from_vec(
                [19000i32, 19001]
                    .iter()
                    .flat_map(|value| value.to_ne_bytes())
                    .collect(),
            ),
            None,
            Validity::all_valid(),
        );
        let array = to_arrow(&dates).expect("convertible");
        let values = array
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date32");
        assert_eq!(values.value(0), 19000);
        assert_eq!(values.value(1), 19001);
        assert_eq!(
            *from_arrow(&array).expect("back").data_type(),
            VeinDataType::Date32
        );
    }

    #[test]
    fn utf8_values_survive_a_round_trip() {
        let vector = Vector::from_utf8(&["alpha", "", "beta", "日本語"]);
        let array = to_arrow(&vector).expect("convertible");
        let strings = array.as_any().downcast_ref::<StringArray>().expect("utf8");
        assert_eq!(strings.value(0), "alpha");
        assert_eq!(strings.value(1), "");
        assert_eq!(strings.value(3), "日本語");

        let back = from_arrow(&array).expect("convertible back");
        assert_eq!(back.len(), vector.len());
        for row in 0..vector.len() {
            assert_eq!(back.get_str(row), vector.get_str(row));
        }
    }

    #[test]
    fn utf8_null_rows_survive_a_round_trip() {
        // Row 0 valid, row 1 null.
        let vector = Vector::from_utf8(&["x", "y"]).with_validity(Validity::from_bitmap(vec![0b01]));
        let array = to_arrow(&vector).expect("convertible");
        assert_eq!(array.null_count(), 1);
        assert!(array.nulls().is_some(), "a null buffer must be present");

        let back = from_arrow(&array).expect("convertible back");
        assert_eq!(back.get_str(0), Some("x"));
        assert!(back.is_null(1));
        assert_eq!(back.get_str(1), None);
    }

    #[test]
    fn constant_strings_broadcast_into_arrow() {
        let vector = Vector::constant(VeinDataType::Utf8, types::LogicalLiteral::String("k".into()), 3);
        let array = to_arrow(&vector).expect("convertible");
        let strings = array.as_any().downcast_ref::<StringArray>().expect("utf8");
        assert_eq!(strings.len(), 3);
        for row in 0..3 {
            assert_eq!(strings.value(row), "k");
        }
        assert!(strings.nulls().is_none());
    }

    #[test]
    fn empty_utf8_vectors_round_trip() {
        let vector = Vector::from_utf8(&[]);
        let back = round_trip(&vector);
        assert!(back.is_empty());
        assert_eq!(back.get_str(0), None);
    }

    /// Rule 5: the boundary preserves the dictionary instead of flattening it.
    #[test]
    fn dictionary_encoding_survives_the_boundary() {
        let dictionary = Arc::new(Vector::from_utf8(&["red", "green", "blue"]));
        let vector = Vector::dictionary(dictionary, &[2, 0, 2, 1]);

        let array = to_arrow(&vector).expect("convertible");
        let arrow_dictionary = array
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
            .expect("Arrow must see a dictionary, not a flat array");
        assert_eq!(arrow_dictionary.len(), 4);
        assert_eq!(arrow_dictionary.keys().values(), &[2, 0, 2, 1]);

        let back = from_arrow(&array).expect("convertible back");
        assert!(
            matches!(back.encoding(), Encoding::Dictionary { .. }),
            "the encoding must round-trip, not degrade to flat"
        );
        for row in 0..vector.len() {
            assert_eq!(back.get_str(row), vector.get_str(row));
        }
        assert_eq!(back.index_at(3), Some(1));
    }

    #[test]
    fn dictionary_nulls_survive_the_boundary() {
        let dictionary = Arc::new(Vector::from_i64(&[10, 20]));
        // Row 0 valid, row 1 null.
        let vector = Vector::dictionary(dictionary, &[0, 1])
            .with_validity(Validity::from_bitmap(vec![0b01]));

        let array = to_arrow(&vector).expect("convertible");
        assert_eq!(array.null_count(), 1);
        let back = from_arrow(&array).expect("convertible back");
        assert_eq!(back.get_i64(0), Some(10));
        assert!(back.is_null(1));
        assert_eq!(back.get_i64(1), None);
    }

    #[test]
    fn flat_vectors_do_not_become_arrow_dictionaries() {
        let array = to_arrow(&Vector::from_utf8(&["a", "b"])).expect("convertible");
        assert!(array.as_any().downcast_ref::<StringArray>().is_some());
    }

    /// The differential check of `09` §4: the reference is Arrow's own view of
    /// the values, not a second Vein-internal path.
    #[test]
    fn arrow_values_match_the_source_vector() {
        let vector = Vector::from_i64(&[5, -6, 7]);
        let array = to_arrow(&vector).expect("convertible");
        let values = array.as_any().downcast_ref::<Int64Array>().expect("int64");
        assert_eq!(values.values(), &[5, -6, 7]);
        for row in 0..3 {
            assert_eq!(Some(values.value(row)), vector.get_i64(row as u32));
        }
    }

    /// A boolean vector round-trips through Arrow's own packed layout.
    #[test]
    fn boolean_vectors_round_trip() {
        // Rows: true, false, true.
        let vector = Vector::from_parts(
            VeinDataType::Boolean,
            3,
            BufferRef::from_words(vec![0b101], 8),
            None,
            Validity::all_valid(),
        );
        let array = to_arrow(&vector).expect("convertible");
        let bools = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean");
        assert!(bools.value(0));
        assert!(!bools.value(1));
        assert!(bools.value(2));
        assert_eq!(bools.null_count(), 0);

        let back = from_arrow(&array).expect("convertible back");
        assert_eq!(*back.data_type(), VeinDataType::Boolean);
        let Encoding::Flat { values, .. } = back.encoding() else {
            panic!("expected a flat boolean");
        };
        assert_eq!(values.as_slice()[0], 0b101);
    }

    /// An unknown boolean bit must reach Arrow as a null, not as false.
    #[test]
    fn an_unknown_boolean_bit_becomes_a_null() {
        // Row 0 known true, row 1 unknown.
        let vector = Vector::from_parts(
            VeinDataType::Boolean,
            2,
            BufferRef::from_words(vec![0b01], 8),
            None,
            Validity::from_bitmap(vec![0b01]),
        );
        let array = to_arrow(&vector).expect("convertible");
        assert_eq!(array.null_count(), 1);
        let back = from_arrow(&array).expect("convertible back");
        assert!(back.is_valid(0));
        assert!(!back.is_valid(1));
    }

    /// A decimal is carried with its precision and scale rather than as a bare
    /// 128-bit integer, and its bytes never need a 16-byte view of Vein storage.
    #[test]
    fn decimal_vectors_round_trip() {
        let mut bytes = Vec::new();
        for value in [1234i128, -5678] {
            bytes.extend_from_slice(&value.to_ne_bytes());
        }
        let vector = Vector::from_parts(
            VeinDataType::Decimal128(15, 2),
            2,
            BufferRef::from_vec(bytes),
            None,
            Validity::all_valid(),
        );
        let array = to_arrow(&vector).expect("convertible");
        let values = array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal128");
        assert_eq!(values.value(0), 1234);
        assert_eq!(values.value(1), -5678);
        assert_eq!(values.precision(), 15);
        assert_eq!(values.scale(), 2);

        let back = from_arrow(&array).expect("convertible back");
        assert_eq!(*back.data_type(), VeinDataType::Decimal128(15, 2));
    }
}
