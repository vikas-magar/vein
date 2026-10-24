//! Reusable typed access to source values; no temporary arrays or gathered inputs.
use super::*;

trait OrderedArray: Array {
    type Value<'a>
    where
        Self: 'a;
    fn value_at(&self, row: usize) -> Self::Value<'_>;
    fn order(left: Self::Value<'_>, right: Self::Value<'_>) -> Ordering;
}

impl<T: ArrowPrimitiveType> OrderedArray for PrimitiveArray<T>
where
    T::Native: ArrowNativeTypeOp,
{
    type Value<'a> = T::Native
    where
        Self: 'a;
    fn value_at(&self, row: usize) -> Self::Value<'_> { self.value(row) }
    fn order(left: T::Native, right: T::Native) -> Ordering { left.compare(right) }
}

impl<O: OffsetSizeTrait> OrderedArray for GenericStringArray<O> {
    type Value<'a> = &'a str
    where
        Self: 'a;
    fn value_at(&self, row: usize) -> &str { self.value(row) }
    fn order(left: &str, right: &str) -> Ordering { left.cmp(right) }
}

impl OrderedArray for StringViewArray {
    type Value<'a> = &'a str;
    fn value_at(&self, row: usize) -> &str { self.value(row) }
    fn order(left: &str, right: &str) -> Ordering { left.cmp(right) }
}

impl OrderedArray for BooleanArray {
    type Value<'a> = bool;
    fn value_at(&self, row: usize) -> bool { self.value(row) }
    fn order(left: bool, right: bool) -> Ordering { left.cmp(&right) }
}

pub(super) fn dispatch(
    op: BinaryExprOp, left: &ExpressionValue, right: &ExpressionValue, rows: usize,
) -> Option<Result<ExpressionValue>> {
    macro_rules! run {
        ($array:ty) => { Some(compare::<$array>(op, left, right, rows)) };
    }
    match left.data_type() {
        DataType::Boolean => run!(BooleanArray),
        DataType::Int8 => run!(Int8Array),
        DataType::Int16 => run!(Int16Array),
        DataType::Int32 => run!(Int32Array),
        DataType::Int64 => run!(Int64Array),
        DataType::UInt8 => run!(UInt8Array),
        DataType::UInt16 => run!(UInt16Array),
        DataType::UInt32 => run!(UInt32Array),
        DataType::UInt64 => run!(UInt64Array),
        DataType::Float16 => run!(Float16Array),
        DataType::Float32 => run!(Float32Array),
        DataType::Float64 => run!(Float64Array),
        DataType::Date32 => run!(Date32Array),
        DataType::Date64 => run!(Date64Array),
        DataType::Decimal128(_, _) => run!(Decimal128Array),
        DataType::Decimal256(_, _) => run!(Decimal256Array),
        DataType::Utf8 => run!(StringArray),
        DataType::LargeUtf8 => run!(LargeStringArray),
        DataType::Utf8View => run!(StringViewArray),
        _ => None,
    }
}

fn compare<A: OrderedArray + 'static>(
    op: BinaryExprOp, left: &ExpressionValue, right: &ExpressionValue, rows: usize,
) -> Result<ExpressionValue> {
    let mut output = BooleanBuilder::with_capacity(rows);
    for row in 0..rows {
        let left = left.source_row(row, rows)?;
        let right = right.source_row(row, rows)?;
        let Some(((left, l, _), (right, r, _))) = left.zip(right) else {
            output.append_null();
            continue;
        };
        if left.is_null(l) || right.is_null(r) {
            output.append_null();
            continue;
        }
        let left = left.as_any().downcast_ref::<A>()
            .ok_or_else(|| VeinError::internal("comparison left input layout differs"))?;
        let right = right.as_any().downcast_ref::<A>()
            .ok_or_else(|| VeinError::internal("comparison right input layout differs"))?;
        output.append_value(matches(op, A::order(left.value_at(l), right.value_at(r))));
    }
    Ok(ExpressionValue::Array(Arc::new(output.finish())))
}
