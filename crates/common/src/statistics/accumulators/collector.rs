//! Generic streaming collector over borrowed Arrow batches.

use super::*;

struct GroupCollector<S> {
    columns: Vec<usize>,
    names: Vec<String>,
    sketch: Option<S>,
    complete: bool,
}

struct ColumnCollector<S> {
    name: String,
    data_type: DataType,
    sketch: Option<S>,
    distinct_complete: bool,
    null_count: u64,
    nan_count: u64,
    min_max_complete: bool,
    min_value: Option<LogicalLiteral>,
    max_value: Option<LogicalLiteral>,
    numeric: Option<NumericAccumulator>,
    byte_length: Option<ByteLengthAccumulator>,
    most_common: Option<MostCommonAccumulator>,
    histogram: Option<HistogramAccumulator>,
}

/// Streaming statistics for a logical table.
///
/// The collector processes batches incrementally. Its state is proportional to
/// column count and sketch precision, independent of table row count.
pub struct StatisticsCollector<S = HyperLogLog, E = ArrowScalarEncoder> {
    schema: SchemaRef,
    rows: u64,
    columns: Vec<ColumnCollector<S>>,
    groups: Vec<GroupCollector<S>>,
    config: StatisticsConfig,
    encoder: E,
}

impl<S, E> StatisticsCollector<S, E>
where
    S: MergeableDistinctSketch,
    E: ArrowValueEncoder,
{
    /// Create a collector for an Arrow schema and a pluggable scalar encoder.
    pub fn with_encoder(schema: SchemaRef, config: StatisticsConfig, encoder: E) -> Result<Self> {
        if config.scan_batch_size == 0 {
            return Err(VeinError::internal("statistics scan batch size must be greater than zero"));
        }
        if !(4..=16).contains(&config.distinct_precision) {
            return Err(VeinError::internal(format!(
                "distinct sketch precision must be between 4 and 16, got {}",
                config.distinct_precision
            )));
        }
        if !(1..1_000_000).contains(&config.histogram_confidence_ppm) {
            return Err(VeinError::internal("histogram confidence must be between 0 and 1,000,000 ppm"));
        }
        let mut columns = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let supported = encoder.supports(field.data_type());
            let bounds_supported = encoder.supports_bounds(field.data_type());
            let numeric = encoder.supports_numeric(field.data_type());
            let byte_length = encoder.supports_byte_lengths(field.data_type());
            columns.push(ColumnCollector {
                name: field.name().clone(),
                data_type: field.data_type().clone(),
                sketch: supported.then(|| S::with_config(&config)).transpose()?,
                distinct_complete: supported,
                null_count: 0,
                nan_count: 0,
                min_max_complete: bounds_supported,
                min_value: None,
                max_value: None,
                numeric: numeric.then(NumericAccumulator::default),
                byte_length: byte_length.then(|| ByteLengthAccumulator::for_type(
                    encoder.supports_non_ascii_tracking(field.data_type()),
                )),
                most_common: (bounds_supported && config.most_common_capacity > 0)
                    .then(|| MostCommonAccumulator::new(config.most_common_capacity)),
                histogram: (bounds_supported
                    && config.histogram_sample_size > 0
                    && config.histogram_bucket_count > 0)
                    .then(|| HistogramAccumulator::new(
                        config.histogram_sample_size,
                        config.histogram_bucket_count,
                        config.histogram_confidence_ppm,
                    )),
            });
        }
        let mut groups = Vec::with_capacity(config.multi_column_sets.len());
        for configured_names in &config.multi_column_sets {
            if configured_names.len() < 2 {
                return Err(VeinError::internal("multi-column statistics require at least two columns"));
            }
            let mut indices = Vec::with_capacity(configured_names.len());
            for name in configured_names {
                let mut matches = schema.fields().iter().enumerate().filter(|(_, field)| field.name() == name);
                let Some((index, _)) = matches.next() else {
                    return Err(VeinError::internal(format!("multi-column statistics reference unknown column '{name}'")));
                };
                if matches.next().is_some() || indices.contains(&index) {
                    return Err(VeinError::internal(format!("multi-column statistics contain an ambiguous or duplicate column '{name}'")));
                }
                indices.push(index);
            }
            indices.sort_unstable();
            if groups.iter().any(|group: &GroupCollector<S>| group.columns == indices) {
                return Err(VeinError::internal("duplicate multi-column statistics group"));
            }
            let complete = indices.iter().all(|index| encoder.supports(schema.field(*index).data_type()));
            let names = indices.iter().map(|index| schema.field(*index).name().clone()).collect();
            groups.push(GroupCollector {
                columns: indices,
                names,
                sketch: complete.then(|| S::with_config(&config)).transpose()?,
                complete,
            });
        }
        Ok(Self {
            schema,
            rows: 0,
            columns,
            groups,
            config,
            encoder,
        })
    }

    /// Observe active values from a reference-backed statistics input.
    /// Column names belong to the collector schema; inputs are matched by
    /// position and type, allowing query-local column identities as keys.
    pub fn update_input<I: super::super::StatisticsInput + ?Sized>(
        &mut self,
        batch: &I,
    ) -> Result<()> {
        if batch.num_columns() != self.columns.len() {
            return Err(VeinError::internal("statistics input column count changed"));
        }
        for (index, column) in self.columns.iter().enumerate() {
            if column.data_type != *batch.column_type(index)? {
                return Err(VeinError::internal("statistics input column type changed"));
            }
        }
        let next_rows = self
            .rows
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| VeinError::internal("statistics row count overflow"))?;

        for (index, column) in self.columns.iter_mut().enumerate() {
            for logical_row in 0..batch.num_rows() {
                let value = batch.value(index, logical_row)?;
                if value.is_none_or(|(array, row)| array.is_null(row)) {
                    column.null_count = column.null_count.checked_add(1)
                        .ok_or_else(|| VeinError::internal("statistics null count overflow"))?;
                    continue;
                }
                let (array, row) = value.expect("non-null source value was checked");

                let is_nan = self.encoder.is_nan(array, row);
                if is_nan {
                    column.nan_count = column.nan_count.checked_add(1)
                        .ok_or_else(|| VeinError::internal("statistics NaN count overflow"))?;
                }

                if let Some(sketch) = &mut column.sketch
                    && column.distinct_complete {
                    if let Some(hash) = self.encoder.hash_value(array, row) {
                        sketch.update_hash(hash);
                    } else {
                        column.distinct_complete = false;
                    }
                }

                let numeric_value = self.encoder.numeric_value(array, row);
                if let Some(numeric) = &mut column.numeric {
                    if let Some(value) = numeric_value {
                        numeric.update(value)?;
                    } else {
                        column.numeric = None;
                    }
                }

                let byte_length_value = self.encoder.byte_length(array, row);
                if let Some(byte_length) = &mut column.byte_length {
                    if let Some(length) = byte_length_value {
                        byte_length.update(length, self.encoder.contains_non_ascii(array, row))?;
                    } else {
                        column.byte_length = None;
                    }
                }

                if byte_length_value.is_some_and(|length| {
                    length > self.config.max_synopsis_value_bytes as u64
                }) {
                    column.most_common = None;
                    column.histogram = None;
                }

                if !is_nan {
                    if let Some(most_common) = &mut column.most_common
                        && !most_common.update(
                        &self.encoder,
                        array,
                        row,
                        self.config.max_synopsis_value_bytes,
                    )? {
                        column.most_common = None;
                    }
                    if let Some(histogram) = &mut column.histogram
                        && !histogram.update(|| {
                        self.encoder.bound_value(
                            array,
                            row,
                            self.config.max_synopsis_value_bytes,
                        )
                    })? {
                        column.histogram = None;
                    }
                }

                if column.min_max_complete && !is_nan {
                    let comparison = match (&column.min_value, &column.max_value) {
                        (None, None) => None,
                        (Some(min), Some(max)) => Some((
                            self.encoder.compare_bound(array, row, min),
                            self.encoder.compare_bound(array, row, max),
                        )),
                        _ => Some((None, None)),
                    };
                    match comparison {
                        None => {
                            let bound = self.encoder.bound_value(
                                array, row, self.config.max_bound_value_bytes,
                            );
                            if let Some(bound) = bound {
                                column.min_value = Some(bound.clone());
                                column.max_value = Some(bound);
                            } else {
                                column.min_max_complete = false;
                            }
                        }
                        Some((Some(min_order), Some(max_order))) => {
                            if min_order.is_lt() {
                                column.min_value = self.encoder.bound_value(
                                    array, row, self.config.max_bound_value_bytes,
                                );
                            }
                            if max_order.is_gt() {
                                column.max_value = self.encoder.bound_value(
                                    array, row, self.config.max_bound_value_bytes,
                                );
                            }
                            if column.min_value.is_none() || column.max_value.is_none() {
                                column.min_max_complete = false;
                            }
                        }
                        Some(_) => column.min_max_complete = false,
                    }
                    if !column.min_max_complete {
                        column.min_value = None;
                        column.max_value = None;
                    }
                }
            }
        }
        for group in &mut self.groups {
            if !group.complete || group.sketch.is_none() { continue; }
            let mut group_complete = true;
            for row in 0..batch.num_rows() {
                let mut tuple_hash = stable_hash(0xfc, &(group.columns.len() as u64).to_le_bytes());
                for (ordinal, column_index) in group.columns.iter().copied().enumerate() {
                    let Some(tag) = type_tag(batch.column_type(column_index)?) else {
                        group_complete = false;
                        break;
                    };
                    let value = batch.value(column_index, row)?;
                    let value_hash = if value.is_none_or(|(array, index)| array.is_null(index)) {
                        stable_hash(0xfb, &[tag, 0])
                    } else if let Some(hash) = value.and_then(|(array, index)| self.encoder.hash_value(array, index)) {
                        let mut tagged_hash = [0_u8; 9];
                        tagged_hash[0] = tag;
                        tagged_hash[1..].copy_from_slice(&hash.to_le_bytes());
                        stable_hash(0xfb, &tagged_hash)
                    } else {
                        group_complete = false;
                        break;
                    };
                    let mut tuple_part = [0_u8; 24];
                    tuple_part[..8].copy_from_slice(&tuple_hash.to_le_bytes());
                    tuple_part[8..16].copy_from_slice(&(ordinal as u64).to_le_bytes());
                    tuple_part[16..].copy_from_slice(&value_hash.to_le_bytes());
                    tuple_hash = stable_hash(0xfa, &tuple_part);
                }
                if !group_complete {
                    break;
                }
                if let Some(sketch) = &mut group.sketch {
                    sketch.update_hash(tuple_hash);
                }
            }
            if !group_complete {
                group.complete = false;
                group.sketch = None;
            }
        }
        self.rows = next_rows;
        Ok(())
    }

    /// Restore a collector from a complete prior table summary for append.
    ///
    /// Unsupported, missing, or malformed synopsis state returns an error so
    /// callers can preserve unknown statistics instead of publishing a partial
    /// table summary as complete.
    pub fn from_statistics(
        schema: SchemaRef,
        config: StatisticsConfig,
        encoder: E,
        statistics: &TableStatistics,
    ) -> Result<Self> {
        let mut collector = Self::with_encoder(schema, config, encoder)?;
        let rows = statistics
            .num_rows
            .ok_or_else(|| VeinError::internal("cannot append to statistics without row count"))?;
        let restore_config = collector.config.clone();
        for column in &mut collector.columns {
            let summary = statistics.column_statistics.get(&column.name).ok_or_else(|| {
                VeinError::internal(format!(
                    "cannot append: statistics are missing column '{}'",
                    column.name
                ))
            })?;
            if !summary.is_consistent_with_rows(rows) {
                return Err(VeinError::internal(format!(
                    "cannot append: statistics are inconsistent for column '{}'",
                    column.name
                )));
            }
            if summary.min_max_complete.is_none() {
                return Err(VeinError::internal(format!(
                    "cannot append: min/max completeness is unknown for column '{}'",
                    column.name
                )));
            }
            let nulls = summary.null_count.ok_or_else(|| {
                VeinError::internal(format!(
                    "cannot append: null count is unknown for column '{}'",
                    column.name
                ))
            })?;
            column.null_count = nulls;
            column.nan_count = summary.nan_count.ok_or_else(|| VeinError::internal(format!(
                "cannot append: NaN count is unknown for column '{}'", column.name
            )))?;
            if column.min_max_complete {
                match summary.min_max_complete {
                    Some(true) => match (&summary.min_value, &summary.max_value) {
                        (None, None) => {}
                        (Some(min), Some(max)) => {
                            column.min_value = Some(min.clone());
                            column.max_value = Some(max.clone());
                        }
                        _ => {
                            return Err(VeinError::internal(format!(
                                "cannot append: min/max bounds are inconsistent for column '{}'",
                                column.name
                            )));
                        }
                    },
                    Some(false) => {
                        column.min_max_complete = false;
                        column.min_value = None;
                        column.max_value = None;
                    }
                    None => return Err(VeinError::internal(format!(
                        "cannot append: min/max completeness is unknown for column '{}'",
                        column.name
                    ))),
                }
            }
            if let Some(numeric) = &mut column.numeric {
                let summary = summary.numeric.as_ref().ok_or_else(|| VeinError::internal(format!(
                    "cannot append: numeric statistics are missing for column '{}'", column.name
                )))?;
                *numeric = NumericAccumulator::restore(summary);
            }
            if let Some(byte_length) = &mut column.byte_length {
                let summary = summary.byte_length.as_ref().ok_or_else(|| VeinError::internal(format!(
                    "cannot append: byte-length statistics are missing for column '{}'", column.name
                )))?;
                *byte_length = ByteLengthAccumulator::restore(summary);
            }
            if column.most_common.is_some() {
                column.most_common = summary.most_common_values.as_ref()
                    .map(|summary| MostCommonAccumulator::restore(summary, restore_config.most_common_capacity))
                    .transpose()?;
            }
            if column.histogram.is_some() {
                column.histogram = summary.histogram.as_ref()
                    .map(|summary| HistogramAccumulator::restore(summary, &restore_config))
                    .transpose()?;
            }
            if column.sketch.is_some() {
                let state = summary.distinct_sketch.as_ref().ok_or_else(|| {
                    VeinError::internal(format!(
                        "cannot append: distinct sketch is missing for column '{}'",
                        column.name
                    ))
                })?;
                column.sketch = Some(S::from_serialized(state)?);
                if !column.distinct_complete {
                    return Err(VeinError::internal(format!(
                        "cannot append: distinct synopsis is incomplete for column '{}'",
                        column.name
                    )));
                }
            }
        }
        for group in &mut collector.groups {
            let mut matches = statistics.column_groups.iter()
                .filter(|summary| summary.columns == group.names);
            if let Some(summary) = matches.next() {
                if matches.next().is_some() {
                    return Err(VeinError::internal("cannot append: duplicate multi-column synopsis"));
                }
                match (&group.sketch, &summary.distinct_sketch) {
                    (Some(_), Some(state)) if summary.distinct_count.is_some() => {
                        group.sketch = Some(S::from_serialized(state)?);
                    }
                    _ => {
                        group.sketch = None;
                        group.complete = false;
                    }
                }
            } else {
                group.sketch = None;
                group.complete = false;
            }
        }
        collector.rows = rows;
        Ok(collector)
    }

    /// Merge two partial collectors built from the same schema/configuration.
    pub fn merge(&mut self, other: &Self) -> Result<()> {
        if self.schema.as_ref() != other.schema.as_ref()
            || self.config != other.config
            || !self.encoder.merge_compatible(&other.encoder)
            || self.columns.len() != other.columns.len()
        {
            return Err(VeinError::internal(
                "cannot merge statistics collected with different schemas or configurations",
            ));
        }
        let rows = self
            .rows
            .checked_add(other.rows)
            .ok_or_else(|| VeinError::internal("statistics row count overflow"))?;
        for (left, right) in self.columns.iter_mut().zip(&other.columns) {
            let left_ordered_values = self.rows.saturating_sub(left.null_count).saturating_sub(left.nan_count);
            let right_ordered_values = other.rows.saturating_sub(right.null_count).saturating_sub(right.nan_count);
            left.null_count = left
                .null_count
                .checked_add(right.null_count)
                .ok_or_else(|| VeinError::internal("statistics null count overflow"))?;
            left.nan_count = left
                .nan_count
                .checked_add(right.nan_count)
                .ok_or_else(|| VeinError::internal("statistics NaN count overflow"))?;
            left.min_max_complete &= right.min_max_complete;
            if left.min_max_complete {
                match (&left.min_value, &left.max_value, &right.min_value, &right.max_value) {
                    (None, None, None, None) if left_ordered_values == 0 && right_ordered_values == 0 => {}
                    (None, None, Some(right_min), Some(right_max)) if left_ordered_values == 0 => {
                        left.min_value = Some(right_min.clone());
                        left.max_value = Some(right_max.clone());
                    }
                    (Some(_), Some(_), None, None) if right_ordered_values == 0 => {}
                    (Some(left_min), Some(left_max), Some(right_min), Some(right_max)) => {
                        let min_order = left_min.try_cmp(right_min);
                        let max_order = left_max.try_cmp(right_max);
                        if let (Some(min_order), Some(max_order)) = (min_order, max_order) {
                            if min_order.is_gt() { left.min_value = Some(right_min.clone()); }
                            if max_order.is_lt() { left.max_value = Some(right_max.clone()); }
                        } else {
                            left.min_max_complete = false;
                            left.min_value = None;
                            left.max_value = None;
                        }
                    }
                    _ => {
                        left.min_max_complete = false;
                        left.min_value = None;
                        left.max_value = None;
                    }
                }
            } else {
                left.min_value = None;
                left.max_value = None;
            }
            match (&mut left.numeric, &right.numeric) {
                (Some(left_numeric), Some(right_numeric)) => left_numeric.merge(right_numeric)?,
                (None, None) => {}
                _ => left.numeric = None,
            }
            match (&mut left.byte_length, &right.byte_length) {
                (Some(left_lengths), Some(right_lengths)) => left_lengths.merge(right_lengths)?,
                (None, None) => {}
                _ => left.byte_length = None,
            }
            match (&mut left.most_common, &right.most_common) {
                (Some(left_values), Some(right_values)) => left_values.merge(right_values)?,
                _ => left.most_common = None,
            }
            match (&mut left.histogram, &right.histogram) {
                (Some(left_histogram), Some(right_histogram)) => left_histogram.merge(right_histogram)?,
                _ => left.histogram = None,
            }
            left.distinct_complete &= right.distinct_complete;
            match (&mut left.sketch, &right.sketch) {
                (Some(left_sketch), Some(right_sketch)) if left.distinct_complete => {
                    left_sketch.merge(right_sketch)?
                }
                (Some(_), Some(_)) => {}
                (None, None) => {}
                _ => {
                    return Err(VeinError::internal(
                        "cannot merge statistics with different synopsis support",
                    ));
                }
            }
        }
        if self.groups.len() != other.groups.len() {
            return Err(VeinError::internal("cannot merge different multi-column statistics groups"));
        }
        for (left, right) in self.groups.iter_mut().zip(&other.groups) {
            if left.names != right.names {
                return Err(VeinError::internal("cannot merge different multi-column statistics groups"));
            }
            match (&mut left.sketch, &right.sketch) {
                (Some(left_sketch), Some(right_sketch)) if left.complete && right.complete => {
                    left_sketch.merge(right_sketch)?;
                }
                _ => {
                    left.sketch = None;
                    left.complete = false;
                }
            }
        }
        self.rows = rows;
        Ok(())
    }

    /// Finalize the table summary. Footer adapters may merge their independent
    /// storage facts and bounds into this value.
    pub fn finish(self) -> TableStatistics {
        let mut statistics = TableStatistics {
            num_rows: Some(self.rows),
            ..TableStatistics::default()
        };
        for column in self.columns {
            let sketch = column.sketch.filter(|_| column.distinct_complete);
            let estimate = sketch.as_ref().and_then(MergeableDistinctSketch::estimate)
                .map(|value| value.min(self.rows.saturating_sub(column.null_count)));
            let summary = ColumnStatistics {
                null_count: Some(column.null_count),
                nan_count: Some(column.nan_count),
                distinct_count: estimate,
                distinct_count_relative_error: sketch.as_ref().and_then(MergeableDistinctSketch::relative_error),
                distinct_sketch: sketch.as_ref().map(MergeableDistinctSketch::to_serialized),
                min_value: column.min_value,
                max_value: column.max_value,
                min_max_complete: Some(column.min_max_complete),
                numeric: column.numeric.map(|numeric| numeric.finish()),
                byte_length: column.byte_length.map(|byte_length| byte_length.finish()),
                most_common_values: column.most_common.map(|most_common| {
                    let mut summary = most_common.summary;
                    summary
                        .candidates
                        .sort_by_key(|candidate| std::cmp::Reverse(candidate.lower_count));
                    summary
                }),
                histogram: column.histogram.and_then(HistogramAccumulator::finish),
            };
            statistics.column_statistics.insert(column.name, summary);
        }
        for group in self.groups {
            let Some(sketch) = group.sketch.filter(|_| group.complete) else { continue };
            statistics.column_groups.push(ColumnGroupStatistics {
                columns: group.names,
                distinct_count: sketch.estimate().map(|estimate| estimate.min(self.rows)),
                distinct_count_relative_error: sketch.relative_error(),
                distinct_sketch: Some(sketch.to_serialized()),
            });
        }
        statistics
    }
}

impl StatisticsCollector<HyperLogLog, ArrowScalarEncoder> {
    /// Create a collector with Vein's stable Arrow scalar encoder.
    pub fn new(schema: SchemaRef, config: StatisticsConfig) -> Result<Self> {
        Self::with_encoder(schema, config, ArrowScalarEncoder)
    }

    /// Restore a standard collector from a prior summary.
    pub fn restore(
        schema: SchemaRef,
        config: StatisticsConfig,
        statistics: &TableStatistics,
    ) -> Result<Self> {
        Self::from_statistics(schema, config, ArrowScalarEncoder, statistics)
    }
}
