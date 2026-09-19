use std::collections::BTreeMap;

use crate::pipeline::{DataChunk, PipelineNode, Source};
use common::protocol::{FieldSchema, FileFormat, ScanPredicate, ScanPredicateValue};
use common::Result;
use error::VeinError;
use opendal::Operator;
use exo::{
    canonical_from_f64, canonical_from_i32, canonical_from_i64, AddressFilter, AddressPredicate,
    ToCanonicalAddress,
};

/// Dual-Plane .exo format decoding logic
pub mod exo_scan;
/// Parquet format decoding logic using OpenDAL
pub mod parquet;

pub use exo_scan::{
    execute_exo_scan, execute_exo_scan_with_filter, execute_exo_scan_with_filter_and_stats,
    ExoScanStats,
};
pub use parquet::execute_parquet_scan;

/// A generic Source operator that scans files using OpenDAL and dispatches based on format.
pub struct ScanSource {
    operator: Operator,
    paths: Vec<String>,
    format: FileFormat,
    projection: Option<Vec<FieldSchema>>,
    address_filters: Vec<AddressFilter>,
    address_filters_unsatisfiable: bool,
}

impl ScanSource {
    /// Create a new [`ScanSource`].
    pub fn new(operator: Operator, paths: Vec<String>, format: FileFormat) -> Self {
        Self {
            operator,
            paths,
            format,
            projection: None,
            address_filters: Vec::new(),
            address_filters_unsatisfiable: false,
        }
    }

    /// Configures authoritative column projection pushdown with field types.
    pub fn with_projection(mut self, projection: Option<Vec<FieldSchema>>) -> Self {
        self.projection = projection;
        self
    }

    /// Configures an optional planner-provided address predicate.
    ///
    /// Unsupported values are intentionally ignored so the VM retains the
    /// complete residual predicate and query semantics remain unchanged.
    pub fn with_scan_predicates(mut self, predicates: &[ScanPredicate]) -> Self {
        let filters: Vec<_> = predicates
            .iter()
            .filter_map(address_filter_from_protocol)
            .collect();
        let (filters, unsatisfiable) = normalize_address_filters(filters);
        self.address_filters = filters;
        self.address_filters_unsatisfiable = unsatisfiable;
        self
    }
}

fn canonical_scan_value(value: &ScanPredicateValue) -> Option<u64> {
    match value {
        ScanPredicateValue::Boolean(value) => Some(*value as u64),
        ScanPredicateValue::Int64(value) => Some(canonical_from_i64(*value)),
        ScanPredicateValue::Float64(value) => Some(canonical_from_f64(*value)),
        ScanPredicateValue::String(value) => Some(value.as_str().to_canonical()),
        ScanPredicateValue::Date(value) => Some(canonical_from_i32(*value)),
        ScanPredicateValue::Timestamp(value) => Some(canonical_from_i64(*value)),
    }
}

fn address_filter_from_protocol(predicate: &ScanPredicate) -> Option<AddressFilter> {
    match predicate {
        ScanPredicate::Address {
            column,
            operator,
            value,
        } => {
            let value = canonical_scan_value(value)?;
            let predicate = match operator.as_str() {
                "eq" => AddressPredicate::Eq(value),
                "neq" => AddressPredicate::NotEq(value),
                "lt" => AddressPredicate::Lt(value),
                "lte" => AddressPredicate::LtEq(value),
                "gt" => AddressPredicate::Gt(value),
                "gte" => AddressPredicate::GtEq(value),
                _ => return None,
            };
            Some(AddressFilter::new(column.clone(), predicate))
        }
        ScanPredicate::AddressNull { column, is_null } => Some(AddressFilter::new(
            column.clone(),
            if *is_null {
                AddressPredicate::IsNull
            } else {
                AddressPredicate::IsNotNull
            },
        )),
        ScanPredicate::AddressIn { column, values } => {
            let values = values
                .iter()
                .map(canonical_scan_value)
                .collect::<Option<Vec<_>>>()?;
            Some(AddressFilter::new(column.clone(), AddressPredicate::In(values)))
        }
    }
}

#[derive(Default)]
struct AddressConstraint {
    equality: Option<u64>,
    lower: Option<(u64, bool)>,
    upper: Option<(u64, bool)>,
    allowed: Option<Vec<u64>>,
    nullability: Option<bool>,
    has_value_predicate: bool,
}

fn normalize_address_filters(filters: Vec<AddressFilter>) -> (Vec<AddressFilter>, bool) {
    let mut unique = Vec::with_capacity(filters.len());
    for filter in filters {
        if !unique.contains(&filter) {
            unique.push(filter);
        }
    }

    let mut constraints = BTreeMap::<String, AddressConstraint>::new();
    for filter in &unique {
        let constraint = constraints.entry(filter.column.clone()).or_default();
        match &filter.predicate {
            AddressPredicate::IsNull => {
                if constraint.nullability == Some(false) {
                    return (unique, true);
                }
                constraint.nullability = Some(true);
            }
            AddressPredicate::IsNotNull => {
                if constraint.nullability == Some(true) {
                    return (unique, true);
                }
                constraint.nullability = Some(false);
            }
            AddressPredicate::Eq(value) => {
                constraint.has_value_predicate = true;
                if let Some(existing) = constraint.equality {
                    if existing != *value {
                        return (unique, true);
                    }
                }
                constraint.equality = Some(*value);
            }
            AddressPredicate::Lt(value) | AddressPredicate::LtEq(value) => {
                constraint.has_value_predicate = true;
                let inclusive = matches!(filter.predicate, AddressPredicate::LtEq(_));
                update_upper_bound(&mut constraint.upper, *value, inclusive);
            }
            AddressPredicate::Gt(value) | AddressPredicate::GtEq(value) => {
                constraint.has_value_predicate = true;
                let inclusive = matches!(filter.predicate, AddressPredicate::GtEq(_));
                update_lower_bound(&mut constraint.lower, *value, inclusive);
            }
            AddressPredicate::In(values) => {
                constraint.has_value_predicate = true;
                let mut values = values.clone();
                values.sort_unstable();
                values.dedup();
                if let Some(allowed) = &mut constraint.allowed {
                    allowed.retain(|value| values.binary_search(value).is_ok());
                } else {
                    constraint.allowed = Some(values);
                }
            }
            AddressPredicate::NotEq(_) => {
                constraint.has_value_predicate = true;
            }
            AddressPredicate::Between { lower, upper } => {
                constraint.has_value_predicate = true;
                update_lower_bound(&mut constraint.lower, *lower, true);
                update_upper_bound(&mut constraint.upper, *upper, true);
            }
        }
    }

    for constraint in constraints.values() {
        if constraint.nullability == Some(true) && constraint.has_value_predicate {
            return (unique, true);
        }
        if let (Some(lower), Some(upper)) = (constraint.lower, constraint.upper) {
            if lower.0 > upper.0 || (lower.0 == upper.0 && (!lower.1 || !upper.1)) {
                return (unique, true);
            }
        }
        if let Some(equality) = constraint.equality {
            if let Some(allowed) = &constraint.allowed {
                if !allowed.contains(&equality) {
                    return (unique, true);
                }
            }
            if let Some((value, inclusive)) = constraint.lower {
                if equality < value || (equality == value && !inclusive) {
                    return (unique, true);
                }
            }
            if let Some((value, inclusive)) = constraint.upper {
                if equality > value || (equality == value && !inclusive) {
                    return (unique, true);
                }
            }
        }
        if let Some(allowed) = &constraint.allowed {
            if allowed.is_empty() {
                return (unique, true);
            }
            if let Some((value, inclusive)) = constraint.lower {
                if !allowed.iter().any(|candidate| {
                    *candidate > value || (*candidate == value && inclusive)
                }) {
                    return (unique, true);
                }
            }
            if let Some((value, inclusive)) = constraint.upper {
                if !allowed.iter().any(|candidate| {
                    *candidate < value || (*candidate == value && inclusive)
                }) {
                    return (unique, true);
                }
            }
        }
    }

    (unique, false)
}

fn update_lower_bound(bound: &mut Option<(u64, bool)>, value: u64, inclusive: bool) {
    match bound {
        Some((current, current_inclusive)) if *current > value => {}
        Some((current, current_inclusive)) if *current == value => {
            *current_inclusive &= inclusive;
        }
        _ => *bound = Some((value, inclusive)),
    }
}

fn update_upper_bound(bound: &mut Option<(u64, bool)>, value: u64, inclusive: bool) {
    match bound {
        Some((current, current_inclusive)) if *current < value => {}
        Some((current, current_inclusive)) if *current == value => {
            *current_inclusive &= inclusive;
        }
        _ => *bound = Some((value, inclusive)),
    }
}

#[async_trait::async_trait(?Send)]
impl Source<DataChunk> for ScanSource {
    #[tracing::instrument(skip_all)]
    async fn execute(&mut self, next: &mut dyn PipelineNode<DataChunk>) -> Result<()> {
        if self.address_filters_unsatisfiable {
            tracing::debug!("Skipping scan because address predicates are contradictory");
            next.flush()?;
            return Ok(());
        }

        match self.format {
            FileFormat::Parquet => {
                parquet::execute_parquet_scan(&self.operator, &self.paths, next).await?;
            }
            FileFormat::Exo => {
                exo_scan::execute_exo_scan_with_filter(
                    &self.paths,
                    self.projection.as_deref(),
                    (!self.address_filters.is_empty()).then_some(self.address_filters.as_slice()),
                    next,
                )
                .await?;
            }
            FileFormat::Csv => {
                return Err(VeinError::internal("CSV decoding not yet implemented"));
            }
            FileFormat::Json => {
                return Err(VeinError::internal("JSON decoding not yet implemented"));
            }
        }

        next.flush()?;
        Ok(())
    }
}

