//! Selects scan batch rows from assigned Parquet row-group geometry.

const FALLBACK_BATCH_ROWS: usize = 8_192;
const MAX_DEFAULT_BATCH_ROWS: usize = 131_072;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BatchSizeSource {
    EnvironmentOverride,
    RowGroupMetadata,
    Fallback,
}

impl BatchSizeSource {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::EnvironmentOverride => "environment_override",
            Self::RowGroupMetadata => "row_group_metadata",
            Self::Fallback => "fallback",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BatchSizeSelection {
    pub(super) rows: usize,
    pub(super) source: BatchSizeSource,
    pub(super) largest_assigned_row_group: Option<usize>,
    pub(super) invalid_override: bool,
}

pub(super) trait ScanBatchSizePolicy {
    fn select<I>(
        &self,
        row_group_rows: I,
        override_rows: Option<&str>,
    ) -> BatchSizeSelection
    where
        I: IntoIterator<Item=i64>;
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RowGroupBatchSizePolicy {
    fallback_rows: usize,
    max_default_rows: usize,
}

impl Default for RowGroupBatchSizePolicy {
    fn default() -> Self {
        Self {
            fallback_rows: FALLBACK_BATCH_ROWS,
            max_default_rows: MAX_DEFAULT_BATCH_ROWS,
        }
    }
}

impl ScanBatchSizePolicy for RowGroupBatchSizePolicy {
    fn select<I>(
        &self,
        row_group_rows: I,
        override_rows: Option<&str>,
    ) -> BatchSizeSelection
    where
        I: IntoIterator<Item=i64>,
    {
        let largest_assigned_row_group = row_group_rows
            .into_iter()
            .filter_map(|rows| usize::try_from(rows).ok().filter(|rows| *rows > 0))
            .max();

        if let Some(rows) = override_rows
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|rows| *rows > 0)
        {
            return BatchSizeSelection {
                rows,
                source: BatchSizeSource::EnvironmentOverride,
                largest_assigned_row_group,
                invalid_override: false,
            };
        }

        let invalid_override = override_rows.is_some();
        let source = if largest_assigned_row_group.is_some() {
            BatchSizeSource::RowGroupMetadata
        } else {
            BatchSizeSource::Fallback
        };
        let rows = largest_assigned_row_group
            .unwrap_or(self.fallback_rows)
            .min(self.max_default_rows)
            .max(1);

        BatchSizeSelection {
            rows,
            source,
            largest_assigned_row_group,
            invalid_override,
        }
    }
}

#[cfg(test)]
#[path = "batch_size_tests.rs"]
mod tests;
