//! Borrowed, format-neutral inputs supplied to a scan provider.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use catalog::MetadataStore;
use common::protocol::{ColumnEncodingHint, RuntimeStatisticsScan, ScanPredicate};
use common::statistics::{QueryStatisticsContext, RuntimeStatisticsStore};
use functions::FunctionRegistry;
use storage::ReadStorage;

use crate::expression::eval::ExpressionKernelProvider;
use crate::runtime::filter::RuntimeFilterSet;
use types::VeinSchema;

use super::{ScanFile, ScanWorkerState};

/// Borrowed inputs for one source scan invocation.
///
/// Providers may interpret encoding hints and reader-defined scan-unit ranges
/// according to their registered file format. The request itself keeps no
/// payload copies and providers emit reference-backed pipeline views.
pub struct ScanRequest<'a> {
    /// Provider-local state retained across this worker's scan work units.
    pub worker_state: &'a ScanWorkerState,
    /// Root-scoped storage interface used to read the scan files.
    pub storage: &'a Arc<dyn ReadStorage>,
    /// Files and optional provider-defined unit ranges assigned to this scan.
    pub files: &'a [ScanFile],
    /// Columns requested by the query plan.
    pub projected: &'a VeinSchema,
    /// Best-effort requested physical output encodings.
    pub column_encoding_hints: &'a [ColumnEncodingHint],
    /// Compiled predicates the provider may evaluate during scanning.
    pub pushdown: Option<&'a ScanPredicate>,
    /// Shared cache for parsed source metadata.
    pub metadata_store: &'a MetadataStore,
    /// Functions available to compiled scan predicates.
    pub registry: &'a Arc<FunctionRegistry>,
    /// Shared typed kernels used by source predicates.
    pub expression_kernels: &'a Arc<dyn ExpressionKernelProvider>,
    /// Shared byte counter for storage reads performed by the scan.
    pub bytes_read: &'a Arc<AtomicU64>,
    /// Query-local filters produced by runtime operators.
    pub runtime_filters: &'a RuntimeFilterSet,
    /// Background collector for lazily computed runtime statistics.
    pub runtime_statistics: &'a RuntimeStatisticsStore,
    /// Optional scan identity and coverage information for statistics.
    pub statistics_scan: Option<&'a RuntimeStatisticsScan>,
    /// Shared statistics context for this query, when available.
    pub query_statistics: Option<&'a Arc<QueryStatisticsContext>>,
}
