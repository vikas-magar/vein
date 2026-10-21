use catalog::StatisticsSourceIdentity;
use storage::StorageObjectVersion;

/// Builds the stable statistics identity shared by catalog and writer paths.
pub fn statistics_source_identity(
    identifier: impl Into<String>,
    size_bytes: u64,
    version: &StorageObjectVersion,
) -> StatisticsSourceIdentity {
    let modified_nanos = version
        .last_modified
        .map_or(0, |timestamp| timestamp.unix_nanos());
    StatisticsSourceIdentity {
        identifier: identifier.into(),
        version: format!(
            "size={size_bytes};etag={:?};provider_version={:?};modified_nanos={modified_nanos}",
            version.etag, version.provider_version
        ),
    }
}

#[cfg(test)]
#[path = "source_identity_tests.rs"]
mod tests;
