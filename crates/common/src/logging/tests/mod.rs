use super::{LogFormat, is_vein_target, parse_ansi, parse_filter};

#[test]
fn format_parser_accepts_only_supported_formats_case_insensitively() {
    assert_eq!(LogFormat::parse(None).unwrap(), LogFormat::Pretty);
    assert_eq!(LogFormat::parse(Some("JSON")).unwrap(), LogFormat::Json);
    assert!(LogFormat::parse(Some("yaml")).is_err());
}

#[test]
fn invalid_filter_and_ansi_values_fail_instead_of_silently_falling_back() {
    assert!(parse_filter(Some("vein=not-a-level")).is_err());
    assert!(!parse_ansi(Some("off")).unwrap());
    assert!(parse_ansi(Some("sometimes")).is_err());
}

#[test]
fn audit_export_selects_all_vein_namespaces_and_excludes_unrelated_targets() {
    assert!(is_vein_target("vein.audit"));
    assert!(is_vein_target("vein.parquet_bloom"));
    assert!(!is_vein_target("exec::operators::scan"));
    assert!(!is_vein_target("parquet::reader"));
}

#[test]
fn path_log_identity_is_correlatable_without_revealing_the_path() {
    let path = "/private/customer-data/orders.parquet";
    let first = super::LogLocation::from(path).to_string();
    let second = super::LogLocation::from(path).to_string();
    assert_eq!(first, second);
    assert!(first.starts_with("loc:"));
    assert!(!first.contains(path));
}
