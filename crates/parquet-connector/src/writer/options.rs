use std::path::Component;

use common::{Result, VeinError};

pub(super) fn safe_table_name(name: &str) -> Result<&str> {
    let mut components = std::path::Path::new(name).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) if !name.is_empty() && name != "." => Ok(name),
        _ => Err(VeinError::planning("WRITE target must be a single logical table name", None)),
    }
}

pub(super) fn validate_target_parts<'a>(
    destination_uri: &str,
    options: impl IntoIterator<Item=(&'a str, &'a str)>,
    allow_partition_id: bool,
) -> Result<(String, String)> {
    let table = safe_table_name(destination_uri)?.to_owned();
    let mut seen = Vec::new();
    let mut mode = "create";
    for (key, value) in options {
        if seen.iter().any(|prior: &&str| prior.eq_ignore_ascii_case(key)) {
            return Err(VeinError::planning(format!("duplicate WRITE option '{key}'"), None));
        }
        seen.push(key);
        if key.eq_ignore_ascii_case("mode") {
            if !["create", "append", "overwrite"].iter().any(|supported| value.eq_ignore_ascii_case(supported)) {
                return Err(VeinError::planning("WRITE mode must be create, append, or overwrite", None));
            }
            mode = if value.eq_ignore_ascii_case("create") { "create" } else if value.eq_ignore_ascii_case("append") { "append" } else { "overwrite" };
        } else if key.eq_ignore_ascii_case("format") {
            if !value.eq_ignore_ascii_case("parquet") {
                return Err(VeinError::planning("local WRITE supports only format='parquet'", None));
            }
        } else if key.eq_ignore_ascii_case("partition_id") && allow_partition_id {
            value.parse::<u16>().map_err(|_| VeinError::planning("invalid internal WRITE partition identifier", None))?;
        } else if key.eq_ignore_ascii_case("partition_id") {
            return Err(VeinError::planning("WRITE option 'partition_id' is reserved for the execution engine", None));
        } else {
            return Err(VeinError::planning(format!("local Parquet WRITE does not support option '{key}'"), None));
        }
    }
    Ok((table, mode.to_owned()))
}
