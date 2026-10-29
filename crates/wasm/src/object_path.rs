#[cfg(any(target_arch = "wasm32", test))]
use storage::StorageObjectPath;

#[cfg(test)]
mod tests;

#[cfg(any(target_arch = "wasm32", test))]
pub(crate) fn valid_object_path(path: &StorageObjectPath) -> bool {
    let value = path.as_str();
    !value.is_empty()
        && !value.starts_with('/')
        && !value
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
}
