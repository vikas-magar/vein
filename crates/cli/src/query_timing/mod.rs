pub(crate) fn machine_timing_line(prepare_us: u64, execute_us: u64, format_us: u64, total_us: u64) -> String {
    format!(
        "VEIN_TIMING prepare_us={prepare_us} execute_us={execute_us} format_us={format_us} total_us={total_us}"
    )
}

#[cfg(test)]
mod tests;
