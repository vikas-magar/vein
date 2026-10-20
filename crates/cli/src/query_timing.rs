pub(crate) fn machine_timing_line(prepare_us: u64, execute_us: u64, format_us: u64, total_us: u64) -> String {
    format!(
        "VEIN_TIMING prepare_us={prepare_us} execute_us={execute_us} format_us={format_us} total_us={total_us}"
    )
}

#[cfg(test)]
mod tests {
    use super::machine_timing_line;

    #[test]
    fn machine_timing_line_has_stable_named_phase_fields() {
        assert_eq!(
            machine_timing_line(11, 22, 3, 36),
            "VEIN_TIMING prepare_us=11 execute_us=22 format_us=3 total_us=36"
        );
    }
}
