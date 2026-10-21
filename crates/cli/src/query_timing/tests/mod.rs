use super::machine_timing_line;

#[test]
fn machine_timing_line_has_stable_named_phase_fields() {
    assert_eq!(
        machine_timing_line(11, 22, 3, 36),
        "VEIN_TIMING prepare_us=11 execute_us=22 format_us=3 total_us=36"
    );
}
