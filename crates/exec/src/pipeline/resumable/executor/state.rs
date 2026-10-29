/// Readiness belongs to the driver; buffered data belongs to operators or edges.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum StageState {
    Ready,
    NeedsInput,
    AwaitingOutput,
    Blocked,
    Finished,
}
