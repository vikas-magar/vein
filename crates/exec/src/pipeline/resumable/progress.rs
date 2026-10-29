use crate::pipeline::DataChunkView;

/// One bounded step from an operator or source.
pub enum OperatorStep<W> {
    /// The scheduler owns this view until downstream accepts it.
    Output(DataChunkView),
    /// More synchronous work is immediately available; requeue the stage.
    Runnable,
    /// The stage needs another input event before it can make progress.
    NeedsInput,
    /// The stage has enough input to finish and asks the driver to drop its
    /// unfinished upstream stages before it emits its terminal EOF.
    StopInput,
    /// The stage cannot progress until the identified resource becomes ready.
    Blocked(W),
    /// Input, buffered output, and finalization are all complete.
    Finished,
}
