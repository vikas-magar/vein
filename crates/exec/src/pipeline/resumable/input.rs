use crate::pipeline::DataChunkView;

/// An owned canonical view event. An empty batch is data; only `EndOfInput` closes input.
pub enum InputEvent {
    /// Transfers one batch to the receiving stage.
    Batch(DataChunkView),
    /// Closes the upstream input; distinct from an empty batch.
    EndOfInput,
}

/// Result of transferring ownership across a pipeline boundary.
pub enum Handoff<T> {
    /// The receiver accepted ownership and the sender must not replay the value.
    Accepted,
    /// The receiver could not accept it; ownership returns to the sender.
    Retry(T),
}
