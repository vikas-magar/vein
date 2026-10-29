use serde::{Deserialize, Serialize};

/// Identity of one child input port, in canonical child order.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct OperatorInputPort(usize);

impl OperatorInputPort {
    /// Creates a port identity; construction validates it against operator metadata.
    pub const fn new(index: usize) -> Self { Self(index) }
    /// Returns the child ordinal without allocation.
    pub const fn index(self) -> usize { self.0 }
}

