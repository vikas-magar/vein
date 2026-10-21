//! Hash join over the engine's chunks.
//!
//! A join is a **pipeline breaker**: the build side is consumed completely
//! before the probe side emits anything, and it needs a second input. That is
//! why it is a first-class pipeline operator: it builds state from one stream,
//! then probes that state with a second stream.
//!
//! Zero copy is the constraint the implementation is built around; see
//! [`hash_table`].

pub mod hash_table;
pub mod rows;

pub mod hash_join;
pub mod spec;
pub mod spill;

pub use hash_table::{HashTable, JoinBloomFilterConfig, JoinBloomFilterFactory, JoinKeys};
pub use hash_join::{BuildSink, HashJoin, JoinFilterCollector};
pub use rows::{JoinType, OutputRow, output_rows};
pub use spec::{JoinSpec, OutputColumn};
