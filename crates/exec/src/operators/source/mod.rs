//! Data source operators generating in-flight data chunks.

/// Memory source for testing and dummy data generation
pub mod memory;
/// Generic scan source for files via OpenDAL
pub mod scan;
/// VFS-backed exchange source
pub mod exchange;

pub use exchange::VfsExchangeSource;
pub use memory::MemorySource;
pub use scan::ScanSource;

