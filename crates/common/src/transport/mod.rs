//! Transport layer abstractions and implementations.

/// Traits for connection management.
pub mod connection;

/// QUIC-specific client implementation with pooling.
pub mod quic_client;

// Re-export core types for easier access
pub use connection::ConnectionManager;
pub use quic_client::QuicClient;
