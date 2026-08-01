//! Transport layer abstractions and implementations.

/// Traits for connection management.
pub mod connection;

/// QUIC-specific client implementation with pooling.
pub mod quic_client;

// Re-export core types for easier access
pub use connection::ConnectionManager;
pub use quic_client::QuicClient;
use s2n_quic::{Client, Server};
use s2n_quic::provider::tls;
use std::path::Path;
use std::net::SocketAddr;
use crate::{VeinError, Result};
use std::fs;

/// Builds an s2n-quic Server using certificates from disk.
pub fn build_quic_server<P: AsRef<Path>>(
    bind_addr: SocketAddr,
    cert_path: P,
    key_path: P,
) -> Result<Server> {
    let cert_pem = fs::read_to_string(cert_path.as_ref())
        .map_err(|e| VeinError::internal(format!("Failed to read cert file: {}", e)))?;
    let key_pem = fs::read_to_string(key_path.as_ref())
        .map_err(|e| VeinError::internal(format!("Failed to read key file: {}", e)))?;

    let tls = tls::default::Server::builder()
        .with_certificate(cert_pem, key_pem)
        .map_err(|e| VeinError::internal(format!("TLS config error: {}", e)))?
        .build()
        .map_err(|e| VeinError::internal(format!("TLS build error: {}", e)))?;

    let server = Server::builder()
        .with_tls(tls)
        .map_err(|e| VeinError::internal(format!("Server TLS bind error: {}", e)))?
        .with_io(bind_addr)
        .map_err(|e| VeinError::internal(format!("Server IO bind error: {}", e)))?
        .start()
        .map_err(|e| VeinError::internal(format!("Server start error: {}", e)))?;

    Ok(server)
}

/// Builds an s2n-quic Client trusting the provided CA certificate.
pub fn build_quic_client<P: AsRef<Path>>(
    ca_cert_path: P,
) -> Result<Client> {
    let ca_pem = fs::read_to_string(ca_cert_path.as_ref())
        .map_err(|e| VeinError::internal(format!("Failed to read CA cert file: {}", e)))?;

    let tls = tls::default::Client::builder()
        .with_certificate(ca_pem)
        .map_err(|e| VeinError::internal(format!("TLS client config error: {}", e)))?
        .build()
        .map_err(|e| VeinError::internal(format!("TLS client build error: {}", e)))?;

    // Bind to any available local ephemeral port
    let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();

    let client = Client::builder()
        .with_tls(tls)
        .map_err(|e| VeinError::internal(format!("Client TLS bind error: {}", e)))?
        .with_io(bind_addr)
        .map_err(|e| VeinError::internal(format!("Client IO bind error: {}", e)))?
        .start()
        .map_err(|e| VeinError::internal(format!("Client start error: {}", e)))?;

    Ok(client)
}
