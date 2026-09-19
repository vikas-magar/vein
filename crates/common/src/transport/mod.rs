//! Transport layer abstractions and implementations.

/// Traits for connection management.
pub mod connection;

/// QUIC-specific client implementation with pooling.
pub mod quic_client;

/// Client for the VCS exchange metadata service.
pub mod vcs_client;

// Re-export core types for easier access
use crate::{Result, VeinError};
pub use connection::ConnectionManager;
pub use quic_client::QuicClient;
pub use vcs_client::VcsExchangeClient;
use s2n_quic::provider::tls;
use s2n_quic::{Client, Server};
use std::fs;
use std::net::SocketAddr;
use std::path::Path;

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
pub fn build_quic_client<P: AsRef<Path>>(ca_cert_path: P) -> Result<Client> {
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

/// Opcode identifying inter-node control plane events over QUIC streams.
pub const CONTROL_STREAM_OPCODE: u8 = 0x02;

/// Frames a serialized control message with opcode 0x02 and a 4-byte little-endian length prefix.
pub fn frame_control_message(payload: &[u8]) -> bytes::Bytes {
    let mut wire = Vec::with_capacity(5 + payload.len());
    wire.push(CONTROL_STREAM_OPCODE);
    wire.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    wire.extend_from_slice(payload);
    wire.into()
}

/// Extracts the inner payload from a wire buffer, stripping the 0x02 opcode and length header.
pub fn extract_control_payload(buffer: &[u8]) -> Option<&[u8]> {
    if buffer.is_empty() {
        return None;
    }
    if buffer.len() >= 5 && buffer[0] == CONTROL_STREAM_OPCODE {
        let len = u32::from_le_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;
        if buffer.len() >= 5 + len {
            Some(&buffer[5..5 + len])
        } else {
            Some(&buffer[5..])
        }
    } else if buffer[0] == CONTROL_STREAM_OPCODE {
        Some(&buffer[1..])
    } else {
        Some(buffer)
    }
}
