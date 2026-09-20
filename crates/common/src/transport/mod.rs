//! Transport layer abstractions and implementations.

/// Traits for connection management.
pub mod connection;

/// QUIC-specific client implementation with pooling.
pub mod quic_client;

/// Client for the VCS exchange metadata service.
pub mod vcs_client;

// Re-export core types for easier access
use crate::{Result, VeinError};
use bytes::{Bytes, BytesMut};
pub use connection::ConnectionManager;
pub use quic_client::QuicClient;
use s2n_quic::provider::tls;
use s2n_quic::{Client, Server};
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
pub use vcs_client::VcsExchangeClient;

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

/// Incrementally decodes length-prefixed control frames from arbitrary QUIC chunks.
///
/// The decoder keeps at most one incomplete frame plus the configured payload
/// limit. Completed payloads share the frame allocation and are not copied.
#[derive(Debug)]
pub struct ControlFrameDecoder {
    buffer: BytesMut,
    max_payload_len: usize,
}

impl ControlFrameDecoder {
    /// Maximum payload accepted by the default inter-node control decoder.
    pub const DEFAULT_MAX_PAYLOAD_LEN: usize = 64 * 1024 * 1024;

    /// Creates a decoder with the default frame size limit.
    pub fn new() -> Self {
        Self::with_max_payload_len(Self::DEFAULT_MAX_PAYLOAD_LEN)
    }

    /// Creates a decoder with an explicit payload size limit.
    pub fn with_max_payload_len(max_payload_len: usize) -> Self {
        Self {
            buffer: BytesMut::new(),
            max_payload_len,
        }
    }

    /// Adds a transport chunk and returns every complete payload now available.
    pub fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<Bytes>> {
        self.buffer.extend_from_slice(chunk);

        let mut frames = Vec::new();
        loop {
            if self.buffer.len() < 5 {
                break;
            }
            if self.buffer[0] != CONTROL_STREAM_OPCODE {
                return Err(VeinError::internal("invalid control stream opcode"));
            }

            let payload_len = u32::from_le_bytes([
                self.buffer[1],
                self.buffer[2],
                self.buffer[3],
                self.buffer[4],
            ]) as usize;
            if payload_len > self.max_payload_len {
                return Err(VeinError::internal(format!(
                    "control frame payload exceeds limit: {} > {}",
                    payload_len, self.max_payload_len
                )));
            }

            let frame_len = 5 + payload_len;
            if self.buffer.len() < frame_len {
                break;
            }

            let frame = self.buffer.split_to(frame_len).freeze();
            frames.push(frame.slice(5..));
        }
        Ok(frames)
    }

    /// Returns whether an incomplete frame remains buffered.
    pub fn has_pending_data(&self) -> bool {
        !self.buffer.is_empty()
    }
}

impl Default for ControlFrameDecoder {
    fn default() -> Self {
        Self::new()
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_decoder_handles_split_and_coalesced_frames() {
        let first = frame_control_message(b"first");
        let second = frame_control_message(b"second");
        let mut decoder = ControlFrameDecoder::with_max_payload_len(16);
        assert!(decoder.push_chunk(&first[..2]).unwrap().is_empty());
        let mut combined = first[2..].to_vec();
        combined.extend_from_slice(&second);
        let frames = decoder.push_chunk(&combined).unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(&frames[0][..], b"first");
        assert_eq!(&frames[1][..], b"second");
        assert!(!decoder.has_pending_data());
    }

    #[test]
    fn control_decoder_rejects_oversized_payload() {
        let frame = frame_control_message(b"0123456789");
        let mut decoder = ControlFrameDecoder::with_max_payload_len(4);
        assert!(decoder.push_chunk(&frame).is_err());
    }
}
