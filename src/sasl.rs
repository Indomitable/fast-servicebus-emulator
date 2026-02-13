//! Mock SASL handshake for Azure Service Bus emulator.
//!
//! Implements a minimal SASL exchange that accepts any credentials.
//! Advertises ANONYMOUS, PLAIN, MSSBCBS, and EXTERNAL mechanisms
//! to satisfy the Azure Service Bus SDK's authentication requirements.

use anyhow::Result;
use fe2o3_amqp_types::sasl::{SaslCode, SaslInit, SaslMechanisms, SaslOutcome};
use fe2o3_amqp_types::primitives::Symbol;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

/// AMQP SASL protocol header: `AMQP 3 1 0 0`
const SASL_HEADER: [u8; 8] = [65, 77, 81, 80, 3, 1, 0, 0];

/// SASL mechanisms advertised to clients.
const MECHANISMS: &[&str] = &["ANONYMOUS", "PLAIN", "MSSBCBS", "EXTERNAL"];

/// Returns true if the first 8 bytes of a peek match the SASL header.
pub fn is_sasl_header(buf: &[u8; 8]) -> bool {
    *buf == SASL_HEADER
}

/// Performs a mock SASL handshake on the given TCP stream.
///
/// 1. Reads the SASL header from the client.
/// 2. Echoes the SASL header back.
/// 3. Sends `SaslMechanisms` listing available mechanisms.
/// 4. Receives `SaslInit` (mechanism selection + optional credentials).
/// 5. Responds with `SaslOutcome { code: Ok }`.
pub async fn perform_handshake(stream: &mut TcpStream) -> Result<()> {
    // 1. Read SASL header from client
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await?;

    // 2. Echo SASL header back
    stream.write_all(&SASL_HEADER).await?;

    // 3. Send SaslMechanisms
    let mechanisms = SaslMechanisms {
        sasl_server_mechanisms: fe2o3_amqp_types::primitives::Array(
            MECHANISMS.iter().map(|m| Symbol::from(*m)).collect(),
        ),
    };
    send_frame(stream, &mechanisms).await?;

    // 4. Receive SaslInit
    let init: SaslInit = recv_frame(stream).await?;
    debug!(mechanism = ?init.mechanism, "SASL init received");

    // 5. Send SaslOutcome (always OK)
    let outcome = SaslOutcome {
        code: SaslCode::Ok,
        additional_data: None,
    };
    send_frame(stream, &outcome).await?;

    Ok(())
}

/// Serializes and sends an AMQP SASL frame.
///
/// Frame layout:
/// - Size (4 bytes, big-endian) = body length + 8
/// - DOFF (1 byte) = 2
/// - Type (1 byte) = 1 (SASL)
/// - Channel (2 bytes) = 0
/// - Body (serialized AMQP data)
async fn send_frame<T: serde::Serialize>(stream: &mut TcpStream, frame: &T) -> Result<()> {
    let body = serde_amqp::to_vec(frame)?;
    let size = (body.len() as u32) + 8;

    let mut header = [0u8; 8];
    header[0..4].copy_from_slice(&size.to_be_bytes());
    header[4] = 2; // DOFF
    header[5] = 1; // SASL type
    // header[6..8] = channel 0 (already zero)

    stream.write_all(&header).await?;
    stream.write_all(&body).await?;
    Ok(())
}

/// Reads and deserializes an AMQP SASL frame from the stream.
async fn recv_frame<T: serde::de::DeserializeOwned>(stream: &mut TcpStream) -> Result<T> {
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await?;

    let size = u32::from_be_bytes(header[0..4].try_into()?);
    if size < 8 {
        return Err(anyhow::anyhow!("Invalid SASL frame size: {}", size));
    }

    let body_len = (size - 8) as usize;
    let mut body = vec![0u8; body_len];
    stream.read_exact(&mut body).await?;

    let frame: T = serde_amqp::from_slice(&body)?;
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_sasl_header_valid() {
        let buf = [65, 77, 81, 80, 3, 1, 0, 0];
        assert!(is_sasl_header(&buf));
    }

    #[test]
    fn test_is_sasl_header_invalid() {
        let buf = [65, 77, 81, 80, 0, 1, 0, 0]; // AMQP 0 1 0 0 (not SASL)
        assert!(!is_sasl_header(&buf));
    }

    #[test]
    fn test_is_sasl_header_zeros() {
        let buf = [0u8; 8];
        assert!(!is_sasl_header(&buf));
    }

    #[tokio::test]
    async fn test_send_and_recv_frame_roundtrip() {
        // Test that SASL frame serialization round-trips correctly
        let mechanisms = SaslMechanisms {
            sasl_server_mechanisms: fe2o3_amqp_types::primitives::Array(vec![
                Symbol::from("ANONYMOUS"),
            ]),
        };

        let body = serde_amqp::to_vec(&mechanisms).unwrap();
        let deserialized: SaslMechanisms = serde_amqp::from_slice(&body).unwrap();
        assert_eq!(deserialized.sasl_server_mechanisms.0.len(), 1);
        assert_eq!(
            deserialized.sasl_server_mechanisms.0[0].as_str(),
            "ANONYMOUS"
        );
    }
}
