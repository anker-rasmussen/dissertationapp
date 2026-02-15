//! Utility functions shared across the marketplace crate.

use crate::error::{MarketError, MarketResult};
use serde::de::DeserializeOwned;

/// Maximum size for DHT values (Veilid's limit).
pub const MAX_DHT_VALUE_SIZE: usize = 32_768;

/// Deserialize CBOR data with a size limit to prevent oversized payloads.
pub fn cbor_from_limited_reader<T: DeserializeOwned>(
    data: &[u8],
    max_bytes: usize,
) -> MarketResult<T> {
    if data.len() > max_bytes {
        return Err(MarketError::Validation(format!(
            "CBOR payload too large: {} bytes (max {})",
            data.len(),
            max_bytes
        )));
    }
    ciborium::from_reader(data)
        .map_err(|e| MarketError::Serialization(format!("CBOR deserialization failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestPayload {
        value: u64,
        message: String,
    }

    #[test]
    fn test_cbor_from_limited_reader_valid() {
        let payload = TestPayload {
            value: 42,
            message: "Hello".to_string(),
        };

        let mut bytes = Vec::new();
        ciborium::into_writer(&payload, &mut bytes).unwrap();

        let result: MarketResult<TestPayload> =
            cbor_from_limited_reader(&bytes, MAX_DHT_VALUE_SIZE);
        assert!(result.is_ok());
        let restored = result.unwrap();
        assert_eq!(restored.value, 42);
        assert_eq!(restored.message, "Hello");
    }

    #[test]
    fn test_cbor_from_limited_reader_oversized() {
        // Create a payload that is larger than the limit
        let large_payload = TestPayload {
            value: 123,
            message: "x".repeat(MAX_DHT_VALUE_SIZE + 1000),
        };

        let mut bytes = Vec::new();
        ciborium::into_writer(&large_payload, &mut bytes).unwrap();

        let result: MarketResult<TestPayload> =
            cbor_from_limited_reader(&bytes, MAX_DHT_VALUE_SIZE);
        assert!(result.is_err());
        match result {
            Err(MarketError::Validation(msg)) => {
                assert!(msg.contains("CBOR payload too large"));
                assert!(msg.contains(&format!("max {}", MAX_DHT_VALUE_SIZE)));
            }
            _ => panic!("Expected MarketError::Validation"),
        }
    }

    #[test]
    fn test_cbor_from_limited_reader_malformed() {
        // Create malformed/garbage bytes
        let garbage = vec![0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0x42, 0x00];

        let result: MarketResult<TestPayload> =
            cbor_from_limited_reader(&garbage, MAX_DHT_VALUE_SIZE);
        assert!(result.is_err());
        match result {
            Err(MarketError::Serialization(msg)) => {
                assert!(msg.contains("CBOR deserialization failed"));
            }
            _ => panic!("Expected MarketError::Serialization"),
        }
    }

    #[test]
    fn test_cbor_from_limited_reader_empty() {
        let empty: Vec<u8> = Vec::new();

        let result: MarketResult<TestPayload> =
            cbor_from_limited_reader(&empty, MAX_DHT_VALUE_SIZE);
        assert!(result.is_err());
        match result {
            Err(MarketError::Serialization(msg)) => {
                assert!(msg.contains("CBOR deserialization failed"));
            }
            _ => panic!("Expected MarketError::Serialization"),
        }
    }

    #[test]
    fn test_cbor_from_limited_reader_custom_limit() {
        let payload = TestPayload {
            value: 42,
            message: "Hello".to_string(),
        };

        let mut bytes = Vec::new();
        ciborium::into_writer(&payload, &mut bytes).unwrap();

        // Set a limit smaller than the payload size
        let small_limit = bytes.len() - 1;
        let result: MarketResult<TestPayload> = cbor_from_limited_reader(&bytes, small_limit);
        assert!(result.is_err());
        match result {
            Err(MarketError::Validation(msg)) => {
                assert!(msg.contains("CBOR payload too large"));
            }
            _ => panic!("Expected MarketError::Validation"),
        }
    }
}
