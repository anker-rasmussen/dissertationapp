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
}
