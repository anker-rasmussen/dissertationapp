/// Domain-specific error types for the marketplace library.
#[derive(Debug, thiserror::Error)]
pub enum MarketError {
    #[error("DHT operation failed: {0}")]
    Dht(String),

    #[error("Serialization failed: {0}")]
    Serialization(String),

    #[error("Cryptographic operation failed: {0}")]
    Crypto(String),

    #[error("MPC execution failed: {0}")]
    Mpc(String),

    #[error("Network operation failed: {0}")]
    Network(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Process execution failed: {0}")]
    Process(String),

    #[error("Validation failed: {0}")]
    Validation(String),

    #[error("Operation timed out: {0}")]
    Timeout(String),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("IO error: {0}")]
    Io(String),

    #[error("{0}")]
    Other(String),
}

impl From<bincode::Error> for MarketError {
    fn from(e: bincode::Error) -> Self {
        Self::Serialization(format!("Bincode error: {e}"))
    }
}

impl From<veilid_core::VeilidAPIError> for MarketError {
    fn from(e: veilid_core::VeilidAPIError) -> Self {
        Self::Network(format!("{e}"))
    }
}

impl From<std::io::Error> for MarketError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(format!("{e}"))
    }
}

/// Convenience type alias.
pub type MarketResult<T> = Result<T, MarketError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_dht_error() {
        let err = MarketError::Dht("Failed to write record".to_string());
        let display = format!("{}", err);
        assert_eq!(display, "DHT operation failed: Failed to write record");
    }

    #[test]
    fn test_display_validation_error() {
        let err = MarketError::Validation("Invalid bid amount".to_string());
        let display = format!("{}", err);
        assert_eq!(display, "Validation failed: Invalid bid amount");
    }

    #[test]
    fn test_from_bincode_error() {
        // Create a bincode error by attempting to deserialize invalid data
        let invalid_data = vec![0xFF, 0xFF, 0xFF];
        let bincode_err = bincode::deserialize::<u64>(&invalid_data).unwrap_err();

        // Convert to MarketError
        let market_err: MarketError = bincode_err.into();

        // Verify it's a Serialization error
        match market_err {
            MarketError::Serialization(msg) => {
                assert!(msg.contains("Bincode error:"));
            }
            _ => panic!("Expected Serialization variant"),
        }
    }
}
