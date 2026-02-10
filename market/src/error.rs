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

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Convenience type alias.
pub type MarketResult<T> = Result<T, MarketError>;
