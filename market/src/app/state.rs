//! Application state — re-exports `SharedAppState` from the library crate
//! and provides the `SHARED_STATE` static for main-to-Dioxus handoff.

pub use market::shared_state::SharedAppState;

/// Single static for the main -> Dioxus handoff.
pub static SHARED_STATE: std::sync::OnceLock<SharedAppState> = std::sync::OnceLock::new();
