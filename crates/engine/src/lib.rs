//! openwa-engine: in-process Rust WhatsApp engine.
//!
//! Implements the method semantics of `docs/SIDECAR_PROTOCOL.md` on top of the
//! pure-Rust [`whatsapp-rust`] client, so the gateway can talk to it either over
//! stdio (`crates/engine-sidecar`) or in-process.
//!
//! The public surface is deliberately tiny: a [`EngineCore`] with a
//! JSON-in/JSON-out [`request`](EngineCore::request) entry point, an event
//! [`subscribe`](EngineCore::subscribe) stream, and lifecycle methods. All the
//! protocol wording (`"session not found"`, `"session is not connected"`, ...)
//! lives here so the gateway's error mapping keeps working unchanged.
//!
//! ## Persistence
//!
//! Per-session credentials, Signal sessions and app-state keys are persisted to
//! `<DATA_DIR>/<sessionId>/state.json` through [`store::FileBackend`]. That
//! backend exists because the upstream SQLite store links a `libsqlite3-sys`
//! version that collides with the gateway's own copy (Cargo forbids two
//! versions of a `links = "sqlite3"` crate in one graph); it mirrors every
//! mutation of `whatsapp-rust`'s in-memory backend into a JSON snapshot.

mod core;
mod media;
mod session;
mod store;

pub use crate::core::{EngineCore, Status};

/// Protocol version reported by `ping`.
pub const VERSION: &str = "0.1.0";

/// Protocol version reported by `ping`.
pub fn version() -> &'static str {
    VERSION
}
