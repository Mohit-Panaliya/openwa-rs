//! openwa-rs gateway library: OpenWA-compatible HTTP surface over a swappable
//! WhatsApp engine (`docs/PHASE1.md`, `docs/PHASE1_5.md`, `docs/SIDECAR_PROTOCOL.md`).

pub mod auth;
pub mod cluster;
pub mod db;
pub mod engine;
pub mod error;
pub mod media;
pub mod middleware;
pub mod rate_limit;
pub mod routes;
pub mod sidecar;
pub mod state;
pub mod util;
pub mod webhooks;
