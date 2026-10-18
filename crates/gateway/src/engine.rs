//! Engine abstraction.
//!
//! Phase 1 runs the Node/Baileys sidecar as a child process (`Engine::Stdio`).
//! Phase 2 adds `Engine::Embedded` — the same protocol semantics implemented
//! in-process by the `openwa-engine` crate — so the gateway can ship as one binary.

use std::sync::Arc;

use serde_json::{json, Value};
use tokio::sync::broadcast;

use crate::sidecar::{Sidecar, SidecarError};

#[cfg(feature = "embedded-engine")]
use openwa_engine::EngineCore;

#[derive(Clone)]
pub enum Engine {
    Stdio(Arc<Sidecar>),
    #[cfg(feature = "embedded-engine")]
    Embedded(Arc<EngineCore>),
}

impl Engine {
    /// Send one JSON-RPC request to the engine.
    pub async fn request(&self, method: &str, params: Value) -> Result<Value, SidecarError> {
        match self {
            Engine::Stdio(engine) => engine.request(method, params).await,
            #[cfg(feature = "embedded-engine")]
            Engine::Embedded(engine) => {
                let result = engine
                    .request(method, params)
                    .await
                    .map_err(SidecarError::Remote)?;
                engine.flush_events();
                Ok(result)
            }
        }
    }

    /// Subscribe to engine events (`session.status`, `session.qr`, `message.received`).
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        match self {
            Engine::Stdio(engine) => engine.subscribe(),
            #[cfg(feature = "embedded-engine")]
            Engine::Embedded(engine) => engine.subscribe(),
        }
    }

    pub async fn is_alive(&self) -> bool {
        match self {
            Engine::Stdio(engine) => engine.is_alive().await,
            #[cfg(feature = "embedded-engine")]
            Engine::Embedded(_) => true,
        }
    }

    pub async fn ping(&self) -> Result<Value, SidecarError> {
        self.request("ping", json!({})).await
    }

    /// Session-scoped convenience wrapper.
    pub async fn session(&self, method: &str, session_id: &str) -> Result<Value, SidecarError> {
        self.request(method, json!({ "sessionId": session_id }))
            .await
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Engine::Stdio(_) => "stdio",
            #[cfg(feature = "embedded-engine")]
            Engine::Embedded(_) => "embedded",
        }
    }
}
