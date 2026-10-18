//! openwa-rs gateway binary: OpenWA-compatible HTTP API in Rust, backed by a
//! swappable WhatsApp engine process (Node/Baileys today, the Rust engine in
//! Phase 2). See docs/PHASE1.md, docs/PHASE1_5.md, docs/SIDECAR_PROTOCOL.md.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::broadcast;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use openwa_gateway::cluster::ClusterConfig;
use openwa_gateway::state::AppState;
use openwa_gateway::{db, engine, media, routes, sidecar, util, webhooks};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = parse_env("PORT", 2785);
    let timeout_ms: u64 = parse_env("REQUEST_TIMEOUT_MS", 15_000);

    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "data".to_string()));
    let db_path = std::env::var("DB_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| data_dir.join("openwa.db"));
    let media_dir = std::env::var("MEDIA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| data_dir.join("media"));
    let dashboard_dir = PathBuf::from(
        std::env::var("DASHBOARD_DIR").unwrap_or_else(|_| "dashboard/dist".to_string()),
    );

    let admin_key = resolve_api_key(&data_dir)?;
    let database = db::Db::connect(&db_path).await?;
    bootstrap_admin_key(&database, &admin_key).await?;

    let media_store = media::MediaStore::new(media_dir);
    let engine_mode = std::env::var("ENGINE_MODE").unwrap_or_else(|_| "stdio".to_string());
    let engine_handle = if engine_mode == "embedded" {
        #[cfg(feature = "embedded-engine")]
        {
            let core = std::sync::Arc::new(openwa_engine::EngineCore::new(data_dir.join("engine")));
            engine::Engine::Embedded(core)
        }
        #[cfg(not(feature = "embedded-engine"))]
        {
            anyhow::bail!("ENGINE_MODE=embedded requires the embedded-engine feature");
        }
    } else {
        let command = sidecar::EngineCommand::from_env();
        engine::Engine::Stdio(sidecar::Sidecar::spawn_command(command, timeout_ms).await?)
    };

    let cluster = ClusterConfig::from_env();
    let state = AppState {
        db: database,
        media: media_store,
        engine: engine_handle.clone(),
        http: reqwest::Client::new(),
        dashboard_dir: Arc::new(dashboard_dir),
        cluster: cluster.clone(),
    };

    tokio::spawn(consume_events(state.clone(), engine_handle.subscribe()));
    tokio::spawn(webhooks::retry_worker(state.clone()));

    let address: SocketAddr = format!("{host}:{port}").parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    info!(%address, database = %db_path.display(), "openwa-rs gateway listening");

    axum::serve(
        listener,
        routes::router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    Ok(())
}

/// Persist every engine event, then fan matching ones out to webhooks.
async fn consume_events(state: AppState, mut receiver: broadcast::Receiver<Value>) {
    while let Ok(event) = receiver.recv().await {
        let Some(event_type) = event.get("type").and_then(Value::as_str) else {
            continue;
        };
        let payload = event.get("payload").cloned().unwrap_or(Value::Null);
        let session_id = payload
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        match event_type {
            "session.status" => {
                if let Some(status) = payload.get("status").and_then(Value::as_str) {
                    if !status.is_empty() {
                        // An empty name keeps whatever the session was created with.
                        if let Err(error) =
                            state.db.upsert_session(&session_id, "", status, None).await
                        {
                            warn!(%error, "could not persist session status");
                        }
                    }
                }
            }
            "message.received" => persist_message(&state, &session_id, &payload).await,
            "message.reaction" => {
                info!(event = event_type, session = %session_id, "message reaction");
            }
            "call.received" => {
                info!(event = event_type, session = %session_id, "call received");
            }
            _ => {
                info!(event = event_type, session = %session_id, "engine event");
            }
        }

        let envelope = webhooks::event_envelope(event_type, &payload);
        match state.db.list_webhooks(&session_id).await {
            Ok(subscriptions) => {
                for webhook in subscriptions {
                    if !webhooks::matches(&webhook.events, event_type) {
                        continue;
                    }
                    tokio::spawn(webhooks::record_and_deliver(
                        state.db.clone(),
                        state.http.clone(),
                        webhook,
                        event_type.to_string(),
                        envelope.clone(),
                    ));
                }
            }
            Err(error) => warn!(%error, "could not load webhook subscriptions"),
        }
    }
}

/// Store an inbound (or echoed outbound) message from an engine event.
async fn persist_message(state: &AppState, session_id: &str, payload: &Value) {
    let chat_id = payload
        .get("chatId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if chat_id.is_empty() || session_id.is_empty() {
        return;
    }

    let message = db::NewMessage {
        session_id,
        chat_id: &chat_id,
        engine_message_id: payload.get("messageId").and_then(Value::as_str),
        sender: payload.get("sender").and_then(Value::as_str),
        from_me: payload
            .get("fromMe")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        kind: payload
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("text"),
        text: payload.get("text").and_then(Value::as_str),
        has_media: payload
            .get("hasMedia")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        media_id: None,
        timestamp: payload
            .get("timestamp")
            .and_then(Value::as_i64)
            .unwrap_or_else(|| util::now_ms() / 1000),
    };

    if let Err(error) = state.db.save_message(&message).await {
        warn!(%error, "could not store a message event");
    }
}

/// Make sure the configured admin key works: seed it when no admin key exists.
async fn bootstrap_admin_key(database: &db::Db, admin_key: &str) -> anyhow::Result<()> {
    if database.count_keys_by_role("admin").await? > 0 {
        return Ok(());
    }

    database
        .insert_key(
            &util::random_hex(8),
            "bootstrap-admin",
            "admin",
            &util::sha256_hex(admin_key),
            &util::key_prefix(admin_key),
            &[],
            &[],
            &[],
        )
        .await?;
    info!("registered the bootstrap admin API key");
    Ok(())
}

/// `API_KEY` env var, else `DATA_DIR/.api-key`, else generate and store one.
fn resolve_api_key(data_dir: &Path) -> anyhow::Result<String> {
    if let Ok(key) = std::env::var("API_KEY") {
        let key = key.trim().to_string();
        if !key.is_empty() {
            return Ok(key);
        }
    }

    let path = data_dir.join(".api-key");
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let key = existing.trim().to_string();
        if !key.is_empty() {
            return Ok(key);
        }
    }

    let key = util::random_hex(24);
    std::fs::create_dir_all(data_dir)?;
    std::fs::write(&path, &key)?;
    println!();
    println!("  Generated admin API key (shown once): {key}");
    println!("  Stored at {}", path.display());
    println!();
    Ok(key)
}

fn parse_env<T: std::str::FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(raw) => match raw.trim().parse() {
            Ok(value) => value,
            Err(_) => {
                warn!(%name, %raw, "invalid value, using default");
                default
            }
        },
        Err(_) => default,
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received");
}
