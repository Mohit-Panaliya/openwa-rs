//! Admin statistics used by the dashboard header.

use axum::extract::{Query, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;

pub async fn stats(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Admin)?;

    let (sessions, messages, media, webhooks, pending_deliveries, keys) = tokio::try_join!(
        state.db.count_sessions(),
        state.db.count_messages(),
        state.db.count_media(),
        state.db.count_webhooks(),
        state.db.count_pending_deliveries(),
        state.db.list_keys(),
    )?;

    Ok(Json(json!({
        "sessions": sessions,
        "messages": messages,
        "media": media,
        "webhooks": webhooks,
        "pendingDeliveries": pending_deliveries,
        "keys": keys.len(),
        "engine": state.engine.kind(),
    })))
}

#[derive(Deserialize)]
pub struct SearchQuery {
    #[serde(default)]
    q: Option<String>,
    #[serde(rename = "chatId")]
    chat_id: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
}

fn default_limit() -> i64 {
    50
}

pub async fn search(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    // Search across sessions the key can access; use first allowed or all.
    // For simplicity, search messages with the term.
    let term = query.q.clone().unwrap_or_default();
    if term.is_empty() {
        return Ok(Json(json!({ "results": [] })));
    }
    // Search in all sessions (best effort) - iterate sessions
    let sessions = state
        .db
        .list_sessions()
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
    let mut results = Vec::new();
    for s in sessions {
        if !identity.allows_session(&s.id) {
            continue;
        }
        let chat = query.chat_id.as_deref();
        if let Some(chat) = chat {
            identity.ensure_chat(chat).ok();
        }
        // Use FTS5 when available, fall back to LIKE.
        let mut msgs = match state
            .db
            .search_messages_fts(&s.id, &term, query.limit)
            .await
        {
            Ok(rows) => rows,
            Err(_) => state
                .db
                .list_messages(&s.id, chat, query.limit, None, Some(&term))
                .await
                .unwrap_or_default(),
        };
        if let Some(chat) = chat {
            msgs.retain(|m| m.chat_id == chat);
        }
        for m in msgs {
            results.push(json!({
                "sessionId": s.id,
                "message": m,
            }));
        }
    }
    Ok(Json(json!({ "results": results })))
}
