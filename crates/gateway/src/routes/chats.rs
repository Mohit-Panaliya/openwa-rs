//! Chat action routes.

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::error::ApiError;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct ChatBody {
    #[serde(rename = "chatId")]
    chat_id: String,
}

fn cid(b: &ChatBody) -> String {
    b.chat_id.trim().to_string()
}

pub async fn mark_read(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<ChatBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = cid(&body);
    identity.ensure_chat(&chat_id)?;
    state
        .engine
        .request(
            "chat.markRead",
            json!({ "sessionId": session_id, "chatId": chat_id }),
        )
        .await?;
    Ok(Json(json!({ "read": true })))
}

pub async fn archive(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<ChatBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = cid(&body);
    identity.ensure_chat(&chat_id)?;
    state
        .engine
        .request(
            "chat.archive",
            json!({ "sessionId": session_id, "chatId": chat_id }),
        )
        .await?;
    Ok(Json(json!({ "archived": true })))
}

pub async fn unarchive(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<ChatBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = cid(&body);
    identity.ensure_chat(&chat_id)?;
    state
        .engine
        .request(
            "chat.unarchive",
            json!({ "sessionId": session_id, "chatId": chat_id }),
        )
        .await?;
    Ok(Json(json!({ "archived": false })))
}

pub async fn pin(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<ChatBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = cid(&body);
    identity.ensure_chat(&chat_id)?;
    state
        .engine
        .request(
            "chat.pin",
            json!({ "sessionId": session_id, "chatId": chat_id }),
        )
        .await?;
    Ok(Json(json!({ "pinned": true })))
}

pub async fn unpin(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<ChatBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = cid(&body);
    identity.ensure_chat(&chat_id)?;
    state
        .engine
        .request(
            "chat.unpin",
            json!({ "sessionId": session_id, "chatId": chat_id }),
        )
        .await?;
    Ok(Json(json!({ "pinned": false })))
}

pub async fn mute(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<ChatBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = cid(&body);
    identity.ensure_chat(&chat_id)?;
    state
        .engine
        .request(
            "chat.mute",
            json!({ "sessionId": session_id, "chatId": chat_id }),
        )
        .await?;
    Ok(Json(json!({ "muted": true })))
}

pub async fn unmute(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<ChatBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = cid(&body);
    identity.ensure_chat(&chat_id)?;
    state
        .engine
        .request(
            "chat.unmute",
            json!({ "sessionId": session_id, "chatId": chat_id }),
        )
        .await?;
    Ok(Json(json!({ "muted": false })))
}

pub async fn delete(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<ChatBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = cid(&body);
    identity.ensure_chat(&chat_id)?;
    state
        .engine
        .request(
            "chat.delete",
            json!({ "sessionId": session_id, "chatId": chat_id }),
        )
        .await?;
    Ok(Json(json!({ "deleted": true })))
}

pub async fn clear(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<ChatBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = cid(&body);
    identity.ensure_chat(&chat_id)?;
    state
        .engine
        .request(
            "chat.clear",
            json!({ "sessionId": session_id, "chatId": chat_id }),
        )
        .await?;
    Ok(Json(json!({ "cleared": true })))
}
