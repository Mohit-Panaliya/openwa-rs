//! Message routes: the persisted store plus the send helpers.

use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{Identity, Role};
use crate::db::NewMessage;
use crate::error::ApiError;
use crate::state::AppState;

const MEDIA_TYPES: [&str; 4] = ["image", "video", "audio", "document"];

fn now_seconds() -> i64 {
    crate::util::now_ms() / 1000
}

#[derive(Deserialize)]
pub struct ListQuery {
    #[serde(rename = "chatId")]
    chat_id: Option<String>,
    limit: Option<i64>,
    before: Option<i64>,
    search: Option<String>,
}

pub async fn list(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Viewer)?;
    identity.ensure_session(&session_id)?;

    // A chat-scoped key must say which chat it is reading.
    let chat_id = match query.chat_id.as_deref() {
        Some(chat) if !chat.trim().is_empty() => {
            identity.ensure_chat(chat)?;
            Some(chat)
        }
        _ => {
            if !identity.is_admin() && !identity.allowed_chats.is_empty() {
                return Err(ApiError::bad_request("chatId is required for this API key"));
            }
            None
        }
    };

    let messages = state
        .db
        .list_messages(
            &session_id,
            chat_id,
            query.limit.unwrap_or(50),
            query.before,
            query.search.as_deref(),
        )
        .await?;

    Ok(Json(json!({ "messages": messages })))
}

#[derive(Deserialize)]
pub struct SendText {
    #[serde(rename = "chatId")]
    chat_id: String,
    text: String,
}

pub async fn send_text(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<SendText>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;

    let chat_id = body.chat_id.trim().to_string();
    if chat_id.is_empty() {
        return Err(ApiError::bad_request("chatId is required"));
    }
    identity.ensure_chat(&chat_id)?;
    if body.text.is_empty() {
        return Err(ApiError::bad_request("text is required"));
    }

    // Send pacing (per-session daily cap), best-effort gated by env.
    if std::env::var("SEND_PACING_ENABLED")
        .map(|v| {
            let v = v.trim().to_lowercase();
            !v.is_empty() && v != "0" && v != "false" && v != "no"
        })
        .unwrap_or(false)
    {
        let cap: i64 = std::env::var("SEND_PACING_DAILY_CAP")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(1000);
        let allowed = state
            .db
            .check_send_pacing(&session_id, cap)
            .await
            .unwrap_or(true);
        if !allowed {
            return Err(
                ApiError::too_many_requests("send pacing daily cap exceeded")
                    .with_code("SEND_PACING_LIMITED"),
            );
        }
        let _ = state.db.increment_send_pacing(&session_id).await;
    }

    let result = state
        .engine
        .request(
            "message.sendText",
            json!({ "sessionId": session_id, "chatId": chat_id, "text": body.text }),
        )
        .await?;

    let message_id = result
        .get("messageId")
        .and_then(Value::as_str)
        .map(str::to_string);
    let timestamp = now_seconds();

    let stored = store(
        &state,
        NewMessage {
            session_id: &session_id,
            chat_id: &chat_id,
            engine_message_id: message_id.as_deref(),
            sender: Some("me"),
            from_me: true,
            kind: "text",
            text: Some(&body.text),
            has_media: false,
            media_id: None,
            timestamp,
        },
    )
    .await;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = message_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("message.send_text", Some(&actor), resource.as_deref(), None)
            .await;
    });

    Ok(Json(json!({
        "messageId": message_id,
        "chatId": chat_id,
        "message": stored,
    })))
}

#[derive(Deserialize)]
pub struct SendMedia {
    #[serde(rename = "chatId")]
    chat_id: String,
    #[serde(rename = "mediaType")]
    media_type: String,
    #[serde(default)]
    mimetype: Option<String>,
    #[serde(default)]
    base64: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    filename: Option<String>,
    #[serde(default)]
    caption: Option<String>,
}

pub async fn send_media(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(session_id): Path<String>,
    Json(body): Json<SendMedia>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;

    let chat_id = body.chat_id.trim().to_string();
    if chat_id.is_empty() {
        return Err(ApiError::bad_request("chatId is required"));
    }
    identity.ensure_chat(&chat_id)?;

    let media_type = body.media_type.to_lowercase();
    if !MEDIA_TYPES.contains(&media_type.as_str()) {
        return Err(ApiError::bad_request(format!(
            "mediaType must be one of: {}",
            MEDIA_TYPES.join(", ")
        )));
    }

    let base64_payload = body
        .base64
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let url_payload = body
        .url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if base64_payload.is_none() && url_payload.is_none() {
        return Err(ApiError::bad_request("either base64 or url is required"));
    }

    // Keep our own copy of inline media so it can be served from /api/media later.
    let mut media_id: Option<String> = None;
    if let Some(payload) = base64_payload {
        let (id, path, bytes) = state
            .media
            .save_base64(&media_type, body.mimetype.as_deref(), payload)
            .await
            .map_err(|error| ApiError::bad_request(error.to_string()))?;

        let path = path.to_string_lossy().to_string();
        state
            .db
            .insert_media(
                &id,
                &session_id,
                Some(&chat_id),
                &media_type,
                body.mimetype.as_deref(),
                body.filename.as_deref(),
                bytes,
                &path,
            )
            .await?;
        media_id = Some(id);
    }

    let result = state
        .engine
        .request(
            "message.sendMedia",
            json!({
                "sessionId": session_id,
                "chatId": chat_id,
                "mediaType": media_type,
                "mimetype": body.mimetype,
                "base64": base64_payload,
                "url": url_payload,
                "filename": body.filename,
                "caption": body.caption,
            }),
        )
        .await?;

    let message_id = result
        .get("messageId")
        .and_then(Value::as_str)
        .map(str::to_string);
    let caption = body.caption.clone();
    let timestamp = now_seconds();

    let stored = store(
        &state,
        NewMessage {
            session_id: &session_id,
            chat_id: &chat_id,
            engine_message_id: message_id.as_deref(),
            sender: Some("me"),
            from_me: true,
            kind: &media_type,
            text: caption.as_deref(),
            has_media: true,
            media_id: media_id.as_deref(),
            timestamp,
        },
    )
    .await;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = message_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit(
                "message.send_media",
                Some(&actor),
                resource.as_deref(),
                None,
            )
            .await;
    });

    Ok(Json(json!({
        "messageId": message_id,
        "chatId": chat_id,
        "mediaId": media_id,
        "message": stored,
    })))
}

#[derive(Deserialize)]
pub struct ReactBody {
    #[serde(rename = "chatId")]
    chat_id: String,
    #[serde(default)]
    emoji: String,
    #[serde(default, rename = "messageId")]
    target_message_id: Option<String>,
}

pub async fn react(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, message_id)): Path<(String, String)>,
    Json(body): Json<ReactBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = body.chat_id.trim().to_string();
    identity.ensure_chat(&chat_id)?;
    let mid = if let Some(t) = body.target_message_id {
        t
    } else {
        message_id.clone()
    };
    let result = state
        .engine
        .request(
            "message.sendReaction",
            json!({
                "sessionId": session_id, "chatId": chat_id, "messageId": mid, "emoji": body.emoji
            }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = mid.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("message.react", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(result))
}

#[derive(Deserialize)]
pub struct EditBody {
    #[serde(rename = "chatId")]
    chat_id: String,
    text: String,
}

pub async fn edit(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, message_id)): Path<(String, String)>,
    Json(body): Json<EditBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = body.chat_id.trim().to_string();
    identity.ensure_chat(&chat_id)?;
    let result = state.engine.request("message.edit", json!({
        "sessionId": session_id, "chatId": chat_id, "messageId": message_id, "text": body.text
    })).await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = message_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("message.edit", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(result))
}

#[derive(Deserialize)]
pub struct DeleteBody {
    #[serde(rename = "chatId")]
    chat_id: String,
}

pub async fn delete(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, message_id)): Path<(String, String)>,
    Json(body): Json<DeleteBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = body.chat_id.trim().to_string();
    identity.ensure_chat(&chat_id)?;
    state
        .engine
        .request(
            "message.delete",
            json!({
                "sessionId": session_id, "chatId": chat_id, "messageId": message_id
            }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = message_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("message.delete", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "deleted": true })))
}

#[derive(Deserialize)]
pub struct StarBody {
    #[serde(rename = "chatId")]
    chat_id: String,
    #[serde(default = "default_true")]
    star: bool,
}

fn default_true() -> bool {
    true
}

pub async fn star(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, message_id)): Path<(String, String)>,
    Json(body): Json<StarBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = body.chat_id.trim().to_string();
    identity.ensure_chat(&chat_id)?;
    state.engine.request("message.star", json!({
        "sessionId": session_id, "chatId": chat_id, "messageId": message_id, "star": body.star
    })).await?;
    let _ = state
        .db
        .set_message_starred(&session_id, &message_id, body.star)
        .await;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = message_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("message.star", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "starred": body.star })))
}

#[derive(Deserialize)]
pub struct ForwardBody {
    #[serde(rename = "chatId")]
    pub chat_id: String,
    #[serde(rename = "toChatId")]
    pub to_chat_id: String,
}

pub async fn forward(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, message_id)): Path<(String, String)>,
    Json(body): Json<ForwardBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = body.chat_id.trim().to_string();
    if chat_id.is_empty() {
        return Err(ApiError::bad_request("chatId is required"));
    }
    identity.ensure_chat(&chat_id)?;
    identity.ensure_chat(&body.to_chat_id)?;
    let result = state
        .engine
        .request(
            "message.forward",
            json!({
                "sessionId": session_id,
                "chatId": chat_id,
                "messageId": message_id,
                "toChatId": body.to_chat_id,
            }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = message_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("message.forward", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(result))
}

#[derive(Deserialize)]
pub struct PinBody {
    #[serde(rename = "chatId")]
    pub chat_id: String,
    #[serde(default = "default_true")]
    pub pin: bool,
}

pub async fn pin(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((session_id, message_id)): Path<(String, String)>,
    Json(body): Json<PinBody>,
) -> Result<Json<Value>, ApiError> {
    identity.require_role(Role::Operator)?;
    identity.ensure_session(&session_id)?;
    let chat_id = body.chat_id.trim().to_string();
    if chat_id.is_empty() {
        return Err(ApiError::bad_request("chatId is required"));
    }
    identity.ensure_chat(&chat_id)?;
    state
        .engine
        .request(
            "message.pin",
            json!({
                "sessionId": session_id,
                "chatId": chat_id,
                "messageId": message_id,
                "pin": body.pin,
            }),
        )
        .await?;

    let db = state.db.clone();
    let actor = identity.key_id.clone();
    let resource = message_id.clone();
    tokio::spawn(async move {
        let _ = db
            .insert_audit("message.pin", Some(&actor), Some(&resource), None)
            .await;
    });

    Ok(Json(json!({ "pinned": body.pin })))
}

/// Persist a message and read it back so responses carry the stored row.
async fn store(state: &AppState, message: NewMessage<'_>) -> Value {
    match state.db.save_message(&message).await {
        Ok(id) => match state.db.get_message(id).await {
            Ok(Some(record)) => serde_json::to_value(record).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        Err(error) => {
            tracing::warn!(%error, "could not store the outgoing message");
            Value::Null
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_type_validation_is_case_insensitive() {
        assert!(MEDIA_TYPES.contains(&"image"));
        assert!(MEDIA_TYPES.contains(&"Document".to_lowercase().as_str()));
        assert!(!MEDIA_TYPES.contains(&"sticker"));
    }
}
